package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OSSTaskUtils;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.executor.operator.scan.WorkPool;
import com.alibaba.polardbx.executor.operator.scan.impl.CsvColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultLazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.MorselColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.impl.SimpleWorkPool;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

// columnar table scan exec.
public class ColumnarScanExec extends SourceExec {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private static final int CPU_CORES = Runtime.getRuntime().availableProcessors();

    private static final ExecutorService IO_EXECUTOR =
        Executors.newFixedThreadPool(CPU_CORES * CPU_CORES, new NamedThreadFactory(
            "columnar-io"
        ));

    private static final ExecutorService SCAN_EXECUTOR =
        Executors.newFixedThreadPool(CPU_CORES * CPU_CORES, new NamedThreadFactory(
            "columnar-scan"
        ));
    private static final AtomicLong SNAPSHOT_FILE_ACCESS_COUNT = new AtomicLong(0);

    public static final double DEFAULT_RATIO = .3D;
    public static final double DEFAULT_GROUPS_RATIO = 1D;
    public static final double DEFAULT_DELETION_RATIO = 0D;

    protected OSSTableScan ossTableScan;
    private List<DataType> outputDataTypes;

    // Shared by all scan operators of one partition in one server node.
    private WorkPool<ColumnarSplit, Chunk> workPool;
    private ExecutorService scanExecutor;

    private ScanPreProcessor preProcessor;
    private ListenableFuture preProcessorFuture;
    private Supplier<ListenableFuture<?>> blockedSupplier;

    /**
     * The running scan work.
     */
    private volatile ScanWork<ColumnarSplit, Chunk> currentWork;
    private volatile boolean lastWorkNotExecutable;
    private volatile boolean noAvailableWork;
    private Map<String, ScanWork<ColumnarSplit, Chunk>> finishedWorks;
    private List<Split> splitList;

    /**
     * NOTE: The current IO status does not necessarily come from the current scan work.
     * Before t1: 1st scan work is running
     * Before t2: the results of 1st IO status is run out.
     * When time between t1 and t2, we would run the 2nd scan work
     * but read the results from 1st IO status.
     * sequence diagram:
     * O-----------------t1--------t2--------------------> t
     * ______1st work____|_____________2st work___________
     * ____ read 1st IO status_____|__ read 2nd IO status__
     */
    private volatile IOStatus<Chunk> currentIOStatus;
    private volatile boolean lastStatusRunOut;

    // To manage the session-level variables.
    private boolean useVerboseMetricsReport;
    private boolean enableMetrics;
    protected boolean enableIndexPruning;
    private boolean enableDebug;

    // memory management.
    private MemoryPool memoryPool;
    private MemoryAllocatorCtx memoryAllocator;

    // plan fragment level runtime filter manager.
    private volatile FragmentRFManager fragmentRFManager;

    public ColumnarScanExec(OSSTableScan ossTableScan, ExecutionContext context, List<DataType> outputDataTypes) {
        super(context);
        this.ossTableScan = ossTableScan;
        this.outputDataTypes = outputDataTypes;

        this.scanExecutor = SCAN_EXECUTOR;

        this.workPool = new SimpleWorkPool();
        this.finishedWorks = new TreeMap<>(String::compareTo);

        // status of ScanWork
        this.lastWorkNotExecutable = true;
        this.noAvailableWork = false;

        // status of IOStatus
        this.lastStatusRunOut = true;

        this.useVerboseMetricsReport =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_VERBOSE_METRICS_REPORT);
        this.enableMetrics =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS);
        this.enableIndexPruning =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_INDEX_PRUNING);
        this.enableDebug = LOGGER.isDebugEnabled();
        this.splitList = new ArrayList<>();

        this.memoryPool = MemoryPoolUtils
            .createOperatorTmpTablePool("ColumnarScanExec@" + System.identityHashCode(this),
                context.getMemoryPool());
        this.memoryAllocator = memoryPool.getMemoryAllocatorCtx();
    }

    public void setFragmentRFManager(FragmentRFManager fragmentRFManager) {
        this.fragmentRFManager = fragmentRFManager;
    }

    private List<String> getOrcFiles(OssSplit ossSplit) {
        List<String> fileNames = ossSplit.getDesignatedFile();

        if (fileNames != null && ossTableScan.getFlashback() instanceof RexDynamicParam) {
            String timestampString = context.getParams().getCurrentParameter()
                .get(((RexDynamicParam) ossTableScan.getFlashback()).getIndex() + 1).getValue().toString();
            TimeZone fromTimeZone;
            if (context.getTimeZone() != null) {
                fromTimeZone = context.getTimeZone().getTimeZone();
            } else {
                fromTimeZone = TimeZone.getDefault();
            }

            long readTs = OSSTaskUtils.getTsFromTimestampWithTimeZone(timestampString, fromTimeZone);
            TableMeta tableMeta = context.getSchemaManager(ossSplit.getLogicalSchema()).getTable(
                ossSplit.getLogicalTableName());
            Set<String> filterSet = ossSplit.getFilterSet(context);
            Map<String, List<FileMeta>> flatFileMetas = tableMeta.getFlatFileMetas();

            return ossSplit.getPhyTableNameList().stream()
                .map(flatFileMetas::get)
                .flatMap(List::stream)
                .filter(x -> {
                    if (filterSet == null || filterSet.contains(x.getFileName())) {
                        if (readTs < x.getCommitTs()) {
                            // not committed yet at this ts
                            return false;
                        }
                        // not removed yet at this ts
                        return x.getRemoveTs() == null || readTs <= x.getRemoveTs();
                    } else {
                        // not designated for this split
                        return false;
                    }
                })
                .map(FileMeta::getFileName)
                .collect(Collectors.toList());
        } else {
            return fileNames;
        }
    }

    @Override
    public void addSplit(Split split) {
        splitList.add(split);
        OssSplit ossSplit = (OssSplit) split.getConnectorSplit();
        List<String> orcFileNames = getOrcFiles(ossSplit);

        // partition info of this split
        int partNum = ossSplit.getPartIndex();
        int nodePartCount = ossSplit.getNodePartCount();

        // base info of table meta.
        String logicalSchema = ossSplit.getLogicalSchema();
        String logicalTableName = ossSplit.getLogicalTableName();
        TableMeta tableMeta = context.getSchemaManager(logicalSchema).getTable(logicalTableName);

        // To distinguish from
        final boolean isColumnar = tableMeta.isColumnar();

        // engine and filesystem for files in split.
        Engine engine = tableMeta.getEngine();
        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();

        // todo it's not the unique id for each table-scan exec in different work thread.
        int sequenceId = getSourceId();

        // todo It's time consuming because the constructor of configuration will initialize a large parameter list.
        Configuration configuration = new Configuration();

        ColumnarManager columnarManager = ColumnarManager.getInstance();
        OSSColumnTransformer columnTransformer = ossSplit.getColumnTransformer(ossTableScan, context);

        // build pre-processor for splits to contain all time-consuming processing
        // like pruning, bitmap loading and metadata preheating.
        if (preProcessor == null) {
            preProcessor = getPreProcessor(
                ossSplit,
                logicalSchema,
                logicalTableName,
                tableMeta,
                fileSystem,
                configuration,
                columnarManager);
        }

        // Schema-level cache manager.
        BlockCacheManager<Block> blockCacheManager = BlockCacheManager.getInstance();

        // Get the push-down predicate.
        // The refs of input-type will be consistent with refs in RexNode.
        LazyEvaluator<Chunk, BitSet> evaluator = null;
        List<Integer> inputRefsForFilter = ImmutableList.of();
        if (!ossTableScan.getOrcNode().getFilters().isEmpty()) {
            RexNode rexNode = ossTableScan.getOrcNode().getFilters().get(0);
            List<DataType<?>> inputTypes = ossTableScan.getOrcNode().getInProjectsDataType();

            // Build evaluator suitable for columnar scan, with the ratio to decide the evaluation strategy.
            evaluator = DefaultLazyEvaluator.builder()
                .setRexNode(rexNode)
                .setRatio(DEFAULT_RATIO)
                .setInputTypes(inputTypes)
                .setContext(context)
                .build();

            // Collect input refs for filter (predicate) and project
            InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
            rexNode.accept(inputRefTypeChecker);

            // The input-ref-indexes is the list of index of in-projects.
            inputRefsForFilter = inputRefTypeChecker.getInputRefIndexes()
                .stream()
                .map(index -> ossTableScan.getOrcNode().getInProjects().get(index))
                .sorted()
                .collect(Collectors.toList());
        }

        // The output projects is the list of index of in-projects.
        List<Integer> inputRefsForProject = ossTableScan.getOrcNode().getOutProjects()
            .stream()
            .map(index -> ossTableScan.getOrcNode().getInProjects().get(index))
            .sorted()
            .collect(Collectors.toList());

        final int chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        final int morselUnit = context.getParamManager().getInt(ConnectionParams.COLUMNAR_WORK_UNIT);

        final OssSplit.DeltaReadOption deltaReadOption = ossSplit.getDeltaReadOption();

        // Build csv split for all csv files in deltaReadOption and fill into work pool.
        if (deltaReadOption != null) {
            final Map<String, List<String>> allCsvFiles = deltaReadOption.getAllCsvFiles();

            List<Integer> finalInputRefsForFilterForCsv = inputRefsForFilter;
            LazyEvaluator<Chunk, BitSet> finalEvaluatorForCsv = evaluator;
            allCsvFiles.values().stream().flatMap(List::stream).forEach(
                csvFile -> {
                    Path filePath = FileSystemUtils.buildPath(fileSystem, csvFile, isColumnar);
                    preProcessor.addFile(filePath);
                    workPool.addSplit(sequenceId, CsvColumnarSplit.newBuilder()
                        .executionContext(context)
                        .columnarManager(columnarManager)
                        .file(filePath, 0)
                        .inputRefs(finalInputRefsForFilterForCsv, inputRefsForProject)
                        .tso(ossSplit.getCheckpointTso())
                        .prepare(preProcessor)
                        .pushDown(finalEvaluatorForCsv)
                        .columnTransformer(columnTransformer)
                        .partNum(partNum)
                        .nodePartCount(nodePartCount)
                        .memoryAllocator(memoryAllocator)
                        .build()
                    );
                }
            );
        }

        // Build columnar style split for all orc files in oss-split and fill into work pool.
        if (orcFileNames != null) {
            for (String fileName : orcFileNames) {
                // The pre-processor shared by all columnar-splits in this table scan.
                Path filePath = FileSystemUtils.buildPath(fileSystem, fileName, isColumnar);
                preProcessor.addFile(filePath);

                // todo need columnar file-id mapping.
                int fileId = 0;

                ColumnarSplit columnarSplit = MorselColumnarSplit.newBuilder()
                    .executionContext(context)
                    .ioExecutor(IO_EXECUTOR)
                    .fileSystem(fileSystem, engine)
                    .configuration(configuration)
                    .sequenceId(sequenceId)
                    .file(filePath, fileId)
                    .columnTransformer(columnTransformer)
                    .inputRefs(inputRefsForFilter, inputRefsForProject)
                    .cacheManager(blockCacheManager)
                    .chunkLimit(chunkLimit)
                    .morselUnit(morselUnit)
                    .pushDown(evaluator)
                    .prepare(preProcessor)
                    .columnarManager(columnarManager)
                    .isColumnarMode(isColumnar)
                    .tso(ossSplit.getCheckpointTso())
                    .partNum(partNum)
                    .nodePartCount(nodePartCount)
                    .memoryAllocator(memoryAllocator)
                    .fragmentRFManager(fragmentRFManager)
                    .operatorStatistic(statistics)
                    .build();

                workPool.addSplit(sequenceId, columnarSplit);
            }
            if (isColumnar) {
                SNAPSHOT_FILE_ACCESS_COUNT.getAndAdd(orcFileNames.size());
            }
        }
    }

    protected DefaultScanPreProcessor getPreProcessor(OssSplit ossSplit,
                                                      String logicalSchema,
                                                      String logicalTableName,
                                                      TableMeta tableMeta,
                                                      FileSystem fileSystem,
                                                      Configuration configuration,
                                                      ColumnarManager columnarManager) {
        return new DefaultScanPreProcessor(
            configuration, fileSystem,

            // for pruning
            logicalSchema,
            logicalTableName,
            enableIndexPruning,
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE),
            tableMeta.getAllColumns(),
            ossTableScan.getOrcNode().getOriFilters(),
            ossSplit.getParams(),

            // for mock
            DEFAULT_GROUPS_RATIO,
            DEFAULT_DELETION_RATIO,

            // for columnar mode.
            columnarManager,
            ossSplit.getCheckpointTso(),
            tableMeta.getColumnarFieldIdList()
        );
    }

    @Override
    public void noMoreSplits() {
        workPool.noMoreSplits(getSourceId());
    }

    @Override
    public Integer getSourceId() {
        return ossTableScan.getRelatedId();
    }

    @Override
    void doOpen() {
        // invoke pre-processor.
        if (preProcessor != null) {
            preProcessorFuture = preProcessor.prepare(scanExecutor, context.getTraceId(), context.getColumnarTracer());
        }
    }

    @Override
    Chunk doSourceNextChunk() {
        // There is no split added.
        if (splitList.isEmpty()) {
            // If there is no split, don't block the Driver.
            blockedSupplier = () -> Futures.immediateFuture(null);
            return null;
        }
        // Firstly, Check if pre-processor is done.
        if (preProcessorFuture != null && !preProcessorFuture.isDone()) {

            // The blocked future is from pre-processor.
            blockedSupplier = () -> preProcessorFuture;
            return null;
        } else {

            // The blocked future is from IOStatus.
            blockedSupplier = () -> currentIOStatus.isBlocked();
        }

        tryInvokeNext();
        if (currentIOStatus == null) {
            // If there is no selected row-group, don't block the Driver.
            blockedSupplier = () -> Futures.immediateFuture(null);
            return null;
        }

        // fetch the next chunk according to the state.
        IOStatus<Chunk> ioStatus = currentIOStatus;
        ScanState state = ioStatus.state();
        Chunk result;
        switch (state) {
        case READY:
        case BLOCKED: {
            result = ioStatus.popResult();
            // if chunk is null, the Driver should call is_blocked
            if (result == null || result.getPositionCount() == 0) {
                return null;
            }
            return result;
        }
        case FINISHED: {
            // We must firstly mark the last work to state of not-executable,
            // so that when fetch the next chunks from the last IOStatus, The Exec can
            // invoke the next work.
            if (currentWork != null && currentWork.getWorkId().equals(ioStatus.workId())) {
                lastWorkNotExecutable = true;
            }

            // Try to pop all the rest results
            while ((result = ioStatus.popResult()) != null) {
                if (result.getPositionCount() == 0) {
                    continue;
                }
                return result;
            }

            // The results of this scan work is run out.
            finishedWorks.put(currentWork.getWorkId(), currentWork);
            lastStatusRunOut = true;
            if (enableDebug) {
                LOGGER.info(MessageFormat.format(
                    "finish IOStatus, exec: {0}, workId: {1}, rowCount: {2}",
                    this.toString(), currentIOStatus.workId(), currentIOStatus.rowCount()
                ));
            }

            tryInvokeNext();

            break;
        }
        case FAILED: {
            if (currentWork != null && currentWork.getWorkId().equals(ioStatus.workId())) {
                lastWorkNotExecutable = true;
            }
            // throw any stored exception in client.
            ioStatus.throwIfFailed();
            break;
        }
        case CLOSED: {
            if (currentWork != null && currentWork.getWorkId().equals(ioStatus.workId())) {
                lastWorkNotExecutable = true;
            }
            // The results of this scan work is run out.
            finishedWorks.put(currentWork.getWorkId(), currentWork);
            lastStatusRunOut = true;
            if (enableDebug) {
                LOGGER.info(MessageFormat.format(
                    "finish IOStatus, exec: {0}, workId: {1}, rowCount: {2}",
                    this.toString(), currentIOStatus.workId(), currentIOStatus.rowCount()
                ));
            }

            tryInvokeNext();

            break;
        }
        }

        return null;
    }

    void tryInvokeNext() {
        // if the current scan work is no longer executable?
        if (lastWorkNotExecutable) {
            if (!noAvailableWork && currentWork != null) {

                if (enableDebug) {
                    LOGGER.info(MessageFormat.format(
                        "finish work, exec: {0}, workId: {1}",
                        this.toString(), currentWork.getWorkId()
                    ));
                }

            }

            // should recycle the resources of the last scan work.
            // pick up the next split from work pool.
            ScanWork<ColumnarSplit, Chunk> newWork = workPool.pickUp(getSourceId());
            if (newWork == null) {
                noAvailableWork = true;
            } else {
                if (enableDebug) {
                    LOGGER.info(MessageFormat.format(
                        "start work, exec: {0}, workId: {1}",
                        this.toString(), newWork.getWorkId()
                    ));
                }

                currentWork = newWork;
                currentWork.invoke(scanExecutor);

                lastWorkNotExecutable = false;
            }
        }

        // if the current io status is run out?
        if (lastStatusRunOut && !noAvailableWork) {
            // switch io status
            currentIOStatus = currentWork.getIOStatus();
            lastStatusRunOut = false;

            if (enableDebug) {
                LOGGER.info(MessageFormat.format(
                    "start IOStatus, exec: {0}, workId: {1}",
                    this.toString(), currentIOStatus.workId()
                ));
            }

        }
    }

    @Override
    void doClose() {
        Throwable t = null;
        RuntimeMetrics summaryMetrics = null;
        for (ScanWork<ColumnarSplit, Chunk> scanWork : finishedWorks.values()) {
            try {
                if (enableMetrics) {
                    RuntimeMetrics metrics = scanWork.getMetrics();
                    if (useVerboseMetricsReport) {
                        // print verbose metrics.
                        String report = metrics.reportAll();
                        LOGGER.info(MessageFormat.format("the scan-work report: {0}", report));
                    }

                    // To merge all metrics into the first one.
                    if (summaryMetrics == null) {
                        summaryMetrics = metrics;
                    } else {
                        summaryMetrics.merge(metrics);
                    }
                }
            } catch (Throwable e) {
                // don't throw here to prevent from memory leak.
                t = e;
            } finally {
                try {
                    scanWork.close(false);
                } catch (Throwable e) {
                    // don't throw here to prevent from memory leak.
                    t = e;
                }
            }
        }

        // print summary metrics.
        if (enableMetrics && summaryMetrics != null) {
            LOGGER.info(MessageFormat.format("the summary of scan-work report: {0}", summaryMetrics.reportAll()));
        }

        if (t != null) {
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputDataTypes;
    }

    @Override
    public boolean produceIsFinished() {
        return splitList.isEmpty() ||
            (noAvailableWork && lastWorkNotExecutable && lastStatusRunOut);
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blockedSupplier.get();
    }

    public static ExecutorService getIoExecutor() {
        return IO_EXECUTOR;
    }

    public static ExecutorService getScanExecutor() {
        return SCAN_EXECUTOR;
    }

    public static long getSnapshotFileAccessCount() {
        return SNAPSHOT_FILE_ACCESS_COUNT.get();
    }
}

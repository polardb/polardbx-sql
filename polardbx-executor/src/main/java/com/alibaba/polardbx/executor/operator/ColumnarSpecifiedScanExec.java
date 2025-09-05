package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.OssSplit;
import com.alibaba.polardbx.executor.mpp.split.SpecifiedOssSplit;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultLazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.SpecifiedCsvColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.impl.SpecifiedDeleteBitmapPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.impl.SpecifiedOrcColumnarSplit;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This columnar scan exec scans specified orc/csv/del files,
 * with specified file position, between specified tso version.
 * Do not use any cache.
 *
 * @author yaozhili
 */
public class ColumnarSpecifiedScanExec extends ColumnarScanExec {
    public ColumnarSpecifiedScanExec(OSSTableScan ossTableScan,
                                     ExecutionContext context,
                                     List<DataType> outputDataTypes) {
        super(ossTableScan, context, outputDataTypes);
    }

    @Override
    public void addSplit(Split split) {
        splitList.add(split);
        SpecifiedOssSplit ossSplit = (SpecifiedOssSplit) split.getConnectorSplit();
        List<String> orcFileNames = ossSplit.getDesignatedFile();
        final SpecifiedOssSplit.DeltaReadWithPositionOption deltaReadOption =
            (SpecifiedOssSplit.DeltaReadWithPositionOption) ossSplit.getDeltaReadOption();

        // partition info of this split
        int partNum = ossSplit.getPartIndex();
        int nodePartCount = ossSplit.getNodePartCount();

        // base info of table meta.
        String logicalSchema = ossSplit.getLogicalSchema();
        String logicalTableName = ossSplit.getLogicalTableName();
        TableMeta tableMeta = context.getSchemaManager(logicalSchema).getTable(logicalTableName);

        // engine and filesystem for files in split.
        Engine engine = tableMeta.getEngine();
        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();

        int sequenceId = getSourceId();

        Configuration configuration = new Configuration();

        ColumnarManager columnarManager = ColumnarManager.getInstance();

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
                columnarManager
            );
        } else if (preProcessor instanceof SpecifiedDeleteBitmapPreProcessor) {
            if (null != deltaReadOption && null != deltaReadOption.getDelFiles()) {
                ((SpecifiedDeleteBitmapPreProcessor) preProcessor).addDelFiles(
                    deltaReadOption.getDelFiles(),
                    deltaReadOption.getDelBeginPos(),
                    deltaReadOption.getDelEndPos(),
                    deltaReadOption.getTableId());
            }
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

        boolean generateTsoInfo = context.isCciIncrementalCheck();
        // Build csv split for all csv files in deltaReadOption and fill into work pool.
        if (deltaReadOption != null && !context.isReadOrcOnly() && deltaReadOption.getCsvFiles() != null) {
            for (int i = 0; i < deltaReadOption.getCsvFiles().size(); i++) {
                String fileName = deltaReadOption.getCsvFiles().get(i);
                long start = deltaReadOption.getCsvStartPos().get(i);
                long end = deltaReadOption.getCsvEndPos().get(i);
                Path filePath = FileSystemUtils.buildPath(fileSystem, fileName, true);
                ColumnarSplit csvSplit = SpecifiedCsvColumnarSplit.newBuilder()
                    .executionContext(context)
                    .columnarManager(columnarManager)
                    .file(filePath, 0)
                    .inputRefs(inputRefsForFilter, inputRefsForProject)
                    .tso(ossSplit.getCheckpointTso())
                    .prepare(preProcessor)
                    .pushDown(evaluator)
                    .columnTransformer(ossSplit.getColumnTransformer(ossTableScan, context, fileName, generateTsoInfo))
                    .partNum(partNum)
                    .nodePartCount(nodePartCount)
                    .memoryAllocator(memoryAllocator)
                    .begin((int) start)
                    .end((int) end)
                    .tsoV0(deltaReadOption.getTsoV0())
                    .tsoV1(deltaReadOption.getTsoV1())
                    .build();
                preProcessor.addFile(filePath);
                workPool.addSplit(sequenceId, csvSplit);
            }
        }

        // Build columnar style split for all orc files in oss-split and fill into work pool.
        if (orcFileNames != null && !context.isReadCsvOnly()) {
            for (String fileName : orcFileNames) {
                // The pre-processor shared by all columnar-splits in this table scan.
                Path filePath = FileSystemUtils.buildPath(fileSystem, fileName, true);
                preProcessor.addFile(filePath);

                int fileId = 0;

                ColumnarSplit columnarSplit = SpecifiedOrcColumnarSplit.newBuilder()
                    .executionContext(context)
                    .ioExecutor(IO_EXECUTOR)
                    .fileSystem(fileSystem, engine)
                    .configuration(configuration)
                    .sequenceId(sequenceId)
                    .file(filePath, fileId)
                    .columnTransformer(ossSplit.getColumnTransformer(ossTableScan, context, fileName, generateTsoInfo))
                    .inputRefs(inputRefsForFilter, inputRefsForProject)
                    .cacheManager(blockCacheManager)
                    .chunkLimit(chunkLimit)
                    .morselUnit(morselUnit)
                    .pushDown(evaluator)
                    .prepare(preProcessor)
                    .columnarManager(columnarManager)
                    .isColumnarMode(true)
                    .tso(ossSplit.getCheckpointTso())
                    .partNum(partNum)
                    .nodePartCount(nodePartCount)
                    .memoryAllocator(memoryAllocator)
                    .fragmentRFManager(fragmentRFManager)
                    .operatorStatistic(statistics)
                    .tsoV0(deltaReadOption == null ? -1 : deltaReadOption.getTsoV0())
                    .tsoV1(deltaReadOption == null ? -1 : deltaReadOption.getTsoV1())
                    .build();

                workPool.addSplit(sequenceId, columnarSplit);
            }
            SNAPSHOT_FILE_ACCESS_COUNT.getAndAdd(orcFileNames.size());
        }
    }

    @Override
    protected DefaultScanPreProcessor getPreProcessor(OssSplit ossSplit,
                                                      String logicalSchema,
                                                      String logicalTableName,
                                                      TableMeta tableMeta,
                                                      FileSystem fileSystem,
                                                      Configuration configuration,
                                                      ColumnarManager columnarManager) {
        final SpecifiedOssSplit.DeltaReadWithPositionOption deltaReadOption =
            (SpecifiedOssSplit.DeltaReadWithPositionOption) ossSplit.getDeltaReadOption();
        List<String> delFiles = null == deltaReadOption ? null : deltaReadOption.getDelFiles();
        List<Long> delBeginPos = null == deltaReadOption ? null : deltaReadOption.getDelBeginPos();
        List<Long> delEndPos = null == deltaReadOption ? null : deltaReadOption.getDelEndPos();
        long tableId = null == deltaReadOption ? -1 : deltaReadOption.getTableId();
        Engine engine = tableMeta.getEngine();
        return new SpecifiedDeleteBitmapPreProcessor(
            configuration, fileSystem,

            // for pruning
            logicalSchema,
            logicalTableName,
            enableIndexPruning,
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE),
            tableMeta.getAllColumns(),
            ossTableScan.getOrcNode().getOriFilters(),
            context.getParams().getCurrentParameter(),
            // for mock
            DEFAULT_GROUPS_RATIO,
            DEFAULT_DELETION_RATIO,

            // for columnar mode.
            columnarManager,
            ossSplit.getCheckpointTso(),
            tableMeta.getColumnarFieldIdList(),

            // specified delete bitmap
            delFiles,
            delBeginPos,
            delEndPos,
            engine,
            tableId
        );
    }
}

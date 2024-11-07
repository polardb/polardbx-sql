package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * This split contains an orc file with specified delete bitmap,
 * and with specified tso versions.
 */
public class SpecifiedOrcColumnarSplit extends MorselColumnarSplit {
    private long tsoV0;
    private long tsoV1;

    public SpecifiedOrcColumnarSplit(ExecutionContext executionContext,
                                     ExecutorService ioExecutor, Engine engine,
                                     FileSystem fileSystem, Configuration configuration,
                                     int sequenceId, int fileId, Path filePath,
                                     OSSColumnTransformer ossColumnTransformer,
                                     int[] primaryKeyColIds, List<Integer> inputRefsForFilter,
                                     List<Integer> inputRefsForProject, int chunkLimit,
                                     BlockCacheManager<Block> blockCacheManager,
                                     int rgThreshold,
                                     LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                                     ScanPreProcessor preProcessor, int partNum,
                                     int nodePartCount, MemoryAllocatorCtx memoryAllocatorCtx,
                                     FragmentRFManager fragmentRFManager,
                                     Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap,
                                     OperatorStatistics operatorStatistics,
                                     long tsoV0, long tsoV1)
        throws IOException {
        super(executionContext, ioExecutor, engine, fileSystem, configuration, sequenceId, fileId, filePath,
            ossColumnTransformer, primaryKeyColIds, inputRefsForFilter, inputRefsForProject, chunkLimit,
            blockCacheManager, rgThreshold, lazyEvaluator, preProcessor, partNum, nodePartCount, memoryAllocatorCtx,
            fragmentRFManager, rfFilterRefInFileMap, operatorStatistics);
        this.tsoV0 = tsoV0;
        this.tsoV1 = tsoV1;
    }

    @Override
    public <SplitT extends ColumnarSplit, BATCH> ScanWork<SplitT, BATCH> nextWork() {
        if (scanWorkIterator == null) {
            // The pre-processor must have been done.
            if (!preProcessor.isPrepared()) {
                GeneralUtil.nestedException("The pre-processor is not prepared");
            }

            try {
                scanWorkIterator = new SpecifiedScanWorkIterator();
            } catch (IOException e) {
                throw GeneralUtil.nestedException("Fail to initialize scan-work iterator", e);
            }
        }

        while (scanWorkIterator.hasNext()) {
            return (ScanWork<SplitT, BATCH>) scanWorkIterator.next();
        }
        return null;
    }

    class SpecifiedScanWorkIterator extends ScanWorkIterator {

        SpecifiedScanWorkIterator() throws IOException {
            super();
        }

        @Override
        public ScanWork<ColumnarSplit, Chunk> next() {
            String scanWorkId = generateScanWorkId(
                executionContext.getTraceId(),
                filePath.toString(),
                currentRange.stripeId,
                scanWorkIndex++
            );

            boolean enableMetrics = executionContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS);
            boolean activeLoading = executionContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_LAZY_BLOCK_ACTIVE_LOADING);

            RuntimeMetrics metrics = RuntimeMetrics.create(scanWorkId);

            // Add parent metrics node
            if (enableMetrics) {
                metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_MEMORY,
                    null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
                metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_TIMER,
                    null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
                metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_BYTES_RANGE,
                    null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

                metrics.addDerivedCounter(ColumnReader.COLUMN_READER_MEMORY,
                    null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
                metrics.addDerivedCounter(ColumnReader.COLUMN_READER_TIMER,
                    null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);

                metrics.addDerivedCounter(LogicalRowGroup.BLOCK_LOAD_TIMER,
                    null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
                metrics.addDerivedCounter(LogicalRowGroup.BLOCK_MEMORY_COUNTER,
                    null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

                metrics.addDerivedCounter(ScanWork.EVALUATION_TIMER,
                    null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
            }

            if (currentRange != null) {
                RowGroupIterator<Block, ColumnStatistics> rowGroupIterator = new RowGroupIteratorImpl(
                    metrics,

                    // The range of this row-group iterator.
                    currentRange.stripeId,
                    currentRange.startRowGroupId,
                    currentRange.effectiveGroupCount,
                    currentRange.rowGroupIncluded,

                    // primary key col ids.
                    primaryKeyColIds,

                    // parameters for IO task.
                    ioExecutor, fileSystem, configuration, filePath,

                    // for compression
                    compressionSize, compressionKind,
                    preheatFileMeta,

                    // for stripe-level parser
                    stripeInformationMap.get(currentRange.stripeId),
                    startRowInStripeMap.get(currentRange.stripeId),

                    fileSchema, version, encryption,
                    encodingMap.get(currentRange.stripeId),
                    ignoreNonUtf8BloomFilter,
                    maxBufferSize, maxDiskRangeChunkLimit, maxMergeDistance,
                    chunkLimit, blockCacheManager, ossColumnTransformer,
                    executionContext, columnIncluded, indexStride, enableDecimal64,
                    memoryAllocatorCtx);

                ScanWork<ColumnarSplit, Chunk> scanWork;
                if (executionContext.isEnableOrcDeletedScan()) {
                    scanWork = new DeletedScanWork(
                        scanWorkId,
                        metrics,
                        enableMetrics,
                        lazyEvaluator,
                        rowGroupIterator, deletion,
                        currentRange, inputRefsForFilter, inputRefsForProject,
                        partNum, nodePartCount,
                        activeLoading, ossColumnTransformer);
                } else {
                    scanWork = new SpecifiedOrcScanWork(
                        scanWorkId,
                        metrics,
                        enableMetrics,
                        lazyEvaluator,
                        rowGroupIterator, deletion,
                        currentRange, inputRefsForFilter, inputRefsForProject,
                        partNum, nodePartCount,
                        ossColumnTransformer,
                        tsoV0, tsoV1, executionContext
                    );
                }

                currentRange = null;
                return scanWork;
            }
            return null;
        }
    }

    public static ColumnarSplitBuilder newBuilder() {
        return new SpecifiedOrcColumnarSplit.SpecifiedOrcColumnarSplitBuilder();
    }

    static class SpecifiedOrcColumnarSplitBuilder implements ColumnarSplitBuilder {
        private ExecutionContext executionContext;
        private ExecutorService ioExecutor;
        private Engine engine;
        private FileSystem fileSystem;
        private Configuration configuration;
        private int sequenceId;
        private int fileId;
        private Path filePath;
        private OSSColumnTransformer ossColumnTransformer;
        private String logicalSchema;
        private String logicalTable;
        private List<Integer> inputRefsForFilter;
        private List<Integer> inputRefsForProject;
        private int chunkLimit;
        private BlockCacheManager<Block> blockCacheManager;
        private int rgThreshold;
        private LazyEvaluator<Chunk, BitSet> lazyEvaluator;
        private ScanPreProcessor preProcessor;
        private ColumnarManager columnarManager;
        private Long tso;
        private boolean isColumnarMode;

        private int partNum;

        private int nodePartCount;
        private MemoryAllocatorCtx memoryAllocatorCtx;

        private FragmentRFManager fragmentRFManager;
        private OperatorStatistics operatorStatistics;

        private long tsoV0;

        private long tsoV1;

        @Override
        public ColumnarSplit build() {
            try {
                // CASE1: when evaluator is null, the input refs for filter must be empty.
                // CASE2: when evaluator is entirely a constant expression, the input refs for filter must be empty.
                Preconditions.checkArgument((lazyEvaluator == null
                        || lazyEvaluator.isConstantExpression()) == (inputRefsForFilter.isEmpty()),
                    "when evaluator is null, the input refs for filter must be empty.");

                Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap = new HashMap<>();
                if (fragmentRFManager != null) {

                    // For each item, mapping the source ref to file column ref.
                    for (Map.Entry<FragmentRFItemKey, FragmentRFItem> itemEntry
                        : fragmentRFManager.getAllItems().entrySet()) {
                        List<Integer> rfFilterChannels = new ArrayList<>();
                        rfFilterChannels.add(itemEntry.getValue().getSourceRefInFile());

                        rfFilterChannels = isColumnarMode
                            ? columnarManager.getPhysicalColumnIndexes(tso, filePath.getName(), rfFilterChannels)
                            : rfFilterChannels;

                        rfFilterRefInFileMap.put(itemEntry.getKey(), rfFilterChannels.get(0));
                    }
                }

                int[] primaryKeyColIds =
                    isColumnarMode ? columnarManager.getPrimaryKeyColumns(filePath.getName()) : null;

                // To distinguish the columnar mode from archive mode
                return new SpecifiedOrcColumnarSplit(
                    executionContext, ioExecutor, engine, fileSystem,
                    configuration, sequenceId, fileId, filePath,
                    ossColumnTransformer, primaryKeyColIds,

                    inputRefsForFilter, inputRefsForProject,

                    chunkLimit, blockCacheManager,
                    rgThreshold, lazyEvaluator,
                    preProcessor,
                    partNum, nodePartCount, memoryAllocatorCtx, fragmentRFManager, rfFilterRefInFileMap,
                    operatorStatistics,

                    tsoV0, tsoV1);
            } catch (IOException e) {
                throw GeneralUtil.nestedException("Fail to build columnar split.", e);
            }
        }

        @Override
        public ColumnarSplitBuilder executionContext(ExecutionContext context) {
            this.executionContext = context;
            return this;
        }

        @Override
        public ColumnarSplitBuilder ioExecutor(ExecutorService ioExecutor) {
            this.ioExecutor = ioExecutor;
            return this;
        }

        @Override
        public ColumnarSplitBuilder fileSystem(FileSystem fileSystem, Engine engine) {
            this.fileSystem = fileSystem;
            this.engine = engine;
            return this;
        }

        @Override
        public ColumnarSplitBuilder configuration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        @Override
        public ColumnarSplitBuilder sequenceId(int sequenceId) {
            this.sequenceId = sequenceId;
            return this;
        }

        @Override
        public ColumnarSplitBuilder file(Path filePath, int fileId) {
            this.filePath = filePath;
            this.fileId = fileId;
            return this;
        }

        @Override
        public ColumnarSplitBuilder tableMeta(String logicalSchema, String logicalTable) {
            this.logicalSchema = logicalSchema;
            this.logicalTable = logicalTable;
            return this;
        }

        @Override
        public ColumnarSplitBuilder columnTransformer(OSSColumnTransformer ossColumnTransformer) {
            this.ossColumnTransformer = ossColumnTransformer;
            return this;
        }

        @Override
        public ColumnarSplitBuilder inputRefs(List<Integer> inputRefsForFilter, List<Integer> inputRefsForProject) {
            this.inputRefsForFilter = inputRefsForFilter;
            this.inputRefsForProject = inputRefsForProject;
            return this;
        }

        @Override
        public ColumnarSplitBuilder cacheManager(BlockCacheManager<Block> blockCacheManager) {
            this.blockCacheManager = blockCacheManager;
            return this;
        }

        @Override
        public ColumnarSplitBuilder chunkLimit(int chunkLimit) {
            this.chunkLimit = chunkLimit;
            return this;
        }

        @Override
        public ColumnarSplitBuilder morselUnit(int rgThreshold) {
            this.rgThreshold = rgThreshold;
            return this;
        }

        @Override
        public ColumnarSplitBuilder pushDown(LazyEvaluator<Chunk, BitSet> lazyEvaluator) {
            this.lazyEvaluator = lazyEvaluator;
            return this;
        }

        @Override
        public ColumnarSplitBuilder prepare(ScanPreProcessor scanPreProcessor) {
            this.preProcessor = scanPreProcessor;
            return this;
        }

        @Override
        public ColumnarSplitBuilder columnarManager(ColumnarManager columnarManager) {
            this.columnarManager = columnarManager;
            return this;
        }

        @Override
        public ColumnarSplitBuilder isColumnarMode(boolean isColumnarMode) {
            this.isColumnarMode = isColumnarMode;
            return this;
        }

        @Override
        public ColumnarSplitBuilder tso(Long tso) {
            this.tso = tso;
            return this;
        }

        @Override
        public ColumnarSplitBuilder position(Long position) {
            return this;
        }

        @Override
        public ColumnarSplitBuilder partNum(int partNum) {
            this.partNum = partNum;
            return this;
        }

        @Override
        public ColumnarSplitBuilder nodePartCount(int nodePartCount) {
            this.nodePartCount = nodePartCount;
            return this;
        }

        @Override
        public ColumnarSplitBuilder memoryAllocator(MemoryAllocatorCtx memoryAllocatorCtx) {
            this.memoryAllocatorCtx = memoryAllocatorCtx;
            return this;
        }

        @Override
        public ColumnarSplitBuilder fragmentRFManager(FragmentRFManager fragmentRFManager) {
            this.fragmentRFManager = fragmentRFManager;
            return this;
        }

        @Override
        public ColumnarSplitBuilder operatorStatistic(OperatorStatistics operatorStatistics) {
            this.operatorStatistics = operatorStatistics;
            return this;
        }

        @Override
        public ColumnarSplitBuilder tsoV0(long tsoV0) {
            this.tsoV0 = tsoV0;
            return this;
        }

        @Override
        public ColumnarSplitBuilder tsoV1(long tsoV1) {
            this.tsoV1 = tsoV1;
            return this;
        }
    }
}

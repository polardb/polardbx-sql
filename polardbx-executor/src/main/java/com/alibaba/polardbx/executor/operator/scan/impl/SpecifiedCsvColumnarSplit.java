package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * This csv split contain a csv file with specified start and end positions,
 * and with specified delete bitmap.
 *
 * @author yaozhili
 */
public class SpecifiedCsvColumnarSplit extends CsvColumnarSplit {
    final private int begin;
    final private int end;

    final private long tsoV0;

    final private long tsoV1;

    public SpecifiedCsvColumnarSplit(ExecutionContext executionContext,
                                     ColumnarManager columnarManager, long tso,
                                     Path csvFile, int fileId, int sequenceId,
                                     List<Integer> inputRefsForFilter,
                                     List<Integer> inputRefsForProject,
                                     ScanPreProcessor preProcessor,
                                     LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                                     int partNum, int nodePartCount,
                                     OSSColumnTransformer columnTransformer,
                                     int begin, int end, long tsoV0, long tsoV1) {
        super(executionContext, columnarManager, tso, null, csvFile, fileId, sequenceId, inputRefsForFilter,
            inputRefsForProject, preProcessor, lazyEvaluator, partNum, nodePartCount, columnTransformer);
        this.begin = begin;
        this.end = end;
        this.tsoV0 = tsoV0;
        this.tsoV1 = tsoV1;
    }

    @Override
    public synchronized <SplitT extends ColumnarSplit, BATCH> ScanWork<SplitT, BATCH> nextWork() {
        if (isScanWorkInvoked) {
            return null;
        }

        if (!preProcessor.isPrepared()) {
            GeneralUtil.nestedException("The pre-processor is not prepared");
        }
        String scanWorkId = generateScanWorkId(executionContext.getTraceId(), csvFile.toString(), 0);
        boolean enableMetrics = executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS);
        boolean useSelection = executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_SELECTION);
        boolean enableCompatible = executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
        RuntimeMetrics metrics = RuntimeMetrics.create(scanWorkId);
        csvScanWork = new SpecifiedCsvScanWork(columnarManager, tso, csvFile, inputRefsForFilter, inputRefsForProject,
            executionContext, scanWorkId, metrics, enableMetrics, lazyEvaluator, preProcessor.getDeletion(csvFile),
            partNum, nodePartCount, useSelection, enableCompatible, columnTransformer, begin, end, tsoV0, tsoV1);
        isScanWorkInvoked = true;
        return (ScanWork<SplitT, BATCH>) csvScanWork;
    }

    public static ColumnarSplitBuilder newBuilder() {
        return new SpecifiedCsvColumnarSplit.SpecifiedCsvColumnarSplitBuilder();
    }

    static class SpecifiedCsvColumnarSplitBuilder implements ColumnarSplitBuilder {
        protected ExecutionContext executionContext;
        protected ColumnarManager columnarManager;
        protected OSSColumnTransformer columnTransformer;
        protected String logicalSchema;
        protected String logicalTable;
        protected Long tso;
        protected Path csvFile;
        protected int fileId;
        protected int sequenceId;
        protected LazyEvaluator<Chunk, BitSet> lazyEvaluator;

        protected List<Integer> inputRefsForFilter;
        protected List<Integer> inputRefsForProject;
        protected ScanPreProcessor preProcessor;

        protected int partNum;

        protected int nodePartCount;
        protected MemoryAllocatorCtx memoryAllocatorCtx;

        protected FragmentRFManager fragmentRFManager;
        protected OperatorStatistics operatorStatistics;

        private int begin;
        private int end;

        private long tsoV0;

        private long tsoV1;

        @Override
        public ColumnarSplit build() {
            return new SpecifiedCsvColumnarSplit(
                executionContext,
                columnarManager,
                tso,
                csvFile,
                fileId,
                sequenceId,
                inputRefsForFilter,
                inputRefsForProject,
                preProcessor,
                lazyEvaluator,
                partNum,
                nodePartCount,
                columnTransformer,
                begin,
                end,
                tsoV0,
                tsoV1);
        }

        @Override
        public ColumnarSplitBuilder executionContext(ExecutionContext context) {
            this.executionContext = context;
            return this;
        }

        @Override
        public ColumnarSplitBuilder columnarManager(ColumnarManager columnarManager) {
            this.columnarManager = columnarManager;
            return this;
        }

        @Override
        public ColumnarSplitBuilder isColumnarMode(boolean isColumnarMode) {
            // do nothing because the csv split is always in columnar mode.
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
        public ColumnarSplitBuilder ioExecutor(ExecutorService ioExecutor) {
            return this;
        }

        @Override
        public ColumnarSplitBuilder fileSystem(FileSystem fileSystem, Engine engine) {
            return this;
        }

        @Override
        public ColumnarSplitBuilder configuration(Configuration configuration) {
            return this;
        }

        @Override
        public ColumnarSplitBuilder sequenceId(int sequenceId) {
            this.sequenceId = sequenceId;
            return this;
        }

        @Override
        public ColumnarSplitBuilder file(Path filePath, int fileId) {
            this.csvFile = filePath;
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
            this.columnTransformer = ossColumnTransformer;
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
            return this;
        }

        @Override
        public ColumnarSplitBuilder chunkLimit(int chunkLimit) {
            return this;
        }

        @Override
        public ColumnarSplitBuilder morselUnit(int rgThreshold) {
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
        public ColumnarSplitBuilder begin(int begin) {
            this.begin = begin;
            return this;
        }

        @Override
        public ColumnarSplitBuilder end(int end) {
            this.end = end;
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

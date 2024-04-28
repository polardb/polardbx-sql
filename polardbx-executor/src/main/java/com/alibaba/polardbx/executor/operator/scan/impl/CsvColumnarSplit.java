/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.ColumnarSchemaTransformer;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class CsvColumnarSplit implements ColumnarSplit {
    private final ExecutionContext executionContext;
    private final ColumnarManager columnarManager;
    private final long tso;
    private final Path csvFile;
    private final int fileId;
    private final int sequenceId;
    private final ScanPreProcessor preProcessor;
    private final OSSColumnTransformer columnTransformer;

    private final List<Integer> inputRefsForFilter;
    private final List<Integer> inputRefsForProject;

    private CsvScanWork csvScanWork;
    private boolean isScanWorkInvoked = false;
    private LazyEvaluator<Chunk, BitSet> lazyEvaluator;

    private int partNum;

    private int nodePartCount;

    public CsvColumnarSplit(ExecutionContext executionContext, ColumnarManager columnarManager, long tso,
                            Path csvFile, int fileId, int sequenceId, List<Integer> inputRefsForFilter,
                            List<Integer> inputRefsForProject, ScanPreProcessor preProcessor,
                            LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                            int partNum, int nodePartCount,
                            OSSColumnTransformer columnTransformer) {
        this.executionContext = executionContext;
        this.columnarManager = columnarManager;
        this.tso = tso;
        this.csvFile = csvFile;
        this.fileId = fileId;
        this.sequenceId = sequenceId;
        this.inputRefsForFilter = inputRefsForFilter;
        this.inputRefsForProject = inputRefsForProject;
        this.preProcessor = preProcessor;
        this.lazyEvaluator = lazyEvaluator;
        this.partNum = partNum;
        this.nodePartCount = nodePartCount;
        this.columnTransformer = columnTransformer;
    }

    @Override
    public String getHostAddress() {
        return csvFile.toString();
    }

    @Override
    public int getSequenceId() {
        return 0;
    }

    @Override
    public int getFileId() {
        return fileId;
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
        csvScanWork = new CsvScanWork(columnarManager, tso, csvFile, inputRefsForFilter, inputRefsForProject,
            executionContext, scanWorkId, metrics, enableMetrics, lazyEvaluator, preProcessor.getDeletion(csvFile),
            partNum, nodePartCount, useSelection, enableCompatible, columnTransformer);
        isScanWorkInvoked = true;
        return (ScanWork<SplitT, BATCH>) csvScanWork;
    }

    @Override
    public ColumnarSplitPriority getPriority() {
        return ColumnarSplitPriority.CSV_SPLIT_PRIORITY;
    }

    private static String generateScanWorkId(String traceId, String file, int workIndex) {
        return "ScanWork$"
            + traceId + '$'
            + file + '$'
            + workIndex;
    }

    public static ColumnarSplitBuilder newBuilder() {
        return new CsvColumnarSplitBuilder();
    }

    static class CsvColumnarSplitBuilder implements ColumnarSplitBuilder {
        private ExecutionContext executionContext;
        private ColumnarManager columnarManager;
        private OSSColumnTransformer columnTransformer;
        private String logicalSchema;
        private String logicalTable;
        private Long tso;
        private Path csvFile;
        private int fileId;
        private int sequenceId;
        private LazyEvaluator<Chunk, BitSet> lazyEvaluator;

        private List<Integer> inputRefsForFilter;
        private List<Integer> inputRefsForProject;
        private ScanPreProcessor preProcessor;

        private int partNum;

        private int nodePartCount;
        private MemoryAllocatorCtx memoryAllocatorCtx;

        private FragmentRFManager fragmentRFManager;
        private OperatorStatistics operatorStatistics;

        @Override
        public ColumnarSplit build() {
            return new CsvColumnarSplit(executionContext, columnarManager, tso, csvFile, fileId, sequenceId,
                inputRefsForFilter,
                inputRefsForProject,
                preProcessor, lazyEvaluator,
                partNum, nodePartCount,
                columnTransformer);
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
    }
}

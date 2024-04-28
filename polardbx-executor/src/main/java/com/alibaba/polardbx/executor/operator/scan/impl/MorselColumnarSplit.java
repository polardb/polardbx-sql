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
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.ScanPolicy;
import com.alibaba.polardbx.executor.operator.scan.ScanPreProcessor;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.UserMetadataUtil;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * A columnar split responsible for morsel-driven scheduling.
 */
public class MorselColumnarSplit implements ColumnarSplit {
    private final ExecutionContext executionContext;

    /**
     * Executor service for IO tasks.
     */
    private final ExecutorService ioExecutor;

    /**
     * Engine of filesystem.
     */
    private final Engine engine;

    /**
     * Filesystem for storing columnar files.
     */
    private final FileSystem fileSystem;

    /**
     * Hadoop-style configuration.
     */
    private final Configuration configuration;

    /**
     * Unique sequence id for Driver-level identification.
     */
    private final int sequenceId;

    /**
     * Unique identification of columnar file.
     */
    private final int fileId;

    /**
     * File path with uri about filesystem.
     */
    private final Path filePath;

    private final OSSColumnTransformer ossColumnTransformer;

    /**
     * The column ids of primary keys in the file.
     * It may be null.
     */
    private final int[] primaryKeyColIds;

    /**
     * Subset of columns in this file.
     */
    private List<Integer> inputRefsForFilter;
    private List<Integer> inputRefsForProject;

    /**
     * The limit of chunk rows fetched from columnar files.
     */
    private final int chunkLimit;

    /**
     * Global block cache manager.
     */
    private final BlockCacheManager<Block> blockCacheManager;

    /**
     * The threshold of row-group count in one morsel-unit.
     */
    private final int rgThreshold;

    private final LazyEvaluator<Chunk, BitSet> lazyEvaluator;

    private final ScanPreProcessor preProcessor;

    /**
     * Inner iterator to get the next scan work.
     */
    private ScanWorkIterator scanWorkIterator;

    private int partNum;

    private int nodePartCount;

    private final MemoryAllocatorCtx memoryAllocatorCtx;

    private final FragmentRFManager fragmentRFManager;
    private final Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap;
    private final OperatorStatistics operatorStatistics;

    public MorselColumnarSplit(ExecutionContext executionContext, ExecutorService ioExecutor, Engine engine,
                               FileSystem fileSystem,
                               Configuration configuration, int sequenceId, int fileId, Path filePath,
                               OSSColumnTransformer ossColumnTransformer, int[] primaryKeyColIds,
                               List<Integer> inputRefsForFilter, List<Integer> inputRefsForProject,
                               int chunkLimit,
                               BlockCacheManager<Block> blockCacheManager,
                               int rgThreshold,
                               LazyEvaluator<Chunk, BitSet> lazyEvaluator, ScanPreProcessor preProcessor,
                               int partNum, int nodePartCount, MemoryAllocatorCtx memoryAllocatorCtx,
                               FragmentRFManager fragmentRFManager,
                               Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap,
                               OperatorStatistics operatorStatistics)
        throws IOException {
        this.executionContext = executionContext;
        this.ioExecutor = ioExecutor;
        this.engine = engine;
        this.fileSystem = fileSystem;
        this.configuration = configuration;
        this.sequenceId = sequenceId;
        this.fileId = fileId;
        this.filePath = filePath;
        this.ossColumnTransformer = ossColumnTransformer;
        this.primaryKeyColIds = primaryKeyColIds;
        this.inputRefsForFilter = inputRefsForFilter;
        this.inputRefsForProject = inputRefsForProject;
        this.chunkLimit = chunkLimit;

        this.blockCacheManager = blockCacheManager;
        this.rgThreshold = rgThreshold;
        this.lazyEvaluator = lazyEvaluator;
        this.preProcessor = preProcessor;

        this.partNum = partNum;
        this.nodePartCount = nodePartCount;
        this.memoryAllocatorCtx = memoryAllocatorCtx;
        this.fragmentRFManager = fragmentRFManager;
        this.rfFilterRefInFileMap = rfFilterRefInFileMap;
        this.operatorStatistics = operatorStatistics;
    }

    public static ColumnarSplitBuilder newBuilder() {
        return new MorselColumnarSplitBuilder();
    }

    /**
     * Location and range of the row-groups for the scan work.
     * <p>
     * For example, if the total number of row-group is 16,
     * the rgIncluded bitmap is {0,1,0,1,0,0,0,1,0,1,1,1,0,0,0,1}, and the rgThreshold is 3.
     * Then the list of scan-range will be:
     * scan-range1: {0,1,0,1,0,0,0,1}
     * scan-range2: {0,1,1,1}
     * scan-range3: {0,0,0,1}
     * <p>
     * To simplify the range representation, we make it share the rgIncluded and use {startGroupId, effectiveCount}
     * to represent the range on rgIncluded:
     * scan-range1: rgIncluded = {0,1,0,1,0,0,0,1,0,1,1,1,0,0,0,1}, startGroupId = 0, effectiveCount = 3
     * scan-range1: rgIncluded = {0,1,0,1,0,0,0,1,0,1,1,1,0,0,0,1}, startGroupId = 8, effectiveCount = 3
     * scan-range1: rgIncluded = {0,1,0,1,0,0,0,1,0,1,1,1,0,0,0,1}, startGroupId = 12, effectiveCount = 1
     */
    public static class ScanRange {
        /**
         * The stripe of this range.
         */
        final int stripeId;

        /**
         * The count of available row group starting from the startRowGroupId.
         */
        final int effectiveGroupCount;

        /**
         * The starting group id of the scan range.
         * NOTE: Not every row-group in this scan-range is selected or readable.
         */
        final int startRowGroupId;
        final boolean[] rowGroupIncluded;

        ScanRange(int stripeId, int startRowGroupId, int effectiveGroupCount, boolean[] rowGroupIncluded) {
            this.stripeId = stripeId;
            this.effectiveGroupCount = effectiveGroupCount;
            this.startRowGroupId = startRowGroupId;
            this.rowGroupIncluded = rowGroupIncluded;
        }

        public int getStripeId() {
            return stripeId;
        }

        public int getEffectiveGroupCount() {
            return effectiveGroupCount;
        }

        public int getStartRowGroupId() {
            return startRowGroupId;
        }

        public boolean[] getRowGroupIncluded() {
            return rowGroupIncluded;
        }

        @Override
        public String toString() {
            return "ScanRange{" +
                "stripeId=" + stripeId +
                ", effectiveGroupCount=" + effectiveGroupCount +
                ", startRowGroupId=" + startRowGroupId +
                ", rowGroupIncluded=" + Arrays.toString(rowGroupIncluded) +
                '}';
        }
    }

    /**
     * Iterator for scan works.
     */
    class ScanWorkIterator implements Iterator<ScanWork<ColumnarSplit, Chunk>> {
        /*================== Inner states for iterator ====================*/

        // start from 0
        private int stripeListIndex;

        // start from 0
        private int rowGroupIndex;

        private ScanRange currentRange;

        private int scanWorkIndex;

        /*================== Come from pre-processor ====================*/

        /**
         * Preheated file meta from columnar file.
         */
        private PreheatFileMeta preheatFileMeta;

        /**
         * Selected stripe ids.
         */
        private List<Integer> stripeIds;

        /**
         * The start row id of each stripe.
         */
        private Map<Integer, Long> startRowInStripeMap;

        /**
         * Filtered row-group bitmaps for each stripe.
         */
        private SortedMap<Integer, boolean[]> rowGroups;

        /**
         * File-level deletion bitmap.
         */
        private final RoaringBitmap deletion;

        /*================== Come from parameter collection ====================*/

        // parsed from file meta and reused by all scan work.
        private SortedMap<Integer, StripeInformation> stripeInformationMap;
        private int compressionSize;
        private CompressionKind compressionKind;
        private TypeDescription fileSchema;
        private boolean[] columnIncluded;
        private OrcFile.WriterVersion version;
        private ReaderEncryption encryption;

        /**
         * Mapping from stripeId to column-encoding info.
         */
        private SortedMap<Integer, OrcProto.ColumnEncoding[]> encodingMap;
        private boolean ignoreNonUtf8BloomFilter;
        private long maxBufferSize;
        private int indexStride;
        private boolean enableDecimal64;
        private int maxDiskRangeChunkLimit;
        private long maxMergeDistance;

        ScanWorkIterator() throws IOException {
            // load from pre-processor.
            Preconditions.checkArgument(preProcessor.isPrepared());
            preheatFileMeta = preProcessor.getPreheated(filePath);
            rowGroups = preProcessor.getPruningResult(filePath);
            stripeIds = rowGroups.keySet().stream().sorted().collect(Collectors.toList());
            deletion = preProcessor.getDeletion(filePath);

            // for iterator inner states.
            stripeListIndex = 0;
            rowGroupIndex = 0;
            currentRange = null;
            scanWorkIndex = 0;

            // collect parameters from preheat meta.
            collectParams();
        }

        private void collectParams() throws IOException {
            OrcTail orcTail = preheatFileMeta.getPreheatTail();

            // compression info
            compressionKind = orcTail.getCompressionKind();
            compressionSize = orcTail.getCompressionBufferSize();

            // get the mapping from stripe id to stripe information.
            List<StripeInformation> stripeInformationList = orcTail.getStripes();
            stripeInformationMap = stripeInformationList.stream().collect(Collectors.toMap(
                stripe -> (int) stripe.getStripeId(),
                stripe -> stripe,
                (s1, s2) -> s1,
                () -> new TreeMap<>()
            ));

            // Get the start rows of each stipe.
            startRowInStripeMap = new HashMap<>();
            long startRows = 0;
            for (int stripeId = 0; stripeId < stripeInformationList.size(); stripeId++) {
                StripeInformation stripeInformation = stripeInformationList.get(stripeId);
                startRowInStripeMap.put(stripeId, startRows);
                long nextStartRows = startRows + stripeInformation.getNumberOfRows();
                startRows = nextStartRows;
            }

            fileSchema = orcTail.getSchema();
            version = orcTail.getWriterVersion();

            // The column included bitmap marking all columnId that should be accessed.
            // The colIndex = 0 means struct column.
            columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
            Arrays.fill(columnIncluded, false);
            columnIncluded[0] = true;

            for (int i = 0; i < ossColumnTransformer.columnCount(); i++) {
                Integer locInOrc = ossColumnTransformer.getLocInOrc(i);
                if (locInOrc != null) {
                    columnIncluded[locInOrc] = true;
                }
            }

            // encryption info for reading.
            OrcProto.Footer footer = orcTail.getFooter();
            encryption = new ReaderEncryption(footer, fileSchema,
                orcTail.getStripeStatisticsOffset(), orcTail.getTailBuffer(), stripeInformationList,
                null, configuration);

            encodingMap = stripeInformationList.stream().collect(Collectors.toMap(
                stripe -> (int) stripe.getStripeId(),
                stripe -> StaticStripePlanner.buildEncodings(
                    encryption,
                    columnIncluded,
                    preheatFileMeta.getStripeFooter((int) stripe.getStripeId())),
                (s1, s2) -> s1,
                () -> new TreeMap<>()
            ));

            // should the reader ignore the obsolete non-UTF8 bloom filters.
            ignoreNonUtf8BloomFilter = false;

            // max buffer size in single IO task.
            maxBufferSize = Integer.MAX_VALUE - 1024;

            // the max row count in one row group.
            indexStride = orcTail.getFooter().getRowIndexStride();

            enableDecimal64 = UserMetadataUtil.extractBooleanValue(orcTail.getFooter().getMetadataList(),
                UserMetadataUtil.ENABLE_DECIMAL_64, false);

            // When reading stripes >2GB, specify max limit for the chunk size.
            maxDiskRangeChunkLimit = Integer.MAX_VALUE - 1024;

            // max merge distance of disk io
            maxMergeDistance = executionContext.getParamManager().getLong(ConnectionParams.OSS_ORC_MAX_MERGE_DISTANCE);
        }

        @Override
        public boolean hasNext() {
            if (stripeIds.isEmpty()) {
                return false;
            }

            // Move row group index in current stripe.
            if (moveRowGroupIndex()) {
                return true;
            }

            // Try to get the next stripe when row-group index is out of bound.
            if (++stripeListIndex < stripeIds.size()) {
                rowGroupIndex = 0;
                if (moveRowGroupIndex()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public ScanWork<ColumnarSplit, Chunk> next() {
            String scanWorkId = generateScanWorkId(
                executionContext.getTraceId(),
                filePath.toString(),
                currentRange.stripeId,
                scanWorkIndex++
            );

            int scanPolicyId = executionContext.getParamManager()
                .getInt(ConnectionParams.SCAN_POLICY);
            boolean enableMetrics = executionContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS);
            boolean enableCancelLoading = executionContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_CANCEL_STRIPE_LOADING);
            boolean activeLoading = executionContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_LAZY_BLOCK_ACTIVE_LOADING);
            boolean useInFlightBlockCache = executionContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_USE_IN_FLIGHT_BLOCK_CACHE);

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

                ScanPolicy scanPolicy = ScanPolicy.of(scanPolicyId);
                if (executionContext.isEnableOrcDeletedScan()) {
                    // Special path for check cci consistency.
                    // Normal oss read should not get here.
                    scanPolicy = ScanPolicy.DELETED_SCAN;
                }

                ScanWork<ColumnarSplit, Chunk> scanWork;
                switch (scanPolicy) {
                case IO_PRIORITY:
                    scanWork = new IOPriorityScanWork(
                        scanWorkId,
                        metrics,
                        enableMetrics,
                        lazyEvaluator,
                        rowGroupIterator, deletion,
                        currentRange, inputRefsForFilter, inputRefsForProject,
                        partNum, nodePartCount,
                        enableCancelLoading,
                        ossColumnTransformer);
                    break;
                case FILTER_PRIORITY:
                    scanWork = new FilterPriorityScanWork(
                        scanWorkId,
                        metrics,
                        enableMetrics,
                        lazyEvaluator,
                        rowGroupIterator, deletion,
                        currentRange, inputRefsForFilter, inputRefsForProject,
                        partNum, nodePartCount,
                        activeLoading, chunkLimit, useInFlightBlockCache,
                        fragmentRFManager, rfFilterRefInFileMap, operatorStatistics,
                        ossColumnTransformer);
                    break;
                case MERGE_IO:
                    scanWork = new MergeIOScanWork(
                        scanWorkId,
                        metrics,
                        enableMetrics,
                        lazyEvaluator,
                        rowGroupIterator, deletion,
                        currentRange, inputRefsForFilter, inputRefsForProject,
                        partNum, nodePartCount,
                        enableCancelLoading,
                        ossColumnTransformer);
                    break;
                case DELETED_SCAN:
                    scanWork = new DeletedScanWork(
                        scanWorkId,
                        metrics,
                        enableMetrics,
                        lazyEvaluator,
                        rowGroupIterator, deletion,
                        currentRange, inputRefsForFilter, inputRefsForProject,
                        partNum, nodePartCount,
                        activeLoading, ossColumnTransformer);
                    break;
                default:
                    throw new UnsupportedOperationException();
                }

                // Must call the hasNext() method again if someone wants to get the next range.
                currentRange = null;
                return scanWork;
            }
            return null;
        }

        private boolean moveRowGroupIndex() {
            // if row group index is less than maximum row group index in stripe, move forward.
            int stripeId = stripeIds.get(stripeListIndex);
            StripeInformation stripe = stripeInformationMap.get(stripeId);
            int rowGroupCountInStripe = (int) ((stripe.getNumberOfRows() + indexStride - 1) / indexStride);

            // Try to get the next row group.
            if (rowGroupIndex < rowGroupCountInStripe) {

                // The maximum row group count we can get from remaining row-groups.
                int maxRowGroupCount = Math.min(rowGroupCountInStripe - rowGroupIndex, rgThreshold);

                // It's the bitmap of all selected row-group in a stripe.
                boolean[] rgIncluded = rowGroups.get(stripeId);
                int effectiveCount = 0;
                int effectiveRowGroupIndex;
                for (effectiveRowGroupIndex = rowGroupIndex;
                     effectiveRowGroupIndex < rgIncluded.length && effectiveCount < maxRowGroupCount;
                     effectiveRowGroupIndex++) {

                    // Find all the effective row-group from the last row-group index,
                    // util the effective count is larger than the maximum count of remaining row groups.
                    if (rgIncluded[effectiveRowGroupIndex]) {
                        effectiveCount++;
                    }
                }

                if (effectiveCount == 0) {
                    // It means no effective row-group is found
                    return false;
                }

                currentRange = new ScanRange(stripeId, rowGroupIndex, effectiveCount, rgIncluded);

                // We must ensure that the ranges of all works are not overlap.
                rowGroupIndex = effectiveRowGroupIndex;
                return true;
            }
            return false;
        }
    }

    @Override
    public <SplitT extends ColumnarSplit, BATCH> ScanWork<SplitT, BATCH> nextWork() {
        if (scanWorkIterator == null) {
            // The pre-processor must have been done.
            if (!preProcessor.isPrepared()) {
                GeneralUtil.nestedException("The pre-processor is not prepared");
            }

            try {
                scanWorkIterator = new ScanWorkIterator();
            } catch (IOException e) {
                throw GeneralUtil.nestedException("Fail to initialize scan-work iterator", e);
            }
        }

        while (scanWorkIterator.hasNext()) {
            return (ScanWork<SplitT, BATCH>) scanWorkIterator.next();
        }
        return null;
    }

    @Override
    public ColumnarSplitPriority getPriority() {
        return ColumnarSplitPriority.ORC_SPLIT_PRIORITY;
    }

    @Override
    public String getHostAddress() {
        return filePath.toString();
    }

    @Override
    public int getSequenceId() {
        return sequenceId;
    }

    @Override
    public int getFileId() {
        return fileId;
    }

    private static String generateScanWorkId(String traceId, String file, int stripeId, int workIndex) {
        return new StringBuilder()
            .append("ScanWork$")
            .append(traceId).append('$')
            .append(file).append('$')
            .append(stripeId).append('$')
            .append(workIndex).toString();
    }

    /**
     * Builder for morsel-columnar-split.
     */
    static class MorselColumnarSplitBuilder implements ColumnarSplitBuilder {
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
                return new MorselColumnarSplit(
                    executionContext, ioExecutor, engine, fileSystem,
                    configuration, sequenceId, fileId, filePath,
                    ossColumnTransformer, primaryKeyColIds,

                    inputRefsForFilter, inputRefsForProject,

                    chunkLimit, blockCacheManager,
                    rgThreshold, lazyEvaluator,
                    preProcessor,
                    partNum, nodePartCount, memoryAllocatorCtx, fragmentRFManager, rfFilterRefInFileMap,
                    operatorStatistics);
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

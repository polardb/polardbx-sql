package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.CommonLazyBlock;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.executor.operator.scan.SeekableIterator;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileKeys;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.StreamName;
import org.jetbrains.annotations.Nullable;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public abstract class AbstractScanWork implements ScanWork<ColumnarSplit, Chunk> {
    protected static final Logger LOGGER = LoggerFactory.getLogger(FilterPriorityScanWork.class);

    /**
     * IO status to manage the output and state of scan work.
     */
    protected final IOStatus<Chunk> ioStatus;

    /**
     * The unique identifier of this work.
     */
    protected final String workId;

    /**
     * The runtime metrics of this scan-work.
     */
    protected final RuntimeMetrics metrics;

    protected final boolean enableMetrics;

    /**
     * To enumerator all available chunks in all available row-group.
     */
    protected RowGroupIterator<Block, ColumnStatistics> rgIterator;

    /**
     * Deletion bitmap for this columnar file in this query session.
     */
    protected final RoaringBitmap deletionBitmap;

    /**
     * To evaluate the chunks produced from RowGroupIterator.
     */
    protected final LazyEvaluator<Chunk, BitSet> lazyEvaluator;

    protected final MorselColumnarSplit.ScanRange scanRange;

    /**
     * The reference number for filter.
     */
    protected final List<Integer> inputRefsForFilter;

    /**
     * The reference number for projection.
     */
    protected final List<Integer> inputRefsForProject;

    /**
     * Mapping from ref to chunk index.
     * For example, the refs for filter is {3, 5} and the refs for project is {3, 7, 8},
     * then we get the chunkRefMap: {(3, 0), (5, 1), (7, 2), (8, 3)}
     */
    protected final int[] chunkRefMap;
    protected final SortedSet<Integer> refSet;

    protected boolean isCanceled;

    /**
     * To count the time cost of evaluation in scan work.
     */
    protected Counter evaluationTimer;

    protected int partNum;

    protected int nodePartCount;

    /**
     * The flag to be checked by IO processing, and the IO processing will stop
     * immediately if this flag was found to be TRUE.
     */
    protected AtomicBoolean isIOCanceled;

    protected OSSColumnTransformer columnTransformer;

    public AbstractScanWork(String workId,
                            RuntimeMetrics metrics, boolean enableMetrics,
                            LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                            RowGroupIterator<Block, ColumnStatistics> rgIterator,
                            RoaringBitmap deletionBitmap,
                            MorselColumnarSplit.ScanRange scanRange,
                            List<Integer> inputRefsForFilter,
                            List<Integer> inputRefsForProject,
                            int partNum,
                            int nodePartCount,
                            OSSColumnTransformer columnTransformer) {
        this.workId = workId;
        this.metrics = metrics;
        this.enableMetrics = enableMetrics;
        this.lazyEvaluator = lazyEvaluator;
        this.ioStatus = IOStatusImpl.create(workId);
        this.rgIterator = rgIterator;
        this.deletionBitmap = deletionBitmap;
        this.scanRange = scanRange;
        this.inputRefsForFilter = inputRefsForFilter.stream().sorted().collect(Collectors.toList());
        this.inputRefsForProject = inputRefsForProject.stream().sorted().collect(Collectors.toList());

        refSet = new TreeSet<>();
        refSet.addAll(inputRefsForFilter);
        refSet.addAll(inputRefsForProject);

        this.chunkRefMap = new int[refSet.last() + 1];
        int chunkIndex = 0;
        for (int ref : refSet) {
            chunkRefMap[ref] = chunkIndex++;
        }

        if (enableMetrics) {
            this.evaluationTimer = metrics.addCounter(
                ProfileKeys.SCAN_WORK_EVALUATION_TIMER.getName(),
                ScanWork.EVALUATION_TIMER,
                ProfileUnit.NANO_SECOND
            );
        }

        this.partNum = partNum;
        this.nodePartCount = nodePartCount;

        this.isIOCanceled = new AtomicBoolean(false);
        this.columnTransformer = columnTransformer;
    }

    abstract protected void handleNextWork() throws Throwable;

    @Override
    public void invoke(ExecutorService executor) {
        executor.submit(() -> {
            try {
                handleNextWork();
            } catch (Throwable e) {
                e.printStackTrace();
                ioStatus.addException(e);
                LOGGER.error("fail to execute sequential scan work: " + e);
            }
        });
    }

    @Override
    public void cancel() {
        this.isCanceled = true;
        this.isIOCanceled.set(true);
    }

    protected Chunk rebuildProject(Chunk chunk) {
        return rebuildProject(chunk, null, chunk.getPositionCount());
    }

    protected Chunk rebuildProject(Chunk chunk, int[] selection, int selSize) {
        Block[] blocks = new Block[inputRefsForProject.size()];
        int blockIndex = 0;

        for (int projectRef : inputRefsForProject) {
            // mapping blocks for projection.
            int chunkIndex = chunkRefMap[projectRef];
            Block block = chunk.getBlock(chunkIndex);
            blocks[blockIndex++] = block;

            // fill with selection.
            if (selection != null && chunk.getPositionCount() != selSize) {
                Preconditions.checkArgument(block instanceof CommonLazyBlock);
                ((CommonLazyBlock) block).setSelection(selection, selSize);
            }
        }

        // NOTE: we must set position count of chunk to selection size.
        Chunk projectChunk = new Chunk(selSize, blocks);
        projectChunk.setPartIndex(partNum);
        projectChunk.setPartCount(nodePartCount);
        return projectChunk;
    }

    protected void releaseRef(Chunk chunk) {
        if (chunk == null) {
            return;
        }
        for (int blockIndex = 0; blockIndex < chunk.getBlockCount(); blockIndex++) {
            Block block = chunk.getBlock(blockIndex);
            if (block instanceof LazyBlock) {
                ((LazyBlock) block).releaseRef();
            }
        }
    }

    protected void singleIO(int filterColumnId, Path filePath, int stripeId, boolean[] prunedRowGroupBitmap,
                            BlockCacheManager<Block> blockCacheManager, boolean useInFlightCache) {
        // Fetch all cached block in this column on different row group,
        // and initialize the cached-block-reader for block loading.
        CacheReader<Block> cacheReader = rgIterator.getCacheReader(filterColumnId);
        if (cacheReader != null && !cacheReader.isInitialized()) {
            // Check the block-cache-manager what row-group of this column have been cached.
            Map<Integer, SeekableIterator<Block>> caches = blockCacheManager
                .getCachedRowGroups(filePath, stripeId, filterColumnId, prunedRowGroupBitmap);

            if (useInFlightCache) {
                // Use in-flight cache but also invoke IO processing for it's row-group.
                // Because we cannot ensure if all needed blocks are contained in in-flight caches.
                Map<Integer, SeekableIterator<Block>> inFlightCaches =
                    blockCacheManager.getInFlightCachedRowGroups(
                        filePath, stripeId, filterColumnId, prunedRowGroupBitmap
                    );
                cacheReader.initialize(caches, inFlightCaches);
            } else {
                cacheReader.initialize(caches);
            }
        }

        // Open column-reader only once and invoke synchronous IO tasks with given row group bitmap.
        ColumnReader columnReader = rgIterator.getColumnReader(filterColumnId);
        if (columnReader != null && cacheReader != null && !columnReader.isOpened()) {
            // Remove cached row groups from original row group bitmap
            // that don't need IO processing.
            boolean[] cachedRowGroupBitmap = cacheReader.cachedRowGroupBitmap();
            boolean[] rowGroupIncluded = remove(prunedRowGroupBitmap, cachedRowGroupBitmap);

            // Proactively invoke the column-level IO processing.
            // It should be aware of the total row group bitmap in Stripe-level.
            // In most cases, we will open column-reader in table scan works.
            columnReader.open(true, rowGroupIncluded);
        }

    }

    protected void singleIO(int filterColumnId, Path filePath, int stripeId, boolean[] prunedRowGroupBitmap,
                            BlockCacheManager<Block> blockCacheManager) {
        singleIO(filterColumnId, filePath, stripeId, prunedRowGroupBitmap, blockCacheManager, false);
    }

    protected void mergeIO(Path filePath, int stripeId,
                           List<Integer> inputRefs,
                           BlockCacheManager<Block> blockCacheManager, boolean[] rowGroupIncluded,
                           boolean useInFlightCache) {
        List<Integer> columnIds = new ArrayList<>();
        Map<Integer, boolean[]> rgMatrix = new HashMap<>();
        for (int i = 0; i < inputRefs.size(); i++) {
            final Integer columnId = columnTransformer.getLocInOrc(chunkRefMap[inputRefs.get(i)]);
            if (columnId == null) {
                continue;
            }
            // Fetch all cached block in this column on different row group,
            // and initialize the cached-block-readr for block loading.
            CacheReader<Block> cacheReader = rgIterator.getCacheReader(columnId);
            if (cacheReader != null && !cacheReader.isInitialized()) {
                // Check the block-cache-manager what row-group of this column have been cached.
                Map<Integer, SeekableIterator<Block>> caches = blockCacheManager
                    .getCachedRowGroups(filePath, stripeId, columnId, rowGroupIncluded);
                if (useInFlightCache) {
                    // Use in-flight cache but also invoke IO processing for it's row-group.
                    // Because we cannot ensure if all needed blocks are contained in in-flight caches.
                    Map<Integer, SeekableIterator<Block>> inFlightCaches =
                        blockCacheManager.getInFlightCachedRowGroups(
                            filePath, stripeId, columnId, rowGroupIncluded
                        );
                    cacheReader.initialize(caches, inFlightCaches);
                } else {
                    cacheReader.initialize(caches);
                }
            }

            // No matter whether rows are selected or not,
            // We must load the whole column stream at once into memory.
            ColumnReader columnReader = rgIterator.getColumnReader(columnId);
            if (columnReader != null && cacheReader != null && !columnReader.isOpened()) {
                // Remove cached row groups from original row group bitmap
                // that don't need IO processing.
                boolean[] cachedRowGroupBitmap = cacheReader.cachedRowGroupBitmap();
                boolean[] rowGroupInColumn = remove(rowGroupIncluded, cachedRowGroupBitmap);

                columnIds.add(columnId);
                rgMatrix.put(columnId, rowGroupInColumn);
            }
        }

        // Invoke stripe-level IO tasks that has been merged.
        StripeLoader stripeLoader = rgIterator.getStripeLoader();
        CompletableFuture<Map<StreamName, InStream>> loadFuture =
            stripeLoader.load(columnIds, rgMatrix, () -> isIOCanceled.get());

        for (int i = 0; i < inputRefs.size(); i++) {
            final Integer columnId = columnTransformer.getLocInOrc(chunkRefMap[inputRefs.get(i)]);
            if (columnId == null) {
                continue;
            }

            ColumnReader columnReader = rgIterator.getColumnReader(columnId);
            if (columnReader != null && !columnReader.isOpened() && rgMatrix.get(columnId) != null) {
                // Open column reader with IO task future.
                columnReader.open(loadFuture, false, rgMatrix.get(columnId));
            }
        }
    }

    protected void mergeIO(Path filePath, int stripeId,
                           List<Integer> inputRefs,
                           BlockCacheManager<Block> blockCacheManager, boolean[] rowGroupIncluded) {
        mergeIO(filePath, stripeId, inputRefs, blockCacheManager, rowGroupIncluded, false);
    }

    /**
     * Not using block cache, do IO and parse for all needed row-groups.
     */
    protected void mergeIONoCache(List<Integer> inputRefs,
                                  boolean[] rowGroupIncluded) {
        List<Integer> columnIds = new ArrayList<>();
        Map<Integer, boolean[]> rgMatrix = new HashMap<>();
        for (int i = 0; i < inputRefs.size(); i++) {
            final Integer columnId = columnTransformer.getLocInOrc(chunkRefMap[inputRefs.get(i)]);
            if (columnId == null) {
                continue;
            }
            // No matter whether rows are selected or not,
            // We must load the whole column stream at once into memory.
            ColumnReader columnReader = rgIterator.getColumnReader(columnId);
            if (!columnReader.isOpened()) {
                columnIds.add(columnId);
                rgMatrix.put(columnId, rowGroupIncluded);
            }
        }

        // Invoke stripe-level IO tasks that has been merged.
        StripeLoader stripeLoader = rgIterator.getStripeLoader();
        CompletableFuture<Map<StreamName, InStream>> loadFuture =
            stripeLoader.load(columnIds, rgMatrix, () -> isIOCanceled.get());

        for (int i = 0; i < inputRefs.size(); i++) {
            final Integer columnId = columnTransformer.getLocInOrc(chunkRefMap[inputRefs.get(i)]);
            if (columnId == null) {
                continue;
            }
            ColumnReader columnReader = rgIterator.getColumnReader(columnId);
            if (!columnReader.isOpened() && rgMatrix.get(columnId) != null) {
                // Open column reader with IO task future.
                columnReader.open(loadFuture, false, rgMatrix.get(columnId));
            }
        }
    }

    protected static boolean[] remove(boolean[] left, boolean[] right) {
        boolean[] result = new boolean[left.length];
        for (int i = 0; i < left.length; i++) {
            result[i] = left[i] &&
                (i >= right.length || (i < right.length && !right[i]));
        }
        return result;
    }

    protected boolean hasNonZeros(int[] selection) {
        for (int selected : selection) {
            if (selected != 0) {
                return true;
            }
        }
        return false;
    }

    protected int countTrue(boolean[] included) {
        int result = 0;
        for (boolean b : included) {
            result += b ? 1 : 0;
        }
        return result;
    }

    protected boolean[] toRowGroupBitmap(int rowGroupCount, List<Integer> selectedRowGroups) {
        boolean[] result = new boolean[rowGroupCount];
        for (Integer selected : selectedRowGroups) {
            if (selected < rowGroupCount) {
                result[selected] = true;
            }
        }
        return result;
    }

    protected int[] selectionOf(boolean[] bitmap, int selectCount) {
        int[] selection = new int[selectCount];
        int selSize = 0;
        for (int i = 0; i < bitmap.length; i++) {
            if (bitmap[i]) {
                selection[selSize++] = i;
            }
        }

        return selection;
    }

    protected int[] selectionOf(BitSet bitmap) {
        int[] selection = new int[bitmap.cardinality()];
        int selSize = 0;
        for (int i = bitmap.nextSetBit(0); i >= 0; i = bitmap.nextSetBit(i + 1)) {
            selection[selSize++] = i;
        }

        return selection;
    }

    /**
     * Generate selection array from deletion bitmap in given range.
     */
    protected int @Nullable [] selectionOf(int[] batchRange, RoaringBitmap deletion) {
        int[] preSelection = null;
        if (deletion != null && !deletion.isEmpty()) {
            final int startPosition = batchRange[0];
            final int positionCount = batchRange[1];

            long cardinality = deletion.rangeCardinality(
                startPosition, startPosition + positionCount);

            if (cardinality != 0) {
                // partial positions are selected.
                preSelection = new int[positionCount - (int) cardinality];

                // remove deleted positions.
                int selectionIndex = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (!deletion.contains(i + startPosition)) {
                        preSelection[selectionIndex++] = i;
                    }
                }
            }
        }
        return preSelection;
    }

    /**
     * Generate deleted selection array from deletion bitmap in given range.
     */
    protected int @Nullable [] selectionOfDeleted(int[] batchRange, RoaringBitmap deletion) {
        int[] preSelection = null;
        if (deletion != null && !deletion.isEmpty()) {
            final int startPosition = batchRange[0];
            final int positionCount = batchRange[1];

            long cardinality = deletion.rangeCardinality(startPosition, startPosition + positionCount);

            if (cardinality != 0) {
                // deleted positions are selected.
                preSelection = new int[(int) cardinality];

                // select deleted positions only
                int selectionIndex = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (deletion.contains(i + startPosition)) {
                        preSelection[selectionIndex++] = i;
                    }
                }
            }
        }
        return preSelection;
    }

    @Override
    public IOStatus<Chunk> getIOStatus() {
        return ioStatus;
    }

    @Override
    public String getWorkId() {
        return workId;
    }

    @Override
    public void close() throws IOException {
        close(true);
    }

    @Override
    public void close(boolean force) {
        // The close method must be idempotent.
        if (rgIterator != null) {
            rgIterator.close(force);
        }

        if (ioStatus != null) {
            ioStatus.close();
        }
    }

    @Override
    public RuntimeMetrics getMetrics() {
        return metrics;
    }

    public MorselColumnarSplit.ScanRange getScanRange() {
        return scanRange;
    }

    /**
     * This method is for test only
     *
     * @return if all readers are closed
     */
    public boolean checkIfAllReadersClosed() {
        if (rgIterator != null) {
            return ((RowGroupIteratorImpl) rgIterator).checkIfAllReadersClosed();
        } else {
            return true;
        }
    }

}


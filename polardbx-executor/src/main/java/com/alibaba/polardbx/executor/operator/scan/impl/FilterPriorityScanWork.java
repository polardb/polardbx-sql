package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.RowGroupReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Example of scan work
 */
public class FilterPriorityScanWork extends AbstractScanWork {

    public static final int INITIAL_LIST_CAPACITY = 16;
    private final boolean activeLoading;
    private final int chunkLimit;
    private final boolean useInFlightBlockCache;

    /**
     * The Fragment-level runtime filter manager.
     */
    private final FragmentRFManager fragmentRFManager;

    /**
     * Record the actual file column channel for a given item key.
     */
    private final Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap;

    /**
     * Record the existence of bloom filter for each item keys.
     */
    private Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilters;

    private final OperatorStatistics operatorStatistics;

    private volatile RFLazyEvaluator rfEvaluator;

    /**
     * no push-down filter, skip evaluation.
     * if evaluator is a constant expression, we should not skip the evaluation.
     */
    private boolean skipEvaluation;

    public FilterPriorityScanWork(String workId,
                                  RuntimeMetrics metrics,
                                  boolean enableMetrics,
                                  LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                                  RowGroupIterator<Block, ColumnStatistics> rgIterator,
                                  RoaringBitmap deletionBitmap,
                                  MorselColumnarSplit.ScanRange scanRange,
                                  List<Integer> inputRefsForFilter,
                                  List<Integer> inputRefsForProject,
                                  int partNum,
                                  int nodePartCount, boolean activeLoading, int chunkLimit,
                                  boolean useInFlightBlockCache, FragmentRFManager fragmentRFManager,
                                  Map<FragmentRFItemKey, Integer> rfFilterRefInFileMap,
                                  OperatorStatistics operatorStatistics,
                                  OSSColumnTransformer columnTransformer) {
        super(workId, metrics, enableMetrics, lazyEvaluator, rgIterator, deletionBitmap, scanRange, inputRefsForFilter,
            inputRefsForProject, partNum, nodePartCount, columnTransformer);
        this.activeLoading = activeLoading;
        this.chunkLimit = chunkLimit;
        this.useInFlightBlockCache = useInFlightBlockCache;
        this.fragmentRFManager = fragmentRFManager;
        this.rfFilterRefInFileMap = rfFilterRefInFileMap;
        this.rfBloomFilters = new HashMap<>();
        this.operatorStatistics = operatorStatistics;

        // Check should we use skip-eval mode.
        int filterColumns = inputRefsForFilter.size();
        this.skipEvaluation = lazyEvaluator == null && filterColumns == 0;

        if (this.fragmentRFManager != null && skipEvaluation) {
            // create a light-weight evaluator for runtime filter.
            this.rfEvaluator = new RFLazyEvaluator(fragmentRFManager, operatorStatistics, rfBloomFilters);
        }

        if (this.fragmentRFManager != null && lazyEvaluator != null) {
            // register RF to predicate.
            ((DefaultLazyEvaluator) lazyEvaluator).registerRF(fragmentRFManager, operatorStatistics, rfBloomFilters);
        }
    }

    @Override
    protected void handleNextWork() throws Throwable {
        final Path filePath = rgIterator.filePath();
        final int stripeId = rgIterator.stripeId();

        // not all row group but those filtered by pruner should be loaded.
        final boolean[] prunedRowGroupBitmap = rgIterator.rgIncluded();
        final int rowGroupCount = prunedRowGroupBitmap.length;
        final BlockCacheManager<Block> blockCacheManager = rgIterator.getCacheManager();

        // Get and lazily evaluate chunks until row group count exceeds the threshold.
        // NOTE: the row-group and chunk must be in order.
        final Map<Integer, List<Chunk>> chunksWithGroup = new TreeMap<>();
        final List<Integer> selectedRowGroups = new ArrayList<>();

        // for filter column, initialize or open the related modules.
        int filterColumns = inputRefsForFilter.size();
        if (filterColumns == 1) {
            // use single IO if filter columns = 1
            final Integer filterColId = columnTransformer.getLocInOrc(chunkRefMap[inputRefsForFilter.get(0)]);
            if (filterColId != null) {
                singleIO(filterColId, filePath, stripeId,
                    prunedRowGroupBitmap, blockCacheManager, useInFlightBlockCache);
            }
        } else if (filterColumns > 1) {
            // use merging IO if filter columns > 1.
            mergeIO(filePath, stripeId,
                inputRefsForFilter,
                blockCacheManager,
                prunedRowGroupBitmap,
                useInFlightBlockCache);
        }

        boolean[] bitmap = new boolean[chunkLimit];

        while (!isCanceled && rgIterator.hasNext()) {
            rgIterator.next();
            LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup = rgIterator.current();
            final int rowGroupId = logicalRowGroup.groupId();

            // The row group id in iterator must be valid.
            Preconditions.checkArgument(prunedRowGroupBitmap[rowGroupId]);

            // A flag for each row group to indicate that at least one block selected in row group.
            boolean rgSelected = false;

            int handledChunksBeforeRF = 0;
            Chunk chunk;
            RowGroupReader<Chunk> rowGroupReader = logicalRowGroup.getReader();
            while ((chunk = rowGroupReader.nextBatch()) != null) {

                // Check the runtime filter and invoke single IO before evaluation.
                if (fragmentRFManager != null) {
                    for (FragmentRFItemKey itemKey : rfFilterRefInFileMap.keySet()) {

                        if (rfBloomFilters.get(itemKey) == null) {

                            // try to fetch the runtime filter of given item key,
                            FragmentRFItem item = fragmentRFManager.getAllItems().get(itemKey);
                            RFBloomFilter[] bfArray = item.getRF();

                            if (bfArray != null) {
                                // Success to get the generated runtime filter.
                                rfBloomFilters.put(itemKey, bfArray);

                                // use single IO to open the filter column.
                                final int filterRefInFile = rfFilterRefInFileMap.get(itemKey);
                                final int filterColId = filterRefInFile + 1;
                                singleIO(filterColId, filePath, stripeId,
                                    prunedRowGroupBitmap, blockCacheManager, useInFlightBlockCache);
                            }
                        }
                    }
                }

                int[] batchRange = rowGroupReader.batchRange();
                if (skipEvaluation) {

                    int[] preSelection = null;
                    if (rfEvaluator != null) {
                        // Filter the chunk use fragment RF.
                        int selectCount =
                            rfEvaluator.eval(chunk, batchRange[0], batchRange[1], deletionBitmap, bitmap);

                        preSelection = selectionOf(bitmap, selectCount);
                    } else {
                        preSelection = selectionOf(batchRange, deletionBitmap);
                    }

                    if (preSelection != null) {
                        // rebuild chunk according to project refs.
                        chunk = rebuildProject(chunk, preSelection, preSelection.length);
                    }

                    // no evaluation, just buffer the unloaded chunks.
                    List<Chunk> chunksInGroup =
                        chunksWithGroup.computeIfAbsent(rowGroupId, any -> new ArrayList<>(INITIAL_LIST_CAPACITY));
                    chunksInGroup.add(chunk);
                    rgSelected = true;
                    continue;
                }

                // Proactively load the filter-blocks
                for (int filterRef : inputRefsForFilter) {
                    int chunkIndex = chunkRefMap[filterRef];
                    Preconditions.checkArgument(chunkIndex >= 0);

                    // all blocks in chunk is lazy
                    // NOTE: explicit type cast?
                    LazyBlock filterBlock = (LazyBlock) chunk.getBlock(chunkIndex);

                    // Proactively invoke loading, or we can load it during evaluation.
                    filterBlock.load();
                }

                long start = System.nanoTime();

                // Get selection array of this range [n * 1000, (n+1) * 1000] in row group,
                // and then evaluate the filter.
                int selectCount =
                    lazyEvaluator.eval(chunk, batchRange[0], batchRange[1], deletionBitmap, bitmap);

                // check zeros in selection array,
                // and mark whether this row group is selected or not
                boolean hasSelectedPositions = selectCount > 0;

                rgSelected |= hasSelectedPositions;
                if (!hasSelectedPositions) {
                    // if all positions are filtered, skip to the next chunk.
                    if (enableMetrics) {
                        evaluationTimer.inc(System.nanoTime() - start);
                    }

                    // The created chunk and block-loader will be abandoned here.
                    releaseRef(chunk);
                    continue;
                }

                Chunk projectChunk;
                if (selectCount == chunk.getPositionCount()) {
                    projectChunk = rebuildProject(chunk);
                } else {
                    // hold this chunk util all row groups in scan work are handled.
                    int[] selection = selectionOf(bitmap, selectCount);
                    if (enableMetrics) {
                        evaluationTimer.inc(System.nanoTime() - start);
                    }

                    // rebuild chunk according to project refs.
                    projectChunk = rebuildProject(chunk, selection, selection.length);
                }

                List<Chunk> chunksInGroup = chunksWithGroup.computeIfAbsent(rowGroupId, any -> new ArrayList<>(
                    INITIAL_LIST_CAPACITY));
                chunksInGroup.add(projectChunk);
            }
            // the chunk in this row group is run out, change to the next.
            if (rgSelected) {
                selectedRowGroups.add(rowGroupId);
            } else {
                // if row-group is not selected, remove all chunks of this row-group from buffer.
                List<Chunk> chunksInGroup;
                if ((chunksInGroup = chunksWithGroup.remove(rowGroupId)) != null) {
                    chunksInGroup.clear();
                }
            }
        }

        // There is no more chunk produced by this row group iterator.
        rgIterator.noMoreChunks();

        // no group is selected.
        if (selectedRowGroups.isEmpty()) {
            ioStatus.finish();
            return;
        }

        // We collect all chunks in several row groups here,
        // so we can merge the IO range of different row group to improve I/O efficiency.
        boolean[] rowGroupIncluded = toRowGroupBitmap(rowGroupCount, selectedRowGroups);

        // collect all row-groups for mering IO tasks.
        mergeIO(filePath, stripeId, inputRefsForProject, blockCacheManager, rowGroupIncluded);

        final int blockIndexSize = inputRefsForProject.size();
        List<Chunk> chunkResults = new ArrayList();

        // load project columns
        for (Map.Entry<Integer, List<Chunk>> entry : chunksWithGroup.entrySet()) {
            List<Chunk> chunksInGroup = entry.getValue();

            chunkResults.clear();
            for (int blockIndex = 0; blockIndex < blockIndexSize; blockIndex++) {
                for (Chunk bufferedChunk : chunksInGroup) {

                    // The target chunk may be in lazy mode or changed to be in normal mode.
                    Chunk targetChunk = bufferedChunk;
                    if (activeLoading) {
                        Block[] blocks = bufferedChunk.getBlocks();

                        LazyBlock lazyBlock = (LazyBlock) blocks[blockIndex];
                        lazyBlock.load();

                        blocks[blockIndex] = lazyBlock.getLoaded();
                    }

                    if (blockIndex == 0) {
                        targetChunk.setPartIndex(partNum);
                        targetChunk.setPartCount(nodePartCount);
                        chunkResults.add(targetChunk);
                    }

                }
            }
            ioStatus.addResults(chunkResults);
        }

        if (activeLoading) {
            // force columnar reader to close.
            forceClose(inputRefsForFilter);
            forceClose(inputRefsForProject);

            // Clear the path to GC root.
            // when using active loading, the row group iterator will not be accessed anymore.
            rgIterator = null;
        }

        ioStatus.finish();
    }

    private void forceClose(List<Integer> inputRefs) {
        for (int i = 0; i < inputRefs.size(); i++) {
            Integer columnId = columnTransformer.getLocInOrc(chunkRefMap[inputRefs.get(i)]);
            if (columnId == null) {
                continue;
            }
            ColumnReader columnReader = rgIterator.getColumnReader(columnId);
            if (columnReader != null) {
                columnReader.close();
            }
        }
    }

}

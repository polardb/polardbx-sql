package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.RowGroupReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MergeIOScanWork extends AbstractScanWork {
    private static final boolean IN_ROW_GROUP_BATCH = false;

    private final boolean enableCancelLoading;

    public MergeIOScanWork(String workId,
                           RuntimeMetrics metrics,
                           boolean enableMetrics,
                           LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                           RowGroupIterator<Block, ColumnStatistics> rgIterator,
                           RoaringBitmap deletionBitmap, MorselColumnarSplit.ScanRange scanRange,
                           List<Integer> inputRefsForFilter, List<Integer> inputRefsForProject,
                           int partNum, int nodePartCount, boolean enableCancelLoading,
                           OSSColumnTransformer ossColumnTransformer) {
        super(workId, metrics, enableMetrics, lazyEvaluator, rgIterator, deletionBitmap, scanRange, inputRefsForFilter,
            inputRefsForProject, partNum, nodePartCount, ossColumnTransformer);
        this.enableCancelLoading = enableCancelLoading;
    }

    @Override
    protected void handleNextWork() throws Throwable {
        final Path filePath = rgIterator.filePath();
        final int stripeId = rgIterator.stripeId();

        // not all row group but those filtered by pruner should be loaded.
        final boolean[] prunedRowGroupBitmap = rgIterator.rgIncluded();
        final BlockCacheManager<Block> blockCacheManager = rgIterator.getCacheManager();

        // Get and lazily evaluate chunks until row group count exceeds the threshold.
        // NOTE: the row-group and chunk must be in order.
        final Map<Integer, List<Chunk>> chunksWithGroup = new TreeMap<>();
        final List<Integer> selectedRowGroups = new ArrayList<>();

        // for filter column, initialize or open the related modules.
        int filterColumns = inputRefsForFilter.size();

        // for all column, invoke IO task. collect all row-groups for mering IO tasks.
        mergeIO(filePath, stripeId,
            refSet.stream().sorted().collect(Collectors.toList()),
            blockCacheManager, prunedRowGroupBitmap);

        // no push-down filter, skip evaluation.
        boolean skipEvaluation = filterColumns == 0;

        while (!isCanceled && rgIterator.hasNext()) {
            rgIterator.next();
            LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup = rgIterator.current();
            final int rowGroupId = logicalRowGroup.groupId();

            // The row group id in iterator must be valid.
            Preconditions.checkArgument(prunedRowGroupBitmap[rowGroupId]);

            // A flag for each row group to indicate that at least one block selected in row group.
            boolean rgSelected = false;

            Chunk chunk;
            RowGroupReader<Chunk> rowGroupReader = logicalRowGroup.getReader();
            while ((chunk = rowGroupReader.nextBatch()) != null) {
                int[] batchRange = rowGroupReader.batchRange();
                if (skipEvaluation) {
                    int[] preSelection = selectionOf(batchRange, deletionBitmap);
                    if (preSelection != null) {
                        // rebuild chunk according to project refs.
                        chunk = rebuildProject(chunk, preSelection, preSelection.length);
                    }

                    chunk.setPartIndex(partNum);
                    chunk.setPartCount(nodePartCount);

                    // no evaluation, just buffer the unloaded chunks.
                    List<Chunk> chunksInGroup = chunksWithGroup.computeIfAbsent(rowGroupId, any -> new ArrayList<>());
                    chunksInGroup.add(chunk);
                    rgSelected = true;
                    if (!IN_ROW_GROUP_BATCH) {
                        // add result and notify the blocked threads.
                        ioStatus.addResult(chunk);
                    }
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
                BitSet bitmap = lazyEvaluator.eval(chunk, batchRange[0], batchRange[1], deletionBitmap);

                // check zeros in selection array,
                // and mark whether this row group is selected or not
                boolean hasSelectedPositions = !bitmap.isEmpty();

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

                // hold this chunk util all row groups in scan work are handled.
                int[] selection = selectionOf(bitmap);
                if (enableMetrics) {
                    evaluationTimer.inc(System.nanoTime() - start);
                }

                // rebuild chunk according to project refs.
                Chunk projectChunk = rebuildProject(chunk, selection, selection.length);

                if (!IN_ROW_GROUP_BATCH) {
                    // add result and notify the blocked threads.
                    ioStatus.addResult(projectChunk);
                }

                List<Chunk> chunksInGroup = chunksWithGroup.computeIfAbsent(rowGroupId, any -> new ArrayList<>());
                chunksInGroup.add(projectChunk);
            }

            // the chunk in this row group is run out, change to the next.
            if (rgSelected) {
                selectedRowGroups.add(rowGroupId);
                if (IN_ROW_GROUP_BATCH) {
                    List<Chunk> chunksInGroup = chunksWithGroup.get(rowGroupId);
                    if (chunksInGroup != null) {
                        for (Chunk result : chunksInGroup) {
                            // add result and notify the blocked threads.
                            ioStatus.addResult(result);
                        }
                    }
                }
            }
            // Remove all chunks of this row-group from buffer.
            List<Chunk> chunksInGroup;
            if ((chunksInGroup = chunksWithGroup.remove(rowGroupId)) != null) {
                chunksInGroup.clear();
            }
        }

        // There is no more chunk produced by this row group iterator.
        rgIterator.noMoreChunks();

        if (enableCancelLoading && selectedRowGroups.isEmpty()) {
            // all row-groups don't match the filter predicate, stop the stripe loading task immediately.
            isIOCanceled.set(true);
        }
        ioStatus.finish();
    }

}


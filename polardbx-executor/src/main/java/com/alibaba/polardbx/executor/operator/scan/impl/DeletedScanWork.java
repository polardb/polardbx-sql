package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.RowGroupReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.base.Preconditions;
import org.apache.orc.ColumnStatistics;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DeletedScanWork extends AbstractScanWork {
    private final boolean activeLoading;

    public DeletedScanWork(String workId,
                           RuntimeMetrics metrics,
                           boolean enableMetrics,
                           LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                           RowGroupIterator<Block, ColumnStatistics> rgIterator,
                           RoaringBitmap deletionBitmap,
                           MorselColumnarSplit.ScanRange scanRange,
                           List<Integer> inputRefsForFilter,
                           List<Integer> inputRefsForProject,
                           int partNum,
                           int nodePartCount,
                           boolean activeLoading,
                           OSSColumnTransformer ossColumnTransformer) {
        super(workId, metrics, enableMetrics, lazyEvaluator, rgIterator, deletionBitmap, scanRange, inputRefsForFilter,
            inputRefsForProject, partNum, nodePartCount, ossColumnTransformer);
        this.activeLoading = activeLoading;
    }

    @Override
    protected void handleNextWork() {

        // not all row group but those filtered by pruner should be loaded.
        final boolean[] prunedRowGroupBitmap = rgIterator.rgIncluded();
        final int rowGroupCount = prunedRowGroupBitmap.length;

        // Get and lazily evaluate chunks until row group count exceeds the threshold.
        // NOTE: the row-group and chunk must be in order.
        final Map<Integer, List<Chunk>> chunksWithGroup = new TreeMap<>();
        final List<Integer> selectedRowGroups = new ArrayList<>();

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
                int[] preSelection = selectionOfDeleted(batchRange, deletionBitmap);
                if (preSelection != null) {
                    // rebuild chunk according to project refs.
                    chunk = rebuildProject(chunk, preSelection, preSelection.length);
                    List<Chunk> chunksInGroup = chunksWithGroup.computeIfAbsent(rowGroupId, any -> new ArrayList<>());
                    chunksInGroup.add(chunk);
                    rgSelected = true;
                } else {
                    // all data in this chunk are preserved, which are not we want, skip to the next chunk.
                    // The created chunk and block-loader will be abandoned here.
                    releaseRef(chunk);
                }
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
        mergeIONoCache(inputRefsForProject, rowGroupIncluded);

        for (Map.Entry<Integer, List<Chunk>> entry : chunksWithGroup.entrySet()) {
            List<Chunk> chunksInGroup = entry.getValue();
            for (Chunk bufferedChunk : chunksInGroup) {

                // The target chunk may be in lazy mode or changed to be in normal mode.
                Chunk targetChunk = bufferedChunk;
                if (activeLoading) {
                    Block[] blocks = new Block[bufferedChunk.getBlockCount()];
                    // load project columns
                    for (int blockIndex = 0; blockIndex < bufferedChunk.getBlockCount(); blockIndex++) {
                        LazyBlock lazyBlock = (LazyBlock) bufferedChunk.getBlock(blockIndex);
                        lazyBlock.load();

                        blocks[blockIndex] = lazyBlock.getLoaded();
                    }

                    targetChunk = new Chunk(bufferedChunk.getPositionCount(), blocks);
                }

                targetChunk.setPartIndex(partNum);
                targetChunk.setPartCount(nodePartCount);
                // add result and notify the blocked threads.
                ioStatus.addResult(targetChunk);
            }
        }

        ioStatus.finish();

        if (activeLoading) {
            // force columnar reader to close.
            forceClose(inputRefsForFilter);
            forceClose(inputRefsForProject);
        }
    }

    private void forceClose(List<Integer> inputRefs) {
        for (int i = 0; i < inputRefs.size(); i++) {
            final int columnId = inputRefs.get(i) + 1;
            ColumnReader columnReader = rgIterator.getColumnReader(columnId);
            if (columnReader != null) {
                columnReader.close();
            }
        }
    }
}

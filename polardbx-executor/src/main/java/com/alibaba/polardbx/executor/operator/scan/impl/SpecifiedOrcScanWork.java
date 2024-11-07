package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.CommonLazyBlock;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.RowGroupReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.TimestampUtils;
import com.google.common.base.Preconditions;
import org.apache.orc.ColumnStatistics;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * This scan work scan specified orc files with specified delete bitmap.
 * It will also filter specified version of rows (filtered by tso).
 */
public class SpecifiedOrcScanWork extends AbstractScanWork {
    private final long tsoV0;
    private final long tsoV1;
    private final boolean useSelection;
    private final boolean enableCompatible;
    private final TimeZone targetTimeZone;

    public SpecifiedOrcScanWork(String workId,
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
                                OSSColumnTransformer ossColumnTransformer,
                                long tsoV0,
                                long tsoV1,
                                ExecutionContext executionContext) {
        super(workId, metrics, enableMetrics, lazyEvaluator, rgIterator, deletionBitmap, scanRange, inputRefsForFilter,
            inputRefsForProject, partNum, nodePartCount, ossColumnTransformer);
        this.tsoV0 = tsoV0;
        this.tsoV1 = tsoV1;
        this.useSelection = executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_SELECTION);
        this.enableCompatible = executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
        this.targetTimeZone = TimestampUtils.getTimeZone(executionContext);
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
        if (tsoV0 > 0 && tsoV1 > 0) {
            mergeIONoCacheWithTsoColumn(inputRefsForProject, rowGroupIncluded);
        } else {
            mergeIONoCache(inputRefsForProject, rowGroupIncluded);
        }

        for (Map.Entry<Integer, List<Chunk>> entry : chunksWithGroup.entrySet()) {
            List<Chunk> chunksInGroup = entry.getValue();
            for (Chunk bufferedChunk : chunksInGroup) {

                // The target chunk may be in lazy mode or changed to be in normal mode.
                Block[] blocks = new Block[bufferedChunk.getBlockCount()];
                // load project columns
                for (int blockIndex = 0; blockIndex < bufferedChunk.getBlockCount(); blockIndex++) {
                    LazyBlock lazyBlock = (LazyBlock) bufferedChunk.getBlock(blockIndex);
                    lazyBlock.load();

                    blocks[blockIndex] = lazyBlock.getLoaded();
                }

                Chunk targetChunk = bufferedChunk;
                if (tsoV0 > 0 && tsoV1 > 0) {
                    // Filter data by specified tso versions.
                    targetChunk = filterByTso(targetChunk, blocks);
                }
                targetChunk.setPartIndex(partNum);
                targetChunk.setPartCount(nodePartCount);
                // add result and notify the blocked threads.
                ioStatus.addResult(targetChunk);
            }
        }

        ioStatus.finish();

        // force columnar reader to close.
        forceClose(inputRefsForFilter);
        forceClose(inputRefsForProject);
        // Close tso column reader if any.
        ColumnReader columnReader = rgIterator.getColumnReader(1);
        if (columnReader != null) {
            columnReader.close();
        }
    }

    private Chunk filterByTso(Chunk chunk, Block[] blocks) {
        int[] selections = chunk.selection();
        Block tsoBlock = blocks[blocks.length - 1];
        List<Integer> newSelectionList = new ArrayList<>();
        if (null == selections) {
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                if (!tsoBlock.isNull(i) && tsoBlock.getLong(i) > tsoV0 && tsoBlock.getLong(i) <= tsoV1) {
                    newSelectionList.add(i);
                }
            }
            if (newSelectionList.size() == tsoBlock.getPositionCount()) {
                // All selected, remove tso block and return.
                Block[] newBlocks = new Block[blocks.length - 1];
                System.arraycopy(blocks, 0, newBlocks, 0, blocks.length - 1);
                return new Chunk(chunk.getPositionCount(), newBlocks);
            }
        } else {
            for (int pos : selections) {
                if (!tsoBlock.isNull(pos) && tsoBlock.getLong(pos) > tsoV0 && tsoBlock.getLong(pos) <= tsoV1) {
                    newSelectionList.add(pos);
                }
            }
        }
        // Build new selection array.
        int[] newSelections = new int[newSelectionList.size()];
        for (int i = 0; i < newSelectionList.size(); i++) {
            newSelections[i] = newSelectionList.get(i);
        }
        // Build new chunk.
        Block[] newBlocks = new Block[blocks.length - 1];
        if (newSelections.length > 0) {
            for (int blockIndex = 0; blockIndex < blocks.length - 1; blockIndex++) {
                newBlocks[blockIndex] =
                    BlockUtils.fillSelection(blocks[blockIndex], newSelections, newSelections.length,
                        useSelection, enableCompatible, targetTimeZone);
            }
        }
        return new Chunk(newSelections.length, newBlocks);
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

    /**
     * Add Tso column in chunk as the last block.
     */
    @Override
    protected Chunk rebuildProject(Chunk chunk, int[] selection, int selSize) {
        Block[] blocks = new Block[inputRefsForProject.size() + 1];
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

        if (tsoV0 > 0 && tsoV1 > 0) {
            // Last block is tso column.
            Block block = chunk.getBlock(blocks.length - 1);
            blocks[blockIndex++] = block;
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
}

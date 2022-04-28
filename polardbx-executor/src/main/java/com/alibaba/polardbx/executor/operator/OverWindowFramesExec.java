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

package com.alibaba.polardbx.executor.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.frame.OverWindowFrame;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OverWindowFramesExec extends AbstractExecutor {

    private static final Logger logger = LoggerFactory.getLogger(OverWindowFramesExec.class);

    protected final Executor input;

    // 每个window frame中有一个window function，用于计算
    private final OverWindowFrame[] overWindowFrames;
    private final List<DataType> columnMetas;
    private Chunk.ChunkRow lastPartition;
    private List<Integer> partitionIndexes;

    // 保存于该partition相关的chunk
    private ChunksIndex chunksIndex;

    private int currentChunkIndex;

    private int lastRowInPreviousChunk = 0;

    // 当前处理的chunk
    private Chunk currentChunk;

    private boolean isFinish;
    private ListenableFuture<?> blocked;
    private DataType[] targetTypes;
    private boolean needToProcessEachRow;
    private int inputCount = 0;
    private int outputCount = 0;
    private int inputRowCount = 0;
    private int outputRowCount = 0;

    public OverWindowFramesExec(Executor input, ExecutionContext context, OverWindowFrame[] overWindowFrames,
                                List<Integer> partitionIndexes) {
        this(input, context, overWindowFrames, partitionIndexes, input.getDataTypes());
    }

    public OverWindowFramesExec(Executor input, ExecutionContext context, OverWindowFrame[] overWindowFrames,
                                List<Integer> partitionIndexes, List<DataType> columnMetas) {
        super(context);
        this.partitionIndexes = partitionIndexes;
        this.input = input;
        this.overWindowFrames = overWindowFrames;
        this.blockBuilders = new BlockBuilder[Arrays.stream(overWindowFrames)
            .mapToInt(overWindowFrame -> overWindowFrame.getAggregators().size()).sum()];
        chunksIndex = new ChunksIndex();
        currentChunkIndex = 0;
        this.columnMetas = columnMetas;
        List<Aggregator> collect =
            Arrays.stream(overWindowFrames).map(OverWindowFrame::getAggregators).flatMap(List::stream)
                .collect(Collectors.toList());
        targetTypes = new DataType[collect.size()];
        for (int i = 0; i < blockBuilders.length; i++) {
            DataType dataType = columnMetas.get(input.getDataTypes().size() + i);
            targetTypes[i] = dataType;
        }
    }

    // open的时候儿就放一个chunk进去
    @Override
    void doOpen() {
        input.open();
        List<Aggregator> collect =
            Arrays.stream(overWindowFrames).map(OverWindowFrame::getAggregators).flatMap(List::stream)
                .collect(Collectors.toList());
        collect.forEach(t -> t.open(1));
        collect.forEach(Aggregator::appendInitValue);
    }

    @Override
    Chunk doNextChunk() {
        // 已处理完所有的chunk，return null
        Chunk inputChunk = input.nextChunk();
        if (inputChunk != null) {
            inputCount++;
            inputRowCount += inputChunk.getPositionCount();
            chunksIndex.addChunk(inputChunk);
        }
        processStatus();
        return processChunk();
    }

    private void processStatus() {
        blocked = ProducerExecutor.NOT_BLOCKED;
        boolean isFinish = input.produceIsFinished();
        if (isFinish) {
            if (inputCount == 0) {
                this.isFinish = true;
            } else {
                this.isFinish = inputCount == outputCount;
            }
        } else {
            blocked = input.produceIsBlocked();
            this.isFinish = false;
        }
    }

    private Chunk processChunk() {
        // 当前处理的chunk
        if (chunksIndex.getChunkCount() == currentChunkIndex) {
            return null;
        }
        needToProcessEachRow = true;
        currentChunk = chunksIndex.getChunk(currentChunkIndex);
        for (int rowIndex = 0; rowIndex < currentChunk.getPositionCount(); ++rowIndex) {
            Chunk.ChunkRow chunkRow = currentChunk.rowAt(rowIndex);
            boolean changePartition =
                lastPartition == null || notEquals(lastPartition, chunkRow) || (currentChunkIndex == 0
                    && rowIndex == 0);
            if (changePartition) {
                // 更新chunksIndex
                lastPartition = chunkRow;
                updateChunksIndex();
                int partitionEndRow = rowIndex + 1;
                boolean currentChunkIsSamePartition = true;
                while (partitionEndRow < currentChunk.getPositionCount()) {
                    Chunk.ChunkRow tempRow = currentChunk.rowAt(partitionEndRow);
                    if (notEquals(lastPartition, tempRow)) {
                        currentChunkIsSamePartition = false;
                        break;
                    }
                    partitionEndRow++;
                }
                if (!currentChunkIsSamePartition) {
                    updateFramesWithinChunk(rowIndex, partitionEndRow);
                } else {
                    int lastRowIndex = lastRowInPreviousChunk + currentChunk.getPositionCount();
                    updateFramesCrossChunk(rowIndex, lastRowIndex);
                }
            }

            // process each row
            processRow(rowIndex);
        }

        return buildResultChunk();
    }

    private void updateChunksIndex() {
        if (lastRowInPreviousChunk == 0) {
            return;
        }
        int i = 0;
        while (chunksIndex.getChunkOffset(i++) != lastRowInPreviousChunk) {
        }
        ChunksIndex temp = new ChunksIndex();
        for (int j = i - 1; j < chunksIndex.getChunkCount(); j++) {
            temp.addChunk(chunksIndex.getChunk(j));
        }
        chunksIndex = temp;
        lastRowInPreviousChunk = 0;
        currentChunkIndex = 0;
    }

    // 当前的partition在同一个chunk中
    private void updateFramesWithinChunk(int leftIndex, int rightIndex) {
        for (OverWindowFrame frame : overWindowFrames) {
            frame.resetChunks(chunksIndex);
            frame.updateIndex(leftIndex, rightIndex);
        }
        needToProcessEachRow = true;
    }

    // 当前的partition有部分数据位于后续的chunk中
    private void updateFramesCrossChunk(int startIndex, int lastRowIndex) {
        boolean currentChunkIsSamePartition = true;
        int currentChunkIndex = this.currentChunkIndex;
        while (currentChunkIsSamePartition) {
            int i = 0;
            int index = ++currentChunkIndex;
            Chunk tempChunk = null;
            if (chunksIndex.getChunkCount() > index) {
                tempChunk = chunksIndex.getChunk(index);
                for (; i < tempChunk.getPositionCount(); i++) {
                    Chunk.ChunkRow tempRow = tempChunk.rowAt(i);
                    if (notEquals(lastPartition, tempRow)) {
                        currentChunkIsSamePartition = false;
                        needToProcessEachRow = true;
                        break;
                    }
                }
            } else {
                if (input.produceIsFinished()) {// finished

                } else {
                    needToProcessEachRow = false;
                    return;
                }
                currentChunkIsSamePartition = false;
            }
            needToProcessEachRow = true;
            if (!currentChunkIsSamePartition) {
                for (OverWindowFrame frame : overWindowFrames) {
                    frame.resetChunks(chunksIndex);
                    frame.updateIndex(lastRowInPreviousChunk + startIndex, lastRowIndex + i);
                }
            }
            if (tempChunk != null) {
                lastRowIndex += tempChunk.getPositionCount();
            }
        }
    }

    private Chunk buildResultChunk() {
        if (!needToProcessEachRow) {
            blocked = input.produceIsBlocked();
            return null;
        }
        lastRowInPreviousChunk += currentChunk.getPositionCount();
        currentChunkIndex++;
        // build result chunk
        int aggregators = blockBuilders.length;
        Block[] blocks = new Block[currentChunk.getBlockCount() + aggregators];
        for (int i = 0; i < blocks.length - aggregators; i++) {
            blocks[i] = currentChunk.getBlock(i);
        }
        for (int i = blocks.length - aggregators; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i - (blocks.length - aggregators)].build();
        }
        needToProcessEachRow = false;
        outputCount++;
        outputRowCount += blocks[0].getPositionCount();
        return new Chunk(blocks);
    }

    private void processRow(int rowIndex) {
        if (!needToProcessEachRow) {
            return;
        }
        for (int i = 0, m = 0; i < overWindowFrames.length; i++) {
            overWindowFrames[i].processData(lastRowInPreviousChunk + rowIndex);
            List<Aggregator> aggregators = overWindowFrames[i].getAggregators();
            if (rowIndex == 0) {
                for (int j = 0; j < aggregators.size(); j++) {
                    //aggregator 
                    int aggPosition = m + j;
                    blockBuilders[aggPosition] = BlockBuilders
                        .create(targetTypes[aggPosition], context);
                }
            }
            for (int j = 0; j < aggregators.size(); j++) {
                aggregators.get(j).writeResultTo(0, blockBuilders[m + j]);
            }
            m += aggregators.size();
        }
    }

    private boolean notEquals(Chunk.ChunkRow previousRow, Chunk.ChunkRow currentRow) {
        for (Integer partitionIndex : partitionIndexes) {
            Object o1 = previousRow.getObject(partitionIndex);
            Object o2 = currentRow.getObject(partitionIndex);
            if ((o1 == null && o2 != null) || (o1 != null && !o1.equals(o2))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<DataType> getDataTypes() {
        return columnMetas;
    }


    @Override
    void doClose() {
        input.close();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean produceIsFinished() {
        if (isFinish) {
            if (logger.isDebugEnabled()) {
                logger.debug("Sort window input and output:" + inputRowCount + "," + outputRowCount);
            }
        }
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}


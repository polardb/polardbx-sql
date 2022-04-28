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
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;

import java.util.List;

// 当前处理行相关的行一定不在其后，可处理的情况 row: between unbounded preceding and current row
// row: between current row and current row，一定都是在row模式下
// 还有一些特殊的函数，如row_number(), rank(), dense_rank()
public class NonFrameOverWindowExec extends AbstractExecutor {
    protected final Executor input;

    // 用于表示是否为row: between current row and current row模式
    private final boolean[] resetAccumulators;

    // 当前处理的chunk
    protected Chunk inputChunk;

    // 上次处理的partition，用于判断是否到达了新的partition
    private Chunk.ChunkRow lastPartition;

    // window functions
    private List<Aggregator> aggregators;

    // partition by 的列表
    private List<Integer> partitionIndexes;

    // 构建window function的输出列
    private BlockBuilder[] blockBuilders;

    private final List<DataType> dataTypes;

    private boolean isFinish;
    private ListenableFuture<?> blocked;

    public NonFrameOverWindowExec(Executor input, ExecutionContext context, List<Aggregator> aggregators,
                                  List<Integer> partitionIndexes, boolean[] resetAccumulators,
                                  List<DataType> dataTypes) {
        super(context);
        this.input = input;
        this.partitionIndexes = partitionIndexes;
        this.aggregators = aggregators;
        this.resetAccumulators = resetAccumulators;
        this.blockBuilders = new BlockBuilder[aggregators.size()];
        this.dataTypes = dataTypes;
    }

    @Override
    void doOpen() {
        input.open();
        aggregators.forEach(t -> t.open(1));
        aggregators.forEach(Aggregator::appendInitValue);
    }

    private void processFirstLine(Chunk.ChunkRow chunkRow, int rowsCount) {
        boolean changePartition = lastPartition == null || isDifferentPartition(lastPartition, chunkRow);
        if (changePartition) {
            lastPartition = chunkRow;
        }
        for (int i = 0; i < aggregators.size(); i++) {
            if (resetAccumulators[i] || changePartition) {
                aggregators.get(i).resetToInitValue(0);
            }
            aggregators.get(i).accumulate(0, chunkRow.getChunk(), chunkRow.getPosition());
            blockBuilders[i] =
                BlockBuilders.create(dataTypes.get(i + input.getDataTypes().size()), context);
            aggregators.get(i).writeResultTo(0, blockBuilders[i]);
        }
    }

    @Override
    Chunk doNextChunk() {
        inputChunk = input.nextChunk();
        if (inputChunk == null) {
            processStatus();
            return null;
        }
        int aggFuncNumber = aggregators.size();

        processFirstLine(inputChunk.rowAt(0), inputChunk.getPositionCount());

        for (int r = 1; r < inputChunk.getPositionCount(); ++r) {
            Chunk.ChunkRow chunkRow = inputChunk.rowAt(r);
            boolean changePartition = isDifferentPartition(lastPartition, chunkRow);
            if (changePartition) {
                lastPartition = chunkRow;
            }
            for (int i = 0; i < aggFuncNumber; i++) {
                if (resetAccumulators[i] || changePartition) {
                    aggregators.get(i).resetToInitValue(0);
                }
                aggregators.get(i).accumulate(0, chunkRow.getChunk(), chunkRow.getPosition());
                aggregators.get(i).writeResultTo(0, blockBuilders[i]);
            }
        }

        return buildResultChunk();
    }

    private Chunk buildResultChunk() {
        // build result chunk
        int frameCount = aggregators.size();
        Block[] blocks = new Block[inputChunk.getBlockCount() + frameCount];
        for (int i = 0; i < blocks.length - frameCount; i++) {
            blocks[i] = inputChunk.getBlock(i);
        }
        for (int i = blocks.length - frameCount; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i - (blocks.length - frameCount)].build();
        }
        return new Chunk(blocks);
    }

    private boolean isDifferentPartition(Chunk.ChunkRow previousRow, Chunk.ChunkRow currentRow) {
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
    void doClose() {
        input.close();
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    private void processStatus() {
        isFinish = input.produceIsFinished();
        blocked = input.produceIsBlocked();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean produceIsFinished() {
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}

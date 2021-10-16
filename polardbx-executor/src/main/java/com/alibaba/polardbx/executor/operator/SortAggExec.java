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
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

import java.util.List;

/**
 * Sort Aggregation Executor
 */
public class SortAggExec extends AbstractExecutor {

    private final Executor input;

    private final List<Aggregator> aggregators;

    private final List<DataType> outputColumnMeta;

    private final int[] groups;

    private Chunk.ChunkRow currentKey;

    private Chunk currentInputChunk;

    private int currentInputChunkPosition;

    private boolean finished;
    private ListenableFuture<?> blocked;

    private boolean hasAddToResult = false;

    public SortAggExec(Executor input, int[] groups, List<Aggregator> aggregators, List<DataType> outputColumnMeta,
                       ExecutionContext context) {
        super(context);
        this.groups = groups;
        this.aggregators = aggregators;
        this.outputColumnMeta = outputColumnMeta;
        this.input = input;
        this.blocked = NOT_BLOCKED;
    }

    @Override
    void doOpen() {
        input.open();
        aggregators.forEach(t -> t.open(1));
        aggregators.forEach(Aggregator::appendInitValue);
        createBlockBuilders();
    }

    @Override
    Chunk doNextChunk() {
        while (currentPosition() < chunkLimit) {
            Chunk.ChunkRow row = nextRow();
            if (row == null) {
                finished = input.produceIsFinished();
                blocked = input.produceIsBlocked();
                if (!finished) {
                    return null;
                }
                if (!hasAddToResult) {
                    buildRow();
                    hasAddToResult = true;
                }
                break;
            }

            // first row, new a group
            if (currentKey == null) {
                aggregators.forEach(t -> t.accumulate(0, currentInputChunk, row.getPosition()));
                currentKey = row;
                // no group by or key equal in the same group
            } else if (groups.length == 0 || checkKeyEqual(currentKey, row)) {
                aggregators.forEach(t -> t.accumulate(0, currentInputChunk, row.getPosition()));
                // key not equal, new a group
            } else {
                buildRow();
                aggregators.forEach(t -> t.resetToInitValue(0));
                aggregators.forEach(t -> t.accumulate(0, currentInputChunk, row.getPosition()));
                hasAddToResult = false;
                currentKey = row;
            }
        }

        if (currentPosition() == 0) {
            return null;
        } else {
            return buildChunkAndReset();
        }
    }

    private void buildRow() {
        int col = 0;
        Chunk.ChunkRow chunkRow = currentKey;
        for (int i = 0; i < groups.length; i++, col++) {
            chunkRow.getChunk().getBlock(groups[i]).writePositionTo(chunkRow.getPosition(), blockBuilders[col]);
        }

        for (int i = 0; i < aggregators.size(); i++, col++) {
            aggregators.get(i).writeResultTo(0, blockBuilders[col]);
        }
    }

    private boolean checkKeyEqual(Chunk.ChunkRow row1, Chunk.ChunkRow row2) {
        for (int i = 0; i < groups.length; i++) {
            if (ExecUtils
                .comp(row1.getObject(groups[i]), row2.getObject(groups[i]), outputColumnMeta.get(i), true)
                != 0) {
                return false;
            }
        }
        return true;
    }

    private Chunk.ChunkRow nextRow() {
        if (currentInputChunk == null || currentInputChunkPosition >= currentInputChunk.getPositionCount()) {
            currentInputChunk = input.nextChunk();
            currentInputChunkPosition = 0;
        }

        if (currentInputChunk == null) {
            return null;
        } else {
            return currentInputChunk.rowAt(currentInputChunkPosition++);
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputColumnMeta;
    }

    @Override
    void doClose() {
        input.close();
        hasAddToResult = true;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }
}

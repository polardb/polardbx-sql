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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.DataTypeUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

/**
 * Sort Aggregation Executor
 */
public class SortAggExec extends AbstractExecutor {

    private final Executor input;

    private final List<Aggregator> aggregators;

    private AggCallsHolder aggCallsHolder;

    private final List<DataType> outputColumnMeta;

    private final int[] groups;

    private Chunk.ChunkRow currentKey;

    private Chunk currentInputChunk;

    private int currentInputChunkPosition;

    private boolean finished;
    private ListenableFuture<?> blocked;

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
        if (groups.length == 0) {
            // we need a initial one when no group by
            aggCallsHolder = new AggCallsHolder(aggregators);
        }
        input.open();
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
                if (aggCallsHolder != null) { // there is a group need to return
                    buildRow(aggCallsHolder);
                    aggCallsHolder = null;
                }
                break;
            }

            if (currentKey == null) { // first row, new a group
                aggCallsHolder = createAggCallsHolder();
                aggCallsHolder.aggregate(row);
                currentKey = row;
            } else if (groups.length == 0 || checkKeyEqual(currentKey,
                row)) { // no group by or key equal in the same group
                aggCallsHolder.aggregate(row);
            } else { // key not equal, new a group
                buildRow(aggCallsHolder);
                aggCallsHolder = createAggCallsHolder();
                aggCallsHolder.aggregate(row);
                currentKey = row;
            }
        }

        if (currentPosition() == 0) {
            return null;
        } else {
            return buildChunkAndReset();
        }
    }

    private void buildRow(AggCallsHolder aggCallsHolder) {
        int col = 0;
        Chunk.ChunkRow chunkRow = currentKey;
        for (int i = 0; i < groups.length; i++, col++) {
            chunkRow.getChunk().getBlock(groups[i]).writePositionTo(chunkRow.getPosition(), blockBuilders[col]);
        }

        for (int i = 0; i < aggregators.size(); i++, col++) {
            Object result = aggCallsHolder.getValue(i);
            blockBuilders[col].writeObject(DataTypeUtils.convert(outputColumnMeta.get(col), result));
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
        aggCallsHolder = null;
    }

    private AggCallsHolder createAggCallsHolder() {
        return new AggCallsHolder(aggregators);
    }

    private static class AggCallsHolder {

        private final List<Aggregator> aggCalls;

        AggCallsHolder(List<Aggregator> aggCalls) {
            this.aggCalls = new ArrayList<>(aggCalls.size());
            for (Aggregator aggCall : aggCalls) {
                this.aggCalls.add(aggCall.getNew());
            }
        }

        void aggregate(Row row) {
            for (Aggregator aggCall : aggCalls) {
                aggCall.aggregate(row);
            }
        }

        Object getValue(int index) {
            return aggCalls.get(index).value();
        }
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

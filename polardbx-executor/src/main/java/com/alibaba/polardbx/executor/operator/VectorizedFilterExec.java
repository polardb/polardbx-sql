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

import com.alibaba.polardbx.executor.utils.ConditionUtils;
import com.alibaba.polardbx.executor.vectorized.BuiltInFunctionVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class VectorizedFilterExec extends AbstractExecutor {
    protected final Executor input;
    protected VectorizedExpression condition;

    private MutableChunk preAllocatedChunk;

    protected Chunk inputChunk;
    protected int position;

    public VectorizedFilterExec(Executor input, VectorizedExpression condition, MutableChunk preAllocatedChunk,
                                ExecutionContext context) {
        super(context);
        this.input = input;
        this.condition = condition;
        this.preAllocatedChunk = preAllocatedChunk;
        position = 0;
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        input.open();
    }

    @Override
    Chunk doNextChunk() {
        while (currentPosition() < chunkLimit) {
            if (inputChunk == null || position == inputChunk.getPositionCount()) {
                inputChunk = nextInputChunk();
                if (inputChunk == null) {
                    break;
                } else {
                    position = 0;
                }
            }

            // Process outer rows in this input chunk
            nextRows();
        }

        if (currentPosition() == 0) {
            return null;
        } else {
            return buildChunkAndReset();
        }
    }

    private Chunk nextInputChunk() {
        Chunk chunk = input.nextChunk();
        if (chunk == null) {
            return null;
        }
        int chunkSize = chunk.getPositionCount();
        int blockCount = chunk.getBlockCount();

        for (int i = 0; i < blockCount; i++) {
            preAllocatedChunk.setSlotAt((RandomAccessBlock) chunk.getBlock(i), i);
        }
        preAllocatedChunk.reallocate(chunkSize, blockCount);

        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        return chunk;
    }

    protected void nextRows() {
        final int positionCount = inputChunk.getPositionCount();
        RandomAccessBlock filteredBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());
        for (; position < positionCount; position++) {
            // Build the filtered data chunk
            if (ConditionUtils.convertConditionToBoolean(filteredBlock.elementAt(position))) {
                for (int c = 0; c < blockBuilders.length; c++) {
                    inputChunk.getBlock(c).writePositionTo(position, blockBuilders[c]);
                }

                if (currentPosition() >= chunkLimit) {
                    position++;
                    return;
                }
            }
        }
    }

    @Override
    void doClose() {
        input.close();
    }

    @Override
    public List<DataType> getDataTypes() {
        return input.getDataTypes();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(input);
    }

    @Override
    public boolean produceIsFinished() {
        return input.produceIsFinished();
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return input.produceIsBlocked();
    }
}

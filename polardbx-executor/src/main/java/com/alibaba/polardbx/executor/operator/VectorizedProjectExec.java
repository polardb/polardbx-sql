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

import com.alibaba.polardbx.optimizer.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ReferenceBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Objects;

public class VectorizedProjectExec extends AbstractExecutor {
    private final Executor input;
    private final List<VectorizedExpression> expressions;
    private final List<DataType> dataTypes;

    /**
     * Pre-allocated chunks for each expression tree.
     * Allocate physical memory at the runtime.
     */
    private List<MutableChunk> preAllocatedChunks;

    /**
     * Replace block builders, because all the block during evaluation use random write/read.
     */
    private Block[] outputBlocks;

    /**
     * Mark the mapping block index.
     * Value = -1 means the expression should be evaluated, otherwise should be ignored.
     */
    private final int[] mappedColumnIndex;

    public VectorizedProjectExec(Executor input, List<VectorizedExpression> expressions,
                                 List<MutableChunk> preAllocatedChunks,
                                 List<DataType> dataTypes,
                                 ExecutionContext context) {
        super(context);
        this.input = input;
        this.expressions = expressions;
        this.mappedColumnIndex = new int[expressions.size()];
        this.preAllocatedChunks = preAllocatedChunks;
        this.dataTypes = dataTypes;
        Preconditions.checkArgument(expressions.size() == dataTypes.size());
    }

    @Override
    void doOpen() {
        this.outputBlocks = new Block[dataTypes.size()];

        for (int i = 0; i < expressions.size(); i++) {
            // Check the kind of expression, and optimize input ref only block
            if (expressions.get(i) instanceof InputRefVectorizedExpression) {
                mappedColumnIndex[i] = expressions.get(i).getOutputIndex();
            } else {
                // Create the block builder if the expression will be evaluated.
                mappedColumnIndex[i] = -1;
            }
        }

        input.open();
    }

    @Override
    Chunk doNextChunk() {
        Chunk inputChunk = input.nextChunk();
        if (inputChunk == null) {
            return null;
        }

        for (int i = 0; i < expressions.size(); i++) {
            if (mappedColumnIndex[i] == -1) {
                evaluateExpression(i, inputChunk);
            }
        }

        return this.buildChunk(inputChunk);
    }

    private void evaluateExpression(int index, Chunk inputChunk) {
        VectorizedExpression expression = this.expressions.get(index);
        Preconditions.checkArgument(!(expression instanceof InputRefVectorizedExpression));

        // Construct the input vector from chunk & blocks
        MutableChunk preAllocatedChunk = this.preAllocatedChunks.get(index);
        int chunkSize = inputChunk.getPositionCount();
        int blockCount = inputChunk.getBlockCount();
        for (int j = 0; j < blockCount; j++) {
            preAllocatedChunk.setSlotAt((RandomAccessBlock) inputChunk.getBlock(j), j);
        }

        // Build selection array according to input chunk.
        if (inputChunk.selection() != null) {
            preAllocatedChunk.setSelection(inputChunk.selection());
            preAllocatedChunk.setSelectionInUse(true);
        }

        // Allocate the memory of output vector at runtime.
        preAllocatedChunk.reallocate(chunkSize, blockCount);

        // Evaluation & Result Output.
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, this.context);
        expression.eval(evaluationContext);

        // Get type-specific block from output.
        int outputIndex = expression.getOutputIndex();

        Block outputBlock = (Block) Objects.requireNonNull(preAllocatedChunk.slotIn(outputIndex));
        if (outputBlock instanceof ReferenceBlock) {
            // If output block is reference block, try to get a type-specific materialized block from it.
            Block typeSpecificBlock = ((ReferenceBlock) outputBlock).toTypeSpecificBlock(evaluationContext);
            outputBlock = typeSpecificBlock;
        } else {
            // compaction by selection array
            boolean selectionInUse = preAllocatedChunk.isSelectionInUse();
            int[] selection = preAllocatedChunk.selection();
            if (selectionInUse && selection != null) {
                ((RandomAccessBlock) outputBlock).compact(selection);
            }
        }
        outputBlocks[index] = outputBlock;
    }

    public Chunk buildChunk(Chunk inputChunk) {
        for (int i = 0; i < outputBlocks.length; i++) {
            int mappedIdx = mappedColumnIndex[i];
            if (mappedIdx != -1) {
                // for input ref expression
                outputBlocks[i] = inputChunk.getBlock(mappedIdx);
            }
        }
        Chunk outputChunk = new Chunk(outputBlocks);

        // make new block array for next process
        this.outputBlocks = new Block[dataTypes.size()];
        // clear the pre-allocated chunk

        return outputChunk;
    }

    @Override
    void doClose() {
        input.close();
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypes;
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



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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.clearspring.analytics.util.Preconditions;
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

    /**
     * pair of {k-th expression - v-th block}
     */
    private Pair<Integer, Integer>[] commonSubExpressions;

    private ObjectPools objectPools;
    private boolean shouldRecycle;

    public VectorizedProjectExec(Executor input, List<VectorizedExpression> expressions,
                                 List<MutableChunk> preAllocatedChunks,
                                 List<DataType> dataTypes,
                                 ExecutionContext context) {
        super(context);
        this.input = input;
        this.expressions = expressions;
        this.mappedColumnIndex = new int[expressions.size()];
        this.preAllocatedChunks = preAllocatedChunks;
        this.commonSubExpressions = new Pair[expressions.size()];
        this.dataTypes = dataTypes;
        this.objectPools = ObjectPools.create();
        this.shouldRecycle = context.getParamManager().getBoolean(ConnectionParams.ENABLE_DRIVER_OBJECT_POOL);
        Preconditions.checkArgument(expressions.size() == dataTypes.size());
    }

    @Override
    void doOpen() {
        this.outputBlocks = new Block[dataTypes.size()];

        if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE)) {
            for (int i = 0; i < expressions.size(); i++) {
                this.commonSubExpressions[i] = null;
                VectorizedExpression e = expressions.get(i);

                if (e instanceof InputRefVectorizedExpression) {
                    continue;
                }

                DataType outputDataType = e.getOutputDataType();

                // find common sub-expression for other expression.
                for (int j = 0; j < i; j++) {
                    VectorizedExpression otherExpression = expressions.get(j);
                    if (otherExpression instanceof InputRefVectorizedExpression) {
                        continue;
                    }

                    int outputIndex = otherExpression.getOutputIndex();
                    List<Integer> otherInputIndexes = VectorizedExpressionUtils.getInputIndex(otherExpression);
                    DataType otherOutputDataType = otherExpression.getOutputDataType();

                    for (int k = 0; k < e.getChildren().length; k++) {
                        VectorizedExpression child = e.getChildren()[k];

                        if (child.getOutputIndex() == outputIndex && DataTypeUtil.equalsSemantically(outputDataType,
                            otherOutputDataType)) {
                            List<Integer> inputIndexes = VectorizedExpressionUtils.getInputIndex(child);
                            if (!otherInputIndexes.equals(inputIndexes)) {
                                break;
                            }

                            // reset expression to input ref
                            e.getChildren()[k] = new InputRefVectorizedExpression(
                                child.getOutputDataType(),
                                child.getOutputIndex(),
                                child.getOutputIndex());

                            // set result block
                            this.commonSubExpressions[i] = Pair.of(j, outputIndex);
                            break;
                        }
                    }
                }
            }
        }

        for (int i = 0; i < expressions.size(); i++) {
            VectorizedExpression e = expressions.get(i);

            // Check the kind of expression, and optimize input ref only block
            if (e instanceof InputRefVectorizedExpression) {
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

        Chunk result = this.buildChunk(inputChunk);
        result.setPartIndex(inputChunk.getPartIndex());
        result.setPartCount(inputChunk.getPartCount());
        return result;
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
        if (this.commonSubExpressions[index] == null) {
            if (shouldRecycle) {
                preAllocatedChunk.allocateWithObjectPool(chunkSize, blockCount, objectPools);
            } else {
                preAllocatedChunk.reallocate(chunkSize, blockCount);
            }

        } else {
            // for common sub expression
            Pair<Integer, Integer> subExpressionInfo = this.commonSubExpressions[index];
            int expressionIndex = subExpressionInfo.getKey();
            int commonBlockIndex = subExpressionInfo.getValue();

            if (shouldRecycle) {
                preAllocatedChunk.allocateWithObjectPool(chunkSize, commonBlockIndex + 1, objectPools);
            } else {
                preAllocatedChunk.reallocate(chunkSize, commonBlockIndex + 1);
            }

            preAllocatedChunk.setSlotAt((RandomAccessBlock) this.outputBlocks[expressionIndex], commonBlockIndex);
        }

        // Evaluation & Result Output.
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, this.context);
        expression.eval(evaluationContext);

        // Get type-specific block from output.
        int outputIndex = expression.getOutputIndex();

        Block outputBlock = (Block) Objects.requireNonNull(preAllocatedChunk.slotIn(outputIndex));
        if (outputBlock instanceof ReferenceBlock) {
            // If output block is reference block, try to get a type-specific materialized block from it.
            Block typeSpecificBlock =
                ((ReferenceBlock) outputBlock).toTypeSpecificBlock(evaluationContext, dataTypes.get(index));
            outputBlock = typeSpecificBlock;
        } else {
            // compaction by selection array
            boolean selectionInUse = preAllocatedChunk.isSelectionInUse();
            int[] selection = preAllocatedChunk.selection();
            if (selectionInUse && selection != null) {
                ((RandomAccessBlock) outputBlock).compact(selection);
            }

            // check simple for decimals
            if (outputBlock instanceof DecimalBlock) {
                ((DecimalBlock) outputBlock).collectDecimalInfo();
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
        if (objectPools != null) {
            objectPools.clear();
        }
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



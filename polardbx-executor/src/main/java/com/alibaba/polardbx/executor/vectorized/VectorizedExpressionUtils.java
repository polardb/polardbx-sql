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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class VectorizedExpressionUtils {
    /**
     * propagate the null-value state to output vector's
     */
    public static void propagateNullState(RandomAccessBlock outputBlock, RandomAccessBlock leftInputBlock,
                                          RandomAccessBlock rightInputBlock, int batchSize, int[] sel,
                                          boolean isSelectionInUse) {
        if (!leftInputBlock.hasNull() && !rightInputBlock.hasNull()) {
            return;
        }
        boolean[] nullsOfOutput = outputBlock.nulls();
        if (leftInputBlock.hasNull() && rightInputBlock.hasNull()) {
            boolean[] nullsOfLeft = leftInputBlock.nulls();
            boolean[] nullsOfRight = rightInputBlock.nulls();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    nullsOfOutput[j] = nullsOfLeft[j] || nullsOfRight[j];
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    nullsOfOutput[i] = nullsOfLeft[i] || nullsOfRight[i];
                }
            }
        } else if (leftInputBlock.hasNull() && !rightInputBlock.hasNull()) {
            boolean[] nullsOfLeft = leftInputBlock.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    nullsOfOutput[j] = nullsOfLeft[j];
                }
            } else {
                System.arraycopy(nullsOfLeft, 0, nullsOfOutput, 0, batchSize);
            }
        } else if (!leftInputBlock.hasNull() && rightInputBlock.hasNull()) {
            boolean[] nullsOfRight = rightInputBlock.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    nullsOfOutput[j] = nullsOfRight[j];
                }
            } else {
                System.arraycopy(nullsOfRight, 0, nullsOfOutput, 0, batchSize);
            }
        }
        outputBlock.setIsNull(nullsOfOutput);
    }

    public static void handleLongNullValue(LongBlock block, int batchSize, int[] sel,
                                           boolean isSelectionInUse) {
        if (!block.hasNull()) {
            return;
        }
        boolean[] nulls = block.nulls();
        long[] values = block.longArray();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                boolean isNull = nulls[j];
                values[j] = isNull ? LongBlock.NULL_VALUE : values[j];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean isNull = nulls[i];
                values[i] = isNull ? LongBlock.NULL_VALUE : values[i];
            }
        }
    }

    /**
     * filter the cursor in selection array, in which the isNull array's value is true.
     *
     * @param vectorSlot vector
     * @param selectedUsed whether selection array be used
     * @param sel selection array
     * @param size batch size or selection array length
     * @return new size of selection array, in which the null value has been filtered.
     */
    public static int filterNulls(RandomAccessBlock vectorSlot, boolean selectedUsed, int[] sel, int size) {
        int newSelSize = 0;
        if (!vectorSlot.hasNull()) {
            return size;
        }
        boolean[] isNull = vectorSlot.nulls();
        if (selectedUsed) {
            for (int i = 0; i < size; i++) {
                int j = sel[i];
                if (!isNull[j]) {
                    sel[newSelSize++] = j;
                }
            }
        } else {
            for (int i = 0; i < size; i++) {
                if (!isNull[i]) {
                    sel[newSelSize++] = i;
                }
            }
        }
        return newSelSize;
    }

    public static void mergeNulls(MutableChunk batch, int outputIndex, int... inputIndices) {
        RandomAccessBlock outputVector = batch.slotIn(outputIndex);
        boolean[] output = outputVector.nulls();

        int len = batch.batchSize();
        if (batch.isSelectionInUse()) {
            int[] selection = batch.selection();
            for (int i = 0; i < len; i++) {
                int j = selection[i];
                output[j] = false;
            }
            for (int k : inputIndices) {
                RandomAccessBlock inputVector = batch.slotIn(k);
                boolean inputHasNull = inputVector.hasNull();
                boolean[] input = inputVector.nulls();
                if (inputHasNull) {
                    for (int i = 0; i < len; i++) {
                        int j = selection[i];
                        output[j] |= input[j];
                    }
                }
            }
        } else {
            for (int i = 0; i < len; i++) {
                output[i] = false;
            }
            for (int k : inputIndices) {
                RandomAccessBlock inputVector = batch.slotIn(k);
                boolean inputHasNull = inputVector.hasNull();
                boolean[] input = inputVector.nulls();
                if (inputHasNull) {
                    for (int i = 0; i < len; i++) {
                        output[i] |= input[i];
                    }
                }
            }
        }
    }

    public static void setNulls(MutableChunk batch, int outputIndex) {
        RandomAccessBlock outputVector = batch.slotIn(outputIndex);
        outputVector.setHasNull(true);

        boolean[] output = outputVector.nulls();

        int len = batch.batchSize();
        if (batch.isSelectionInUse()) {
            int[] selection = batch.selection();
            for (int i = 0; i < len; i++) {
                int j = selection[i];
                output[j] = true;
            }
        } else {
            for (int i = 0; i < len; i++) {
                output[i] = true;
            }
        }
    }

    /**
     * Remove (subtract) members from an array and produce the results into
     * a difference array.
     */
    public static int subtract(int[] all, int allSize,
                               int[] remove, int removeSize, int[] difference) {
        Preconditions.checkState((all != remove) && (remove != difference) && (difference != all));

        int differenceCount = 0;

        // Determine which rows are left.
        int removeIndex = 0;
        for (int i = 0; i < allSize; i++) {
            int candidateIndex = all[i];
            if (removeIndex < removeSize && candidateIndex == remove[removeIndex]) {
                removeIndex++;
            } else {
                difference[differenceCount++] = candidateIndex;
            }
        }

        if (removeIndex != removeSize) {
            throw new RuntimeException("Not all batch indices removed");
        }
        return differenceCount;
    }

    public static int conditionalEval(EvaluationContext ctx, MutableChunk chunk,
                                      VectorizedExpression vectorizedExpression,
                                      int[] sel, int selSize) {
        // save batch state
        boolean preservedSelectedInUse = chunk.isSelectionInUse();
        int[] preservedSel = chunk.selection();
        int preservedBatchSize = chunk.batchSize();

        // prepare the context for conditional evaluation
        chunk.setSelectionInUse(true);
        chunk.setSelection(sel);
        chunk.setBatchSize(selSize);

        // the sel[] will be update, or a new vector will be generated.
        vectorizedExpression.eval(ctx);
        int newSelSize = chunk.batchSize();

        // restore batch state
        chunk.setSelectionInUse(preservedSelectedInUse);
        chunk.setSelection(preservedSel);
        chunk.setBatchSize(preservedBatchSize);

        return newSelSize;
    }

    /**
     * Print the tree structure of specific vectorized expression.
     */
    public static String digest(VectorizedExpression vectorizedExpression) {
        StringBuilder builder = new StringBuilder();
        new VectorizedExpressionPrinter().visit(vectorizedExpression, 0, builder);
        return builder.toString();
    }

    private static class VectorizedExpressionPrinter {
        private static String PREFIX = StringUtils.repeat(" ", 3);

        VectorizedExpressionPrinter() {
        }

        public void visit(VectorizedExpression expression, int level, StringBuilder builder) {
            builder.append(printTreeNode(expression, level));
            VectorizedExpression[] children = expression.getChildren();
            for (int i = 0; i < children.length; i++) {
                visit(children[i], level + 1, builder);
            }
        }

        private String printTreeNode(VectorizedExpression expression, int level) {
            String prefix = StringUtils.repeat(PREFIX, level);
            StringBuilder builder = new StringBuilder(prefix);

            if (level != 0) {
                builder.append("└ ");
            }

            if (expression instanceof BuiltInFunctionVectorizedExpression) {
                builder
                    .append("[BuiltIn].")
                    .append(((BuiltInFunctionVectorizedExpression) expression)
                        .getScalarFunction()
                        .getClass()
                        .getSimpleName());
            } else {
                builder.append(expression.getClass().getSimpleName());
            }
            builder.append(", { ");
            String outputDataType = expression.getOutputDataType() == null ? "[Filter]" :
                expression.getOutputDataType().getClass().getSimpleName();
            builder
                .append(outputDataType).append(", ")
                .append(expression.getOutputIndex()).append(" }\n");

            return builder.toString();
        }
    }

    public static List<Integer> getInputIndex(VectorizedExpression vectorizedExpression) {
        List<Integer> inputIndex = new ArrayList<>();
        getInputIndex(vectorizedExpression, inputIndex);
        return inputIndex;
    }
    public static void getInputIndex(VectorizedExpression vectorizedExpression, List<Integer> inputIndex) {
        VectorizedExpression[] children = vectorizedExpression.getChildren();
        if (children == null || children.length == 0) {
            inputIndex.add(vectorizedExpression.getOutputIndex());
            return;
        }
        for (int i = 0; i < children.length; i++) {
            getInputIndex(children[i], inputIndex);
        }
    }
}
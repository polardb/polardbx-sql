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

package com.alibaba.polardbx.executor.vectorized.controlflow;

import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.IfVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.IntStream;

public class IfVectorizedExpressionTest {
    private final static int SIZE = 5;

    private EvaluationContext evaluationContext;
    private ExecutionContext executionContext;

    @Before
    public void prev() {
        executionContext = new ExecutionContext();
        executionContext.setMemoryPool(
            MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "test", MemorySetting.UNLIMITED_SIZE, MemoryType.QUERY));

        DoubleBlock vectorA = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesA = vectorA.doubleArray();
        DoubleBlock vectorB = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesB = vectorB.doubleArray();
        vectorA.setHasNull(false);
        vectorB.setHasNull(false);

        int[] selections = new int[] {0, 1, 2, 3, 4};
        double[] data1 = new double[] {1, 2, 3, 4, 5};
        double[] data2 = new double[] {5, 4, 3, 2, 1};
        System.arraycopy(data1, 0, doublesA, 0, SIZE);
        System.arraycopy(data2, 0, doublesB, 0, SIZE);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(SIZE)
            .withSelection(selections)
            .addSlot(vectorA)
            .addSlot(vectorB)
            .addSlotsByTypes(ImmutableList.of(
                DataTypes.LongType,  // a > b
                DataTypes.DoubleType,    // 66
                DataTypes.DoubleType,    // 77
                DataTypes.DoubleType     // if
            ))
            .build();
        executionContext = new ExecutionContext();
        evaluationContext = new EvaluationContext(preAllocatedChunk, executionContext);
    }

    /**
     * a = {1, 2, 3, 4, 5}
     * b = {5, 4, 3, 2, 1}
     * if(a > b, 66, 77)
     * = {77, 77, 77, 66, 66}
     */
    @Test
    public void test() {
        VectorizedExpression inputRefA = new InputRefVectorizedExpression(DataTypes.DoubleType, 0, 0);
        VectorizedExpression inputRefB = new InputRefVectorizedExpression(DataTypes.DoubleType, 1, 1);
        VectorizedExpression gt = new MockGtProjector(2,
            new VectorizedExpression[] {inputRefA, inputRefB});
        VectorizedExpression literal1 = new LiteralVectorizedExpression(DataTypes.DoubleType, 66d, 3);
        VectorizedExpression literal2 = new LiteralVectorizedExpression(DataTypes.DoubleType, 77d, 4);
        VectorizedExpression ifExpr =
            new IfVectorizedExpression(5, new VectorizedExpression[] {gt, literal1, literal2});

        ifExpr.eval(evaluationContext);
        check(5, new double[] {77, 77, 77, 66, 66});
    }

    private void check(int index, double[] ans) {
        RandomAccessBlock vector = evaluationContext.getPreAllocatedChunk().slotIn(index);
        IntStream.range(0, SIZE)
            .forEach(i -> Assert.assertEquals(vector.elementAt(i), ans[i]));
    }

    /**
     * mock a GREATER_THAN vectorized expression
     * with PROJECT mode
     */
    private class MockGtProjector extends AbstractVectorizedExpression {

        public MockGtProjector(int outputIndex, VectorizedExpression[] children) {
            super(DataTypes.LongType, outputIndex, children);
        }

        @Override
        public void eval(EvaluationContext ctx) {
            super.evalChildren(ctx);
            MutableChunk chunk = ctx.getPreAllocatedChunk();
            int batchSize = chunk.batchSize();
            boolean isSelectionInUse = chunk.isSelectionInUse();
            int[] sel = chunk.selection();

            RandomAccessBlock leftInputBlock =
                chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
            RandomAccessBlock rightInputBlock =
                chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());
            RandomAccessBlock outputBlock = chunk.slotIn(outputIndex, outputDataType);

            VectorizedExpressionUtils
                .propagateNullState(outputBlock, leftInputBlock, rightInputBlock, batchSize, sel,
                    isSelectionInUse);

            /*
             * these code should be generated ->>
             */
            double[] array1 = ((DoubleBlock) leftInputBlock).doubleArray();
            double[] array2 = ((DoubleBlock) rightInputBlock).doubleArray();

            long[] array = ((LongBlock) outputBlock).longArray();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    array[j] = array1[j] > array2[j] ? 1 : 0;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    array[i] = array1[i] > array2[i] ? 1 : 0;
                }
            }

            VectorizedExpressionUtils
                .handleLongNullValue((LongBlock) outputBlock, batchSize, sel, isSelectionInUse);
        }
    }
}
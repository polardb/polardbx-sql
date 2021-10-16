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

import com.alibaba.polardbx.optimizer.chunk.BlockUtils;
import com.alibaba.polardbx.optimizer.chunk.DoubleBlock;
import com.alibaba.polardbx.optimizer.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.BuiltInFunctionVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.CaseVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.IntStream;

public class CaseVectorizedExpressionTest {
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
        DoubleBlock vectorC = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesC = vectorC.doubleArray();
        DoubleBlock vectorD = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesD = vectorD.doubleArray();
        DoubleBlock vectorE = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesE = vectorE.doubleArray();
        DoubleBlock vectorF = (DoubleBlock) BlockUtils.createBlock(DataTypes.DoubleType, SIZE);
        double[] doublesF = vectorF.doubleArray();
        vectorA.setHasNull(false);
        vectorB.setHasNull(false);
        vectorC.setHasNull(false);
        vectorD.setHasNull(false);
        vectorE.setHasNull(false);
        vectorF.setHasNull(false);

        int[] selections = new int[] {0, 1, 2, 3, 4};
        double[] data1 = new double[] {1, 2, 3, 4, 5};
        double[] data2 = new double[] {5, 4, 3, 2, 1};
        System.arraycopy(data1, 0, doublesA, 0, SIZE);
        System.arraycopy(data2, 0, doublesB, 0, SIZE);
        System.arraycopy(data1, 0, doublesC, 0, SIZE);
        System.arraycopy(data2, 0, doublesD, 0, SIZE);
        System.arraycopy(data1, 0, doublesE, 0, SIZE);
        System.arraycopy(data2, 0, doublesF, 0, SIZE);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(SIZE)
            .withSelection(selections)
            .addSlot(vectorA)
            .addSlot(vectorB)
            .addSlot(vectorC)
            .addSlot(vectorD)
            .addSlot(vectorE)
            .addSlot(vectorF)
            .addSlotsByTypes(ImmutableList.of(
                DataTypes.DoubleType,    // a + b
                DataTypes.DoubleType,    // c - d
                DataTypes.DoubleType,    // e + f
                DataTypes.DoubleType     // case when
            ))
            .build();
        executionContext = new ExecutionContext();
        evaluationContext = new EvaluationContext(preAllocatedChunk, executionContext);
    }

    /**
     * a = {1, 2, 3, 4, 5}
     * b = {5, 4, 3, 2, 1}
     * c = {1, 2, 3, 4, 5}
     * d = {5, 4, 3, 2, 1}
     * e = {1, 2, 3, 4, 5}
     * f = {5, 4, 3, 2, 1}
     * <p>
     * case when a > b then a + b
     * when c < d then c - d
     * else e + f
     * end
     * = {-4, -2, 6, 6, 6}
     */
    @Test
    public void test() {
        VectorizedExpression inputRefA = new InputRefVectorizedExpression(DataTypes.DoubleType, 0, 0);
        VectorizedExpression inputRefB = new InputRefVectorizedExpression(DataTypes.DoubleType, 1, 1);
        VectorizedExpression inputRefC = new InputRefVectorizedExpression(DataTypes.DoubleType, 2, 2);
        VectorizedExpression inputRefD = new InputRefVectorizedExpression(DataTypes.DoubleType, 3, 3);
        VectorizedExpression inputRefE = new InputRefVectorizedExpression(DataTypes.DoubleType, 4, 4);
        VectorizedExpression inputRefF = new InputRefVectorizedExpression(DataTypes.DoubleType, 5, 5);

        VectorizedExpression aGtB = new MockGt(new VectorizedExpression[] {inputRefA, inputRefB});

        VectorizedExpression cLeD = new MockLe(new VectorizedExpression[] {inputRefC, inputRefD});

        VectorizedExpression aPlusB = BuiltInFunctionVectorizedExpression.from(
            new VectorizedExpression[] {inputRefA, inputRefB},
            6,
            builtInFunc("ADD", DataTypes.DoubleType),
            executionContext);

        VectorizedExpression cSubD = BuiltInFunctionVectorizedExpression.from(
            new VectorizedExpression[] {inputRefC, inputRefD},
            7,
            builtInFunc("SUB", DataTypes.DoubleType),
            executionContext);

        VectorizedExpression ePlusF = BuiltInFunctionVectorizedExpression.from(
            new VectorizedExpression[] {inputRefE, inputRefF},
            8,
            builtInFunc("ADD", DataTypes.DoubleType),
            executionContext);

        VectorizedExpression caseWhen = new CaseVectorizedExpression(
            DataTypes.DoubleType,
            9,
            new VectorizedExpression[] {
                aGtB,
                aPlusB,
                cLeD,
                cSubD,
                ePlusF
            }
        );

        caseWhen.eval(evaluationContext);

        check(9, new double[] {-4, -2, 6, 6, 6});
    }

    private void check(int index, double[] ans) {
        RandomAccessBlock vector = evaluationContext.getPreAllocatedChunk().slotIn(index);
        IntStream.range(0, SIZE)
            .forEach(i -> Assert.assertEquals(vector.elementAt(i), ans[i]));
    }

    private AbstractScalarFunction builtInFunc(String functionName, DataType dataType) {
        // deal with extra parameters like type in cast function.
        AbstractScalarFunction scalarFunction = ExtraFunctionManager.getExtraFunction(functionName, null, dataType);
        return scalarFunction;
    }

    /**
     * mock a GREATER_THAN vectorized expression
     * with FILTER mode
     */
    private class MockGt extends AbstractVectorizedExpression {

        public MockGt(VectorizedExpression[] children) {
            super(null, -1, children);
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

            /*
             * these code should be generated ->>
             */
            double[] array1 = ((DoubleBlock) leftInputBlock).doubleArray();
            double[] array2 = ((DoubleBlock) rightInputBlock).doubleArray();

            int newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if (array1[j] > array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if (array1[i] > array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }

            if (newSize < batchSize) {
                chunk.setBatchSize(newSize);
                chunk.setSelectionInUse(true);
            }
        }
    }

    /**
     * mock a LESS_THAN vectorized expression
     * with FILTER mode
     */
    private class MockLe extends AbstractVectorizedExpression {

        public MockLe(VectorizedExpression[] children) {
            super(null, -1, children);
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

            /*
             * these code should be generated ->>
             */
            double[] array1 = ((DoubleBlock) leftInputBlock).doubleArray();
            double[] array2 = ((DoubleBlock) rightInputBlock).doubleArray();

            int newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if (array1[j] < array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if (array1[i] < array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }

            if (newSize < batchSize) {
                chunk.setBatchSize(newSize);
                chunk.setSelectionInUse(true);
            }
        }
    }
}
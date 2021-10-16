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
import com.alibaba.polardbx.executor.vectorized.CoalesceVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
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

public class CoalesceVectorizedExpressionTest {
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

        vectorA.setIsNull(new boolean[] {false, true, true, true, true});
        vectorA.setHasNull(true);
        vectorB.setIsNull(new boolean[] {true, false, true, true, true});
        vectorB.setHasNull(true);
        vectorC.setIsNull(new boolean[] {true, true, false, true, true});
        vectorC.setHasNull(true);

        int[] selections = new int[] {0, 1, 2, 3, 4};
        System.arraycopy(new double[] {1, 2, 3, 4, 5}, 0, doublesA, 0, SIZE);
        System.arraycopy(new double[] {5, 4, 3, 2, 1}, 0, doublesB, 0, SIZE);
        System.arraycopy(new double[] {1, 1, 1, 1, 1}, 0, doublesC, 0, SIZE);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(SIZE)
            .withSelection(selections)
            .addSlot(vectorA)
            .addSlot(vectorB)
            .addSlot(vectorC)
            .addSlotsByTypes(ImmutableList.of(
                DataTypes.DoubleType    // coalesce
            ))
            .build();
        executionContext = new ExecutionContext();
        evaluationContext = new EvaluationContext(preAllocatedChunk, executionContext);
    }

    /**
     * a = {1, 2, 3, 4, 5}
     * a_nulls = {false, true, true, true, true}
     * b = {5, 4, 3, 2, 1}
     * b_nulls = {true, false, true, true, true}
     * c = {1, 1, 1, 1, 1}
     * c_nulls = {true, true, false, true, true}
     * <p>
     * coalesce(a, b, c) = {1, 4, 1, X, X}
     * nulls = {false, false, false, true, true}
     */
    @Test
    public void test() {
        VectorizedExpression inputRefA = new InputRefVectorizedExpression(DataTypes.DoubleType, 0, 0);
        VectorizedExpression inputRefB = new InputRefVectorizedExpression(DataTypes.DoubleType, 1, 1);
        VectorizedExpression inputRefC = new InputRefVectorizedExpression(DataTypes.DoubleType, 2, 2);

        VectorizedExpression coalesce = new CoalesceVectorizedExpression(
            DataTypes.DoubleType,
            3,
            new VectorizedExpression[] {
                inputRefA,
                inputRefB,
                inputRefC
            });

        coalesce.eval(evaluationContext);
        check(3, new double[] {1, 4, 1, 0, 0}, new boolean[] {false, false, false, true, true});
    }

    private void check(int index, double[] ans1, boolean[] ans2) {
        RandomAccessBlock vector = evaluationContext.getPreAllocatedChunk().slotIn(index);
        double[] array = ((DoubleBlock) vector).doubleArray();
        boolean[] valueIsNull = vector.nulls();
        IntStream.range(0, SIZE).forEach((i) -> {
                Assert.assertTrue(ans1[i] == array[i]);
                Assert.assertTrue(ans2[i] == valueIsNull[i]);
            }
        );
    }
}
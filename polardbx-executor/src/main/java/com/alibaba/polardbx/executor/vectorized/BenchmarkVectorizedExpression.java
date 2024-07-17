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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class BenchmarkVectorizedExpression extends AbstractVectorizedExpression {
    /**
     * The BENCHMARK() function executes the expression expr repeatedly count times
     */
    private long count;

    public BenchmarkVectorizedExpression(DataType<?> outputDataType,
                                         int outputIndex,
                                         VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);
        Object left = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        count = (long) left;
    }

    @Override
    public void eval(EvaluationContext ctx) {
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        // benchmark
        //   /     \
        // count  function
        VectorizedExpression child = children[1];
        for (int i = 0; i < count; i++) {
            // try count times
            child.eval(ctx);
        }

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                res[j] = 0L;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                res[i] = 0L;
            }
        }
    }
}

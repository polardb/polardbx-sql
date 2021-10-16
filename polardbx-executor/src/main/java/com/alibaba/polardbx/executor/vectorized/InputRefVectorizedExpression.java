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

import com.alibaba.polardbx.optimizer.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class InputRefVectorizedExpression extends AbstractVectorizedExpression {
    private final int inputIndex;

    public InputRefVectorizedExpression(DataType<?> dataType, int inputIndex, int outputIndex) {
        super(dataType, outputIndex, new VectorizedExpression[0]);
        this.inputIndex = inputIndex;
    }

    @Override
    public void eval(EvaluationContext ctx) {
        if (outputIndex != inputIndex) {
            MutableChunk chunk = ctx.getPreAllocatedChunk();
            boolean isSelectionInUse = chunk.isSelectionInUse();
            int[] selection = chunk.selection();
            chunk.slotIn(outputIndex).copySelected(isSelectionInUse, selection, chunk.batchSize(),
                chunk.slotIn(inputIndex));
        }
    }
}
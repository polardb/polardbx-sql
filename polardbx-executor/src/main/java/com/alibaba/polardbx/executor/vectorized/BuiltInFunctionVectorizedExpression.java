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

import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction;

public class BuiltInFunctionVectorizedExpression extends AbstractVectorizedExpression {
    private final IScalarFunction scalarFunction;
    private ExecutionContext executionContext;
    private final int[] variablesPositions;
    private final int variablesPositionCounts;
    private final Object[] args;

    public BuiltInFunctionVectorizedExpression(DataType<?> outputDataType, int outputIndex,
                                               VectorizedExpression[] children,
                                               IScalarFunction scalarFunction, ExecutionContext executionContext) {
        super(outputDataType, outputIndex, children);
        this.scalarFunction = scalarFunction;
        this.executionContext = executionContext;

        this.args = new Object[children.length];

        // fill the positions of variable values.
        this.variablesPositions = new int[children.length];
        int j = 0;
        for (int i = 0; i < children.length; i++) {
            // fill the args with constant values.
            if (children[i] instanceof LiteralVectorizedExpression) {
                args[i] = ((LiteralVectorizedExpression) children[i]).getValue();
            } else {
                variablesPositions[j++] = i;
            }
        }
        this.variablesPositionCounts = j;
    }

    public static BuiltInFunctionVectorizedExpression from(VectorizedExpression[] args, int outputIndex,
                                                           IScalarFunction function, ExecutionContext context) {
        DataType<?> outputType = function.getReturnType();

        return new BuiltInFunctionVectorizedExpression(outputType, outputIndex, args,
            function, context);
    }

    public IScalarFunction getScalarFunction() {
        return scalarFunction;
    }

    @Override
    public void eval(EvaluationContext ctx) {
        for (int i = 0; i < variablesPositionCounts; i++) {
            children[variablesPositions[i]].eval(ctx);
        }

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int len = chunk.batchSize();
        int[] sel = chunk.selection();

        RandomAccessBlock outputSlot = chunk.slotIn(outputIndex, outputDataType);
        if (chunk.isSelectionInUse()) {
            for (int i = 0; i < len; i++) {
                for (int j = 0; j < variablesPositionCounts; j++) {
                    int k = variablesPositions[j];
                    args[k] = chunk.slotIn(children[k].getOutputIndex()).elementAt(sel[i]);
                }
                Object ret = scalarFunction.compute(args, executionContext);
                Object convertedRet = scalarFunction.getReturnType().convertFrom(ret);
                outputSlot.setElementAt(sel[i], convertedRet);
            }
        } else {
            for (int i = 0; i < len; i++) {
                for (int j = 0; j < variablesPositionCounts; j++) {
                    int k = variablesPositions[j];
                    args[k] = chunk.slotIn(children[k].getOutputIndex()).elementAt(i);
                }
                Object ret = scalarFunction.compute(args, executionContext);
                Object convertedRet = scalarFunction.getReturnType().convertFrom(ret);
                outputSlot.setElementAt(i, convertedRet);
            }
        }
    }

    public void setInFilter() {
        this.executionContext = executionContext.copy();
        this.executionContext.setIsInFilter(true);
    }
}
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

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.function.calc.Cartesian;
import com.alibaba.polardbx.executor.operator.util.DataTypeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.ScalarFunctionExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class DynamicValueExec extends AbstractExecutor {

    private int calExpressionIndex;

    private int expressionCount;

    private List<DataType> outputColumnMeta;
    private List<List<IExpression>> expressionLists;
    private boolean isCartesian;
    private boolean isFinished;

    public DynamicValueExec(
        List<List<IExpression>> expressionLists, List<DataType> outputColumnMeta, ExecutionContext context) {
        super(context);
        this.outputColumnMeta = outputColumnMeta;
        this.expressionCount = expressionLists.size();
        this.chunkLimit = Math.min(chunkLimit, expressionLists.size());
        this.expressionLists = expressionLists;
        this.isCartesian = isCartesian();
    }

    private boolean isCartesian() {
        IExpression iExpression = expressionLists.get(0).get(0);
        if (iExpression instanceof ScalarFunctionExpression) {
            return ((ScalarFunctionExpression) iExpression).isA(Cartesian.class);
        }
        return false;
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        calExpressionIndex = 0;
    }

    @Override
    Chunk doNextChunk() {
        if (isCartesian) {
            if (isFinished) {
                return null;
            }
            // only output the origin chunk from the java type
            final IExpression expression = expressionLists.get(0).get(0);
            Chunk result = (Chunk) expression.eval(null);
            this.isFinished = true;
            return result;
        } else if (calExpressionIndex < expressionCount) {
            while (calExpressionIndex < expressionCount) {
                calExpressionIndex++;
                for (int i = 0; i < outputColumnMeta.size(); i++) {
                    evaluateExpression(calExpressionIndex - 1, i);
                }
                if (blockBuilders[0].getPositionCount() >= chunkLimit) {
                    return buildChunkAndReset();
                }
            }
            return buildChunkAndReset();
        } else {
            return null;
        }
    }

    protected void evaluateExpression(int nRow, int c) {
        final BlockBuilder blockBuilder = blockBuilders[c];
        final IExpression expression = expressionLists.get(nRow).get(c);

        Object result = expression.eval(null);
        if (result instanceof List) {
            for (Object obj : (List<Object>) result) {
                blockBuilder.writeObject(DataTypeUtils.convert(outputColumnMeta.get(c), obj));
            }
        } else {
            blockBuilder.writeObject(DataTypeUtils.convert(outputColumnMeta.get(c), result));
        }
    }

    @Override
    void doClose() {
        this.expressionLists.clear();
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputColumnMeta;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    public boolean produceIsFinished() {
        return calExpressionIndex >= expressionCount || isFinished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}

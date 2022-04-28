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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.DataTypeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;

import java.util.List;

public class DynamicValueExec extends AbstractExecutor {

    private int currCount;

    private int count;

    private List<DataType> outputColumnMeta;
    private List<List<IExpression>> expressionLists;

    public DynamicValueExec(
        List<List<IExpression>> expressionLists, List<DataType> outputColumnMeta, ExecutionContext context) {
        super(context);
        this.outputColumnMeta = outputColumnMeta;
        this.count = expressionLists.size();
        this.chunkLimit = Math.min(chunkLimit, expressionLists.size());
        this.expressionLists = expressionLists;
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        currCount = 0;
    }

    @Override
    Chunk doNextChunk() {
        if (currCount < count) {
            while (currentPosition() < chunkLimit && currCount < count) {
                currCount++;
                for (int i = 0; i < outputColumnMeta.size(); i++) {
                    evaluateExpression(currCount - 1, i);
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
        blockBuilder.writeObject(DataTypeUtils.convert(outputColumnMeta.get(c), result));
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
        return currCount >= count;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}

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

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.DataTypeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class ExpandExec extends AbstractExecutor {

    protected final Executor input;
    private final List<List<IExpression>> expressions;
    protected final List<DataType> columns;
    private int processIdx = -1;
    private Chunk inputChunk;

    public ExpandExec(Executor input, List<List<IExpression>> expressions, List<DataType> columns,
                      ExecutionContext context) {
        super(context);
        this.input = input;
        this.expressions = expressions;
        this.columns = columns;
    }

    @Override
    Chunk doNextChunk() {
        if (processIdx <= 0 || processIdx == expressions.size()) {
            inputChunk = input.nextChunk();
            if (inputChunk == null) {
                return null;
            }
            processIdx = 0;
        }
        List<IExpression> projects = expressions.get(processIdx);
        processIdx++;
        for (int r = 0; r < inputChunk.getPositionCount(); ++r) {
            Chunk.ChunkRow chunkRow = inputChunk.rowAt(r);
            for (int c = 0; c < columns.size(); c++) {
                final BlockBuilder blockBuilder = blockBuilders[c];
                final IExpression expression = projects.get(c);
                Object result = expression.eval(chunkRow);
                blockBuilder.writeObject(DataTypeUtils.convert(columns.get(c), result));
            }
        }

        return this.buildChunk();
    }

    public Chunk buildChunk() {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    @Override
    public List<DataType> getDataTypes() {
        return columns;
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        input.open();
    }

    @Override
    void doClose() {
        input.close();
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

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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.DataTypeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;

import java.util.List;

/**
 * Project Executor
 *
 */
public class ProjectExec extends AbstractExecutor {

    protected final Executor input;
    private final List<IExpression> expressions;
    protected final List<DataType> columns;

    protected int[] mappedColumnIndex;

    public ProjectExec(Executor input, List<IExpression> expressions, List<DataType> columns,
                       ExecutionContext context) {
        super(context);
        this.input = input;
        this.expressions = expressions;
        this.columns = columns;
        Preconditions.checkArgument(expressions.size() == columns.size());

        blockBuilders = new BlockBuilder[columns.size()];
        mappedColumnIndex = new int[expressions.size()];

    }

    @Override
    void doOpen() {
        for (int i = 0; i < expressions.size(); i++) {
            if (expressions.get(i) instanceof InputRefExpression) {
                InputRefExpression ref = (InputRefExpression) expressions.get(i);
                mappedColumnIndex[i] = ref.getInputRefIndex();
                blockBuilders[i] = null;
            } else {
                mappedColumnIndex[i] = -1;
                blockBuilders[i] = BlockBuilders.create(columns.get(i), context);
            }
        }
        createBlockBuilders();
        input.open();
    }

    @Override
    Chunk doNextChunk() {
        Chunk inputChunk = input.nextChunk();
        if (inputChunk == null) {
            return null;
        }

        for (int r = 0; r < inputChunk.getPositionCount(); ++r) {
            Chunk.ChunkRow chunkRow = inputChunk.rowAt(r);
            for (int c = 0; c < columns.size(); c++) {
                if (mappedColumnIndex[c] < 0) {
                    // Need to evaluate the expression
                    evaluateExpression(chunkRow, c);
                }
            }
        }

        return this.buildChunk(inputChunk);
    }

    public Chunk buildChunk(Chunk inputChunk) {
        Block[] blocks = new Block[blockBuilders.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            if (mappedColumnIndex[i] >= 0) {
                // The quick path - only column to column map
                blocks[i] = inputChunk.getBlock(mappedColumnIndex[i]);
            } else {
                blocks[i] = blockBuilders[i].build();
            }
        }
        for (int i = 0; i < blockBuilders.length; i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilder();
        }
        return new Chunk(blocks);
    }

    protected void evaluateExpression(Chunk.ChunkRow chunkRow, int c) {
        final BlockBuilder blockBuilder = blockBuilders[c];
        final IExpression expression = expressions.get(c);

        Object result = expression.eval(chunkRow);
        blockBuilder.writeObject(DataTypeUtils.convert(columns.get(c), result));
    }

    @Override
    void doClose() {
        input.close();
    }

    @Override
    public List<DataType> getDataTypes() {
        return columns;
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

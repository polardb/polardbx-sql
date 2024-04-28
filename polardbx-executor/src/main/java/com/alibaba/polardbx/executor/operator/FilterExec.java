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
import com.alibaba.polardbx.executor.chunk.BooleanBlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterExpression;
import com.alibaba.polardbx.executor.operator.util.bloomfilter.BloomFilterExpressionFilter;
import com.alibaba.polardbx.executor.utils.ConditionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.ScalarFunctionExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;

import java.util.List;

/**
 * Filter Executor
 *
 */
public class FilterExec extends AbstractExecutor {

    protected final Executor input;
    protected IExpression condition;

    protected BlockBuilder conditionBlockBuilder;

    // Internal States
    protected Chunk inputChunk;
    protected Block conditionBlock;
    protected int position;
    private BloomFilterExpressionFilter bloomFilterExpressionFilter;
    private BooleanArrayList bloomFilterResult;

    public FilterExec(Executor input,
                      IExpression condition,
                      BloomFilterExpression bloomFilterExpression,
                      ExecutionContext context) {
        super(context);
        this.input = input;
        if (bloomFilterExpression != null) {
            this.bloomFilterExpressionFilter = new BloomFilterExpressionFilter(bloomFilterExpression);
        } else {
            this.bloomFilterExpressionFilter = null;
        }
        this.condition = condition;
        bloomFilterResult = null;
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        conditionBlockBuilder = new BooleanBlockBuilder(chunkLimit);
        input.open();
    }

    @Override
    Chunk doNextChunk() {
        if (condition == null && bloomFilterExpressionFilter != null && !bloomFilterExpressionFilter
            .isExistBloomFilter()) {
            return input.nextChunk();
        }
        while (currentPosition() < chunkLimit) {
            if (inputChunk == null || position == inputChunk.getPositionCount()) {
                inputChunk = nextInputChunk();
                if (inputChunk == null) {
                    break;
                } else {
                    position = 0;
                }
            }

            // Process outer rows in this input chunk
            nextRows();
        }

        if (currentPosition() == 0) {
            return null;
        } else {
            return buildChunkAndReset();
        }
    }

    private Chunk nextInputChunk() {
        Chunk chunk = input.nextChunk();
        if (chunk == null) {
            conditionBlock = null;
            bloomFilterResult = null;
            return null;
        }

        if (bloomFilterExpressionFilter != null) {
            ensureBloomFilterResultSize(chunk.getPositionCount());
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                bloomFilterResult.set(i, !bloomFilterExpressionFilter.filter(chunk.rowAt(i)));
            }
        }

        for (int i = 0; i < chunk.getPositionCount(); i++) {
            Chunk.ChunkRow chunkRow = chunk.rowAt(i);
            boolean resultBoolean = true;
            if (bloomFilterExpressionFilter != null) {
                resultBoolean = bloomFilterResult.getBoolean(i);
                if (!resultBoolean) {
                    conditionBlockBuilder.writeBoolean(false);
                    continue;
                }
            }
            if (condition != null) {
                Object result = condition.eval(chunkRow);
                resultBoolean = ConditionUtils.convertConditionToBoolean(result);
            }
            conditionBlockBuilder.writeBoolean(resultBoolean);
        }

        conditionBlock = conditionBlockBuilder.build();
        conditionBlockBuilder = conditionBlockBuilder.newBlockBuilder();
        return chunk;
    }

    protected void nextRows() {
        final int positionCount = inputChunk.getPositionCount();
        for (; position < positionCount; position++) {

            // Build the filtered data chunk
            if (conditionBlock.getBoolean(position)) {
                for (int c = 0; c < blockBuilders.length; c++) {
                    inputChunk.getBlock(c).writePositionTo(position, blockBuilders[c]);
                }

                if (currentPosition() >= chunkLimit) {
                    position++;
                    return;
                }
            }
        }
    }

    @Override
    void doClose() {
        this.bloomFilterExpressionFilter = null;
        this.bloomFilterResult = null;
        input.close();
    }

    @Override
    public List<DataType> getDataTypes() {
        return input.getDataTypes();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(input);
    }

    private void ensureBloomFilterResultSize(int size) {
        if (bloomFilterResult == null) {
            bloomFilterResult = new BooleanArrayList(size);
        }
        bloomFilterResult.size(size);
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

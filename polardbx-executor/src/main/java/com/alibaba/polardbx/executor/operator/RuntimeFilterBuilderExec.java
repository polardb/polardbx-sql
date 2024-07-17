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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.BloomFilterProduce;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class RuntimeFilterBuilderExec extends AbstractExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterBuilderExec.class);

    protected final Executor input;
    protected BloomFilterProduce filterClient;

    // Internal States
    protected Chunk inputChunk;
    protected int position;
    /**
     * idx for parallel execution
     */
    private final int idx;

    public RuntimeFilterBuilderExec(Executor input, BloomFilterProduce filterClient,
                                    ExecutionContext context, int idx) {
        super(context);
        this.input = input;
        this.filterClient = filterClient;
        this.idx = idx;
        createBlockBuilders();
    }

    @Override
    void doOpen() {
        input.open();
    }

    @Override
    Chunk doNextChunk() {
        Chunk ret = input.nextChunk();
        if (ret != null) {
            filterClient.addChunk(ret, idx);
        }
        return ret;
    }

    @Override
    void doClose() {
        try {
            filterClient.close();
        } catch (Throwable t) {
            logger.warn("ignore the error!", t);
        }
        this.filterClient = null;
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

    @Override
    public boolean produceIsFinished() {
        return input.produceIsFinished();
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return input.produceIsBlocked();
    }
}

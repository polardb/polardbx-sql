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

package com.alibaba.polardbx.executor.mpp.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.ProducerExecutor;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

public class WorkProcessorExec implements Executor {

    private WorkProcessor<Chunk> chunks;
    private boolean closed = false;
    private boolean isFinished;

    public WorkProcessorExec(WorkProcessor<Chunk> chunks) {
        this.chunks = chunks;
    }

    @Override
    public void open() {

    }

    @Override
    public Chunk nextChunk() {
        if (closed || !chunks.process() || chunks.isFinished()) {
            this.isFinished = true;
            return null;
        }
        Chunk page = chunks.getResult();
        return page;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public List<DataType> getDataTypes() {
        return null;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    public boolean produceIsFinished() {
        return isFinished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}

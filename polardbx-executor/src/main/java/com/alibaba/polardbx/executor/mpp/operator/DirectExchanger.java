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
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;

public class DirectExchanger extends LocalExchanger {

    public DirectExchanger(OutputBufferMemoryManager bufferMemoryManager, ConsumerExecutor executor,
                           LocalExchangersStatus status) {
        super(bufferMemoryManager, ImmutableList.of(executor), status, true);
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        executors.get(0).consumeChunk(chunk);
    }

    @Override
    public void openConsume() {
        if (opened.compareAndSet(false, true)) {
            executors.get(0).openConsume();
        }
    }

    @Override
    public void closeConsume(boolean force) {
        if (opened.get() || force) {
            executors.get(0).closeConsume(force);
        }
    }

    @Override
    public void buildConsume() {
        this.executors.get(0).buildConsume();
    }
}

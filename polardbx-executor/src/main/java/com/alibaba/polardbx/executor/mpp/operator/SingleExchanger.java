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

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SingleExchanger extends LocalExchanger {
    private final AtomicBoolean consuming;

    public SingleExchanger(OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
                           LocalExchangersStatus status, boolean asyncConsume) {
        super(bufferMemoryManager, executors, status, asyncConsume);
        this.consuming = status.getConsumings().get(0);
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        if (asyncConsume) {
            executors.get(0).consumeChunk(chunk);
        } else {
            while (true) {
                if (consuming.compareAndSet(false, true)) {
                    try {
                        executors.get(0).consumeChunk(chunk);
                    } finally {
                        consuming.set(false);
                    }
                    return;
                }
            }
        }
    }
}

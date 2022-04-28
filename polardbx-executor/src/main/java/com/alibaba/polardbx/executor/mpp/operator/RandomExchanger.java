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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class RandomExchanger extends LocalExchanger {
    private final List<AtomicBoolean> consumings;
    private final Random random;

    public RandomExchanger(OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
                           LocalExchangersStatus status, boolean asyncConsume) {
        super(bufferMemoryManager, executors, status, asyncConsume);
        this.consumings = status.getConsumings();
        this.random = new Random(executors.size());
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        int randomIndex = executors.size() > 1 ? random.nextInt(executors.size()) : 0;
        if (asyncConsume) {
            executors.get(randomIndex).consumeChunk(chunk);
        } else {
            while (true) {
                AtomicBoolean consuming = consumings.get(randomIndex);
                if (consuming.compareAndSet(false, true)) {
                    try {
                        executors.get(randomIndex).consumeChunk(chunk);
                    } finally {
                        consuming.set(false);
                    }
                    return;
                }
                randomIndex++;
                if (randomIndex == executors.size()) {
                    randomIndex = 0;
                }
            }
        }
    }
}

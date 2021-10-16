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

public class BroadcastExchanger extends LocalExchanger {

    public BroadcastExchanger(OutputBufferMemoryManager bufferMemoryManager, List<ConsumerExecutor> executors,
                              LocalExchangersStatus status, boolean asyncConsume) {
        super(bufferMemoryManager, executors, status, asyncConsume);
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        if (asyncConsume) {
            executors.stream().forEach(executor -> executor.consumeChunk(chunk));
        } else {
            executors.stream().forEach(executor -> {
                synchronized (executor) {
                    executor.consumeChunk(chunk);
                }
            });
        }
    }
}

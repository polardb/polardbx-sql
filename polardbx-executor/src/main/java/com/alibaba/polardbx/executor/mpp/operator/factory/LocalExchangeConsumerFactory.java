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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.operator.BroadcastExchanger;
import com.alibaba.polardbx.executor.mpp.operator.DirectExchanger;
import com.alibaba.polardbx.executor.mpp.operator.LocalExchanger;
import com.alibaba.polardbx.executor.mpp.operator.LocalExchangersStatus;
import com.alibaba.polardbx.executor.mpp.operator.PartitioningBucketExchanger;
import com.alibaba.polardbx.executor.mpp.operator.PartitioningExchanger;
import com.alibaba.polardbx.executor.mpp.operator.RandomExchanger;
import com.alibaba.polardbx.executor.mpp.operator.SingleExchanger;
import com.alibaba.polardbx.executor.mpp.planner.LocalExchange;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.ArrayList;
import java.util.List;

public class LocalExchangeConsumerFactory implements ConsumeExecutorFactory {

    private ExecutorFactory parentExecutorFactory;
    private LocalExchange localExchange;
    private List<ConsumerExecutor> consumerExecutors = new ArrayList<>();
    private OutputBufferMemoryManager outputBufferMemoryManager;
    private LocalExchangersStatus status;

    public LocalExchangeConsumerFactory(ExecutorFactory parentExecutorFactory,
                                        OutputBufferMemoryManager outputBufferMemoryManager,
                                        LocalExchange localExchange) {
        this.parentExecutorFactory = parentExecutorFactory;
        this.outputBufferMemoryManager = outputBufferMemoryManager;
        this.localExchange = localExchange;
    }

    @Override
    public ConsumerExecutor createExecutor(ExecutionContext context, int index) {
        synchronized (this) {
            if (consumerExecutors.isEmpty()) {
                List<Executor> executors = parentExecutorFactory.getAllExecutors(context);
                for (Executor executor : executors) {
                    consumerExecutors.add((ConsumerExecutor) executor);
                }
                this.status = new LocalExchangersStatus(executors.size());
            }
        }

        LocalExchanger localExchanger = null;
        switch (localExchange.getMode()) {
        case SINGLE:
            localExchanger = new SingleExchanger(outputBufferMemoryManager, consumerExecutors,
                this.status, localExchange.isAsyncConsume());
            break;
        case RANDOM:
            localExchanger = new RandomExchanger(outputBufferMemoryManager, consumerExecutors,
                this.status, localExchange.isAsyncConsume(), index,
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_OPTIMIZE_RANDOM_EXCHANGE));
            break;
        case BORADCAST:
            localExchanger = new BroadcastExchanger(outputBufferMemoryManager, consumerExecutors,
                this.status, localExchange.isAsyncConsume());
            break;
        case PARTITION:
            if (localExchange.getBucketNum() > 1) {
                int chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
                localExchanger = new PartitioningBucketExchanger(outputBufferMemoryManager, consumerExecutors,
                    this.status,
                    localExchange.isAsyncConsume(), localExchange.getTypes(),
                    localExchange
                        .getPartitionChannels(),
                    localExchange.getKeyTypes(), localExchange.getBucketNum(),
                    chunkLimit, context);
            } else {
                localExchanger = new PartitioningExchanger(outputBufferMemoryManager, consumerExecutors,
                    this.status,
                    localExchange.isAsyncConsume(), localExchange.getTypes(),
                    localExchange.getPartitionChannels(),
                    localExchange.getKeyTypes(), context, false);
            }
            break;
        case CHUNK_PARTITION:
            localExchanger = new PartitioningExchanger(outputBufferMemoryManager, consumerExecutors,
                this.status,
                localExchange.isAsyncConsume(), localExchange.getTypes(),
                localExchange.getPartitionChannels(),
                localExchange.getKeyTypes(), context, true);
            break;
        case DIRECT:
            localExchanger = new DirectExchanger(
                outputBufferMemoryManager, consumerExecutors.get(index), this.status);
            break;
        default:
            throw new IllegalArgumentException();
        }
        status.incrementParallelism();
        return localExchanger;
    }

    // for test
    public List<ConsumerExecutor> getConsumerExecutors() {
        return consumerExecutors;
    }
}

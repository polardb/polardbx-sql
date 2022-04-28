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

import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;

import java.util.List;

import static io.airlift.concurrent.MoreFutures.getFutureValue;

/**
 * test for only one op
 *
 */
public class SingleExecTest {

    public enum ConsumeSource {
        PRODUCER,
        CHUNK_LIST,
        NOT_CONSUMER,
    }

    ;
    final Executor exec;
    final ExecTestDriver driver;
    final ExecTestDriver.ProduceTask produceTask; // to get result
    final Runnable afterRevoker;

    private void handleMemoryRevoking() {
        MemoryRevoker memoryRevoker = (MemoryRevoker) exec;
        ListenableFuture<?> future = memoryRevoker.startMemoryRevoke();
        getFutureValue(future);
        memoryRevoker.finishMemoryRevoke();
        memoryRevoker.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
        if (afterRevoker != null) {
            afterRevoker.run();
        }
    }

    private SingleExecTest(Builder builder) {
        this.exec = builder.exec;
        this.afterRevoker = builder.afterRevoke;

        ConsumerExecutor consumer = null;
        Executor producer = null;

        try {
            final int maxSpillCnt = builder.maxSpillCnt;
            if (builder.consumeSource == ConsumeSource.NOT_CONSUMER) {
                consumer = null;
            } else if (builder.consumeNeedRevoke()) {
                TriggerProxy.Builder proxyBuilder = new TriggerProxy.Builder(exec);
                if (builder.revokeChunkNumInConsume > 0) {

                    proxyBuilder
                        .addTrigger(ConsumerExecutor.class.getDeclaredMethod("consumeChunk", Chunk.class), () -> {
                            if (maxSpillCnt != 0
                                && ((AbstractExecutor) exec).getStatistics().getSpillCnt() >= maxSpillCnt) {
                                return;
                            }
                            getFutureValue(((ConsumerExecutor) exec).consumeIsBlocked());
                            handleMemoryRevoking();
                        }, builder.revokeChunkNumInConsume);
                }
                if (builder.revokeAfterConsume) {
                    proxyBuilder.addTrigger(ConsumerExecutor.class.getDeclaredMethod("buildConsume"), () -> {
                        if (maxSpillCnt != 0
                            && ((AbstractExecutor) exec).getStatistics().getSpillCnt() >= maxSpillCnt) {
                            return;
                        }
                        handleMemoryRevoking();
                    });
                }
                consumer = (ConsumerExecutor) proxyBuilder.build().getProxyWithInterface(ConsumerExecutor.class);
            } else {
                consumer = (ConsumerExecutor) exec;
            }

            if (builder.produceNeedRevoke()) {
                TriggerProxy.Builder proxyBuilder = new TriggerProxy.Builder(exec);
                if (builder.revokeChunkNumInProduce > 0) {
                    proxyBuilder.addTrigger(Executor.class.getDeclaredMethod("nextChunk"), () -> {
                        if (maxSpillCnt != 0
                            && ((AbstractExecutor) exec).getStatistics().getSpillCnt() >= maxSpillCnt) {
                            return;
                        }
                        getFutureValue(exec.produceIsBlocked());
                        handleMemoryRevoking();
                    }, builder.revokeChunkNumInProduce);
                }
                // TODO: only use Excutor
                producer = (Executor) proxyBuilder.build()
                    .getProxyWithInterfaces(new Class<?>[] {Executor.class, ProducerExecutor.class});
            } else {
                producer = exec;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.produceTask =
            ExecTestDriver.TaskFactory.getProduce(producer, builder.produceParallelism);
        ExecTestDriver.ConsumeTask consumeTask;
        switch (builder.consumeSource) {
        case CHUNK_LIST:
            consumeTask = ExecTestDriver.TaskFactory
                .getConsume(consumer, builder.inputChunks, builder.consumeParallelism);
            break;
        case PRODUCER:
            consumeTask = ExecTestDriver.TaskFactory
                .getConsume(consumer, builder.inputProducer, builder.consumeParallelism);
            break;
        case NOT_CONSUMER:
            consumeTask = ExecTestDriver.TaskFactory.NULL_CONSUME_TASK;
            break;
        default:
            throw new UnsupportedOperationException(String.valueOf(builder.consumeSource));
        }
        this.driver = new ExecTestDriver.Builder().addConsumer(consumeTask).addProducer(this.produceTask).build();
    }

    void exec() {
        driver.exec();
    }

    public List<Chunk> result() {
        return produceTask.result();
    }

    static class Builder {
        final Executor exec;
        final List<Chunk> inputChunks;
        final Executor inputProducer;
        final ConsumeSource consumeSource;

        int consumeParallelism = 1;
        int produceParallelism = 1;

        int revokeChunkNumInConsume = 0;
        public boolean revokeAfterConsume;
        int revokeChunkNumInProduce = 0;
        int maxSpillCnt = 0;
        Runnable afterRevoke = null;

        boolean consumeNeedRevoke() {
            return revokeChunkNumInConsume > 0 || revokeAfterConsume;
        }

        boolean produceNeedRevoke() {
            return revokeChunkNumInProduce > 0;
        }

        Builder(Executor exec) {
            this.consumeSource = ConsumeSource.NOT_CONSUMER;
            this.exec = exec;
            this.inputChunks = null;
            this.inputProducer = null;
        }

        Builder(Executor exec, List<Chunk> inputChunks) {
            this.consumeSource = ConsumeSource.CHUNK_LIST;
            this.exec = exec;
            this.inputChunks = inputChunks;
            this.inputProducer = null;
        }

        Builder(Executor exec, Executor inputProducer) {
            this.consumeSource = ConsumeSource.PRODUCER;
            this.exec = exec;
            this.inputChunks = null;
            this.inputProducer = inputProducer;
        }

        Builder setParallelism(int consumeParallelism, int produceParallelism) {
            this.consumeParallelism = consumeParallelism;
            this.produceParallelism = produceParallelism;
            return this;
        }

        // when revokeNum == 0, then not revoke
        Builder setRevoke(int revokeChunkNumInConsume, boolean revokeAfterConsume, int revokeChunkNumInProduce,
                          int maxSpillCnt) {
            this.revokeChunkNumInConsume = revokeChunkNumInConsume;
            this.revokeChunkNumInProduce = revokeChunkNumInProduce;
            this.revokeAfterConsume = revokeAfterConsume;
            this.maxSpillCnt = maxSpillCnt;
            return this;
        }

        Builder setRevoke(int revokeChunkNumInConsume, boolean revokeAfterConsume, int revokeChunkNumInProduce) {
            return setRevoke(revokeChunkNumInConsume, revokeAfterConsume, revokeChunkNumInProduce, 0);
        }

        public void setAfterRevoke(Runnable afterRevoker) {
            this.afterRevoke = afterRevoker;
        }

        SingleExecTest build() {
            return new SingleExecTest(this);
        }
    }

}

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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.CorrelateExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.ProducerExecutor;
import com.alibaba.polardbx.executor.operator.SortMergeExchangeExec;
import com.alibaba.polardbx.executor.operator.SourceExec;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DriverExec {

    private static final Logger log = LoggerFactory.getLogger(DriverExec.class);

    private int pipelineId;
    private Executor producer;
    private ConsumerExecutor consumer;

    private final DriverYieldSignal yieldSignal;

    private boolean isSpill;

    private boolean opened = false;
    private boolean closed = false;

    private boolean producerIsClosed = false;
    private boolean consumerIsClosed = false;

    private boolean consumerIsBuffer;

    private List<ProducerExecutor> forceCloseExecs = new ArrayList<>();
    private List<Integer> allInputIds = new ArrayList<>();
    private List<MemoryRevoker> memoryRevokers = new ArrayList<>();
    private HashMap<Integer, List<SourceExec>> sourceExecs = new HashMap<>();

    public DriverExec(int pipelineId, DriverContext driverContext, Executor producer, ConsumerExecutor consumer,
                      int parallelism) {
        this.pipelineId = pipelineId;
        this.producer = producer;
        this.consumer = consumer;
        this.yieldSignal = driverContext.getYieldSignal();
        this.isSpill = driverContext.getPipelineContext().getTaskContext().isSpillable();
        visitChild(producer);
        if (isSpill) {
            if (consumer instanceof MemoryRevoker) {
                memoryRevokers.add((MemoryRevoker) consumer);
            }
            if (consumer instanceof LocalExchanger) {
                for (ConsumerExecutor executor : ((LocalExchanger) consumer).getExecutors()) {
                    if (executor instanceof MemoryRevoker) {
                        if (parallelism > 1 && ((LocalExchanger) consumer).getExecutors().size() > 1) {
                            throw new RuntimeException(
                                "The MemoryRevoker must only be contain by one driver in spill mode!");
                        }
                        memoryRevokers.add((MemoryRevoker) executor);
                    }
                }
            }
        }

        for (List<SourceExec> sourceExecs : sourceExecs.values()) {
            for (SourceExec sourceExec : sourceExecs) {
                if (sourceExec instanceof SortMergeExchangeExec) {
                    ((SortMergeExchangeExec) sourceExec).setYieldSignal(yieldSignal);
                }
            }
        }

        driverContext.setDriverExecRef(this);
        if (consumer instanceof LocalBufferExec) {
            this.consumerIsBuffer = true;
        } else if (consumer instanceof LocalExchanger) {
            this.consumerIsBuffer = ((LocalExchanger) consumer).executorIsLocalBuffer();
        }
    }

    private void visitChild(Executor executor) {
        if (executor instanceof EmptyExecutor) {
            return;
        }

        if (executor instanceof SourceExec) {
            SourceExec sourceExec = (SourceExec) executor;
            List<SourceExec> list = sourceExecs.get(sourceExec.getSourceId());
            if (list == null) {
                list = new ArrayList<>();
                sourceExecs.put(sourceExec.getSourceId(), list);
            }
            list.add(sourceExec);
            this.forceCloseExecs.add(sourceExec);
        }
        if (executor instanceof MemoryRevoker) {
            this.memoryRevokers.add((MemoryRevoker) executor);
        }
        if (executor instanceof CorrelateExec) {
            this.forceCloseExecs.add(executor);
        }
        this.allInputIds.add(executor.getId());

        for (Executor child : executor.getInputs()) {
            visitChild(child);
        }
    }

    public synchronized void open() {
        if (opened) {
            return;
        }
        try {
            if (!closed) {
                producer.open();
                consumer.openConsume();
            }
        } finally {
            opened = true;
        }
    }

    public synchronized void closeProducer() {
        try {
            if (!producerIsClosed && opened) {
                producer.close();
            }
        } finally {
            producerIsClosed = true;
        }
    }

    public synchronized boolean isProducerIsClosed() {
        return producerIsClosed;
    }

    public synchronized void close() {
        try {
            if (!closed) {
                if (opened) {
                    try {
                        producer.close();
                    } catch (Throwable e) {
                        //ignore
                    }
                    try {
                        consumer.buildConsume();
                    } catch (Throwable e) {
                        log.warn("buildConsume consumer:" + consumer, e);
                    }
                } else if (consumerIsBuffer) {
                    try {
                        consumer.buildConsume();
                    } catch (Throwable e) {
                        log.warn("buildConsume consumer:" + consumer, e);
                    }
                }

            }
        } finally {
            closed = true;
        }
    }

    public synchronized void closeOnException() {
        close();
        if (!consumerIsClosed) {
            try {
                consumer.closeConsume(false);
            } catch (Throwable e) {
                log.warn("closeConsume consumer:" + consumer, e);
            } finally {
                consumerIsClosed = true;
            }
        }
    }

    public synchronized boolean isOpened() {
        return opened;
    }

    public synchronized boolean isFinished() {
        return closed;
    }

    public List<MemoryRevoker> getMemoryRevokers() {
        return memoryRevokers;
    }

    public ConsumerExecutor getConsumer() {
        return consumer;
    }

    public Executor getProducer() {
        return producer;
    }

    public List<Integer> getAllInputIds() {
        return allInputIds;
    }

    public int getPipelineId() {
        return pipelineId;
    }

    public HashMap<Integer, List<SourceExec>> getSourceExecs() {
        return sourceExecs;
    }

    public List<ProducerExecutor> getForceCloseExecs() {
        return forceCloseExecs;
    }
}

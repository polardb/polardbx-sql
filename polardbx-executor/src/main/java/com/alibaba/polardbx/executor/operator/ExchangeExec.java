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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.operator.IExchangeClient;
import com.alibaba.polardbx.executor.mpp.split.RemoteSplit;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ExchangeExec extends SourceExec implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ExchangeExec.class);

    private final Integer sourceId;
    private final PagesSerde serde;
    private final List<DataType> outputColumnMeta;

    private final IExchangeClient exchangeClient;
    private final MemoryPool memoryPool;

    private final List<TaskLocation> cacheLocations = new ArrayList<>();

    private volatile boolean doOpen = false;

    @GuardedBy("this")
    private AtomicBoolean noMoreLocations = new AtomicBoolean(false);

    public ExchangeExec(
        ExecutionContext context, Integer sourceId, IExchangeClient exchangeClient,
        MemoryPool memoryPool, PagesSerde serde,
        List<DataType> outputColumnMeta) {
        super(context);
        requireNonNull(sourceId, "sourceId is null");
        this.sourceId = sourceId;
        this.serde = serde;
        this.outputColumnMeta = outputColumnMeta;
        this.exchangeClient = exchangeClient;
        this.memoryPool = memoryPool;
    }

    @Override
    public void addSplit(Split split) {
        requireNonNull(split, "split is null");
        checkArgument(split.isRemoteSplit(), "split is not a remote split");
        checkState(!noMoreLocations.get(), "No more locations already set");
        TaskLocation location = ((RemoteSplit) split.getConnectorSplit()).getLocation();
        synchronized (noMoreLocations) {
            if (doOpen) {
                exchangeClient.addLocation(location);
            } else {
                cacheLocations.add(location);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("sourceId: " + sourceId + ", location:" + location);
        }
    }

    @Override
    public void noMoreSplits() {
        synchronized (noMoreLocations) {
            noMoreLocations.set(true);
            if (doOpen) {
                exchangeClient.noMoreLocations();
            }
        }
    }

    @Override
    public Integer getSourceId() {
        return sourceId;
    }

    @Override
    public List<DataType> getDataTypes() {
        return this.outputColumnMeta;
    }

    @Override
    void doOpen() {
        synchronized (noMoreLocations) {
            this.doOpen = true;
            for (TaskLocation taskLocation : cacheLocations) {
                exchangeClient.addLocation(taskLocation);
            }
            if (noMoreLocations.get()) {
                exchangeClient.noMoreLocations();
            }
            cacheLocations.clear();
        }
    }

    @Override
    Chunk doSourceNextChunk() {
        try {
            SerializedChunk page = exchangeClient.pollPage();
            if (page != null) {
                return serde.deserialize(page);
            }
            return null;
        } catch (Throwable t) {
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    public boolean produceIsFinished() {
        return exchangeClient.isFinished();
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        ListenableFuture<?> blocked = exchangeClient.isBlocked();
        if (blocked.isDone()) {
            return NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    void doClose() {
        forceClose();
    }

    @Override
    public void forceClose() {
        try {
            cacheLocations.clear();
            exchangeClient.close();
            memoryPool.destroy();
        } catch (IOException e) {
            log.error("exchange operator close exception", e);
        }
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }
}

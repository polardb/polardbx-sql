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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.operator.DriverYieldSignal;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClient;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClientSupplier;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.mpp.split.RemoteSplit;
import com.alibaba.polardbx.executor.operator.util.ChunkWithPositionComparator;
import com.alibaba.polardbx.executor.operator.util.MergeSortedChunks;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SortMergeExchangeExec extends SourceExec implements Closeable {

    private final Integer sourceId;
    private final PagesSerde serde;
    private final List<OrderByOption> orderBys;

    private final ExchangeClientSupplier supplier;
    private final Closer closer = Closer.create();
    private final SettableFuture<Void> blockedOnSplits = SettableFuture.create();
    private final List<WorkProcessor<Chunk>> pageProducers = new ArrayList<>();
    private WorkProcessor<Chunk> mergedPages;
    private boolean closed;
    private List<DataType> types;
    private DriverYieldSignal yieldSignal;

    private final List<TaskLocation> cacheLocations = new ArrayList<>();
    private boolean noMoreLocation = false;
    private boolean doOpen = false;
    private MemoryPool memoryPool;

    public SortMergeExchangeExec(ExecutionContext context, Integer sourceId, ExchangeClientSupplier supplier,
                                 PagesSerde serde, List<OrderByOption> orderBys,
                                 List<DataType> types) {
        super(context);
        requireNonNull(sourceId, "sourceId is null");
        this.sourceId = sourceId;
        this.supplier = supplier;
        this.types = types;
        this.serde = serde;
        this.orderBys = orderBys;
        this.memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
    }

    public void setYieldSignal(DriverYieldSignal yieldSignal) {
        this.yieldSignal = yieldSignal;
        this.yieldSignal.setEnable(true);
    }

    @Override
    public void addSplit(Split split) {

        if (closed) {
            //the operator is already closed!
            return;
        }
        requireNonNull(split, "split is null");
        checkArgument(split.isRemoteSplit(), "split is not a remote split");
        checkState(!blockedOnSplits.isDone(), "noMoreSplits has been called already");

        RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();

        if (doOpen) {
            addExchangeClient(remoteSplit.getLocation());
        } else {
            cacheLocations.add(remoteSplit.getLocation());
        }

    }

    @Override
    public void noMoreSplits() {
        noMoreLocation = true;
        blockedOnSplits.set(null);
        if (closed) {
            //the operator is already closed!
            return;
        }
        if (doOpen) {
            noMoreClient();
        }
    }

    @Override
    public Integer getSourceId() {
        return sourceId;
    }

    @Override
    public List<DataType> getDataTypes() {
        return this.types;
    }

    @Override
    void doOpen() {
        try {
            if (!doOpen) {
                for (TaskLocation taskLocation : cacheLocations) {
                    addExchangeClient(taskLocation);
                }
                cacheLocations.clear();
                if (noMoreLocation) {
                    noMoreClient();
                }
            }
        } finally {
            this.doOpen = true;
        }
    }

    private void noMoreClient() {
        if (pageProducers.size() > 0) {
            mergedPages = MergeSortedChunks.mergeSortedPages(
                pageProducers, new ChunkWithPositionComparator(
                    orderBys,
                    types
                ), types, chunkLimit,
                (pageBuilder, ChunkWithPosition) -> pageBuilder.isFull(), yieldSignal, context);
        }
    }

    private void addExchangeClient(TaskLocation taskLocation) {

        ExchangeClient exchangeClient =
            closer.register((ExchangeClient) supplier
                .get(new RecordMemSystemListener(memoryPool.getMemoryAllocatorCtx()), context));
        exchangeClient.addLocation(taskLocation);
        exchangeClient.noMoreLocations();
        pageProducers.add(exchangeClient.pages().map(serializedPage -> serde.deserialize(serializedPage)));
    }

    @Override
    Chunk doSourceNextChunk() {
        if (closed || mergedPages == null || !mergedPages.process() || mergedPages.isFinished()) {
            return null;
        }
        Chunk page = mergedPages.getResult();
        return page;
    }

    @Override
    public boolean produceIsFinished() {
        return closed || (mergedPages != null && mergedPages.isFinished());
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        if (doOpen) {
            if (!blockedOnSplits.isDone()) {
                return blockedOnSplits;
            }

            if (mergedPages != null && mergedPages.isBlocked()) {
                return mergedPages.getBlockedFuture();
            }
        }
        return NOT_BLOCKED;
    }

    @Override
    void doClose() {
        forceClose();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    public synchronized void forceClose() {
        try {
            if (!closed) {
                closer.close();
            }
            memoryPool.destroy();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closed = true;
        }
    }
}

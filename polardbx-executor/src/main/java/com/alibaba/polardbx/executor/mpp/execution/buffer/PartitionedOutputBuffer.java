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

package com.alibaba.polardbx.executor.mpp.execution.buffer;

import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.alibaba.polardbx.common.properties.MetricLevel.isSQLMetricEnabled;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputBuffer
    implements OutputBuffer {
    private static final AtomicLongFieldUpdater<PartitionedOutputBuffer> totalPagesAddedUpdater =
        AtomicLongFieldUpdater.newUpdater(PartitionedOutputBuffer.class, "totalPagesAddedLong");
    private static final AtomicLongFieldUpdater<PartitionedOutputBuffer> totalRowsAddedUpdater =
        AtomicLongFieldUpdater.newUpdater(PartitionedOutputBuffer.class, "totalRowsAddedLong");
    private final StateMachine<BufferState> state;
    private final OutputBuffers outputBuffers;
    private final OutputBufferMemoryManager memoryManager;
    private final int metricLevel;

    private final List<ClientBuffer> partitions;

    private volatile long totalPagesAddedLong = 0L;
    private volatile long totalRowsAddedLong = 0L;

    public PartitionedOutputBuffer(
        String taskInstanceId,
        StateMachine<BufferState> state,
        OutputBuffers outputBuffers,
        OutputBufferMemoryManager memoryManager,
        int metricLevel) {
        this.state = requireNonNull(state, "state is null");
        checkArgument(outputBuffers.getType() == OutputBuffers.BufferType.PARTITIONED,
            "Expected a PARTITIONED output buffer descriptor");
        checkArgument(outputBuffers.isNoMoreBufferIds(), "Expected a final output buffer descriptor");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.metricLevel = metricLevel;

        this.memoryManager = memoryManager;

        ImmutableList.Builder<ClientBuffer> partitions = ImmutableList.builder();
        for (OutputBuffers.OutputBufferId bufferId : outputBuffers.getBuffers().keySet()) {
            ClientBuffer partition = new ClientBuffer(taskInstanceId, bufferId);
            partitions.add(partition);
        }
        this.partitions = partitions.build();

        state.compareAndSet(BufferState.OPEN, BufferState.NO_MORE_BUFFERS);
        state.compareAndSet(BufferState.NO_MORE_PAGES, BufferState.FLUSHING);
        checkFlushComplete();
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener) {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished() {
        return state.get() == BufferState.FINISHED;
    }

    @Override
    public double getUtilization() {
        return memoryManager.getUtilization();
    }

    @Override
    public OutputBufferInfo getInfo() {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = this.state.get();

        int totalBufferedBytes = 0;
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (ClientBuffer partition : partitions) {
            BufferInfo bufferInfo = partition.getInfo();
            if (isSQLMetricEnabled(metricLevel)) {
                infos.add(bufferInfo);
            }
            PageBufferInfo pageBufferInfo = bufferInfo.getPageBufferInfo();
            totalBufferedBytes += pageBufferInfo.getBufferedBytes();
        }

        return new OutputBufferInfo(state, totalBufferedBytes);
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers, ExecutionContext context) {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<?> enqueue(List<SerializedChunk> pages) {
        checkState(partitions.size() == 1, "Expected exactly one partition");
        return enqueue(0, pages);
    }

    @Override
    public ListenableFuture<?> enqueue(int partitionNumber, List<SerializedChunk> pages) {
        requireNonNull(pages, "pages is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return immediateFuture(true);
        }

        // reserve memory
        long bytesAdded = 0L;
        for (int i = 0; i < pages.size(); i++) {
            bytesAdded += pages.get(i).getRetainedSizeInBytes();
        }
        memoryManager.updateMemoryUsage(bytesAdded);

        // update stats
        long rowCount = 0L;
        for (int i = 0; i < pages.size(); i++) {
            rowCount += pages.get(i).getPositionCount();
        }
        totalRowsAddedUpdater.addAndGet(this, rowCount);
        totalPagesAddedUpdater.addAndGet(this, pages.size());

        // create page reference counts with an initial single reference
        List<ClientBuffer.SerializedChunkReference> serializedPageReferences = new ArrayList<>(pages.size());
        for (int i = 0; i < pages.size(); i++) {
            SerializedChunk bufferedPage = pages.get(i);
            ClientBuffer.SerializedChunkReference
                serializedPageReference = new ClientBuffer.SerializedChunkReference(bufferedPage, 1,
                () -> memoryManager.updateMemoryUsage(-bufferedPage.getRetainedSizeInBytes()));
            serializedPageReferences.add(serializedPageReference);
        }

        // add pages to the buffer (this will increase the reference count by one)
        partitions.get(partitionNumber).enqueuePages(serializedPageReferences);

        // drop the initial reference
        serializedPageReferences.forEach(ClientBuffer.SerializedChunkReference::dereferencePage);

        return memoryManager.getNotFullFuture();
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId outputBufferId, boolean preferLocal,
                                              long startingSequenceId, DataSize maxSize) {
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");
        ClientBuffer buffer = partitions.get(outputBufferId.getId());
        if (preferLocal) {
            buffer.setPreferLocal(true);
        }
        return buffer.getPages(startingSequenceId, maxSize);
    }

    @Override
    public void abort(OutputBuffers.OutputBufferId bufferId) {
        requireNonNull(bufferId, "bufferId is null");

        partitions.get(bufferId.getId()).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages() {
        state.compareAndSet(BufferState.OPEN, BufferState.NO_MORE_PAGES);
        state.compareAndSet(BufferState.NO_MORE_BUFFERS, BufferState.FLUSHING);
        memoryManager.setNoBlockOnFull();

        partitions.forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy() {
        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(BufferState.FINISHED, oldState -> !oldState.isTerminal())) {
            partitions.forEach(ClientBuffer::destroy);
            memoryManager.setNoBlockOnFull();
        }
    }

    @Override
    public void fail() {
        // ignore fail if the buffer already in a terminal state.
        if (state.setIf(BufferState.FAILED, oldState -> !oldState.isTerminal())) {
            partitions.forEach(ClientBuffer::forceDestroy);
            memoryManager.setNoBlockOnFull();
        }
    }

    @Override
    public void checkFlushComplete() {
        if (state.get() != BufferState.FLUSHING) {
            return;
        }

        if (partitions.stream().allMatch(ClientBuffer::isDestroyed)) {
            destroy();
        }
    }

    @Override
    public ClientBuffer getClientBuffer(int partition) {
        return partitions.get(partition);
    }
}
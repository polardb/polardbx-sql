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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.executor.mpp.Threads.ENABLE_WISP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class BroadcastOutputBuffer
    implements OutputBuffer {
    private final String taskInstanceId;
    private final StateMachine<BufferState> state;
    private final OutputBufferMemoryManager memoryManager;

    @GuardedBy("this")
    private OutputBuffers outputBuffers =
        OutputBuffers.createInitialEmptyOutputBuffers(OutputBuffers.BufferType.BROADCAST);

    @GuardedBy("this")
    private final Map<OutputBuffers.OutputBufferId, ClientBuffer> buffers = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final List<ClientBuffer.SerializedChunkReference> initialPagesForNewBuffers = new ArrayList<>();

    private final int metricLevel;

    private final AtomicLong totalPagesAdded = new AtomicLong();
    private final AtomicLong totalRowsAdded = new AtomicLong();
    private final AtomicLong totalBufferedPages = new AtomicLong();

    public BroadcastOutputBuffer(
        String taskInstanceId,
        StateMachine<BufferState> state,
        OutputBufferMemoryManager memoryManager,
        int metricLevel) {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.state = requireNonNull(state, "state is null");
        this.memoryManager = memoryManager;
        this.metricLevel = metricLevel;
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

        return new OutputBufferInfo(state, memoryManager.getBufferedBytes());
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers, ExecutionContext context) {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        synchronized (this) {
            // ignore buffers added after query finishes, which can happen when a query is canceled
            // also ignore old versions, which is normal
            BufferState state = this.state.get();
            if (state.isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
                return;
            }

            // verify this is valid state change
            outputBuffers.checkValidTransition(newOutputBuffers);
            outputBuffers = newOutputBuffers;

            // add the new buffers
            for (Map.Entry<OutputBuffers.OutputBufferId, Integer> entry : outputBuffers.getBuffers().entrySet()) {
                if (!buffers.containsKey(entry.getKey())) {
                    ClientBuffer buffer = getBuffer(entry.getKey());
                    if (!state.canAddPages()) {
                        buffer.setNoMorePages();
                    }
                }
            }

            // update state if no more buffers is set
            if (outputBuffers.isNoMoreBufferIds()) {
                this.state.compareAndSet(BufferState.OPEN, BufferState.NO_MORE_BUFFERS);
                this.state.compareAndSet(BufferState.NO_MORE_PAGES, BufferState.FLUSHING);
            }
        }

        if (!state.get().canAddBuffers()) {
            noMoreBuffers();
        }

        checkFlushComplete();
    }

    @Override
    public ListenableFuture<?> enqueue(List<SerializedChunk> pages) {
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
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(pages.size());
        totalBufferedPages.addAndGet(pages.size());

        // create page reference counts with an initial single reference
        List<ClientBuffer.SerializedChunkReference> serializedPageReferences = new ArrayList<>(pages.size());
        for (int i = 0; i < pages.size(); i++) {
            SerializedChunk pageSplit = pages.get(i);
            ClientBuffer.SerializedChunkReference serializedPageReference =
                new ClientBuffer.SerializedChunkReference(pageSplit, 1, () -> {
                    checkState(totalBufferedPages.decrementAndGet() >= 0);
                    memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes());
                });
            serializedPageReferences.add(serializedPageReference);
        }

        // if we can still add buffers, remember the pages for the future buffers
        Collection<ClientBuffer> buffers;
        synchronized (this) {
            if (state.get().canAddBuffers()) {
                for (int i = 0; i < serializedPageReferences.size(); i++) {
                    serializedPageReferences.get(i).addReference();
                }
                initialPagesForNewBuffers.addAll(serializedPageReferences);
            }

            // make a copy while holding the lock to avoid race with initialPagesForNewBuffers.addAll above
            buffers = safeGetBuffersSnapshot();
        }

        // add pages to all existing buffers (each buffer will increment the reference count)
        buffers.forEach(partition -> partition.enqueuePages(serializedPageReferences));

        // drop the initial reference
        for (int i = 0; i < serializedPageReferences.size(); i++) {
            serializedPageReferences.get(i).dereferencePage();
        }

        return memoryManager.getNotFullFuture();
    }

    @Override
    public ListenableFuture<?> enqueue(int partitionNumber, List<SerializedChunk> pages) {
        checkState(partitionNumber == 0, "Expected partition number to be zero");
        return enqueue(pages);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId outputBufferId, boolean preferLocal,
                                              long startingSequenceId, DataSize maxSize) {
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(outputBufferId).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void abort(OutputBuffers.OutputBufferId bufferId) {
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages() {
        state.compareAndSet(BufferState.OPEN, BufferState.NO_MORE_PAGES);
        state.compareAndSet(BufferState.NO_MORE_BUFFERS, BufferState.FLUSHING);
        memoryManager.setNoBlockOnFull();

        safeGetBuffersSnapshot().forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy() {
        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(BufferState.FINISHED, oldState -> !oldState.isTerminal())) {
            noMoreBuffers();

            safeGetBuffersSnapshot().forEach(ClientBuffer::destroy);

            memoryManager.setNoBlockOnFull();
        }
    }

    @Override
    public void fail() {
        // ignore fail if the buffer already in a terminal state.
        if (state.setIf(BufferState.FAILED, oldState -> !oldState.isTerminal())) {
            noMoreBuffers();

            safeGetBuffersSnapshot().forEach(ClientBuffer::forceDestroy);

            memoryManager.setNoBlockOnFull();
        }
    }

    private synchronized ClientBuffer getBuffer(OutputBuffers.OutputBufferId id) {
        ClientBuffer buffer = buffers.get(id);
        if (buffer != null) {
            return buffer;
        }

        checkState(state.get().canAddBuffers(), "No more buffers already set");

        // NOTE: buffers are allowed to be created before they are explicitly declared by setOutputBuffers
        // When no-more-buffers is set, we verify that all created buffers have been declared
        buffer = new ClientBuffer(taskInstanceId, id);

        // add initial pages
        buffer.enqueuePages(initialPagesForNewBuffers);

        // update state
        if (!state.get().canAddPages()) {
            buffer.setNoMorePages();
        }

        // buffer may have finished immediately before calling this method
        if (state.get() == BufferState.FINISHED) {
            buffer.destroy();
        }

        buffers.put(id, buffer);
        return buffer;
    }

    private synchronized Collection<ClientBuffer> safeGetBuffersSnapshot() {
        return ImmutableList.copyOf(this.buffers.values());
    }

    private void noMoreBuffers() {
        if (!ENABLE_WISP) {
            checkState(!Thread.holdsLock(this), "Can not set no more buffers while holding a lock on this");
        }
        List<ClientBuffer.SerializedChunkReference> pages;
        synchronized (this) {
            pages = ImmutableList.copyOf(initialPagesForNewBuffers);
            initialPagesForNewBuffers.clear();

            // verify all created buffers have been declared
            Sets.SetView<OutputBuffers.OutputBufferId>
                undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
            checkState(undeclaredCreatedBuffers.isEmpty(),
                "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
        }

        // dereference outside of synchronized to avoid making a callback while holding a lock
        for (int i = 0; i < pages.size(); i++) {
            pages.get(i).dereferencePage();
        }
    }

    @Override
    public void checkFlushComplete() {
        if (state.get() != BufferState.FLUSHING) {
            return;
        }

        for (ClientBuffer buffer : safeGetBuffersSnapshot()) {
            if (!buffer.isDestroyed()) {
                return;
            }
        }
        destroy();
    }
}
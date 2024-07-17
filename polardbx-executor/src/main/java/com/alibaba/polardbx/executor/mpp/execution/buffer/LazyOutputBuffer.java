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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.execution.RecordMemSystemListener;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ExtendedSettableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.alibaba.polardbx.executor.mpp.execution.buffer.BufferResult.emptyResults;
import static com.alibaba.polardbx.executor.mpp.execution.buffer.BufferState.FAILED;
import static com.alibaba.polardbx.executor.mpp.execution.buffer.BufferState.FINISHED;
import static com.alibaba.polardbx.executor.mpp.execution.buffer.BufferState.OPEN;
import static com.alibaba.polardbx.executor.mpp.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class LazyOutputBuffer implements OutputBuffer {
    private final StateMachine<BufferState> state;
    private final String taskInstanceId;
    private final Executor executor;

    @GuardedBy("this")
    private OutputBuffer delegate;

    @GuardedBy("this")
    private final Set<OutputBuffers.OutputBufferId> abortedBuffers = new HashSet<>();

    @GuardedBy("this")
    private final List<PendingRead> pendingReads = new ArrayList<>();

    public LazyOutputBuffer(
        TaskId taskId,
        String taskInstanceId,
        Executor executor) {
        requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.executor = requireNonNull(executor, "executor is null");
        state = new StateMachine<>(taskId + "-buffer", executor, OPEN, TERMINAL_BUFFER_STATES);
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener) {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isFinished() {
        return state.get() == FINISHED;
    }

    @Override
    public double getUtilization() {
        return 0.0;
    }

    @Override
    public OutputBufferInfo getInfo() {
        OutputBuffer outputBuffer;
        synchronized (this) {
            outputBuffer = delegate;
        }

        if (outputBuffer == null) {
            //
            // NOTE: this code must be lock free to not hanging state machine updates
            //
            BufferState state = this.state.get();

            return new OutputBufferInfo(state, 0);
        }
        return outputBuffer.getInfo();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers, ExecutionContext context) {
        Set<OutputBuffers.OutputBufferId> abortedBuffers = ImmutableSet.of();
        List<PendingRead> pendingReads = ImmutableList.of();
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore set output if buffer was already destroyed or failed
                if (state.get().isTerminal()) {
                    return;
                }

                long maxOutputBufferSize =
                    context.getParamManager().getLong(ConnectionParams.MPP_OUTPUT_MAX_BUFFER_SIZE);
                int metricLevel = context.getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL);
//                boolean isSpill =
//                    MemorySetting.ENABLE_SPILL && context.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL);

                //这里不能使用SpilledOutputBufferMemoryManager,否则会出现死锁问题
                MemoryPool taskBufferMemoryPool = context.getMemoryPool().getOrCreatePool("outputBuffer@");
                MemoryAllocatorCtx allocator = taskBufferMemoryPool.getMemoryAllocatorCtx();
                SystemMemoryUsageListener outputBufferMemoryUsageListener = new RecordMemSystemListener(allocator);
                OutputBufferMemoryManager memoryManager =
                    new OutputBufferMemoryManager(maxOutputBufferSize, outputBufferMemoryUsageListener, executor);

                switch (newOutputBuffers.getType()) {
                case PARTITIONED:
                    delegate = new PartitionedOutputBuffer(taskInstanceId, state, newOutputBuffers, memoryManager,
                        metricLevel);
                    break;
                case BROADCAST:
                    delegate =
                        new BroadcastOutputBuffer(taskInstanceId, state, memoryManager, metricLevel);
                    break;
                }

                // process pending aborts and reads outside of synchronized lock
                abortedBuffers = ImmutableSet.copyOf(this.abortedBuffers);
                this.abortedBuffers.clear();
                pendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            outputBuffer = delegate;
        }

        outputBuffer.setOutputBuffers(newOutputBuffers, context);

        // process pending aborts and reads outside of synchronized lock
        abortedBuffers.forEach(outputBuffer::abort);
        for (PendingRead pendingRead : pendingReads) {
            pendingRead.process(outputBuffer);
        }
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId bufferId, boolean preferLocal, long token,
                                              DataSize maxSize) {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                if (state.get() == FINISHED) {
                    return immediateFuture(emptyResults(taskInstanceId, 0, true));
                }

                PendingRead pendingRead = new PendingRead(bufferId, preferLocal, token, maxSize);
                pendingReads.add(pendingRead);
                return pendingRead.getFutureResult();
            }
            outputBuffer = delegate;
        }
        return outputBuffer.get(bufferId, preferLocal, token, maxSize);
    }

    @Override
    public ClientBuffer getClientBuffer(int partition) {
        return delegate != null ? delegate.getClientBuffer(partition) : null;
    }

    @Override
    public void abort(OutputBuffers.OutputBufferId bufferId) {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                abortedBuffers.add(bufferId);
                // Normally, we should free any pending readers for this buffer,
                // but we assume that the real buffer will be created quickly.
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.abort(bufferId);
    }

    @Override
    public ListenableFuture<?> enqueue(List<SerializedChunk> pages) {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        return outputBuffer.enqueue(pages);
    }

    @Override
    public ListenableFuture<?> enqueue(int partition, List<SerializedChunk> pages) {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        return outputBuffer.enqueue(partition, pages);
    }

    @Override
    public void setNoMorePages() {
        OutputBuffer outputBuffer;
        synchronized (this) {
            checkState(delegate != null, "Buffer has not been initialized");
            outputBuffer = delegate;
        }
        outputBuffer.setNoMorePages();
    }

    @Override
    public void destroy() {
        OutputBuffer outputBuffer;
        List<PendingRead> pendingReads = ImmutableList.of();
        synchronized (this) {
            if (delegate == null) {
                // ignore destroy if the buffer already in a terminal state.
                if (!state.setIf(FINISHED, state -> !state.isTerminal())) {
                    return;
                }

                pendingReads = ImmutableList.copyOf(this.pendingReads);
                this.pendingReads.clear();
            }
            outputBuffer = delegate;
        }

        // if there is no output buffer, free the pending reads
        if (outputBuffer == null) {
            for (PendingRead pendingRead : pendingReads) {
                pendingRead.getFutureResult().set(emptyResults(taskInstanceId, 0, true));
            }
            return;
        }

        outputBuffer.destroy();
    }

    @Override
    public void fail() {
        OutputBuffer outputBuffer;
        synchronized (this) {
            if (delegate == null) {
                // ignore fail if the buffer already in a terminal state.
                state.setIf(FAILED, state -> !state.isTerminal());

                // Do not free readers on fail
                return;
            }
            outputBuffer = delegate;
        }
        outputBuffer.fail();
    }

    @Override
    public void checkFlushComplete() {
        if (delegate != null) {
            delegate.checkFlushComplete();
        }
    }

    private static class PendingRead {
        private final OutputBuffers.OutputBufferId bufferId;
        private final boolean preferLocal;
        private final long startingSequenceId;
        private final DataSize maxSize;

        private final ExtendedSettableFuture<BufferResult> futureResult = ExtendedSettableFuture.create();

        public PendingRead(OutputBuffers.OutputBufferId bufferId, boolean preferLocal, long startingSequenceId,
                           DataSize maxSize) {
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
            this.preferLocal = preferLocal;
            this.startingSequenceId = startingSequenceId;
            this.maxSize = requireNonNull(maxSize, "maxSize is null");
        }

        public ExtendedSettableFuture<BufferResult> getFutureResult() {
            return futureResult;
        }

        public void process(OutputBuffer delegate) {
            if (futureResult.isDone()) {
                return;
            }

            try {
                ListenableFuture<BufferResult> result =
                    delegate.get(bufferId, preferLocal, startingSequenceId, maxSize);
                futureResult.setAsync(result);
            } catch (Exception e) {
                futureResult.setException(e);
            }
        }
    }
}
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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.ScheduledSplit;
import com.alibaba.polardbx.executor.mpp.execution.TaskContext;
import com.alibaba.polardbx.executor.mpp.execution.TaskSource;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.ProducerExecutor;
import com.alibaba.polardbx.executor.operator.SourceExec;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.utils.ExecUtils;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.alibaba.polardbx.executor.operator.ConsumerExecutor.NOT_BLOCKED;

public class Driver implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(Driver.class);

    private final ConcurrentMap<Integer, TaskSource> newSources = new ConcurrentHashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);

    private final ReentrantLock exclusiveLock = new ReentrantLock();

    @GuardedBy("this")
    private Thread lockHolder;

    @GuardedBy("exclusiveLock")
    private final Map<Integer, TaskSource> currentSources = new ConcurrentHashMap<>();

    private final DriverContext driverContext;

    private enum State {
        ALIVE, NEED_DESTRUCTION, DESTROYED
    }

    private final DriverExec driverExec;

    private final Collection<Integer> sources; //单个Driver 可能包含多个Source

    private final PipelineDepTree pipelineDepTree;

    private final Map<MemoryRevoker, ListenableFuture<?>> revokingExecutors = new HashMap<>();

    private boolean isSpill;

    private final ArrayList<ListenableFuture<?>> revokingRequestedFuturesList = new ArrayList<>(4);

    private int revokeDriverId;

    private boolean buildDepOnAllConsumers;

    private AtomicReference forceClosed = new AtomicReference(false);

    private boolean flagConsumerFinished;

    public Driver(DriverContext driverContext, DriverExec driverExec) {
        this.driverContext = driverContext;
        this.driverExec = driverExec;
        this.sources = driverExec.getSourceExecs().keySet();
        TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
        this.pipelineDepTree = taskContext.getPipelineDepTree();
        this.isSpill = driverContext.getPipelineContext().getTaskContext().isSpillable();
        if (isSpill && driverExec.getConsumer() instanceof MemoryRevoker) {
            buildDepOnAllConsumers = true;
        } else {
            PipelineDepTree.TreeNode pipeline = pipelineDepTree.getNode(driverExec.getPipelineId());
            buildDepOnAllConsumers = pipeline.isBuildDepOnAllConsumers();
        }
    }

    @Override
    public void close() {

        //极端case依然会导致sourceExec 无法正常close，这里强制close一下.作为兜底策略
        if (forceClosed.compareAndSet(false, true)) {
            for (ProducerExecutor producerExecutor : driverExec.getForceCloseExecs()) {
                try {
                    producerExecutor.forceClose();
                } catch (Throwable t) {
                    log.warn("close producerExec happen error again!", t);
                }
            }
        }

        // mark the service for destruction
        if (!state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION)) {
            return;
        }

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        try (DriverLockResult lockResult = tryLockAndProcessPendingStateChanges(0, TimeUnit.MILLISECONDS)) {
            // if we did not get the lock, interrupt the lock holder
            if (!lockResult.wasAcquired()) {
                // there is a benign race condition here were the lock holder
                // can be change between attempting to get lock and grabbing
                // the synchronized lock here, but in either case we want to
                // interrupt the lock holder thread
                synchronized (this) {
                    if (lockHolder != null) {
                        lockHolder.interrupt();
                    }
                }
            }

            // clean shutdown is automatically triggered during lock release
        }
    }

    public boolean isFinished() {
        checkLockNotHeld("Can not check finished status while holding the driver lock");

        // if we can get the lock, attempt a clean shutdown; otherwise someone else will shutdown
        try (DriverLockResult lockResult = tryLockAndProcessPendingStateChanges(0, TimeUnit.MILLISECONDS)) {
            if (lockResult.wasAcquired()) {
                return isFinishedInternal();
            } else {
                // did not get the lock, so we can't check operators, or destroy
                return state.get() != State.ALIVE || driverContext.isDone();
            }
        }
    }

    private boolean isFinishedInternal() {
        checkLockHeld("Lock must be held to call isFinishedInternal");

        boolean end = state.get() != State.ALIVE || driverContext.isDone() || pipelineDepTree.hasParentFinish(
            driverExec.getPipelineId());
        if (end) {
            state.compareAndSet(State.ALIVE, State.NEED_DESTRUCTION);
        }
        return end;
    }

    public boolean matchSource(Integer sourceId) {
        if (sources.size() == 0 || !sources.contains(sourceId)) {
            return false;
        } else {
            return true;
        }
    }

    public Collection<Integer> getSources() {
        return sources;
    }

    public void updateSource(TaskSource source) {
        checkLockNotHeld("Can not update sources while holding the driver lock");

        // does this driver have an operator for the specified source?
        if (!matchSource(source.getIntegerId())) {
            return;
        }

        // stage the new updates
        while (true) {
            // attempt to update directly to the new source
            TaskSource currentNewSource = newSources.putIfAbsent(source.getIntegerId(), source);

            // if update succeeded, just break
            if (currentNewSource == null) {
                break;
            }

            // merge source into the current new source
            TaskSource newSource = currentNewSource.update(source);

            // if this is not a new source, just return
            if (newSource == currentNewSource) {
                break;
            }

            // attempt to replace the currentNewSource with the new source
            if (newSources.replace(source.getIntegerId(), currentNewSource, newSource)) {
                break;
            }

            // someone else updated while we were processing
        }

        // attempt to get the lock and process the updates we staged above
        // updates will be processed in close if and only if we got the lock
        tryLockAndProcessPendingStateChanges(0, TimeUnit.MILLISECONDS).close();
    }

    public void processNewSources() {
        // only update if the driver is still alive
        if (state.get() != State.ALIVE) {
            return;
        }

        // copy the pending sources
        // it is ok to "miss" a source added during the copy as it will be
        // handled on the next call to this method
        Map<Integer, TaskSource> sources = new HashMap<>(newSources);
        for (Map.Entry<Integer, TaskSource> entry : sources.entrySet()) {
            // Remove the entries we are going to process from the newSources map.
            // It is ok if someone already updated the entry; we will catch it on
            // the next iteration.
            newSources.remove(entry.getKey(), entry.getValue());

            if (driverExec.getSourceExecs().containsKey(entry.getKey())) {
                processNewSource(entry.getValue(), driverExec.getSourceExecs().get(entry.getKey()));
            }
        }
    }

    //for local mode
    public void processNewSources(Integer sourceId, List<Split> localSplits, boolean expand, boolean finish) {
        if (driverExec.getSourceExecs().containsKey(sourceId)) {
            List<SourceExec> sourceExecs = driverExec.getSourceExecs().get(sourceId);

            if (expand) {
                for (Split split : localSplits) {
                    for (SourceExec sourceExec : sourceExecs) {
                        sourceExec.addSplit(split);
                    }
                }
            } else {
                // add new splits
                int start = 0;
                for (Split split : localSplits) {
                    if (start >= sourceExecs.size()) {
                        start = 0;
                    }
                    sourceExecs.get(start++).addSplit(split);
                }
            }

            if (finish) {
                // set no more splits
                sourceExecs.stream().forEach(SourceExec::noMoreSplits);
            }
        }
    }

    private void processNewSource(TaskSource source, List<SourceExec> sourceExecs) {

        Preconditions.checkArgument(sourceExecs != null && sourceExecs.size() > 0, "sourceExec shouldn't be null!");
        // create new source
        Set<ScheduledSplit> newSplits;
        TaskSource currentSource = currentSources.get(source.getIntegerId());
        if (currentSource == null) {
            newSplits = source.getSplits();
            currentSources.put(source.getIntegerId(), source);
        } else {
            // merge the current source and the specified source
            TaskSource newSource = currentSource.update(source);

            // if this is not a new source, just return
            if (newSource == currentSource) {
                return;
            }

            // find the new splits to add
            newSplits = Sets.difference(newSource.getSplits(), currentSource.getSplits());
            currentSources.put(source.getIntegerId(), newSource);
        }

        if (source.isExpand() && sourceExecs.size() > 1) {
            for (ScheduledSplit newSplit : newSplits) {
                for (SourceExec sourceExec : sourceExecs) {
                    sourceExec.addSplit(newSplit.getSplit());
                }
            }
        } else {
            // add new splits
            int start = 0;
            for (ScheduledSplit newSplit : newSplits) {
                Split split = newSplit.getSplit();
                if (start >= sourceExecs.size()) {
                    start = 0;
                }
                sourceExecs.get(start++).addSplit(split);
            }
        }

        // set no more splits
        if (source.isNoMoreSplits()) {
            sourceExecs.stream().forEach(SourceExec::noMoreSplits);
        }
    }

    public ListenableFuture<?> processFor(long maxRuntime, long start) {
        checkLockNotHeld("Can not process for a duration while holding the driver lock");
        try (DriverLockResult lockResult = tryLockAndProcessPendingStateChanges(100, TimeUnit.MILLISECONDS)) {
            if (lockResult.wasAcquired()) {
                driverContext.startProcessTimer();
                driverContext.getYieldSignal().setWithDelay(
                    maxRuntime, driverContext.getPipelineContext().getTaskContext().getYieldExecutor());
                try {
                    do {
                        ListenableFuture<?> future = processInternal();
                        if (!future.isDone()) {
                            return future;
                        }
                    }
                    while (System.currentTimeMillis() - start < maxRuntime && !isFinishedInternal());
                } finally {
                    driverContext.getYieldSignal().reset();
                    driverContext.recordProcessed();
                }
            }
        }
        return NOT_BLOCKED;
    }

    private ListenableFuture<?> wakeUpOnRevokeRequest(ListenableFuture<?> sourceBlockedFuture) {
        if (!isSpill) {
            //mini optimizer! Don't wake up when the blockedFuture is not MemoryNotFuture.
            return sourceBlockedFuture;
        }
        revokingRequestedFuturesList.clear();
        revokingRequestedFuturesList.add(sourceBlockedFuture);
        for (MemoryRevoker memoryRevoker : driverExec.getMemoryRevokers()) {
            if (memoryRevoker.getMemoryAllocatorCtx() != null && memoryRevoker.getMemoryAllocatorCtx().isRevocable()) {
                SettableFuture<?> doneRevokingRequestedFuture =
                    memoryRevoker.getMemoryAllocatorCtx().getMemoryRevokingRequestedFuture();
                if (doneRevokingRequestedFuture.isDone()) {
                    return doneRevokingRequestedFuture;
                }
                revokingRequestedFuturesList.add(doneRevokingRequestedFuture);
            }
        }
        ListenableFuture<?> retFuture = firstFinishedFuture(revokingRequestedFuturesList);
        revokingRequestedFuturesList.clear();
        return retFuture;
    }

    private ListenableFuture<?> firstFinishedFuture(List<ListenableFuture<?>> futures) {
        if (futures.size() == 1) {
            return futures.get(0);
        }

        SettableFuture<?> result = SettableFuture.create();

        Runnable runnable = () -> {
            result.set(null);
        };
        for (ListenableFuture<?> future : futures) {
            future.addListener(runnable, directExecutor());
        }

        return result;
    }

    private void checkExecutorFinishedRevoking(MemoryRevoker memoryRevoker) {
        ListenableFuture<?> future = revokingExecutors.get(memoryRevoker);
        if (future.isDone()) {
            ExecUtils.checkException(future);
            revokingExecutors.remove(memoryRevoker);
            if (driverContext.getDriverId() != revokeDriverId) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "started driverId is not the finished driverId");
            }
            memoryRevoker.finishMemoryRevoke();
            if (memoryRevoker.getMemoryAllocatorCtx() != null) {
                memoryRevoker.getMemoryAllocatorCtx().resetMemoryRevokingRequested();
            }
        }
    }

    private ListenableFuture<?> handleMemoryRevoke() {
        if (isSpill) {
            if (!driverContext.isDone()) {
                for (MemoryRevoker memoryRevoker : driverExec.getMemoryRevokers()) {
                    if (revokingExecutors.containsKey(memoryRevoker)) {
                        checkExecutorFinishedRevoking(memoryRevoker);
                    } else if (memoryRevoker.getMemoryAllocatorCtx() != null
                        && memoryRevoker.getMemoryAllocatorCtx().isMemoryRevokingRequested()) {
                        revokeDriverId = driverContext.getDriverId();
                        ListenableFuture<?> future = memoryRevoker.startMemoryRevoke();
                        revokingExecutors.put(memoryRevoker, future);
                        checkExecutorFinishedRevoking(memoryRevoker);
                    }
                }
                ListenableFuture<?> revokingBlocked = NOT_BLOCKED;
                if (revokingExecutors.size() > 0) {
                    revokingBlocked = Futures.allAsList(
                        revokingExecutors.values().stream().collect(Collectors.toList()));
                }

                if (!revokingBlocked.isDone()) {
                    return revokingBlocked;
                }

                for (MemoryRevoker memoryRevoker : driverExec.getMemoryRevokers()) {
                    if (memoryRevoker.getMemoryAllocatorCtx() != null) {
                        ListenableFuture<?> blocked = memoryRevoker.getMemoryAllocatorCtx().isWaitingForMemory();
                        if (blocked != null && !blocked.isDone()) {
                            return blocked;
                        }
                        blocked = memoryRevoker.getMemoryAllocatorCtx().isWaitingForTryMemory();
                        if (blocked != null && !blocked.isDone()) {
                            return blocked;
                        }
                    }
                }
            }
        }
        return NOT_BLOCKED;
    }

    private ListenableFuture<?> processInternal() {

        try {
            boolean movedPage = false;

            if (!newSources.isEmpty()) {
                processNewSources();
            }

            boolean parentIsFinished = pipelineDepTree.hasParentFinish(driverExec.getPipelineId());
            ListenableFuture<?> blocked = NOT_BLOCKED;
            ListenableFuture<?> listenableFuture = pipelineDepTree.hasAllDepsFinish(driverExec.getPipelineId());
            if (parentIsFinished) {
                driverExec.close();
            } else if (listenableFuture.isDone() && !driverExec.isFinished()) {
                // 只有依赖的Pipeline结束了，才可以做MemoryRevoke，防止并发调用
                blocked = handleMemoryRevoke();
                if (blocked.isDone()) {
                    for (MemoryRevoker memoryRevoker : revokingExecutors.keySet()) {
                        checkExecutorFinishedRevoking(memoryRevoker);
                    }
                    driverExec.open();
                    Executor producer = driverExec.getProducer();
                    ConsumerExecutor consumer = driverExec.getConsumer();
                    if (driverExec.isProducerIsClosed()) {
                        // wait for all consumers of the pipeline to finish
                        blocked = buildConsumerAndClose(false);
                    } else if (consumer.needsInput()) {
                        try {
                            Chunk ret = producer.nextChunk();
                            if (ret != null) {
                                driverContext.addOutputSize(ret.getPositionCount());
                                consumer.consumeChunk(ret);
                                movedPage = true;
                                blocked = consumer.consumeIsBlocked();
                            } else {
                                boolean finished = driverExec.getProducer().produceIsFinished();
                                if (finished) {
                                    blocked = buildConsumerAndClose(true);
                                } else {
                                    blocked = driverExec.getProducer().produceIsBlocked();
                                }
                            }
                        } catch (Throwable e) {
                            log.error(driverContext.getUniqueId() + ":nextChunk error!", e);
                            if (!driverContext.isDone()) {
                                throw e;
                            }
                        }

                    } else {
                        if (consumer.consumeIsFinished()) {
                            blocked = buildConsumerAndClose(true);
                        } else {
                            blocked = consumer.consumeIsBlocked();
                        }
                    }
                }
            }

            if (!blocked.isDone()) {
                return wakeUpOnRevokeRequest(blocked);
            }

            if (driverExec.isFinished()) {
                driverContext.finished();
            }

            if (!movedPage && !driverExec.isFinished()) {
                return listenableFuture;
            }
            return NOT_BLOCKED;
        } catch (Throwable t) {
            driverContext.failed(t);
            throw t;
        }
    }

    private ListenableFuture<?> buildConsumerAndClose(boolean closeProducer) {
        if (driverContext.isDone()) {
            driverExec.close();
        } else {
            if (buildDepOnAllConsumers) {
                PipelineDepTree.TreeNode pipeline = pipelineDepTree.getNode(driverExec.getPipelineId());
                if (closeProducer) {
                    if (!flagConsumerFinished) {
                        pipeline.setDriverConsumerFinished();
                        flagConsumerFinished = true;
                    }
                    driverExec.closeProducer();
                }
                ListenableFuture<?> blocked = pipeline.getConsumerFuture();
                if (!blocked.isDone()) {
                    return blocked;
                }
            }
            driverExec.close();
        }
        return NOT_BLOCKED;
    }

    private DriverLockResult tryLockAndProcessPendingStateChanges(int timeout, TimeUnit unit) {
        checkLockNotHeld("Can not acquire the driver lock while already holding the driver lock");

        return new DriverLockResult(timeout, unit);
    }

    private synchronized void checkLockNotHeld(String message) {
        checkState(Thread.currentThread() != lockHolder, message);
    }

    private synchronized void checkLockHeld(String message) {
        checkState(Thread.currentThread() == lockHolder, message);
    }

    private class DriverLockResult
        implements AutoCloseable {
        private final boolean acquired;

        private DriverLockResult(int timeout, TimeUnit unit) {
            acquired = tryAcquire(timeout, unit);
        }

        private boolean tryAcquire(int timeout, TimeUnit unit) {
            boolean acquired = false;
            try {
                acquired = exclusiveLock.tryLock(timeout, unit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (acquired) {
                synchronized (Driver.this) {
                    lockHolder = Thread.currentThread();
                }
            }

            return acquired;
        }

        public boolean wasAcquired() {
            return acquired;
        }

        @Override
        public void close() {
            if (!acquired) {
                return;
            }

            boolean done = false;
            while (!done) {
                done = true;
                // before releasing the lock, process any new sources and/or destroy the driver
                try {
                    try {
                        processNewSources();
                    } finally {
                        destroyIfNecessary();
                    }
                } finally {
                    synchronized (Driver.this) {
                        lockHolder = null;
                    }
                    exclusiveLock.unlock();

                    // if new sources were added after we processed them, go around and try again
                    // in case someone else failed to acquire the lock and as a result won't update them
                    if (!newSources.isEmpty() && state.get() == State.ALIVE && tryAcquire(0, TimeUnit.MILLISECONDS)) {
                        done = false;
                    }
                }
            }
        }
    }

    private void destroyIfNecessary() {
        checkLockHeld("Lock must be held to call destroyIfNecessary");

        if (!state.compareAndSet(State.NEED_DESTRUCTION, State.DESTROYED)) {
            return;
        }

        // record the current interrupted status (and clear the flag); we'll reset it later
        boolean wasInterrupted = Thread.interrupted();

        // if we get an error while closing a driver, record it and we will throw it at the end
        Throwable inFlightException = null;
        try {
            Throwable lastError = null;
            try {
                driverExec.close();
            } catch (Throwable t) {
                lastError = t;
            }
            driverContext.finished();
            if (lastError != null) {
                throw lastError;
            }
        } catch (Throwable t) {
            // this shouldn't happen but be safe
            inFlightException = addSuppressedException(
                inFlightException,
                t,
                "Error destroying driver for task %s",
                driverContext.getTaskId());
        } finally {
            // reset the interrupted flag
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }

        if (inFlightException != null) {
            // this will always be an Error or Runtime
            throw Throwables.propagate(inFlightException);
        }
    }

    private static Throwable addSuppressedException(
        Throwable inFlightException, Throwable newException, String message, Object... args) {
        if (newException instanceof Error) {
            if (inFlightException == null) {
                inFlightException = newException;
            } else {
                // Self-suppression not permitted
                if (inFlightException != newException) {
                    inFlightException.addSuppressed(newException);
                }
            }
        } else {
            // log normal exceptions instead of rethrowing them
            String formatStr = String.format(message, args);
            log.error(formatStr, newException);
        }
        return inFlightException;
    }

    public DriverContext getDriverContext() {
        return driverContext;
    }
}
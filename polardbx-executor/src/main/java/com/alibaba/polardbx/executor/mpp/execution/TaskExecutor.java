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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.PriorityExecutorInfo;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.memory.ApMemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

@ThreadSafe
public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private static final AtomicLong NEXT_LOW_RUNNER_ID = new AtomicLong();
    private static final AtomicLong NEXT_HIGH_RUNNER_ID = new AtomicLong();
    private static final AtomicLong NEXT_WORKER_ID = new AtomicLong();

    private final int highRunnerThreads;
    private final int lowRunnerThreads;

    private final ExecutorService highExecutor;
    private final ExecutorService lowExecutor;

    private final ExecutorService blockedExecutor;

    /**
     * Splits waiting for tp runner thread
     */
    protected final BlockingQueue<PrioritizedSplitRunner> highPendingSplits;

    /**
     * Splits waiting for ap runner thread.
     */
    protected final BlockingQueue<PrioritizedSplitRunner> lowPendingSplits;

    /**
     * Splits blocked by the driver (typically output buffer is full or input buffer is empty).
     */
    protected final Map<PrioritizedSplitRunner, Future<?>> blockedSplits = new ConcurrentHashMap<>();

    private volatile boolean closed;

    private volatile long runnerLowProcessCount = 0;
    private volatile long runnerHighProcessCount = 0;

    private AtomicInteger runningHighSplits = new AtomicInteger(0);
    private AtomicInteger runningLowSplits = new AtomicInteger(0);

    private AtomicLong highCompletedSplitNum = new AtomicLong(0);
    private AtomicLong lowCompletedSplitNum = new AtomicLong(0);

    private AtomicInteger highBlockedSplitNum = new AtomicInteger(0);
    private AtomicInteger lowBlockedSplitNum = new AtomicInteger(0);

    @Inject
    public TaskExecutor() {

        this.lowRunnerThreads = Math.min(MppConfig.getInstance().getMaxWorkerThreads(),
            ThreadCpuStatUtil.NUM_CORES * MppConfig.getInstance().getTaskWorkerThreadsRatio());

        this.highRunnerThreads = Math.min(MppConfig.getInstance().getMaxWorkerThreads(),
            ThreadCpuStatUtil.NUM_CORES * MppConfig.getInstance().getTpTaskWorkerThreadsRatio());

        log.warn("TaskExecutor init with runnerThreads:" + lowRunnerThreads
            + ",CpuCfsPeriodUs=" + MppConfig.getInstance().getCpuCfsPeriodUs()
        );

        this.highExecutor = newFixedThreadPool(highRunnerThreads, Threads.tpDaemonThreadsNamed("tptask-runner"));
        this.lowExecutor = newFixedThreadPool(lowRunnerThreads, Threads.daemonThreadsNamed("aptask-runner"));

        this.blockedExecutor = newFixedThreadPool(
            Math.max(2,
                Math.min(MppConfig.getInstance().getTaskFutureCallbackThreads(), ThreadCpuStatUtil.NUM_CORES / 2)),
            new NamedThreadFactory("blockedSplits-processor", true));

        this.highPendingSplits = new LinkedBlockingQueue<>();
        this.lowPendingSplits = new LinkedBlockingQueue<>();
    }

    @PostConstruct
    public synchronized void start() {
        checkState(!closed, "TaskExecutor is closed");
        for (int i = 0; i < highRunnerThreads; i++) {
            addTpRunnerThread();
        }
        for (int i = 0; i < lowRunnerThreads; i++) {
            addApRunnerThread();
        }
        new Thread(new RunnerMonitor()).start();
        blockedExecutor.execute(new BlockedQueueMonitor());
    }

    @PreDestroy
    public synchronized void stop() {
        closed = true;
        highExecutor.shutdownNow();
        lowExecutor.shutdownNow();
    }

    private synchronized void addApRunnerThread() {
        try {
            lowExecutor.execute(new ApRunner());
        } catch (RejectedExecutionException ignored) {
        }
    }

    private synchronized void addTpRunnerThread() {
        try {
            highExecutor.execute(new TpRunner());
        } catch (RejectedExecutionException ignored) {
        }
    }

    public TaskHandle addTask(TaskId taskId) {
        requireNonNull(taskId, "taskId is null");
        TaskHandle taskHandle = new TaskHandle(taskId);
        return taskHandle;
    }

    public void removeTask(TaskHandle taskHandle) {
        try {
            taskHandle.destroy();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * @param highPriority true - 高优先级调度模式
     */
    public List<ListenableFuture<?>> enqueueSplits(TaskHandle taskHandle, boolean highPriority,
                                                   List<? extends SplitRunner> taskSplits) {
        List<PrioritizedSplitRunner> splitsToDestroy = new ArrayList<>();
        List<ListenableFuture<?>> finishedFutures = new ArrayList<>(taskSplits.size());

        for (SplitRunner taskSplit : taskSplits) {
            if (taskHandle == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "task already done, before SplitRunner start"
                );
            }
            PrioritizedSplitRunner prioritizedSplitRunner = new PrioritizedSplitRunner(taskHandle, taskSplit);

            if (taskHandle.isDestroyed()) {
                // If the handle is destroyed, we destroy the task splits to complete the future
                splitsToDestroy.add(prioritizedSplitRunner);
            } else {
                startSplit(prioritizedSplitRunner, highPriority);
            }

            finishedFutures.add(prioritizedSplitRunner.getFinishedFuture());
        }
        for (PrioritizedSplitRunner split : splitsToDestroy) {
            split.destroy();
        }
        return finishedFutures;
    }

    private void splitFinished(PrioritizedSplitRunner split) {
        split.destroy();
    }

    private void startSplit(PrioritizedSplitRunner split, boolean highPriority) {
        try {
            if (highPriority) {
                highPendingSplits.put(split);
            } else {
                lowPendingSplits.put(split);
            }
        } catch (Exception e) {
            log.error("error", e);
        }
    }

    public PriorityExecutorInfo getLowPriorityInfo() {
        ThreadPoolExecutor lowPriorityExecutor = getLowExecutor();
        PriorityExecutorInfo info =
            new PriorityExecutorInfo("LowTaskExecutor", lowPriorityExecutor.getPoolSize(),
                runningLowSplits.get(), runnerLowProcessCount, lowCompletedSplitNum.get(),
                lowPendingSplits.size(), lowBlockedSplitNum.get());
        return info;
    }

    public PriorityExecutorInfo getHighPriorityInfo() {
        ThreadPoolExecutor highPriorityExecutor = getHighExecutor();
        PriorityExecutorInfo info =
            new PriorityExecutorInfo("HighTaskExecutor", highPriorityExecutor.getPoolSize(),
                runningHighSplits.get(), runnerHighProcessCount, highCompletedSplitNum.get(),
                highPendingSplits.size(), highBlockedSplitNum.get());
        return info;
    }

    public ThreadPoolExecutor getHighExecutor() {
        return ((ThreadPoolExecutor) highExecutor);
    }

    public ThreadPoolExecutor getLowExecutor() {
        return ((ThreadPoolExecutor) lowExecutor);
    }

    private class BlockedQueueMonitor implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    int cleanIntervalSeconds = 5;
                    Thread.sleep(cleanIntervalSeconds * 1000);

                    log.debug("BlockedQueueMonitor run with interval seconds " + cleanIntervalSeconds);
                    for (Iterator<PrioritizedSplitRunner> ie = blockedSplits.keySet().iterator(); ie.hasNext(); ) {
                        PrioritizedSplitRunner split = ie.next();
                        if (split.getTaskHandle().isDestroyed()) {
                            if (!split.isDestroyed()) {
                                splitFinished(split);
                                log.info("blockedSplits:" + split.getTaskHandle().getTaskId() + " destroy");
                            }
                            ie.remove();
                        }
                    }
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    @ThreadSafe
    public static class TaskHandle {
        private final TaskId taskId;
        @GuardedBy("this")
        private boolean destroyed;

        private final AtomicInteger nextSplitId = new AtomicInteger();

        private TaskHandle(TaskId taskId) {
            this.taskId = taskId;
        }

        public TaskId getTaskId() {
            return taskId;
        }

        public synchronized boolean isDestroyed() {
            return destroyed;
        }

        // Returns any remaining splits. The caller must destroy these.
        private synchronized void destroy() {
            destroyed = true;
        }

        private int getNextSplitId() {
            return nextSplitId.getAndIncrement();
        }

        @Override
        public String toString() {
            return toStringHelper(this).add("taskId", taskId).toString();
        }
    }

    private static class PrioritizedSplitRunner
        implements Comparable<PrioritizedSplitRunner> {

        private static final AtomicLongFieldUpdater<PrioritizedSplitRunner> cpuTimeUpdater =
            AtomicLongFieldUpdater.newUpdater(PrioritizedSplitRunner.class, "cpuTimeLong");
        private static final AtomicLongFieldUpdater<PrioritizedSplitRunner> startUpdater =
            AtomicLongFieldUpdater.newUpdater(PrioritizedSplitRunner.class, "startLong");
        private static final AtomicLongFieldUpdater<PrioritizedSplitRunner> processCallsUpdater =
            AtomicLongFieldUpdater.newUpdater(PrioritizedSplitRunner.class, "processCallsLong");
        private static final AtomicLongFieldUpdater<PrioritizedSplitRunner> spiltCostUpdater =
            AtomicLongFieldUpdater.newUpdater(PrioritizedSplitRunner.class, "spiltCostLong");
        private final TaskHandle taskHandle;
        private final int splitId;
        private final long workerId;
        private final SplitRunner split;
        private final SettableFuture<?> finishedFuture = SettableFuture.create();
        private final AtomicBoolean destroyed = new AtomicBoolean();

        private volatile long startLong = 0L;
        private volatile long cpuTimeLong = 0L;
        private volatile long processCallsLong = 0L;
        private volatile long spiltCostLong = 0L;

        // each time we run a split, run it for this length before returning to the pool
        public final long splitRunQuanta;

        private PrioritizedSplitRunner(TaskHandle taskHandle, SplitRunner split) {
            this.taskHandle = taskHandle;
            this.splitId = taskHandle.getNextSplitId();
            this.split = split;
            this.workerId = NEXT_WORKER_ID.getAndIncrement();
            this.splitRunQuanta = MppConfig.getInstance().getSplitRunQuanta();
        }

        public long getSplitCost() {
            return spiltCostUpdater.get(this);
        }

        public void spiltCostAdd(long cost) {
            spiltCostUpdater.getAndAdd(this, cost);
        }

        public boolean moveToLowQueue(long cost) {
            return split.moveLowPrioritizedQuery(cost);
        }

        public void recordBlocked() {
            if (!isDestroyed() && split != null) {
                split.recordBlocked();
            }
        }

        public void recordBlockedFinished() {
            if (!isDestroyed() && split != null) {
                split.recordBlockedFinished();
            }
        }

        private TaskHandle getTaskHandle() {
            return taskHandle;
        }

        private ListenableFuture<?> getFinishedFuture() {
            return finishedFuture;
        }

        public boolean isDestroyed() {
            return destroyed.get();
        }

        public void buildMDC() {
            if (!isDestroyed() && split != null) {
                split.buildMDC();
            }
        }

        public void destroy() {
            destroyed.set(true);
            try {
                split.close();
            } catch (RuntimeException e) {
                log.error("Error closing split for task " + taskHandle.getTaskId(), e);
            }
        }

        public boolean isFinished() {
            boolean finished = split.isFinished();
            if (finished) {
                finishedFuture.set(null);
            }
            return finished || destroyed.get() || taskHandle.isDestroyed();
        }

        public ListenableFuture<?> process(long start)
            throws Exception {
            try {
                //                start.compareAndSet(0, System.currentTimeMillis());
                processCallsUpdater.incrementAndGet(this);
                ListenableFuture<?> blocked = split.processFor(splitRunQuanta, start);
                return blocked;
            } catch (Throwable e) {
                finishedFuture.setException(e);
                throw e;
            }
        }

        @Override
        public int compareTo(PrioritizedSplitRunner o) {
            return Long.compare(workerId, o.workerId);
        }

        public int getSplitId() {
            return splitId;
        }

        public String getInfo() {
            return String.format("Split %-15s-%d %s (start = %s, wall = %s ms, cpu = %s ms, calls = %s)",
                taskHandle.getTaskId(),
                splitId,
                split.getInfo(),
                startUpdater.get(this),
                System.currentTimeMillis() - startUpdater.get(this),
                (int) (cpuTimeUpdater.get(this) / 1.0e6),
                processCallsUpdater.get(this));
        }

        @Override
        public String toString() {
            return String.format("Split %-15s-%d", split.getInfo(), splitId);
        }
    }

    private class RunnerMonitor implements Runnable {
        private long lastLowRunnerProcessCount = 0;
        private long lastHighRunnerProcessCount = 0;

        private void logPriorityExecutorInfo(PriorityExecutorInfo priorityExecutorInfo, long lastRunnerProcessCount) {
            long runnerProcessRate = (priorityExecutorInfo.getRunnerProcessCount() - lastRunnerProcessCount) / 30;
            log.info(
                " [Task executor - " + priorityExecutorInfo.getName() + " ] " + " pool size: " + priorityExecutorInfo
                    .getPoolSize()
                    + " active count: " + priorityExecutorInfo.getActiveCount() + " pending splits: "
                    + priorityExecutorInfo.getPendingSplitsSize()
                    + " blocked splits: " + priorityExecutorInfo.getBlockedSplitSize()
                    + " last time runner process count: " + lastRunnerProcessCount
                    + " this time runner process count: " + priorityExecutorInfo.getRunnerProcessCount()
                    + " runner process rate: " + runnerProcessRate
            );
        }

        @Override
        public void run() {
            while (true) {
                try {
                    if (log.isInfoEnabled()) {
                        PriorityExecutorInfo lowPriorityExecutorInfo = getLowPriorityInfo();
                        logPriorityExecutorInfo(lowPriorityExecutorInfo, lastLowRunnerProcessCount);
                        lastLowRunnerProcessCount = lowPriorityExecutorInfo.getRunnerProcessCount();
                        PriorityExecutorInfo highPriorityExecutorInfo = getHighPriorityInfo();
                        logPriorityExecutorInfo(highPriorityExecutorInfo, lastHighRunnerProcessCount);
                        lastHighRunnerProcessCount = highPriorityExecutorInfo.getRunnerProcessCount();
                        ApMemoryPool apMemoryPool = MemoryManager.getInstance().getApMemoryPool();
                        log.info("Global Memory Pool: used " +
                            MemoryManager.getInstance().getGlobalMemoryPool().getMemoryUsage() +
                            " total " + MemoryManager.getInstance().getGlobalMemoryPool().getMaxLimit());
                        /**
                         * 超过30分钟，ap的MemoryPool都没有超过高水位线时，清除ap的并发度限制
                         */
                        if (apMemoryPool.getApSemaphore() != null && apMemoryPool.getOverloadTime() != null
                            && (System.currentTimeMillis() - apMemoryPool.getOverloadTime()) > 1800000L) {
                            apMemoryPool.setApSemaphore(null);
                        }

                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }

                try {
                    Thread.sleep(1000 * 30L);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }
        }
    }

    private class ApRunner implements Runnable {
        private final long runnerId = NEXT_LOW_RUNNER_ID.getAndIncrement();
        private boolean isRunning = false;
        // flag used to record if current thread has record as running
        private boolean runningCounted = false;

        @Override
        public void run() {
            //现在MPP在执行过程中都有可能使用DrdsRelMetadataProvider，所以这里在各个运行线程统一设置吧。
            RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(DrdsRelMetadataProvider.INSTANCE));

            runningCounted = true;
            runningLowSplits.incrementAndGet();
            try (SetThreadName runnerName = new SetThreadName("ApSplitRunner-%s", runnerId)) {
                while (!closed && !Thread.currentThread().isInterrupted()) {
                    // select next worker
                    final PrioritizedSplitRunner split;
                    try {
                        runningCounted = false;
                        runningLowSplits.decrementAndGet();
                        split = lowPendingSplits.take();
                        split.buildMDC();
                        runningCounted = true;
                        runningLowSplits.incrementAndGet();
                        isRunning = true;
                        runnerLowProcessCount++;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        isRunning = false;
                        return;
                    }

                    try {
                        ListenableFuture<?> blocked;
                        if (split.getTaskHandle().isDestroyed()) {
                            if (!split.isDestroyed()) {
                                split.destroy();
                            }
                        } else {
                            long start = System.currentTimeMillis();
                            if (log.isDebugEnabled()) {
                                log.debug(String.format("%s is started", split.getInfo()));
                            }
                            blocked = split.process(start);
                            long cost = System.currentTimeMillis() - start;
                            try {
                                split.spiltCostAdd(cost);
                            } catch (Exception e) {
                                log.error("error", e);
                            }
                            if (split.isFinished()) {
                                lowCompletedSplitNum.getAndIncrement();
                                if (log.isDebugEnabled()) {
                                    log.debug(String.format("%s is finished", split.getInfo()));
                                    log.debug(String.format("%s", "Runner split.getTaskHandle().getTaskId()"
                                        + split.getTaskHandle().getTaskId() + " and query:"
                                        + " split cost:" + split.getSplitCost()));
                                }
                                splitFinished(split);
                            } else {
                                if (blocked.isDone()) {
                                    lowPendingSplits.put(split);
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug(String.format("%s is bloked", split.getInfo()));
                                    }
                                    lowBlockedSplitNum.getAndIncrement();
                                    blockedSplits.put(split, blocked);
                                    split.recordBlocked();
                                    blocked.addListener(() -> {
                                        if (log.isDebugEnabled()) {
                                            log.debug(String.format("%s is pending", split.getInfo()));
                                        }
                                        split.recordBlockedFinished();
                                        lowBlockedSplitNum.getAndDecrement();
                                        blockedSplits.remove(split);
                                        try {
                                            lowPendingSplits.put(split);
                                        } catch (Exception e) {
                                            log.error("error", e);
                                        }
                                    }, blockedExecutor);
                                }
                            }
                        }
                    } catch (Throwable t) {
                        if (split.isDestroyed()) {
                            log.error("Error driver thread interruption for " + t.getMessage());
                        } else if (t instanceof TddlRuntimeException) {
                            TddlRuntimeException e = (TddlRuntimeException) t;
                            log.error(String
                                .format("Error processing %s: %s: %s", split.getInfo(), e.getErrorCodeType().getName(),
                                    e.getMessage()), t);
                        } else {
                            log.error("Error processing " + split.getInfo(), t);
                        }
                        splitFinished(split);
                    } finally {
                        isRunning = false;
                    }
                }
            } finally {
                if (runningCounted) {
                    runningLowSplits.decrementAndGet();
                }
                log.error("task executor runner encounter exception and thread exit");
                if (!closed) {
                    addApRunnerThread();
                }
            }
        }

        public boolean isRunning() {
            return isRunning;
        }
    }

    private class TpRunner implements Runnable {
        private final long runnerId = NEXT_HIGH_RUNNER_ID.getAndIncrement();
        private boolean isRunning = false;
        // flag used to record if current thread has record as running
        private boolean runningCounted = false;

        @Override
        public void run() {
            //现在MPP在执行过程中都有可能使用DrdsRelMetadataProvider，所以这里在各个运行线程统一设置吧。
            RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(DrdsRelMetadataProvider.INSTANCE));

            runningCounted = true;
            runningHighSplits.incrementAndGet();
            try (SetThreadName runnerName = new SetThreadName("TpSplitRunner-%s", runnerId)) {
                while (!closed && !Thread.currentThread().isInterrupted()) {
                    // select next worker
                    final PrioritizedSplitRunner split;
                    try {
                        runningCounted = false;
                        runningHighSplits.decrementAndGet();
                        split = highPendingSplits.take();
                        split.buildMDC();
                        runningCounted = true;
                        runningHighSplits.incrementAndGet();
                        isRunning = true;
                        runnerHighProcessCount++;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        isRunning = false;
                        return;
                    }

                    try {
                        if (split.getTaskHandle().isDestroyed()) {
                            if (!split.isDestroyed()) {
                                split.destroy();
                            }
                        } else {
                            long start = System.currentTimeMillis();
                            if (log.isDebugEnabled()) {
                                log.debug(String.format("%s is started", split.getInfo()));
                            }
                            ListenableFuture<?> blocked = split.process(start);
                            long cost = System.currentTimeMillis() - start;
                            split.spiltCostAdd(cost);
                            final boolean moveToLowQueue = split.moveToLowQueue(cost);
                            if (split.isFinished()) {
                                highCompletedSplitNum.getAndIncrement();
                                if (log.isDebugEnabled()) {
                                    log.debug(String.format("%s is finished", split.getInfo()));
                                    log.debug(String.format("%s", "Runner split.getTaskHandle().getTaskId()"
                                        + split.getTaskHandle().getTaskId() + " and query:"
                                        + " split cost:" + split.getSplitCost()));
                                }
                                splitFinished(split);
                            } else {
                                if (blocked.isDone()) {
                                    if (moveToLowQueue) {
                                        lowPendingSplits.put(split);
                                    } else {
                                        highPendingSplits.put(split);
                                    }
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug(String.format("%s is bloked", split.getInfo()));
                                    }
                                    highBlockedSplitNum.getAndIncrement();
                                    blockedSplits.put(split, blocked);
                                    split.recordBlocked();
                                    blocked.addListener(() -> {
                                        if (log.isDebugEnabled()) {
                                            log.debug(String.format("%s is pending", split.getInfo()));
                                        }
                                        split.recordBlockedFinished();
                                        highBlockedSplitNum.getAndDecrement();
                                        blockedSplits.remove(split);
                                        try {
                                            if (moveToLowQueue) {
                                                lowPendingSplits.put(split);
                                            } else {
                                                highPendingSplits.put(split);
                                            }
                                        } catch (Exception e) {
                                            log.error("error", e);
                                        }
                                    }, blockedExecutor);
                                }
                            }
                        }
                    } catch (Throwable t) {
                        if (split.isDestroyed()) {
                            log.error("Error driver thread interruption for " + t.getMessage());
                        } else if (t instanceof TddlRuntimeException) {
                            TddlRuntimeException e = (TddlRuntimeException) t;
                            log.error(String
                                .format("Error processing %s: %s: %s", split.getInfo(), e.getErrorCodeType().getName(),
                                    e.getMessage()), t);
                        } else {
                            log.error("Error processing " + split.getInfo(), t);
                        }
                        splitFinished(split);
                    } finally {
                        isRunning = false;
                    }
                }
            } finally {
                if (runningCounted) {
                    runningHighSplits.decrementAndGet();
                }
                log.error("task executor runner encounter exception and thread exit");
                if (!closed) {
                    addTpRunnerThread();
                }
            }
        }

        public boolean isRunning() {
            return isRunning;
        }
    }
}

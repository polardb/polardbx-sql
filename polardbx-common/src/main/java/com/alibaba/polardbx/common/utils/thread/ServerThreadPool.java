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

package com.alibaba.polardbx.common.utils.thread;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.alibaba.polardbx.common.utils.BooleanMutex;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


public class ServerThreadPool extends AbstractExecutorService {

    public final static Logger logger = LoggerFactory.getLogger(ServerThreadPool.class);
    public static final String SERVER_THREAD_POOL_TIME_CHECK = "ServerThreadPool-TimeCheck";
    private static final String DEFAULT_SCHEMA_NAME = "__drds_system__";
    private String poolName;
    private int poolSize = 1024;
    private int deadLockCheckPeriod = 5 * 100;
    private ThreadPoolExecutor executor;

    private int numBuckets;
    private ThreadPoolExecutor[] executorBuckets;
    private AtomicLong roundrobinIndex = new AtomicLong(0);

    private AtomicBoolean repairing = new AtomicBoolean(false);
    private long lastCompletedTaskCount = 0;
    private long[] lastCompletedTaskCountBuckets;
    private Timer timer;
    private TimerTask checkTask = null;
    private LoadingCache<String, AppStats> appStats = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, AppStats>() {

            @Override
            public AppStats load(String key) throws Exception {
                AppStats stat = new AppStats();
                stat.taskCount = new AtomicLong(0);
                return stat;
            }
        });
    private LoadingCache<String, TraceStats> traceStats = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, TraceStats>() {

            @Override
            public TraceStats load(String key) throws Exception {
                TraceStats stat = new TraceStats();
                stat.closed = new AtomicBoolean(false);
                stat.count = new AtomicLong(0);
                stat.mutex = new BooleanMutex(true);
                return stat;
            }
        });

    public ServerThreadPool(String poolName, int poolSize, int deadLockCheckPeiod) {
        this.poolSize = poolSize;
        this.poolName = poolName;
        this.deadLockCheckPeriod = deadLockCheckPeiod;
        this.executor = new ThreadPoolExecutor(poolSize,
            poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory(poolName, true));


        if (deadLockCheckPeiod > 0) {
            this.timer = new Timer(SERVER_THREAD_POOL_TIME_CHECK, true);
            buildCheckTask();
            this.timer.scheduleAtFixedRate(checkTask, deadLockCheckPeiod, deadLockCheckPeiod);
        }
    }

    public ServerThreadPool(String poolName, int poolSize, int deadLockCheckPeriod, int bucketSize) {
        this.poolName = poolName;
        this.deadLockCheckPeriod = deadLockCheckPeriod;

        this.numBuckets = bucketSize;
        this.executorBuckets = new ThreadPoolExecutor[bucketSize];
        int bucketPoolSize = poolSize / bucketSize;
        this.poolSize = bucketPoolSize;
        for (int bucketIndex = 0; bucketIndex < bucketSize; bucketIndex++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(bucketPoolSize,
                bucketPoolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory(poolName + "-bucket-" + bucketIndex, true));

            executorBuckets[bucketIndex] = executor;
        }

        this.lastCompletedTaskCountBuckets = new long[bucketSize];

        if (deadLockCheckPeriod > 0) {
            this.timer = new Timer(SERVER_THREAD_POOL_TIME_CHECK, true);
            buildCheckTask();
            this.timer.scheduleAtFixedRate(checkTask, deadLockCheckPeriod, deadLockCheckPeriod);
        }
    }

    public Future<?> submit(String schemaName, String traceId, Runnable task) {
        return submit(schemaName, traceId, -1, task);
    }

    public Future<?> submit(String schemaName, String traceId, int bucketIndex, Runnable task) {
        return submit(schemaName, traceId, bucketIndex, task, null);
    }

    public Future<?> submit(String schemaName, String traceId, int bucketIndex, Runnable task,
                            BlockingQueue completionQueue) {
        return submit(schemaName, traceId, bucketIndex, task, completionQueue, null);
    }

    public Future<?> submit(String schemaName, String traceId, int bucketIndex, Runnable task,
                            BlockingQueue completionQueue, CpuCollector cpuCollector) {
        if (task == null) {
            throw new NullPointerException();
        }

        if (bucketIndex < 0) {
            bucketIndex = hashBucketIndex(schemaName, traceId);
        } else {
            bucketIndex = bucketIndex % numBuckets;
        }
        NamedFutureTask<Object> ftask =
            newTaskForRunnable(schemaName, traceId, task, null, completionQueue, cpuCollector);
        execute(bucketIndex, ftask);
        return ftask;
    }

    public <T> Future<T> submit(String schemaName, String traceId, Callable<T> task) {
        return submit(schemaName, traceId, -1, task, null, null);
    }

    public <T> Future<T> submit(String schemaName, String traceId, Callable<T> task, BlockingQueue completionQueue) {
        return submit(schemaName, traceId, -1, task, completionQueue, null);
    }

    public <T> Future<T> submit(String schemaName, String traceId, int bucketIndex, Callable<T> task) {
        return submit(schemaName, traceId, bucketIndex, task, null, null);
    }

    public <T> Future<T> submit(String schemaName, String traceId, int bucketIndex, Callable<T> task,
                                BlockingQueue completionQueue) {
        return submit(schemaName, traceId, bucketIndex, task, completionQueue, null);
    }

    public <T> Future<T> submit(String schemaName, String traceId, int bucketIndex, Callable<T> task,
                                BlockingQueue completionQueue, CpuCollector cpuCollector) {
        if (task == null) {
            throw new NullPointerException();
        }

        if (bucketIndex < 0) {
            bucketIndex = hashBucketIndex(schemaName, traceId);
        } else {
            bucketIndex = bucketIndex % numBuckets;
        }
        NamedFutureTask<T> ftask = newTaskForCallable(schemaName, traceId, task, completionQueue, cpuCollector);
        execute(bucketIndex, ftask);
        return ftask;
    }

    public <T> ListenableFuture<T> submitListenableFuture(
        String schemaName, String traceId, int bucketIndex, Callable<T> task, CpuCollector cpuCollector) {
        if (task == null) {
            throw new NullPointerException();
        }
        if (bucketIndex < 0) {
            bucketIndex = hashBucketIndex(schemaName, traceId);
        } else {
            bucketIndex = bucketIndex % numBuckets;
        }
        AppStats stat = null;
        if (!DynamicConfig.getInstance().enableExtremePerformance()) {
            stat = appStats.getUnchecked(schemaName);
            stat.taskCount.incrementAndGet();
            AppStats.nodeTaskCount.incrementAndGet();
        }
        try {
            if (executorBuckets != null) {
                return MoreExecutors.listeningDecorator(executorBuckets[bucketIndex])
                    .submit(new ListenCallableAdapter(schemaName, task, traceId, cpuCollector));
            } else {
                return MoreExecutors.listeningDecorator(executor)
                    .submit(new ListenCallableAdapter(schemaName, task, traceId, cpuCollector));
            }
        } catch (Throwable e) {

            decreTaskCount(stat);
            throw e;
        }
    }

    private int hashBucketIndex(String schemaName, String traceId) {
        if (executorBuckets == null) {

            return 0;
        }

        long index = 0;
        if (StringUtils.isNotEmpty(traceId)) {
            index = traceId.hashCode();
        } else if (StringUtils.isNotEmpty(schemaName)) {
            index = schemaName.hashCode();
        } else {
            index = roundrobinIndex.getAndIncrement();
        }
        int result = (int) index & Integer.MAX_VALUE;
        return result % numBuckets;
    }

    private void execute(int bucketIndex, NamedFutureTask command) {
        AppStats stat = null;
        if (!DynamicConfig.getInstance().enableExtremePerformance()) {
            stat = appStats.getUnchecked(command.getSchemaName());
            stat.taskCount.incrementAndGet();
            AppStats.nodeTaskCount.incrementAndGet();
        }

        try {
            if (executorBuckets != null) {
                executorBuckets[bucketIndex].execute(command);
            } else {
                executor.execute(command);
            }
        } catch (Throwable e) {

            decreTaskCount(stat);
            throw e;
        }
    }

    @Override
    public void execute(Runnable command) {
        AppStats stat = null;
        if (!DynamicConfig.getInstance().enableExtremePerformance()) {
            stat = appStats.getUnchecked(DEFAULT_SCHEMA_NAME);
            stat.taskCount.incrementAndGet();
        }

        try {
            if (executorBuckets != null) {
                int bucketIndex = (int) (roundrobinIndex.getAndIncrement() % numBuckets);
                executorBuckets[bucketIndex].execute(command);
            } else {
                executor.execute(command);
            }
        } catch (Throwable e) {

            if (!DynamicConfig.getInstance().enableExtremePerformance() && stat != null) {
                stat.taskCount.decrementAndGet();
            }
            throw e;
        }
    }

    protected <T> ServerThreadPool.NamedFutureTask<T> newTaskForRunnable(String schemaName, String traceId, Runnable
        runnable,
                                                                         T value,
                                                                         BlockingQueue completionQueue,
                                                                         CpuCollector cpuCollector) {
        return new ServerThreadPool.NamedFutureTask<T>(schemaName, traceId, runnable, value, completionQueue,
            cpuCollector);
    }

    protected <T> ServerThreadPool.NamedFutureTask<T> newTaskForCallable(String schemaName, String traceId, Callable<T>
        callable,
                                                                         BlockingQueue completionQueue,
                                                                         CpuCollector cpuCollector) {
        return new ServerThreadPool.NamedFutureTask<T>(schemaName, traceId, callable, completionQueue, cpuCollector);
    }

    public BlockingQueue<Runnable> getQueue() {
        return executor.getQueue();
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    private boolean isPossibleDeadLock(ThreadPoolExecutor executor, long lastCompletedTaskCount) {

        return (lastCompletedTaskCount == executor.getCompletedTaskCount()) && (executor.getActiveCount() >= executor
            .getCorePoolSize());
    }

    private boolean isDeadLockFixed(ThreadPoolExecutor executor, long lastCompletedTaskCount) {

        return (lastCompletedTaskCount < executor.getCompletedTaskCount()) && (executor.getActiveCount() < poolSize);
    }

    private boolean buildCheckTask() {
        if (this.checkTask != null) {
            if (!this.checkTask.cancel()) {
                return false;
            }
        }

        this.checkTask = new TimerTask() {

            @Override
            public void run() {
                try {
                    if (executorBuckets != null) {
                        for (int bucketIndex = 0; bucketIndex < executorBuckets.length; bucketIndex++) {
                            ThreadPoolExecutor executor = executorBuckets[bucketIndex];
                            if ((executor.getCorePoolSize() < (poolSize << 2)) && isPossibleDeadLock(executor,
                                lastCompletedTaskCountBuckets[bucketIndex])) {

                                int addPermit = poolSize / 10;
                                if (addPermit == 0) {
                                    addPermit = 1;
                                }

                                int newPoolSize = executor.getCorePoolSize() + addPermit;
                                executor.setMaximumPoolSize(newPoolSize);
                                executor.setCorePoolSize(newPoolSize);
                                logger.warn("bucket " + bucketIndex + " possible deadlock found, change poolSize to: "
                                    + newPoolSize);
                            } else if ((executor.getCorePoolSize() > poolSize) && isDeadLockFixed(executor,
                                lastCompletedTaskCountBuckets[bucketIndex])) {

                                int newPoolSize = poolSize;
                                executor.setCorePoolSize(newPoolSize);
                                executor.setMaximumPoolSize(newPoolSize);
                                logger.warn(
                                    "bucket " + bucketIndex + " deadlock fixed, reset poolSize to: " + newPoolSize);
                            }

                            lastCompletedTaskCountBuckets[bucketIndex] = executor.getCompletedTaskCount();
                        }

                    } else {
                        if ((executor.getCorePoolSize() < (poolSize << 2)) && isPossibleDeadLock(executor,
                            lastCompletedTaskCount)) {

                            int addPermit = poolSize / 10;
                            if (addPermit == 0) {
                                addPermit = 1;
                            }

                            int newPoolSize = executor.getCorePoolSize() + addPermit;
                            executor.setMaximumPoolSize(newPoolSize);
                            executor.setCorePoolSize(newPoolSize);
                            logger.warn("possible deadlock found, change poolSize to: " + newPoolSize);
                        } else if ((executor.getCorePoolSize() > poolSize) && isDeadLockFixed(executor,
                            lastCompletedTaskCount)) {

                            int newPoolSize = poolSize;
                            executor.setCorePoolSize(newPoolSize);
                            executor.setMaximumPoolSize(newPoolSize);
                            logger.warn("deadlock fixed, reset poolSize to: " + newPoolSize);
                        }

                        lastCompletedTaskCount = executor.getCompletedTaskCount();
                    }
                } catch (Throwable e) {
                    logger.error(e);
                }
            }
        };

        return true;
    }

    public void initTraceStats(String traceId) {
        traceStats.getUnchecked(traceId);
    }

    public void closeByTraceId(String traceId) {
        TraceStats traceStat = traceStats.getIfPresent(traceId);
        if (traceStat != null) {
            traceStat.closed.set(true);
        } else {

        }
    }

    public void waitByTraceId(String traceId) throws InterruptedException {
        TraceStats stats = traceStats.getIfPresent(traceId);
        if (stats != null) {

            try {
                stats.mutex.get();
            } finally {

                traceStats.invalidate(traceId);
            }
        } else {

            return;
        }
    }

    public long getTaskCountBySchemaName(String schemaName) {
        try {
            if (!DynamicConfig.getInstance().enableExtremePerformance()) {
                AppStats stat = appStats.get(schemaName);
                return stat.taskCount.get();
            } else {
                return 0;
            }

        } catch (ExecutionException e) {
            return 0;
        }
    }

    public void setDeadLockCheckPeriod(int deadLockCheckPeriod) {
        if (this.deadLockCheckPeriod != deadLockCheckPeriod) {
            if (this.timer == null) {
                this.timer = new Timer(SERVER_THREAD_POOL_TIME_CHECK, true);
            }
            if (buildCheckTask()) {
                this.timer.scheduleAtFixedRate(checkTask, deadLockCheckPeriod, deadLockCheckPeriod);
            }
            this.deadLockCheckPeriod = deadLockCheckPeriod;
        }
    }

    public void shutdown() {
        if (executorBuckets != null) {
            for (ThreadPoolExecutor executor : executorBuckets) {
                executor.shutdown();
            }
        } else {
            executor.shutdown();
        }
    }

    public List<Runnable> shutdownNow() {
        if (executorBuckets != null) {
            List result = Lists.newArrayList();
            for (ThreadPoolExecutor executor : executorBuckets) {
                result.addAll(executor.shutdownNow());
            }
            return result;
        } else {
            return executor.shutdownNow();
        }
    }

    public boolean isShutdown() {
        if (executorBuckets != null) {
            boolean isShutdown = true;
            for (ThreadPoolExecutor executor : executorBuckets) {
                isShutdown &= executor.isShutdown();
            }
            return isShutdown;
        } else {
            return executor.isShutdown();
        }
    }

    public boolean isTerminated() {
        if (executorBuckets != null) {
            boolean isTerminated = true;
            for (ThreadPoolExecutor executor : executorBuckets) {
                isTerminated &= executor.isTerminated();
            }
            return isTerminated;
        } else {
            return executor.isTerminated();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (executorBuckets != null) {
            boolean flag = true;
            for (ThreadPoolExecutor executor : executorBuckets) {
                flag &= executor.awaitTermination(timeout, unit);
            }
            return flag;
        } else {
            return executor.awaitTermination(timeout, unit);
        }
    }

    public int getPoolSize() {
        if (executorBuckets != null) {
            int poolSize = 0;
            for (ThreadPoolExecutor executor : executorBuckets) {
                poolSize += executor.getCorePoolSize();
            }
            return poolSize;
        } else {
            return executor.getCorePoolSize();
        }
    }

    public void setPoolSize(int poolSize) {
        if (executorBuckets != null) {

            int totalPoolSize = this.poolSize * numBuckets;
            if (poolSize != totalPoolSize) {
                int newBucketPoolSize = poolSize / numBuckets;
                for (ThreadPoolExecutor executor : executorBuckets) {
                    if (totalPoolSize > poolSize) {

                        executor.setCorePoolSize(newBucketPoolSize);
                        executor.setMaximumPoolSize(newBucketPoolSize);
                    } else if (this.poolSize < poolSize) {

                        executor.setMaximumPoolSize(newBucketPoolSize);
                        executor.setCorePoolSize(newBucketPoolSize);
                    }
                }

                this.poolSize = newBucketPoolSize;
            }
        } else if (executor != null) {
            if (this.poolSize > poolSize) {

                executor.setCorePoolSize(poolSize);
                executor.setMaximumPoolSize(poolSize);
            } else if (this.poolSize < poolSize) {

                executor.setMaximumPoolSize(poolSize);
                executor.setCorePoolSize(poolSize);
            }

            this.poolSize = poolSize;
        }
    }

    public int getActiveCount() {
        if (executorBuckets != null) {
            int activeCount = 0;
            for (ThreadPoolExecutor executor : executorBuckets) {
                activeCount += executor.getActiveCount();
            }
            return activeCount;
        } else {
            return executor.getActiveCount();
        }
    }

    public int getQueuedCount() {
        if (executorBuckets != null) {
            int queueSize = 0;
            for (ThreadPoolExecutor executor : executorBuckets) {
                queueSize += executor.getQueue().size();
            }
            return queueSize;
        } else {
            return executor.getQueue().size();
        }
    }

    public long getCompletedTaskCount() {
        if (executorBuckets != null) {
            long completedTaskCount = 0;
            for (ThreadPoolExecutor executor : executorBuckets) {
                completedTaskCount += executor.getCompletedTaskCount();
            }
            return completedTaskCount;
        } else {
            return executor.getCompletedTaskCount();
        }
    }

    public long getTaskCount() {
        if (executorBuckets != null) {
            long taskCount = 0;
            for (ThreadPoolExecutor executor : executorBuckets) {
                taskCount += executor.getTaskCount();
            }
            return taskCount;
        } else {
            return executor.getTaskCount();
        }
    }

    public static final class AppStats {

        public AtomicLong taskCount;

        public static AtomicLong nodeTaskCount = new AtomicLong(0);
    }

    public static class TraceStats {

        public AtomicBoolean closed;
        public AtomicLong count;
        public BooleanMutex mutex;
    }

    public class NamedFutureTask<V> extends FutureTask<V> {

        private String schemaName;
        private String traceId;
        private BlockingQueue completionQueue;

        public NamedFutureTask(String schemaName, String traceId, Callable<V> callable, BlockingQueue completionQueue,
                               CpuCollector cpuCollector) {
            super(new CallableAdapter<V>(callable, traceId, cpuCollector));
            if (StringUtils.isEmpty(schemaName)) {
                this.schemaName = DEFAULT_SCHEMA_NAME;
            } else {
                this.schemaName = schemaName;
            }
            this.traceId = traceId;
            this.completionQueue = completionQueue;
        }

        public NamedFutureTask(String schemaName, String traceId, Runnable runnable, V result,
                               BlockingQueue completionQueue, CpuCollector cpuCollector) {
            super(new RunnableAdapter(runnable, traceId, cpuCollector), result);
            if (StringUtils.isEmpty(schemaName)) {
                this.schemaName = DEFAULT_SCHEMA_NAME;
            } else {
                this.schemaName = schemaName;
            }
            this.traceId = traceId;
            this.completionQueue = completionQueue;
        }

        @Override
        protected void done() {
            super.done();

            if (completionQueue != null) {
                completionQueue.add(this);
            }

            // permit count
            afterTaskDone(schemaName);

            if (traceId != null) {
                TraceStats traceStat = traceStats.getUnchecked(traceId);
                long traceCount = traceStat.count.decrementAndGet();
                if (traceCount <= 0) {

                    traceStat.mutex.set(true);
                }
            }

        }

        public String getSchemaName() {
            return schemaName;
        }
    }

    public class CallableAdapter<T> implements Callable<T> {

        final Callable<T> task;
        final String traceId;

        CallableAdapter(Callable<T> task, String traceId, CpuCollector cpuCollector) {
            this.task = new CallableWithCpuCollector(task, cpuCollector);
            this.traceId = traceId;
        }

        public T call() throws Exception {
            if (traceId != null) {
                TraceStats traceStat = traceStats.getUnchecked(traceId);

                traceStat.mutex.set(false);
                traceStat.count.incrementAndGet();

                if (traceStat.closed.get()) {
                    throw new RuntimeException("TraceId:" + traceId + " is already closed , so ignore");
                }
            }

            return task.call();
        }
    }

    public class ListenCallableAdapter implements Callable {

        final Callable task;
        final String traceId;
        final String schemaName;

        ListenCallableAdapter(String schemaName, Callable task, String traceId, CpuCollector cpuCollector) {
            this.task = new CallableWithCpuCollector(task, cpuCollector);
            this.traceId = traceId;
            this.schemaName = schemaName;
        }

        @Override
        public Object call() throws Exception {
            try {
                return task.call();
            } finally {
                afterTaskDone(schemaName);
            }
        }
    }

    public class RunnableAdapter implements Runnable {

        final Runnable task;
        final String traceId;

        RunnableAdapter(Runnable task, String traceId, CpuCollector cpuCollector) {
            this.task = new RunnableWithCpuCollector(task, cpuCollector);
            this.traceId = traceId;
        }

        public void run() {
            if (traceId != null) {
                TraceStats traceStat = traceStats.getUnchecked(traceId);

                traceStat.mutex.set(false);
                traceStat.count.incrementAndGet();

                if (traceStat.closed.get()) {
                    throw new RuntimeException("TraceId:" + traceId + " is already closed , so ignore");
                }
            }

            task.run();
        }
    }

    private void afterTaskDone(String schemaName) {
        if (!DynamicConfig.getInstance().enableExtremePerformance()) {
            AppStats stat = appStats.getUnchecked(schemaName);
            decreTaskCount(stat);
        }
    }

    private void decreTaskCount(AppStats stat) {
        if (!DynamicConfig.getInstance().enableExtremePerformance() && stat != null) {
            stat.taskCount.decrementAndGet();
            AppStats.nodeTaskCount.decrementAndGet();
        }
    }
}

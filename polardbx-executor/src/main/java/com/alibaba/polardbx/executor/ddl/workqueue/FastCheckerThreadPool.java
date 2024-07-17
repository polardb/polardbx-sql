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

package com.alibaba.polardbx.executor.ddl.workqueue;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.calcite.util.Pair;

import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Math.max;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class FastCheckerThreadPool {
    private static volatile FastCheckerThreadPool instance = null;

    /**
     * fastChecker Thread pool:
     * concurrentMap< storageInstId, executor >
     */
    private final CaseInsensitiveConcurrentHashMap<ThreadPoolExecutor> executors;

    /**
     * this cache is used to count fastChecker's progress by ddl jobId
     * cache < ddlJobId, FastCheckerInfo>
     */
    Cache<Long, FastCheckerInfo> cache;

    private final ScheduledExecutorService threadPoolCleanThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("FastChecker-ThreadPool-Cleaner-Thread-"),
            new ThreadPoolExecutor.DiscardPolicy());

    private FastCheckerThreadPool() {
        executors = new CaseInsensitiveConcurrentHashMap<>();

        cache = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .expireAfterWrite(7, TimeUnit.DAYS)
            .expireAfterAccess(7, TimeUnit.DAYS)
            .build();

        threadPoolCleanThread.scheduleAtFixedRate(
            new OfflineStorageThreadPoolCleaner(),
            0L,
            3,
            TimeUnit.DAYS
        );
    }

    public static FastCheckerThreadPool getInstance() {
        if (instance == null) {
            synchronized (FastCheckerThreadPool.class) {
                if (instance == null) {
                    instance = new FastCheckerThreadPool();
                }
            }
        }
        return instance;
    }

    /**
     * 根据storageInstName分配线程池
     * pair< storageInstName, task>
     */
    public void submitTasks(List<Pair<String, Runnable>> storageInstNameAndTasks) {
        synchronized (this.executors) {
            for (Pair<String, Runnable> instNameAndTask : storageInstNameAndTasks) {
                String instName = instNameAndTask.getKey();
                Runnable task = instNameAndTask.getValue();

                if (!executors.containsKey(instName)) {
                    int defaultThreadPoolSize = Integer.valueOf(
                        MetaDbInstConfigManager.getInstance().getInstProperty(
                            ConnectionProperties.FASTCHECKER_THREAD_POOL_SIZE,
                            ConnectionParams.FASTCHECKER_THREAD_POOL_SIZE.getDefault()
                        )
                    );

                    ThreadPoolExecutor executor = new ThreadPoolExecutor(
                        defaultThreadPoolSize,
                        defaultThreadPoolSize,
                        5, TimeUnit.MINUTES,
                        new LinkedBlockingQueue<>(),
                        new NamedThreadFactory(String.format("FastChecker-Thread-on-[%s]", instName), true)
                    );
                    /**
                     * when core thread idle exceeds 5 minutes, it will be destroyed
                     * */
                    executor.allowCoreThreadTimeOut(true);

                    executors.put(
                        instName, executor
                    );

                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "FastChecker create new thread pool for storage inst [%s]",
                        instName
                    ));
                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "Now FastChecker Thread Pool is %s, %s",
                        executors.keySet(),
                        executors.values()
                    ));
                }

                ThreadPoolExecutor executor = executors.get(instName);
                executor.execute(task);
            }
        }
    }

    public void updateStats() {
        int taskUnfinished = 0, runningThreads = 0;
        int threadPoolMaxSize = 0, threadPoolNowSize = 0, threadPoolNum = 0;
        synchronized (this.executors) {
            for (ThreadPoolExecutor executor : this.executors.values()) {
                runningThreads += executor.getActiveCount();
                taskUnfinished += executor.getQueue().size();
                threadPoolMaxSize = max(threadPoolMaxSize, executor.getCorePoolSize());
                threadPoolNowSize = max(threadPoolNowSize, executor.getPoolSize());
            }
            threadPoolNum = this.executors.values().size();
        }

        DdlEngineStats.METRIC_FASTCHECKER_TASK_RUNNING.set(runningThreads);
        DdlEngineStats.METRIC_FASTCHECKER_TASK_WAITING.set(taskUnfinished);
        DdlEngineStats.METRIC_FASTCHECKER_THREAD_POOL_NOW_SIZE.set(threadPoolNowSize);
        DdlEngineStats.METRIC_FASTCHECKER_THREAD_POOL_MAX_SIZE.set(threadPoolMaxSize);
        DdlEngineStats.METRIC_FASTCHECKER_THREAD_POOL_NUM.set(threadPoolNum);
    }

    public void clearStats() {
        DdlEngineStats.METRIC_FASTCHECKER_TASK_RUNNING.set(0);
        DdlEngineStats.METRIC_FASTCHECKER_TASK_WAITING.set(0);
        DdlEngineStats.METRIC_FASTCHECKER_THREAD_POOL_NOW_SIZE.set(0);
        DdlEngineStats.METRIC_FASTCHECKER_THREAD_POOL_MAX_SIZE.set(0);
        DdlEngineStats.METRIC_FASTCHECKER_THREAD_POOL_NUM.set(0);
    }

    public void setParallelism(int parallelism) {
        if (parallelism <= 0) {
            return;
        }
        synchronized (this.executors) {
            for (ThreadPoolExecutor executor : executors.values()) {
                if (parallelism > executor.getMaximumPoolSize()) {
                    executor.setMaximumPoolSize(parallelism);
                    executor.setCorePoolSize(parallelism);
                } else {
                    executor.setCorePoolSize(parallelism);
                    executor.setMaximumPoolSize(parallelism);
                }
            }
        }
    }

    private class OfflineStorageThreadPoolCleaner implements Runnable {
        @Override
        public void run() {
            try {
                cleanupOfflineStorageThreadPool();
                SQLRecorderLogger.ddlLogger.info(
                    "fastChecker threadPool cleaner is working..."
                );
            } catch (Throwable t) {
                SQLRecorderLogger.ddlLogger.error(
                    "failed to clean up fastChecker threadPool", t
                );
            }
        }

        private void cleanupOfflineStorageThreadPool() {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                StorageInfoAccessor accessor = new StorageInfoAccessor();
                accessor.setConnection(metaDbConn);
                Set<String> validStorageNameList = new TreeSet<>(String::compareToIgnoreCase);
                List<String> nameList = accessor
                    .getStorageInfosByInstKind(StorageInfoRecord.INST_KIND_MASTER)
                    .stream()
                    .map(StorageInfoRecord::getStorageInstId)
                    .collect(Collectors.toList());
                validStorageNameList.addAll(nameList);

                List<String> tobeRemoveNameList = executors.keySet()
                    .stream()
                    .filter(x -> !validStorageNameList.contains(x))
                    .collect(Collectors.toList());

                for (String toBeRemove : tobeRemoveNameList) {
                    ThreadPoolExecutor executor = executors.get(toBeRemove);
                    executor.shutdownNow();
                    executors.remove(toBeRemove);
                    SQLRecorderLogger.ddlLogger.info(
                        String.format("FastChecker shutdown and remove threadPool [%s]", toBeRemove)
                    );
                }

            } catch (Exception e) {
                throw new TddlNestableRuntimeException("FastChecker clean offline storage thread pool failed", e);
            }
        }
    }

    public void increaseCheckTaskInfo(long ddlJobId, int newTaskSum, int newTaskFinished) {
        try {
            FastCheckerInfo info = this.cache.get(ddlJobId, FastCheckerInfo::new);
            info.getPhyTaskSum().addAndGet(newTaskSum);
            info.getPhyTaskFinished().addAndGet(newTaskFinished);
        } catch (Exception e) {
            SQLRecorderLogger.ddlLogger.error(
                "failed to increase fastChecker task info", e
            );
        }
    }

    public void rollbackCheckTaskInfo(long ddlJobId, int alreadyTaskSum, int alreadyTaskFinished) {
        try {
            FastCheckerInfo info = this.cache.getIfPresent(ddlJobId);
            if (info == null) {
                return;
            }
            info.getPhyTaskSum().addAndGet(-1 * alreadyTaskSum);
            info.getPhyTaskFinished().addAndGet(-1 * alreadyTaskFinished);
        } catch (Exception e) {
            SQLRecorderLogger.ddlLogger.error(
                "failed to rollback fastChecker task info", e
            );
        }
    }

    public FastCheckerInfo queryCheckTaskInfo(long ddlJobId) {
        try {
            return this.cache.getIfPresent(ddlJobId);
        } catch (Exception e) {
            SQLRecorderLogger.ddlLogger.error(
                "failed to query fastChecker task info", e
            );
        }
        return null;
    }

    public void invalidateTaskInfo(long ddlJobId) {
        this.cache.invalidate(ddlJobId);
    }

    private static class CaseInsensitiveConcurrentHashMap<T> extends ConcurrentHashMap<String, T> {

        @Override
        public T put(String key, T value) {
            return super.put(key.toLowerCase(), value);
        }

        public T get(String key) {
            return super.get(key.toLowerCase());
        }

        public boolean containsKey(String key) {
            return super.containsKey(key.toLowerCase());
        }

        public T remove(String key) {
            return super.remove(key.toLowerCase());
        }
    }

    public static class FastCheckerInfo {
        private volatile AtomicInteger phyTaskSum;
        private volatile AtomicInteger phyTaskFinished;

        public FastCheckerInfo() {
            phyTaskSum = new AtomicInteger(0);
            phyTaskFinished = new AtomicInteger(0);
        }

        public AtomicInteger getPhyTaskSum() {
            return phyTaskSum;
        }

        public AtomicInteger getPhyTaskFinished() {
            return phyTaskFinished;
        }
    }
}

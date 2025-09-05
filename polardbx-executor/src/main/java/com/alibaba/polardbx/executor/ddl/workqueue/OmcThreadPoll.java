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
 * @author wumu
 */
public class OmcThreadPoll {
    private static volatile OmcThreadPoll instance = null;

    /**
     * OmcThreadPoll Thread pool:
     * concurrentMap< storageInstId, executor >
     */
    private final CaseInsensitiveConcurrentHashMap<ThreadPoolExecutor> executors;

    /**
     * this cache is used to count the progress of omc backfill by ddl jobId
     * cache < ddlJobId, FastCheckerInfo>
     */
    Cache<Long, OmcInfo> cache;

    private final ScheduledExecutorService threadPoolCleanThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("OmcBackfill-ThreadPool-Cleaner-Thread-"),
            new ThreadPoolExecutor.DiscardPolicy());

    private OmcThreadPoll() {
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

    public static OmcThreadPoll getInstance() {
        if (instance == null) {
            synchronized (OmcThreadPoll.class) {
                if (instance == null) {
                    instance = new OmcThreadPoll();
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
                    int defaultThreadPoolSize = Integer.parseInt(
                        MetaDbInstConfigManager.getInstance().getInstProperty(
                            ConnectionProperties.OMC_THREAD_POOL_SIZE,
                            ConnectionParams.OMC_THREAD_POOL_SIZE.getDefault()
                        )
                    );

                    ThreadPoolExecutor executor = new ThreadPoolExecutor(
                        defaultThreadPoolSize,
                        defaultThreadPoolSize,
                        5, TimeUnit.MINUTES,
                        new LinkedBlockingQueue<>(),
                        new NamedThreadFactory(String.format("OmcBackfill-Thread-on-[%s]", instName), true)
                    );
                    /**
                     * when core thread idle exceeds 5 minutes, it will be destroyed
                     * */
                    executor.allowCoreThreadTimeOut(true);

                    executors.put(
                        instName, executor
                    );

                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "Omc create new thread pool for storage inst [%s]",
                        instName
                    ));
                    SQLRecorderLogger.ddlLogger.info(String.format(
                        "Now Omc Thread Pool is %s, %s",
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
        int threadPoolNum = 0, threadPoolSize = 0;
        synchronized (this.executors) {
            for (ThreadPoolExecutor executor : this.executors.values()) {
                runningThreads += executor.getActiveCount();
                taskUnfinished += executor.getQueue().size();
                threadPoolSize = executor.getCorePoolSize();
            }
            threadPoolNum = this.executors.values().size();
        }

        DdlEngineStats.METRIC_OMC_CURRENT_PARALLELISM.set(runningThreads);
        DdlEngineStats.METRIC_OMC_BACKFILL_BATCH_WAITING.set(taskUnfinished);
        DdlEngineStats.METRIC_OMC_THREAD_POOL_NUM.set(threadPoolNum);
        DdlEngineStats.METRIC_OMC_THREAD_POOL_SIZE.set(threadPoolSize);
    }

    public void clearStats() {
        DdlEngineStats.METRIC_OMC_CURRENT_PARALLELISM.set(0);
        DdlEngineStats.METRIC_OMC_BACKFILL_BATCH_WAITING.set(0);
        DdlEngineStats.METRIC_OMC_THREAD_POOL_NUM.set(0);
        DdlEngineStats.METRIC_OMC_THREAD_POOL_SIZE.set(0);
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
                    "Omc threadPool cleaner is working..."
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
                        String.format("Omc shutdown and remove threadPool [%s]", toBeRemove)
                    );
                }

            } catch (Exception e) {
                throw new TddlNestableRuntimeException("Omc clean offline storage thread pool failed", e);
            }
        }
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

    public static class OmcInfo {
        private volatile AtomicInteger phyTaskSum;
        private volatile AtomicInteger phyTaskFinished;

        public OmcInfo() {
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

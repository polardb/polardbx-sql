package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chenghui.lch
 */
public class TtlIntraTaskExecutor extends AbstractLifecycle {
    protected static final Logger logger = LoggerFactory.getLogger(TtlIntraTaskExecutor.class);

    public static final int SELECT_TASK_EXECUTOR_TYPE = 0;
    public static final int DELETE_TASK_EXECUTOR_TYPE = 1;

    protected static TtlIntraTaskExecutor instance = new TtlIntraTaskExecutor();

    /**
     * The thread pool for exec the select intra-task of PrepareClearIntervalTask
     */
    protected ReentrantLock selectTaskExecutorLock = new ReentrantLock();
    protected int selectTaskQueueSize = 8192 * 128;
    protected volatile ThreadPoolExecutor selectTaskExecutor;

    /**
     * The thread pool for exec the select intra-task of PrepareClearIntervalTask
     */
    protected ReentrantLock deleteTaskExecutorLock = new ReentrantLock();
    protected int deleteTaskQueueSize = 8192 * 128;
    protected volatile ThreadPoolExecutor deleteTaskExecutor;
    protected TtlIntraTaskExecutorAutoAdjustWorkerSubListener subListener =
        new TtlIntraTaskExecutorAutoAdjustWorkerSubListener();

    protected class TtlIntraTaskExecutorAutoAdjustWorkerSubListener
        implements StorageHaManager.StorageInfoConfigSubListener {

        @Override
        public String getSubListenerName() {
            return "TtlIntraTaskExecutorAutoAdjustWorkerSubListener";
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            TtlIntraTaskExecutor.getInstance().autoAdjustWorkerCountForChangedRwDnList();
        }
    }

    private TtlIntraTaskExecutor() {
    }

    protected void iniTtlIntraTaskExecutor() {

        int selectTaskExecutorSize = TtlConfigUtil.getTtlGlobalSelectWorkerCount();
        if (selectTaskExecutorSize == 0) {
            /**
             * For invalid worker count
             */
            selectTaskExecutorSize = 4;
        }
        selectTaskExecutor =
            createWorkerThreadPool("Ttl-SelectTaskExecutor", selectTaskExecutorSize, selectTaskQueueSize);

        int deleteTaskExecutorSize = TtlConfigUtil.getTtlGlobalDeleteWorkerCount();
        if (deleteTaskExecutorSize == 0) {
            /**
             * For invalid worker count
             */
            deleteTaskExecutorSize = 4;
        }
        deleteTaskExecutor =
            createWorkerThreadPool("Ttl-DeleteTaskExecutor", deleteTaskExecutorSize, deleteTaskQueueSize);

        StorageHaManager.getInstance().registerStorageInfoSubListener(subListener);

    }

    protected ThreadPoolExecutor createWorkerThreadPool(String poolName,
                                                        int poolSize,
                                                        int queueSize) {
        int selectTaskExecutorSize = poolSize;
        BlockingQueue<Runnable> selectTaskQueue = new LinkedBlockingQueue<>(queueSize);
        ThreadPoolExecutor newTaskExecutor = new ThreadPoolExecutor(selectTaskExecutorSize,
            selectTaskExecutorSize,
            1800,
            TimeUnit.SECONDS,
            selectTaskQueue,
            new NamedThreadFactory(poolName, true),
            new ThreadPoolExecutor.DiscardPolicy());
        return newTaskExecutor;
    }

    public static TtlIntraTaskExecutor getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    @Override
    protected void doInit() {
        super.doInit();
        iniTtlIntraTaskExecutor();
    }

    public List<Pair<Future, TtlIntraTaskRunner>> submitSelectTaskRunners(List<TtlIntraTaskRunner> runners) {
        this.selectTaskExecutorLock.lock();
        try {
            List<Pair<Future, TtlIntraTaskRunner>> futureInfos = new ArrayList<>();
            for (int i = 0; i < runners.size(); i++) {
                TtlIntraTaskRunner runner = runners.get(i);
                Future future = this.selectTaskExecutor.submit(runner);
                Pair<Future, TtlIntraTaskRunner> futureInfo = new Pair<Future, TtlIntraTaskRunner>(future, runner);
                futureInfos.add(futureInfo);
            }
            return futureInfos;
        } finally {
            this.selectTaskExecutorLock.unlock();
        }
    }

    public List<Pair<Future, TtlIntraTaskRunner>> submitDeleteTaskRunners(List<TtlIntraTaskRunner> runners) {
        this.deleteTaskExecutorLock.lock();
        try {
            List<Pair<Future, TtlIntraTaskRunner>> futureInfos = new ArrayList<>();
            for (int i = 0; i < runners.size(); i++) {
                TtlIntraTaskRunner runner = runners.get(i);
                Future future = this.deleteTaskExecutor.submit(runner);
                Pair<Future, TtlIntraTaskRunner> futureInfo = new Pair<Future, TtlIntraTaskRunner>(future, runner);
                futureInfos.add(futureInfo);
            }
            return futureInfos;
        } finally {
            this.deleteTaskExecutorLock.unlock();
        }
    }

    /**
     * Auto adjust the task worker count when rw-dn list changed
     */
    public static void autoAdjustWorkerCountForChangedRwDnList() {
        try {
            if (TtlConfigUtil.getTtlGlobalSelectWorkerCount() == 0) {
                TtlIntraTaskExecutor.getInstance()
                    .adjustTaskExecutorWorkerCount(TtlIntraTaskExecutor.SELECT_TASK_EXECUTOR_TYPE, 0);
            }
            if (TtlConfigUtil.getTtlGlobalDeleteWorkerCount() == 0) {
                TtlIntraTaskExecutor.getInstance()
                    .adjustTaskExecutorWorkerCount(TtlIntraTaskExecutor.DELETE_TASK_EXECUTOR_TYPE, 0);
            }
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            logger.warn(ex.getMessage(), ex);
        }
    }

    public synchronized void adjustTaskExecutorWorkerCount(int taskType, int newMaxWorkerCount) {
        try {
            int actualWorkerCount = newMaxWorkerCount;
            if (actualWorkerCount == 0) {
                actualWorkerCount = autoDecideWorkerCount();
            }
            if (taskType == SELECT_TASK_EXECUTOR_TYPE) {
                if (actualWorkerCount != this.selectTaskExecutor.getMaximumPoolSize()) {
                    int queueSize = this.selectTaskQueueSize;
                    ThreadPoolExecutor newExecutor =
                        createWorkerThreadPool("Ttl-SelectTaskExecutor", actualWorkerCount, queueSize);
                    ThreadPoolExecutor oldExecutor = this.selectTaskExecutor;
                    this.selectTaskExecutor = newExecutor;
                    if (oldExecutor != null) {
                        oldExecutor.shutdown();
                    }
                    TtlConfigUtil.setTtlGlobalSelectWorkerCount(newMaxWorkerCount);
                }
            } else {

                if (actualWorkerCount != this.deleteTaskExecutor.getMaximumPoolSize()) {
                    int queueSize = this.deleteTaskQueueSize;
                    ThreadPoolExecutor newExecutor =
                        createWorkerThreadPool("Ttl-DeleteTaskExecutor", actualWorkerCount, queueSize);
                    ThreadPoolExecutor oldExecutor = this.deleteTaskExecutor;
                    this.deleteTaskExecutor = newExecutor;
                    if (oldExecutor != null) {
                        oldExecutor.shutdown();
                    }
                    TtlConfigUtil.setTtlGlobalDeleteWorkerCount(newMaxWorkerCount);
                }
            }
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            logger.warn(ex.getMessage(), ex);
        }
    }

    private static int autoDecideWorkerCount() {
        int dnCount = StorageHaManager.getInstance().getMasterStorageList().size();
        int workerDnRatio = TtlConfigUtil.getTtlGlobalWorkerDnRatio();
        int actualWorkerCount = dnCount * workerDnRatio;
        if (actualWorkerCount == 0) {
            /**
             * For invalid worker count
             */
            actualWorkerCount = 4;
        }
        return actualWorkerCount;
    }

    public ThreadPoolExecutor getDeleteTaskExecutor() {
        return deleteTaskExecutor;
    }

    public ThreadPoolExecutor getSelectTaskExecutor() {
        return selectTaskExecutor;
    }

}

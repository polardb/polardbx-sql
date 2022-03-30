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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferResult;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClientSupplier;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.web.ForWorkerInfo;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import io.airlift.http.client.HttpClient;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlTaskManager
    implements TaskManager, Closeable {
    private static final Logger log = LoggerFactory.getLogger(SqlTaskManager.class);

    private final ExecutorService taskNotificationExecutor;
    private final ScheduledExecutorService driverYieldExecutor;

    private final ScheduledExecutorService taskManagementExecutor;

    private final long infoCacheTime;
    private final long clientTimeout;

    private final LoadingCache<String, QueryContext> queryContexts;
    private final LoadingCache<TaskId, SqlTask> tasks;
    /**
     * key: schemaName,
     * val: MemoryPool For one Schema
     */
    private final ConcurrentMap<String, List<TaskId>> queryTasks;

    private Object doRemoveTaskLock = new Object();

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private class QueryMonitor
        implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    log.info("current task count " + tasks.size() + " current query count " + queryContexts.size()
                        + " current query task count " + queryTasks.size());
                    Thread.sleep(1000 * 120);
                } catch (Exception e) {
                }
            }
        }
    }

    @Inject
    public SqlTaskManager(
        ExchangeClientSupplier exchangeClientSupplier,
        LocationFactory locationFactory,
        TaskExecutor taskExecutor,
        NodeInfo nodeInfo,
        TaskManagementExecutor taskManagementExecutor,
        SpillerFactory spillerFactory,
        @ForTaskNotificationExecutor ExecutorService taskNotificationExecutor,
        @ForDriverYieldExecutor ScheduledExecutorService driverYieldExecutor,
        @ForWorkerInfo HttpClient httpClient) {
        requireNonNull(nodeInfo, "nodeInfo is null");
        this.infoCacheTime = MppConfig.getInstance().getTaskInfoCacheMaxAliveMillis();
        this.clientTimeout = MppConfig.getInstance().getTaskClientTimeout();
        this.taskNotificationExecutor = taskNotificationExecutor;

        this.taskManagementExecutor =
            requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor();
        this.driverYieldExecutor = driverYieldExecutor;
        this.sqlTaskExecutionFactory =
            new SqlTaskExecutionFactory(
                taskNotificationExecutor, taskExecutor, exchangeClientSupplier, spillerFactory, httpClient);

        queryContexts =
            CacheBuilder.newBuilder().weakValues().removalListener(new RemovalListener<String, QueryContext>() {
                @Override
                public void onRemoval(RemovalNotification<String, QueryContext> notification) {
                    //由于同属于一个query的多个task在不同时机调度到worker端，所以worker其实不支持query到底啥时候结束，
                    //所以这里通过引用方式需要对workerQueryMemoryPool做destory操作
                    notification.getValue().destroyQueryMemoryPool();
                }
            }).build(new CacheLoader<String, QueryContext>() {
                @Override
                public QueryContext load(String key) throws Exception {
                    return new QueryContext(
                        driverYieldExecutor,
                        key);
                }
            });

        tasks = CacheBuilder.newBuilder().build(new CacheLoader<TaskId, SqlTask>() {
            @Override
            public SqlTask load(TaskId taskId) {
                QueryContext queryContext = queryContexts.getUnchecked(taskId.getQueryId());
                List<TaskId> taskIds = queryTasks.get(taskId.getQueryId());
                if (taskIds == null) {
                    queryTasks.putIfAbsent(taskId.getQueryId(), new ArrayList<TaskId>());
                    taskIds = queryTasks.get(taskId.getQueryId());
                }
                synchronized (taskIds) {
                    if (!taskIds.contains(taskId)) {
                        taskIds.add(taskId);
                    }
                }
                return new SqlTask(
                    nodeInfo.getNodeId(),
                    taskId,
                    locationFactory.createLocalTaskLocation(taskId),
                    queryContext,
                    sqlTaskExecutionFactory,
                    taskNotificationExecutor);
            }
        });

        queryTasks = new ConcurrentHashMap<>();

        new Thread(new QueryMonitor()).start();
    }

    @PostConstruct
    public void start() {
        taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                removeOldTasks();
            } catch (Throwable e) {
                log.warn("Error removing old tasks", e);
            }
            try {
                failAbandonedTasks();
            } catch (Throwable e) {
                log.warn("Error canceling abandoned tasks", e);
            }
            try {
                forceDeleteTasks();
            } catch (Throwable e) {
                log.error("Error force remove timeout tasks", e);
            }
        }, 200, 200, TimeUnit.MILLISECONDS);
    }

    @Override
    @PreDestroy
    public void close() {
        boolean taskCanceled = false;
        for (SqlTask task : tasks.asMap().values()) {
            if (task.isDone()) {
                continue;
            }
            task.failed(new TddlRuntimeException(ErrorCode.ERR_SERVER_SHUTTING_DOWN,
                format("Task %s has been canceled", task.getTaskId())));
            taskCanceled = true;
        }
        if (taskCanceled) {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        taskNotificationExecutor.shutdownNow();
    }

    @Override
    public List<SqlTask> getAllTasks() {
        return ImmutableList.copyOf(tasks.asMap().values());
    }

    @Override
    public List<TaskInfo> getAllTaskInfo() {
        return ImmutableList.copyOf(transform(tasks.asMap().values(), SqlTask::getTaskInfo));
    }

    @Override
    public TaskInfo getTaskInfo(TaskId taskId) {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getIfPresent(taskId);
        if (sqlTask != null) {
            sqlTask.recordHeartbeat();
            return sqlTask.getTaskInfo();
        } else {
            return TaskInfo.getEmptyTaskInfo();
        }
    }

    @Override
    public TaskStatus getTaskStatus(TaskId taskId) {
        requireNonNull(taskId, "taskId is null");
        SqlTask sqlTask = tasks.getIfPresent(taskId);
        if (sqlTask != null) {
            sqlTask.recordHeartbeat();
            return sqlTask.getTaskStatus();
        } else {
            return TaskStatus.getEmptyTaskStatus();
        }
    }

    @Override
    public ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState) {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(currentState, "currentState is null");

        SqlTask sqlTask = tasks.getIfPresent(taskId);
        if (sqlTask != null) {
            sqlTask.recordHeartbeat();
            return sqlTask.getTaskInfo(currentState);
        } else {
            return immediateFuture(TaskInfo.getEmptyTaskInfo());
        }
    }

    @Override
    public ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState) {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(currentState, "currentState is null");

        SqlTask sqlTask = tasks.getIfPresent(taskId);
        if (sqlTask != null) {
            sqlTask.recordHeartbeat();
            return sqlTask.getTaskStatus(currentState);
        } else {
            return immediateFuture(TaskStatus.getEmptyTaskStatus());
        }
    }

    @Override
    public TaskInfo updateTask(SessionRepresentation session, TaskId taskId, Optional<PlanFragment> fragment,
                               List<TaskSource> sources, OutputBuffers outputBuffers,
                               Optional<List<BloomFilterInfo>> bloomFilterInfos, URI uri) {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(outputBuffers, "outputBuffers is null");

        //first update task, so here create task.
        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        MDC.put(MDC.MDC_KEY_APP, session.getSchema().toLowerCase());
        MDC.put(MDC.MDC_KEY_CON, session.getMdcConnString());
        return sqlTask.updateTask(session, fragment, sources, outputBuffers, bloomFilterInfos, uri);
    }

    @Override
    public ListenableFuture<BufferResult> getTaskResults(TaskId taskId, boolean preferLocal,
                                                         OutputBuffers.OutputBufferId bufferId, long startingSequenceId,
                                                         DataSize maxSize) {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        Preconditions.checkArgument(startingSequenceId >= 0, "startingSequenceId is negative");
        requireNonNull(maxSize, "maxSize is null");
        SqlTask sqlTask = tasks.getIfPresent(taskId);
        if (sqlTask != null) {
            return sqlTask.getTaskResults(bufferId, preferLocal, startingSequenceId, maxSize);
        } else {
            return immediateFuture(BufferResult.emptyResults(taskId.toString(), startingSequenceId, false));
        }
    }

    @Override
    public void abortTaskResults(TaskId taskId, OutputBuffers.OutputBufferId bufferId) {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        tasks.getUnchecked(taskId).abortTaskResults(bufferId);
    }

    @Override
    public TaskInfo cancelTask(TaskId taskId) {
        requireNonNull(taskId, "taskId is null");

        TaskInfo taskInfo = tasks.getUnchecked(taskId).cancel();
        doRemoveFinishedTask(taskId, false);
        return taskInfo;
    }

    @Override
    public TaskInfo abortTask(TaskId taskId) {
        requireNonNull(taskId, "taskId is null");

        TaskInfo taskInfo = tasks.getUnchecked(taskId).abort();
        doRemoveFinishedTask(taskId, false);
        return taskInfo;
    }

    public void removeOldTasks() {
        DateTime oldestAllowedTask = DateTime.now().minus(infoCacheTime);
        for (SqlTask sqlTask : tasks.asMap().values()) {
            if (sqlTask != null) {
                TaskId taskId = sqlTask.getTaskId();
                try {
                    DateTime endTime = sqlTask.getTaskEndTime();
                    if (endTime != null && endTime.isBefore(oldestAllowedTask)) {
                        doRemoveFinishedTask(taskId);
                    }
                } catch (RuntimeException e) {
                    log.warn("Error while inspecting age of complete task " + taskId, e);
                }
            }
        }
    }

    private void doRemoveFinishedTask(TaskId taskId) {
        doRemoveFinishedTask(taskId, true);
    }

    private void doRemoveFinishedTask(TaskId taskId, boolean removeTask) {
        synchronized (doRemoveTaskLock) {
            SqlTask sqlTask;
            if (removeTask) {
                sqlTask = tasks.asMap().remove(taskId);
                sqlTask.clean();
            } else {
                sqlTask = tasks.getUnchecked(taskId);
                if (sqlTask != null) {
                    sqlTask.releaseTaskMemoryInAdvance();
                }
            }
            List<TaskId> taskIds = queryTasks.get(taskId.getQueryId());
            if (taskIds != null) {
                boolean lastTask = false;
                synchronized (taskIds) {
                    taskIds.remove(taskId);
                    lastTask = taskIds.size() == 0;
                }
                if (lastTask) {
                    queryTasks.remove(taskId.getQueryId());
                    QueryContext context = queryContexts.asMap().remove(taskId.getQueryId());
                    if (context != null) {
                        context.destroyQueryMemoryPool();
                    }
                }
            }
        }
    }

    public void failAbandonedTasks() {
        DateTime now = DateTime.now();
        DateTime oldestAllowedHeartbeat = now.minus(clientTimeout);
        // use 3 times (clientTimeout * 3 = 3m) for spill query avoid gc to kill task
        for (SqlTask sqlTask : tasks.asMap().values()) {
            try {
                if (sqlTask.isDone()) {
                    continue;
                }
                DateTime lastHeartbeat = sqlTask.getLastHeartbeat();
                boolean needFailAbandoned = lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat);
                if (needFailAbandoned) {
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Failing abandoned task %s", sqlTask.getTaskId()));
                    }
                    sqlTask.failed(new TddlRuntimeException(ErrorCode.ERR_ABANDONED_TASK,
                        format("Task %s has not been accessed since %s: currentTime %s", sqlTask.getTaskId(),
                            lastHeartbeat, now)));
                }
            } catch (RuntimeException e) {
                log.warn("Error while inspecting age of task " + sqlTask.getTaskId(), e);
            }
        }
    }

    protected void forceDeleteTasks() {
        try {
            long configMaxRunningTime = MppConfig.getInstance().getTaskMaxRunTime();
            if (configMaxRunningTime <= 0) {
                return;
            }
            long currentTime = System.currentTimeMillis();
            for (SqlTask sqlTask : tasks.asMap().values()) {
                if (sqlTask.isDone()) {
                    continue;
                }
                long taskMaxRunningTime = configMaxRunningTime;
                if (currentTime - sqlTask.getCreateTime() > taskMaxRunningTime) {
                    log.error("forceDeleteTasks sqlTask " + sqlTask.getTaskId() + " instanceId " +
                        sqlTask.getTaskId().toString() + " createTime " + sqlTask.getCreateTime() +
                        " currentTime " + currentTime + " force delete " + taskMaxRunningTime);
                    sqlTask.failed(new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, format(
                        "Task %s enforcing query timeout " + "limits %s", sqlTask.getTaskId(), taskMaxRunningTime)));
                }
            }
        } catch (Exception e) {
            log.error("sqlTask force delete error " + e.getMessage(), e);
        }
    }
}

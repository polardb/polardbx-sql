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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.server.remotetask;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.mpp.server.StatementResource;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.GuardedBy;
import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TaskInfoFetcher
    implements SimpleHttpResponseCallback<TaskInfo> {
    private static final Logger log = LoggerFactory.getLogger(TaskInfoFetcher.class);
    private final Session session;
    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<TaskInfo> taskInfo;
    private final JsonCodec<TaskInfo> taskInfoCodec;

    private final long updateIntervalMillis;
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final ScheduledExecutorService updateScheduledExecutor;

    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;

    private final boolean summarizeTaskInfo;

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    @GuardedBy("this")
    private Future<?> future;

    public TaskInfoFetcher(
        Session session,
        Consumer<Throwable> onFail,
        TaskInfo initialTask,
        HttpClient httpClient,
        long updateInterval,
        JsonCodec<TaskInfo> taskInfoCodec,
        long minErrorDuration,
        long maxErrorDuration,
        boolean summarizeTaskInfo,
        Executor executor,
        ScheduledExecutorService updateScheduledExecutor,
        ScheduledExecutorService errorScheduledExecutor) {
        requireNonNull(initialTask, "initialTask is null");
        requireNonNull(minErrorDuration, "minErrorDuration is null");
        requireNonNull(errorScheduledExecutor, "errorScheduledExecutor is null");

        this.session = session;
        this.taskId = initialTask.getTaskStatus().getTaskId();
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskInfo = new StateMachine<>("task " + taskId, executor, initialTask);
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");

        this.updateIntervalMillis = updateInterval;
        this.updateScheduledExecutor = requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        this.errorTracker =
            new RequestErrorTracker(taskId, initialTask.getTaskStatus().getSelf(), minErrorDuration, maxErrorDuration,
                errorScheduledExecutor, "getting info for task");

        this.summarizeTaskInfo = summarizeTaskInfo;

        this.executor = requireNonNull(executor, "executor is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public TaskInfo getTaskInfo() {
        return taskInfo.get();
    }

    public synchronized void start() {
        if (running) {
            // already running
            return;
        }
        running = true;
        scheduleUpdate();
    }

    private synchronized void stop() {
        running = false;

        if (session.getClientContext().getRuntimeStatistics() != null) {
            RuntimeStatistics rootRuntimeStats = (RuntimeStatistics) session.getClientContext().getRuntimeStatistics();
            rootRuntimeStats.collectMppStatistics(taskInfo.get().getTaskStatus(), session.getClientContext());
        }
        if (future != null) {
            future.cancel(true);
            future = null;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    private synchronized void scheduleUpdate() {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() -> {
            synchronized (this) {
                // if the previous request still running, don't schedule a new request
                if (future != null && !future.isDone()) {
                    return;
                }
            }
            if (nanosSince(lastUpdateNanos.get()).toMillis() >= updateIntervalMillis) {
                sendNextRequest();
            }
        }, 200, 200, MILLISECONDS);
    }

    private synchronized void sendNextRequest() {
        TaskStatus taskStatus = getTaskInfo().getTaskStatus();

        if (!running) {
            return;
        }

        // we already have the final task info
        if (isDone(getTaskInfo())) {
            stop();
            return;
        }

        // if we have an outstanding request
        if (future != null && !future.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendNextRequest, executor);
            return;
        }

        if (session.isPreferLocal() && taskStatus.getSelf().isLocal()) {
            TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
            ListenableFuture<TaskInfo> futureTaskInfo = taskManager.getTaskInfo(taskId, taskStatus.getState());
            Futures.addCallback(futureTaskInfo, new SimpleLocalResponseHandler(this), executor);
        } else {
            HttpUriBuilder httpUriBuilder = uriBuilderFrom(taskStatus.getSelf().getUri());
            URI uri = summarizeTaskInfo ? httpUriBuilder.addParameter("summarize").build() : httpUriBuilder.build();
            Request request = prepareGet()
                .setUri(uri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
            ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskInfo>> curFuture = httpClient.executeAsync(
                request, createFullJsonResponseHandler(taskInfoCodec));
            future = curFuture;
            Futures.addCallback(curFuture, new SimpleHttpResponseHandler<>(this, request.getUri()), executor);
        }
    }

    synchronized void updateTaskInfo(TaskInfo newValue, boolean forceUpdate) {
        boolean updated = taskInfo.setIf(newValue, oldValue -> {
            TaskStatus oldTaskStatus = oldValue.getTaskStatus();
            TaskStatus newTaskStatus = newValue.getTaskStatus();
            if (oldTaskStatus.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            // don't update to an older version (same version is ok)
            return newTaskStatus.getVersion() >= oldTaskStatus.getVersion();
        });

        if (!updated && forceUpdate && !taskInfo.get().getTaskStatus().getState().isDone()) {
            log.warn("TaskInfoFetcher update failed( " + "newTask version " + newValue.getTaskStatus().getVersion()
                + " oldTask version" + getTaskInfo().getTaskStatus().getVersion() + " )" + " but forceUpdate ");
            TaskInfo forceUpdateTaskInfo = newValue.withTaskStatus(newValue.getTaskStatus().forceAbort());
            updated = taskInfo.setIf(forceUpdateTaskInfo, oldValue -> {
                TaskStatus oldTaskStatus = oldValue.getTaskStatus();
                TaskStatus forceUpdateTaskStatus = forceUpdateTaskInfo.getTaskStatus();
                if (oldTaskStatus.getState().isDone()) {
                    // never update if the task has reached a terminal state
                    return false;
                }
                // don't update to an older version (same version is ok)
                return forceUpdateTaskStatus.getVersion() >= oldTaskStatus.getVersion();
            });
        }

        if (updated && newValue.getTaskStatus().getState().isDone()) {
            stop();
        }
    }

    @Override
    public void success(TaskInfo newValue) {
        if (newValue.getTaskStatus().getTaskId().equals(TaskId.getEmptyTask())) {
            return;
        }

        //try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
        if (newValue.getTaskStatus().getTaskId().getStageId().getId() == 0 && !newValue.getOutputBuffers().getState()
            .canAddBuffers()) {
            StatementResource.Query query = StatementResource
                .getInstance().getQuery(newValue.getTaskStatus().getTaskId().getQueryId());
            if (query != null) {
                query.finishWaitOutputStageCanAddBuffers();
            }
        }
        lastUpdateNanos.set(System.nanoTime());

        errorTracker.requestSucceeded();
        updateTaskInfo(newValue, false);
        //}
    }

    @Override
    public void failed(Throwable cause) {
        //try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
        lastUpdateNanos.set(System.nanoTime());

        try {
            // if task not already done, record error
            if (!isDone(getTaskInfo())) {
                errorTracker.requestFailed(cause);
            }
        } catch (Error e) {
            onFail.accept(e);
            throw e;
        } catch (RuntimeException e) {
            onFail.accept(e);
        }
        //}
    }

    @Override
    public void fatal(Throwable cause) {
        //try (SetThreadName ignored = new SetThreadName("TaskInfoFetcher-%s", taskId)) {
        onFail.accept(cause);
        //}
    }

    private static boolean isDone(TaskInfo taskInfo) {
        return taskInfo.getTaskStatus().getState().isDone();
    }
}

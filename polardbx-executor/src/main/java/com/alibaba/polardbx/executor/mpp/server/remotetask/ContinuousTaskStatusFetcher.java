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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.client.MppMediaTypes;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.QueryBloomFilter;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.mpp.server.TaskResource;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import com.alibaba.polardbx.gms.node.HostAddressCache;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import java.net.URI;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;

class ContinuousTaskStatusFetcher
    implements SimpleHttpResponseCallback<TaskStatus> {
    private static final Logger log = LoggerFactory.getLogger(ContinuousTaskStatusFetcher.class);

    private final Session session;
    private final TaskId taskId;
    private final Consumer<Throwable> onFail;
    private final StateMachine<TaskStatus> taskStatus;
    private final JsonCodec<TaskStatus> taskStatusCodec;

    private final Duration refreshMaxWait;
    private final Executor executor;
    private final HttpClient httpClient;
    private final RequestErrorTracker errorTracker;

    private final AtomicLong currentRequestStartNanos = new AtomicLong();

    private final ScheduledExecutorService timeoutExecutor;

    @GuardedBy("this")
    private boolean running;

    @GuardedBy("this")
    private boolean stop;

    @GuardedBy("this")
    private Future<?> future;

    private final QueryBloomFilter bloomfilterDepedency = null;

    public ContinuousTaskStatusFetcher(
        Session session,
        Consumer<Throwable> onFail,
        TaskStatus initialTaskStatus,
        Duration refreshMaxWait,
        JsonCodec<TaskStatus> taskStatusCodec,
        ExecutorService executor,
        ScheduledExecutorService timeoutExecutor,
        HttpClient httpClient,
        long minErrorDuration,
        long maxErrorDuration,
        ScheduledExecutorService errorScheduledExecutor) {
        requireNonNull(initialTaskStatus, "initialTaskStatus is null");
        this.session = session;
        this.taskId = initialTaskStatus.getTaskId();
        this.onFail = requireNonNull(onFail, "onFail is null");
        this.taskStatus = new StateMachine<>("task-" + taskId, executor, initialTaskStatus);

        this.refreshMaxWait = requireNonNull(refreshMaxWait, "refreshMaxWait is null");
        this.taskStatusCodec = requireNonNull(taskStatusCodec, "taskStatusCodec is null");

        this.executor = requireNonNull(executor, "executor is null");
        this.timeoutExecutor = timeoutExecutor;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        this.errorTracker =
            new RequestErrorTracker(taskId, initialTaskStatus.getSelf(), minErrorDuration, maxErrorDuration,
                errorScheduledExecutor, "getting task status");
    }

    public synchronized void start() {
        if (running || stop) {
            // already running
            return;
        }
        running = true;
        scheduleNextRequest();
    }

    public synchronized void stop() {
        stop = true;
        running = false;
        if (session.getClientContext().getRuntimeStatistics() != null) {
            RuntimeStatistics rootRuntimeStats = (RuntimeStatistics) session.getClientContext().getRuntimeStatistics();
            rootRuntimeStats.collectMppStatistics(getTaskStatus(), session.getClientContext());
        }
        if (future != null) {
            future.cancel(true);
            future = null;
        }
    }

    private synchronized void scheduleNextRequest() {
        // stopped or done?
        TaskStatus taskStatus = getTaskStatus();
        if (!running || taskStatus.getState().isDone()) {
            return;
        }

        // outstanding request?
        if (future != null && !future.isDone()) {
            // this should never happen
            log.error("Can not reschedule update because an update is already running");
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::scheduleNextRequest, executor);
            return;
        }

        URI uri = uriBuilderFrom(taskStatus.getSelf().getUri()).appendPath("status").build();

        errorTracker.startRequest();
        if (session.isPreferLocal() && taskStatus.getSelf().isLocal()) {
            TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
            Duration waitTime = TaskResource.randomizeWaitTime(refreshMaxWait);
            ListenableFuture<TaskStatus> futureTaskStatus = addTimeout(
                taskManager.getTaskStatus(taskId, taskStatus.getState()),
                () -> taskManager.getTaskStatus(taskId),
                waitTime,
                timeoutExecutor);
            currentRequestStartNanos.set(System.nanoTime());
            Futures.addCallback(
                futureTaskStatus, new SimpleLocalResponseHandler(this),
                executor);
        } else {
            Request request = prepareGet()
                .setUri(uri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(MppMediaTypes.MPP_CURRENT_STATE, taskStatus.getState().toString())
                .setHeader(MppMediaTypes.MPP_MAX_WAIT, refreshMaxWait.toString())
                .build();

            ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskStatus>> curFuture =
                httpClient.executeAsync(request, createFullJsonResponseHandler(taskStatusCodec));
            future = curFuture;
            currentRequestStartNanos.set(System.nanoTime());
            Futures.addCallback(curFuture, new SimpleHttpResponseHandler<>(this, uri), executor);
        }
    }

    TaskStatus getTaskStatus() {
        return taskStatus.get();
    }

    @Override
    public void success(TaskStatus value) {
        //try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
        try {
            if (value.getTaskId().equals(TaskId.getEmptyTask())) {
                return;
            }
            updateTaskStatus(value);
            errorTracker.requestSucceeded();
        } finally {
            scheduleNextRequest();
        }
        //}
    }

    @Override
    public void failed(Throwable cause) {
        //try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
        try {
            // if task not already done, record error
            TaskStatus taskStatus = getTaskStatus();
            if (!taskStatus.getState().isDone()) {
                errorTracker.requestFailed(cause);
            }
        } catch (Error e) {
            onFail.accept(e);
            throw e;
        } catch (RuntimeException e) {
            onFail.accept(e);
        } finally {
            scheduleNextRequest();
        }
        // }
    }

    @Override
    public void fatal(Throwable cause) {
        //try (SetThreadName ignored = new SetThreadName("ContinuousTaskStatusFetcher-%s", taskId)) {
        onFail.accept(cause);
        //}
    }

    void updateTaskStatus(TaskStatus newValue) {
        // change to new value if old value is not changed and new value has a newer version
        AtomicBoolean taskMismatch = new AtomicBoolean();
        taskStatus.setIf(newValue, oldValue -> {
            // did the task instance id change
            if (!isNullOrEmpty(oldValue.getTaskInstanceId()) && !oldValue.getTaskInstanceId()
                .equals(newValue.getTaskInstanceId())) {
                taskMismatch.set(true);
                return false;
            }

            if (oldValue.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            if (newValue.getVersion() < oldValue.getVersion()) {
                // don't update to an older version (same version is ok)
                return false;
            }
            return true;
        });

        if (taskMismatch.get()) {
            // This will also set the task status to FAILED state directly.
            // Additionally, this will issue a DELETE for the task to the worker.
            // While sending the DELETE is not required, it is preferred because a task was created by the previous request.
            onFail.accept(new TddlRuntimeException(ErrorCode.ERR_REMOTE_TASK,
                String.format("%s (%s)", Failures.REMOTE_TASK_MISMATCH_ERROR,
                    HostAddressCache.fromUri(getTaskStatus().getSelf().getUri()))));
        }
    }

    public void addStateChangeListener(StateMachine.StateChangeListener<TaskStatus> stateChangeListener) {
        taskStatus.addStateChangeListener(stateChangeListener);
    }

    public ListenableFuture<TaskStatus> getStateChange(TaskStatus taskStatus) {
        return this.taskStatus.getStateChange(taskStatus);
    }
}

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

package com.alibaba.polardbx.executor.mpp.server;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.client.MppMediaTypes;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskSource;
import com.alibaba.polardbx.executor.mpp.execution.TaskState;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferResult;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/task")
public class TaskResource {
    private static final Logger log = LoggerFactory.getLogger(TaskResource.class);

    private static final long ADDITIONAL_WAIT_TIME = 5000L;
    public static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(2, SECONDS);

    private final TaskManager taskManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private static DrdsContextHandler drdsContextHandler = null;

    @Inject
    public TaskResource(
        TaskManager taskManager,
        @ForAsyncHttp BoundedExecutor responseExecutor,
        @ForAsyncHttp ScheduledExecutorService timeoutExecutor) {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TaskInfo> getAllTaskInfo(@Context UriInfo uriInfo) {
        List<TaskInfo> allTaskInfo = taskManager.getAllTaskInfo();
        return allTaskInfo;
    }

    @POST
    @Path("{taskId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createOrUpdateTask(
        @PathParam("taskId") TaskId taskId,
        TaskUpdateRequest taskUpdateRequest,
        @Context UriInfo uriInfo) {
        requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");
        List<TaskSource> sources = taskUpdateRequest.getSources();
        TaskInfo taskInfo = taskManager.updateTask(taskUpdateRequest.getSession(),
            taskId,
            taskUpdateRequest.getFragment(),
            sources,
            taskUpdateRequest.getOutputIds(),
            taskUpdateRequest.getBloomfilters(),
            taskUpdateRequest.getRuntimeFilterUpdateUri());
        return Response.ok().entity(taskInfo).build();
    }

    @GET
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getTaskInfo(
        @PathParam("taskId") final TaskId taskId,
        @HeaderParam(MppMediaTypes.MPP_CURRENT_STATE) TaskState currentState,
        @HeaderParam(MppMediaTypes.MPP_MAX_WAIT) Duration maxWait,
        @Context UriInfo uriInfo,
        @Suspended AsyncResponse asyncResponse) {
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskInfo taskInfo = taskManager.getTaskInfo(taskId);
            asyncResponse.resume(taskInfo);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);

        ListenableFuture<TaskInfo> futureTaskInfo = addTimeout(
            taskManager.getTaskInfo(taskId, currentState),
            () -> taskManager.getTaskInfo(taskId),
            maxWait,
            timeoutExecutor);

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME, MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskInfo, responseExecutor)
            .withTimeout(timeout);
    }

    @GET
    @Path("{taskId}/status")
    @Produces(MediaType.APPLICATION_JSON)
    public void getTaskStatus(
        @PathParam("taskId") TaskId taskId,
        @HeaderParam(MppMediaTypes.MPP_CURRENT_STATE) TaskState currentState,
        @HeaderParam(MppMediaTypes.MPP_MAX_WAIT) Duration maxWait,
        @Context UriInfo uriInfo,
        @Suspended AsyncResponse asyncResponse) {
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskStatus taskStatus = taskManager.getTaskInfo(taskId).getTaskStatus();
            asyncResponse.resume(taskStatus);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        ListenableFuture<TaskStatus> futureTaskStatus = addTimeout(
            taskManager.getTaskStatus(taskId, currentState),
            () -> taskManager.getTaskStatus(taskId),
            waitTime,
            timeoutExecutor);

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(maxWait.toMillis() + ADDITIONAL_WAIT_TIME, MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskStatus, responseExecutor)
            .withTimeout(timeout);
    }

    @DELETE
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public TaskInfo deleteTask(
        @PathParam("taskId") TaskId taskId,
        @QueryParam("abort") @DefaultValue("true") boolean abort,
        @Context UriInfo uriInfo) {
        requireNonNull(taskId, "taskId is null");
        if (log.isDebugEnabled()) {
            log.debug("deleteTask:[abort=" + abort + "],taskId=" + taskId);
        }
        TaskInfo taskInfo;
        if (abort) {
            taskInfo = taskManager.abortTask(taskId);
        } else {
            taskInfo = taskManager.cancelTask(taskId);
        }
        if (log.isDebugEnabled()) {
            log.debug("deleteTask:[taskInfo=" + taskInfo + "],taskId=" + taskId);
        }
        return taskInfo;
    }

    @GET
    @Path("{taskId}/results/{bufferId}/{token}")
    @Produces(MppMediaTypes.MPP_PAGES)
    public void getResults(
        @PathParam("taskId") TaskId taskId,
        @PathParam("bufferId") OutputBuffers.OutputBufferId bufferId,
        @PathParam("token") final long token,
        @HeaderParam(MppMediaTypes.MPP_MAX_SIZE) DataSize maxSize,
        @Suspended AsyncResponse asyncResponse) {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        long start = System.nanoTime();
        ListenableFuture<BufferResult> bufferResultFuture =
            taskManager.getTaskResults(taskId, false, bufferId, token, maxSize);
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        bufferResultFuture = addTimeout(
            bufferResultFuture,
            () -> BufferResult.emptyResults(taskId.toString(), token, false),
            waitTime,
            timeoutExecutor);

        ListenableFuture<Response> responseFuture = Futures.transform(bufferResultFuture, result -> {
            List<SerializedChunk> serializedPages = result.getSerializedPages();

            GenericEntity<?> entity = null;
            Response.Status status;
            if (serializedPages.isEmpty()) {
                status = Response.Status.NO_CONTENT;
            } else {
                entity = new GenericEntity<>(serializedPages, new TypeToken<List<Chunk>>() {
                }.getType());
                status = Response.Status.OK;
            }

            return Response.status(status)
                .entity(entity)
                .header(MppMediaTypes.MPP_TASK_INSTANCE_ID, result.getTaskInstanceId())
                .header(MppMediaTypes.MPP_PAGE_TOKEN, result.getToken())
                .header(MppMediaTypes.MPP_PAGE_NEXT_TOKEN, result.getNextToken())
                .header(MppMediaTypes.MPP_BUFFER_COMPLETE, result.isBufferComplete())
                .build();
        }, directExecutor());

        // For hard timeout, add an additional 5 seconds to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + 5000, MILLISECONDS);
        bindAsyncResponse(asyncResponse, responseFuture, responseExecutor)
            .withTimeout(timeout,
                Response.status(Response.Status.NO_CONTENT)
                    .header(MppMediaTypes.MPP_TASK_INSTANCE_ID, taskId.toString())
                    .header(MppMediaTypes.MPP_PAGE_TOKEN, token)
                    .header(MppMediaTypes.MPP_PAGE_NEXT_TOKEN, token)
                    .header(MppMediaTypes.MPP_BUFFER_COMPLETE, false)
                    .build());
    }

    @DELETE
    @Path("{taskId}/results/{bufferId}")
    @Produces(MediaType.APPLICATION_JSON)
    public void abortResults(
        @PathParam("taskId") TaskId taskId,
        @PathParam("bufferId") OutputBuffers.OutputBufferId bufferId,
        @Context UriInfo uriInfo) {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        taskManager.abortTaskResults(taskId, bufferId);
    }

    public static Duration randomizeWaitTime(Duration waitTime) {
        // Randomize in [T/2, T], so wait is not near zero and the client-supplied max wait time is respected
        long halfWaitMillis = waitTime.toMillis() / 2;
        return new Duration(halfWaitMillis + ThreadLocalRandom.current().nextLong(halfWaitMillis), MILLISECONDS);
    }

    public static DrdsContextHandler getDrdsContextHandler() {
        return drdsContextHandler;
    }

    public static void setDrdsContextHandler(DrdsContextHandler drdsContextHandler) {
        TaskResource.drdsContextHandler = drdsContextHandler;
    }
}
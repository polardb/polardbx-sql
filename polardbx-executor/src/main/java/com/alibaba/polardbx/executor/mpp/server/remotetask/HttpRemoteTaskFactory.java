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

package com.alibaba.polardbx.executor.mpp.server.remotetask;

import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.Threads;
import com.alibaba.polardbx.executor.mpp.execution.LocationFactory;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTaskFactory;
import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.operator.ForScheduler;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.server.ForAsyncHttp;
import com.alibaba.polardbx.executor.mpp.server.TaskUpdateRequest;
import com.alibaba.polardbx.gms.node.Node;
import com.google.common.collect.Multimap;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * @author puyuan.wlh
 */
public class HttpRemoteTaskFactory implements RemoteTaskFactory {

    private HttpClient httpClient;

    private final ExecutorService executor;
    private final ScheduledExecutorService timeoutExecutor;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;

    private final JsonCodec<TaskStatus> taskStatusCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;

    private final LocationFactory locationFactory;
    private final long minErrorDuration;
    private final long maxErrorDuration;
    private final long taskStatusRefreshMaxWait;
    private final long taskInfoUpdateInterval;

    @Inject
    public HttpRemoteTaskFactory(@ForScheduler HttpClient httpClient,
                                 @ForAsyncHttp ScheduledExecutorService timeoutExecutor,
                                 LocationFactory locationFactory,
                                 JsonCodec<TaskStatus> taskStatusCodec,
                                 JsonCodec<TaskInfo> taskInfoCodec,
                                 JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec) {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;
        this.executor = newFixedThreadPool(MppConfig.getInstance().getRemoteTaskMaxCallbackThreads(),
            Threads.daemonThreadsNamed("remote-task-runner"));
        this.minErrorDuration = MppConfig.getInstance().getRemoteTaskMinErrorDuration();
        this.maxErrorDuration = MppConfig.getInstance().getRemoteTaskMaxErrorDuration();
        this.taskStatusRefreshMaxWait = MppConfig.getInstance().getStatusRefreshMaxWait();
        this.taskInfoUpdateInterval = MppConfig.getInstance().getInfoUpdateInterval();
        this.taskStatusCodec = taskStatusCodec;
        this.taskInfoCodec = taskInfoCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.timeoutExecutor = timeoutExecutor;

        this.updateScheduledExecutor =
            newSingleThreadScheduledExecutor(
                Threads.daemonThreadsNamed("task-info-update-scheduler"));
        this.errorScheduledExecutor =
            newSingleThreadScheduledExecutor(
                Threads.daemonThreadsNamed("remote-task-error-delay"));
    }

    @Override
    public RemoteTask createRemoteTask(Session session, TaskId taskId, Node node, PlanFragment fragment,
                                       boolean noMoreSplits, OutputBuffers outputBuffers,
                                       NodeTaskMap.PartitionedSplitCountTracker partitionedSplitCountTracker,
                                       boolean summarizeTaskInfo,
                                       Multimap<Integer, Split> splits, SqlStageExecution sqlStageExecution) {

        return new HttpRemoteTask(
            session,
            taskId,
            node.getNodeIdentifier(),
            locationFactory.createTaskLocation(node, taskId),
            fragment,
            outputBuffers,
            noMoreSplits,
            httpClient,
            executor,
            timeoutExecutor,
            updateScheduledExecutor,
            errorScheduledExecutor,
            minErrorDuration,
            maxErrorDuration,
            taskStatusRefreshMaxWait,
            taskInfoUpdateInterval,
            summarizeTaskInfo,
            taskStatusCodec,
            taskInfoCodec,
            taskUpdateRequestCodec,
            partitionedSplitCountTracker,
            splits,
            locationFactory.createRuntimeFilterLocation(taskId.getQueryId()));
    }

    @Override
    public TaskLocation createLocation(Node node, TaskId taskId) {
        return locationFactory.createTaskLocation(node, taskId);
    }

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }
}

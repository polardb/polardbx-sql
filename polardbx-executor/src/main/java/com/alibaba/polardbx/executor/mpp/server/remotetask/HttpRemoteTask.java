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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.FutureStateChange;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.execution.ScheduledSplit;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.executor.mpp.execution.TaskSource;
import com.alibaba.polardbx.executor.mpp.execution.TaskState;
import com.alibaba.polardbx.executor.mpp.execution.TaskStatus;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.operator.TaskStats;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.server.StatementResource;
import com.alibaba.polardbx.executor.mpp.server.TaskUpdateRequest;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.apache.calcite.rel.RelNode;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.mpp.execution.TaskInfo.createInitialTask;
import static com.alibaba.polardbx.executor.mpp.execution.TaskState.ABORTED;
import static com.alibaba.polardbx.executor.mpp.execution.TaskState.FAILED;
import static com.alibaba.polardbx.executor.mpp.execution.TaskState.PLANNED;
import static com.alibaba.polardbx.executor.mpp.execution.TaskStatus.failWith;
import static com.alibaba.polardbx.executor.mpp.server.remotetask.RequestErrorTracker.logError;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static java.util.Objects.requireNonNull;

public class HttpRemoteTask implements RemoteTask {

    private static final Logger log = LoggerFactory.getLogger(HttpRemoteTask.class);

    private static final long MAX_CLEANUP_RETRY_TIME = 120000L; //2min
    private static final int MIN_RETRIES = 3;

    private final TaskId taskId;
    private boolean started = false;
    private final String nodeId;
    private final PlanFragment planFragment;
    private final Session session;

    private final HttpClient httpClient;
    private final ExecutorService executor;
    private final ScheduledExecutorService errorScheduledExecutor;

    private final AtomicLong nextSplitId = new AtomicLong();
    private final SetMultimap<Integer, ScheduledSplit> pendingSplits = HashMultimap.create();
    private volatile int pendingSourceSplitCount;
    private final Set<Integer> noMoreSplits = new HashSet<>();
    private final Map<Integer, Boolean> isNoMoreSplits = new HashMap<Integer, Boolean>();

    private final RequestErrorTracker updateErrorTracker;
    private final TaskInfoFetcher taskInfoFetcher;
    private final ContinuousTaskStatusFetcher taskStatusFetcher;

    private final boolean summarizeTaskInfo;
    private final long requestTimeout;
    private Future<?> currentRequest;
    private long currentRequestStartMills;

    private final AtomicBoolean aborting = new AtomicBoolean(false);
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);
    private final AtomicBoolean sendPlan = new AtomicBoolean(true);

    private final NodeTaskMap.PartitionedSplitCountTracker partitionedSplitCountTracker;
    private final FutureStateChange<?> whenSplitQueueHasSpace = new FutureStateChange<>();
    @GuardedBy("this")
    private boolean splitQueueHasSpace = true;
    @GuardedBy("this")
    private OptionalInt whenSplitQueueHasSpaceThreshold = OptionalInt.empty();

    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;

    private final AtomicReference<List<BloomFilterInfo>> sendBloomFilter = new AtomicReference();

    // URI for updating runtime filter from worker to current coordination.
    private final URI runtimeFilterUpdateURI;

    public HttpRemoteTask(Session session,
                          TaskId taskId,
                          String nodeId,
                          TaskLocation location,
                          PlanFragment planFragment,
                          OutputBuffers outputBuffers,
                          boolean isNoMoreSplits,
                          HttpClient httpClient,
                          ExecutorService executor,
                          ScheduledExecutorService timeoutExecutor,
                          ScheduledExecutorService updateScheduledExecutor,
                          ScheduledExecutorService errorScheduledExecutor,
                          long minErrorDuration,
                          long maxErrorDuration,
                          long taskStatusRefreshMaxWait,
                          long taskInfoUpdateInterval,
                          boolean summarizeTaskInfo,
                          JsonCodec<TaskStatus> taskStatusCodec,
                          JsonCodec<TaskInfo> taskInfoCodec,
                          JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
                          NodeTaskMap.PartitionedSplitCountTracker partitionedSplitCountTracker,
                          Multimap<Integer, Split> initialSplits,
                          URI runtimeFilterUpdateURI) {

        requireNonNull(taskId, "taskId is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(location, "location is null");
        requireNonNull(planFragment, "planFragment is null");
        this.session = session;
        this.taskId = taskId;
        this.nodeId = nodeId;
        this.planFragment = planFragment;
        this.outputBuffers.set(outputBuffers);
        this.httpClient = httpClient;
        this.executor = executor;
        this.errorScheduledExecutor = errorScheduledExecutor;
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.taskInfoCodec = taskInfoCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.partitionedSplitCountTracker =
            requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
        this.runtimeFilterUpdateURI = runtimeFilterUpdateURI;

        for (Map.Entry<Integer, Split> entry : requireNonNull(initialSplits, "initialSplits is null").entries()) {
            if (entry.getValue() != Split.EMPTY_SPLIT) {
                ScheduledSplit scheduledSplit =
                    new ScheduledSplit(nextSplitId.getAndIncrement(), entry.getValue());
                pendingSplits.put(entry.getKey(), scheduledSplit);
            }
            if (isNoMoreSplits) {
                this.isNoMoreSplits.put(entry.getKey(), true);
                noMoreSplits.add(entry.getKey());
            } else {
                this.isNoMoreSplits.put(entry.getKey(), false);
            }
        }

        pendingSourceSplitCount = planFragment.getPartitionedSources().stream()
            .filter(initialSplits::containsKey)
            .mapToInt(partitionedSource -> initialSplits.get(partitionedSource).size())
            .sum();

        TaskInfo initialTask = createInitialTask(
            nodeId, taskId, location, new TaskStats(DateTime.now(), null));

        this.updateErrorTracker =
            new RequestErrorTracker(taskId, location, minErrorDuration, maxErrorDuration, errorScheduledExecutor,
                "updating task");

        this.taskStatusFetcher = new ContinuousTaskStatusFetcher(
            this.session,
            this::failTask,
            initialTask.getTaskStatus(),
            new Duration(taskStatusRefreshMaxWait, TimeUnit.MILLISECONDS),
            taskStatusCodec,
            executor,
            timeoutExecutor,
            httpClient,
            minErrorDuration,
            maxErrorDuration,
            errorScheduledExecutor);

        this.taskInfoFetcher = new TaskInfoFetcher(
            this.session,
            this::failTask,
            initialTask,
            httpClient,
            taskInfoUpdateInterval,
            taskInfoCodec,
            minErrorDuration,
            maxErrorDuration,
            summarizeTaskInfo,
            executor,
            updateScheduledExecutor,
            errorScheduledExecutor);

        taskStatusFetcher.addStateChangeListener(newStatus -> {
            TaskState state = newStatus.getState();
            if (state.isDone()) {
                cleanUpTask();
            } else {
                partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
                updateSplitQueueSpace();
            }
        });

        long timeout = minErrorDuration / MIN_RETRIES;
        this.requestTimeout = timeout + taskStatusRefreshMaxWait;
        partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
        updateSplitQueueSpace();
    }

    private synchronized void cleanUpTask() {
        checkState(getTaskStatus().getState().isDone(), "attempt to clean up a task that is not done yet");

        // clear pending splits to free memory
        pendingSplits.clear();
        pendingSourceSplitCount = 0;
        partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
        splitQueueHasSpace = true;
        whenSplitQueueHasSpace.complete(null, executor);

        // cancel pending request
        if (currentRequest != null) {
            currentRequest.cancel(true);
            currentRequest = null;
            currentRequestStartMills = 0;
        }

        taskStatusFetcher.stop();

        // The remote task is likely to get a delete from the PageBufferClient first.
        // We send an additional delete anyway to get the final TaskInfo
        TaskStatus taskStatus = getTaskStatus();
        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus);
        Request request = prepareDelete()
            .setUri(uriBuilder.build())
            .build();

        if (session.isPreferLocal() && taskStatus.getSelf().isLocal()) {
            scheduleLocalCleanupRequest(createCleanupBackoff(), request, "cleanup");
        } else {
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "cleanup");
        }
    }

    private synchronized void updateSplitQueueSpace() {
        if (!whenSplitQueueHasSpaceThreshold.isPresent()) {
            return;
        }
        splitQueueHasSpace = getQueuedPartitionedSplitCount() < whenSplitQueueHasSpaceThreshold.getAsInt();
        if (splitQueueHasSpace) {
            whenSplitQueueHasSpace.complete(null, executor);
        }
    }

    @Override
    public TaskId getTaskId() {
        return taskId;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfoFetcher.getTaskInfo();
    }

    @Override
    public TaskStatus getTaskStatus() {
        return taskStatusFetcher.getTaskStatus();
    }

    @Override
    public synchronized void start() {
        if (!started) {
            started = true;
            //try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            // to start we just need to trigger an update
            scheduleUpdate();
            taskInfoFetcher.start();
            //}
        }
    }

    @Override
    public synchronized void addSplits(Multimap<Integer, Split> splitsBySource, boolean isNoMoreSplits) {
        requireNonNull(splitsBySource, "splitsBySource is null");

        // only add pending split if not done
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        for (Map.Entry<Integer, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
            Integer sourceId = entry.getKey();
            Collection<Split> splits = entry.getValue();

            checkState(!noMoreSplits.contains(sourceId), "noMoreSplits has already been set for %s", sourceId);
            int added = 0;
            for (Split split : splits) {
                if (pendingSplits.put(sourceId, new ScheduledSplit(nextSplitId.getAndIncrement(), split))) {
                    added++;
                }
            }
            if (planFragment.isPartitionedSources(sourceId)) {
                pendingSourceSplitCount += added;
                partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            }
            needsUpdate.set(true);

            if (isNoMoreSplits) {
                this.isNoMoreSplits.put(sourceId, true);
                this.noMoreSplits.add(sourceId);
            }
        }
        updateSplitQueueSpace();

        scheduleUpdate();
    }

    @Override
    public synchronized void noMoreSplits(Integer sourceId) {
        if (isNoMoreSplits.get(sourceId) == null || !isNoMoreSplits.get(sourceId)) {
            if (noMoreSplits.add(sourceId)) {
                needsUpdate.set(true);
                scheduleUpdate();
            }
        }
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers) {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        if (newOutputBuffers.getVersion() > outputBuffers.get().getVersion()) {
            outputBuffers.set(newOutputBuffers);
            needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<TaskStatus> stateChangeListener) {
        taskStatusFetcher.addStateChangeListener(stateChangeListener);
    }

    @Override
    public synchronized ListenableFuture<?> whenSplitQueueHasSpace(int threshold) {
        if (whenSplitQueueHasSpaceThreshold.isPresent()) {
            checkArgument(threshold == whenSplitQueueHasSpaceThreshold.getAsInt(),
                "Multiple split queue space notification thresholds not supported");
        } else {
            whenSplitQueueHasSpaceThreshold = OptionalInt.of(threshold);
            updateSplitQueueSpace();
        }
        if (splitQueueHasSpace) {
            return immediateFuture(null);
        }
        return whenSplitQueueHasSpace.createNewListener();
    }

    @Override
    public synchronized void cancel() {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return;
        }

        // send cancel to task and ignore response
        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus).addParameter("abort", "false");
        Request request = prepareDelete()
            .setUri(uriBuilder.build())
            .build();

        if (session.isPreferLocal() && taskStatus.getSelf().isLocal()) {
            scheduleLocalCleanupRequest(createCleanupBackoff(), request, "cancel", false);
        } else {
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "cancel");
        }
    }

    @Override
    public synchronized void abort() {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        abort(failWith(getTaskStatus(), ABORTED, ImmutableList.of()));
    }

    private synchronized void abort(TaskStatus status) {
        checkState(status.getState().isDone(), "cannot abort task with an incomplete status");

        //try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
        taskStatusFetcher.updateTaskStatus(status);

        TaskStatus taskStatus = getTaskStatus();
        // send abort to task

        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus);
        Request request = prepareDelete().setUri(uriBuilder.build()).build();
        if (session.isPreferLocal() && taskStatus.getSelf().isLocal()) {
            scheduleLocalCleanupRequest(createCleanupBackoff(), request, "abort");
        } else {
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "abort");
        }
    }

    private void scheduleLocalCleanupRequest(Backoff cleanupBackoff, Request request, String action) {
        scheduleLocalCleanupRequest(cleanupBackoff, request, action, true);
    }

    private void scheduleLocalCleanupRequest(Backoff cleanupBackoff, Request request, String action,
                                             boolean abort) {
        if (!aborting.compareAndSet(false, true)) {
            // Do not initiate another round of cleanup requests if one had been initiated.
            // Otherwise, we can get into an asynchronous recursion here. For example, when aborting a task after REMOTE_TASK_MISMATCH.
            return;
        }
        doScheduleLocalCleanupRequest(cleanupBackoff, request, abort, action);
    }

    private void doScheduleLocalCleanupRequest(Backoff cleanupBackoff, Request request, boolean abort,
                                               String action) {
        TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
        Future<TaskInfo> taskInfoFuture = executor.submit(new Callable<TaskInfo>() {
            @Override
            public TaskInfo call() {
                TaskInfo taskInfo;
                if (abort) {
                    taskInfo = taskManager.abortTask(taskId);
                } else {
                    taskInfo = taskManager.cancelTask(taskId);
                }
                return taskInfo;
            }
        });
        ListenableFuture<TaskInfo> future = JdkFutureAdapters.listenInPoolThread(taskInfoFuture, executor);
        Futures.addCallback(future, new FutureCallback<TaskInfo>() {
            @Override
            public void onSuccess(TaskInfo result) {
                cleanupTaskInfo(result);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof RejectedExecutionException) {
                    // client has been shutdown
                    return;
                }

                // record failure
                if (cleanupBackoff.failure()) {
                    logError(t, "Unable to %s task at %s", action, request.getUri());
                    // Update the taskInfo with the new taskStatus.
                    // This is required because the query state machine depends on TaskInfo (instead of task status)
                    // to transition its own state.
                    // Also, since this TaskInfo is updated in the client the "finalInfo" flag will not be set,
                    // indicating that the stats are stale.
                    // TODO: Update the query state machine and stage state machine to depend on TaskStatus instead
                    cleanupTaskInfo(getTaskInfo().withTaskStatus(getTaskStatus()));
                    return;
                }

                // reschedule
                long delayMills = cleanupBackoff.getBackoffDelayMills();
                if (delayMills == 0) {
                    doScheduleLocalCleanupRequest(cleanupBackoff, request, abort, action);
                } else {
                    errorScheduledExecutor.schedule(() -> doScheduleLocalCleanupRequest(cleanupBackoff, request,
                        abort, action), delayMills, TimeUnit.MILLISECONDS);
                }
            }
        }, executor);
    }

    private void scheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action) {
        if (!aborting.compareAndSet(false, true)) {
            // Do not initiate another round of cleanup requests if one had been initiated.
            // Otherwise, we can get into an asynchronous recursion here. For example, when aborting a task after REMOTE_TASK_MISMATCH.
            return;
        }
        doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
    }

    private synchronized void processTaskUpdate(TaskInfo newValue, List<TaskSource> sources) {
        //begin fetch TaskStatusFetcher
        taskStatusFetcher.start();
        taskStatusFetcher.updateTaskStatus(newValue.getTaskStatus());
        taskInfoFetcher.updateTaskInfo(newValue, false);
        if (newValue.getTaskStatus().getTaskId().getStageId().getId() == 0 && !newValue.getOutputBuffers().getState()
            .canAddBuffers()) {
            StatementResource.Query query = StatementResource
                .getInstance().getQuery(newValue.getTaskStatus().getTaskId().getQueryId());
            if (query != null) {
                query.finishWaitOutputStageCanAddBuffers();
            }
        }

        // remove acknowledged splits, which frees memory
        for (TaskSource source : sources) {
            Integer planNodeId = source.getPlanNodeId();
            int removed = 0;
            for (ScheduledSplit split : source.getSplits()) {
                if (pendingSplits.remove(planNodeId, split)) {
                    removed++;
                }
            }
            if (planFragment.isPartitionedSources(planNodeId)) {
                pendingSourceSplitCount -= removed;
            }
        }
        updateSplitQueueSpace();

        partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
    }

    private void doScheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action) {
        ListenableFuture future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        Futures.addCallback(future, new FutureCallback<FullJsonResponseHandler.JsonResponse<TaskInfo>>() {
            @Override
            public void onSuccess(FullJsonResponseHandler.JsonResponse<TaskInfo> result) {
                cleanupTaskInfo(result.getValue());
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof RejectedExecutionException) {
                    // client has been shutdown
                    return;
                }

                // record failure
                if (cleanupBackoff.failure()) {
                    logError(t, "Unable to %s task at %s", action, request.getUri());
                    // Update the taskInfo with the new taskStatus.
                    // This is required because the query state machine depends on TaskInfo (instead of task status)
                    // to transition its own state.
                    // Also, since this TaskInfo is updated in the client the "finalInfo" flag will not be set,
                    // indicating that the stats are stale.
                    // TODO: Update the query state machine and stage state machine to depend on TaskStatus instead
                    cleanupTaskInfo(getTaskInfo().withTaskStatus(getTaskStatus()));
                    return;
                }

                // reschedule
                long delayMills = cleanupBackoff.getBackoffDelayMills();
                if (delayMills == 0) {
                    doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
                } else {
                    errorScheduledExecutor
                        .schedule(() -> doScheduleAsyncCleanupRequest(cleanupBackoff, request, action), delayMills,
                            TimeUnit.MILLISECONDS);
                }
            }
        }, executor);
    }

    private HttpUriBuilder getHttpUriBuilder(TaskStatus taskStatus) {
        HttpUriBuilder uriBuilder = uriBuilderFrom(taskStatus.getSelf().getUri());
        if (summarizeTaskInfo) {
            uriBuilder.addParameter("summarize");
        }
        return uriBuilder;
    }

    @Override
    public int getPartitionedSplitCount() {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitCount() + taskStatus.getQueuedPartitionedDrivers() + taskStatus
            .getRunningPartitionedDrivers();
    }

    private int getPendingSourceSplitCount() {
        return pendingSourceSplitCount;
    }

    @Override
    public int getQueuedPartitionedSplitCount() {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitCount() + taskStatus.getQueuedPartitionedDrivers();
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .addValue(getTaskInfo())
            .toString();
    }

    private static Backoff createCleanupBackoff() {
        return new Backoff(MAX_CLEANUP_RETRY_TIME, MAX_CLEANUP_RETRY_TIME);
    }

    private void cleanupTaskInfo(TaskInfo taskInfo) {
        taskStatusFetcher.updateTaskStatus(taskInfo.getTaskStatus());
        taskInfoFetcher.updateTaskInfo(taskInfo, true);
    }

    private void scheduleUpdate() {
        executor.execute(this::sendUpdateThrowable);
    }

    public synchronized void sendBloomFilters(List<BloomFilterInfo> sendBloomFilterInfo) {
        try {
            this.sendBloomFilter.set(sendBloomFilterInfo);
            sendBloomFilterThrowable();
        } catch (Throwable t) {
            t.printStackTrace();
            log.error("task " + taskId, t);
            failTask(t);
        } finally {
            this.sendBloomFilter.set(null);
        }
    }

    @VisibleForTesting
    synchronized void sendUpdateThrowable() {
        try {
            sendUpdate();
        } catch (Throwable t) {
            t.printStackTrace();
            log.error("task " + taskId, t);
            failTask(t);
        }
    }

    synchronized void sendBloomFilterThrowable() {
        try {
            sendBoomFilter();
        } catch (Throwable t) {
            t.printStackTrace();
            log.error("task " + taskId, t);
            failTask(t);
        }
    }

    private synchronized void getSource(int planNodeId, List<TaskSource> taskSources, boolean expand) {
        Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
        boolean noMoreSplits = this.noMoreSplits.contains(planNodeId);
        if (!splits.isEmpty() || noMoreSplits) {
            taskSources.add(new TaskSource(planNodeId, splits, noMoreSplits, expand));
        }
    }

    private synchronized List<TaskSource> getSources() {
        List<TaskSource> taskSources = new ArrayList<>();
        for (Integer sourceNode : planFragment.getPartitionedSources()) {
            this.getSource(sourceNode, taskSources, planFragment.isExpandSource(sourceNode));
        }
        for (RelNode remoteNode : planFragment.getRemoteSourceNodes()) {
            this.getSource(remoteNode.getRelatedId(), taskSources, false);
        }
        return taskSources;
    }

    private synchronized void sendUpdate() {
        TaskStatus taskStatus = getTaskStatus();
        // don't update if the task hasn't been started yet or if it is already finished
        if (!needsUpdate.get() || taskStatus.getState().isDone()) {
            return;
        }

        // if we have an old request outstanding, cancel it
        if (currentRequest != null && (System.currentTimeMillis() - currentRequestStartMills) > requestTimeout) {
            needsUpdate.set(true);
            currentRequest.cancel(true);
            currentRequest = null;
            currentRequestStartMills = 0;
        }

        // if there is a request already running, wait for it to complete
        if (this.currentRequest != null && !this.currentRequest.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = updateErrorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendUpdate, executor);
            return;
        }

        List<TaskSource> sources = getSources();
        requireNonNull(sources, "sources is null");

        Optional<PlanFragment> fragment = Optional.empty();
        if (sendPlan.get()) {
            fragment = Optional.of(planFragment);
        }

        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
            fragment,
            sources,
            outputBuffers.get(),
            session.toSessionRepresentation(),
            Optional.empty(),
            runtimeFilterUpdateURI);

        if (log.isDebugEnabled()) {
            log.debug("sendUpdate: taskId=" + taskId + ", sources=" + sources);
        }

        if (session.isPreferLocal() && taskStatus.getSelf().isLocal()) {
            updateErrorTracker.startRequest();
            TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
            Future<TaskInfo> taskInfoFuture = executor.submit(new Callable<TaskInfo>() {
                @Override
                public TaskInfo call() throws Exception {
                    TaskInfo taskInfo = taskManager.updateTask(updateRequest.getSession(),
                        taskId,
                        updateRequest.getFragment(),
                        sources,
                        updateRequest.getOutputIds(),
                        updateRequest.getBloomfilters(), null);
                    return taskInfo;
                }
            });
            ListenableFuture<TaskInfo> future = JdkFutureAdapters.listenInPoolThread(taskInfoFuture, executor);
            currentRequest = future;
            currentRequestStartMills = System.currentTimeMillis();
            needsUpdate.set(false);
            Futures.addCallback(future, new SimpleLocalResponseHandler(new UpdateResponseHandler(sources)), executor);

        } else {
            JsonBodyGenerator<TaskUpdateRequest> jsonBodyGenerator = jsonBodyGenerator(taskUpdateRequestCodec,
                updateRequest);

            HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus);
            Request request = preparePost()
                .setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator)
                .build();

            updateErrorTracker.startRequest();

            ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskInfo>> future = httpClient.executeAsync(request,
                createFullJsonResponseHandler(taskInfoCodec));
            currentRequest = future;
            currentRequestStartMills = System.currentTimeMillis();

            // The needsUpdate flag needs to be set to false BEFORE adding the Future callback since callback might change
            // the flag value
            // and does so without grabbing the instance lock.
            needsUpdate.set(false);
            Futures.addCallback(future, new SimpleHttpResponseHandler<>(
                new UpdateResponseHandler(sources), request.getUri()), executor);
        }
    }

    private synchronized void sendBoomFilter() {
        TaskStatus taskStatus = getTaskStatus();
        List<TaskSource> sources = getSources();
        requireNonNull(sources, "sources is null");
        Optional<PlanFragment> fragment = Optional.empty();

        Optional<List<BloomFilterInfo>> bloomfilters;
        List<BloomFilterInfo> needBloomfilters = sendBloomFilter.getAndSet(null);
        if (needBloomfilters != null) {
            bloomfilters = Optional.of(needBloomfilters);
        } else {
            log.warn("bloomfilter is empty!");
            return;
        }

        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
            fragment,
            sources,
            outputBuffers.get(),
            session.toSessionRepresentation(),
            bloomfilters,
            runtimeFilterUpdateURI);
        if (session.isPreferLocal() && taskStatus.getSelf().isLocal()) {
            TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
            Future<TaskInfo> taskInfoFuture = executor.submit(new Callable<TaskInfo>() {
                @Override
                public TaskInfo call() throws Exception {
                    TaskInfo taskInfo = taskManager.updateTask(updateRequest.getSession(),
                        taskId,
                        updateRequest.getFragment(),
                        sources,
                        updateRequest.getOutputIds(),
                        updateRequest.getBloomfilters(), null);
                    return taskInfo;
                }
            });
            ListenableFuture<TaskInfo> future = JdkFutureAdapters.listenInPoolThread(taskInfoFuture, executor);
            Futures.addCallback(future, new SimpleLocalResponseHandler<>(
                new UpdateBloomFilterResponseHandler()), executor);

        } else {
            JsonBodyGenerator<TaskUpdateRequest> jsonBodyGenerator = jsonBodyGenerator(taskUpdateRequestCodec,
                updateRequest);

            HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus);
            Request request = preparePost()
                .setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator)
                .build();
            ListenableFuture<FullJsonResponseHandler.JsonResponse<TaskInfo>> future = httpClient.executeAsync(request,
                createFullJsonResponseHandler(taskInfoCodec));
            // The needsUpdate flag needs to be set to false BEFORE adding the Future callback since callback might change
            // the flag value
            // and does so without grabbing the instance lock.
            Futures.addCallback(future, new SimpleHttpResponseHandler<>(
                new UpdateBloomFilterResponseHandler(), request.getUri()), executor);
        }
    }

    /**
     * Move the task directly to the failed state if there was a failure in this task
     */
    private void failTask(Throwable cause) {
        TaskStatus taskStatus = getTaskStatus();
        if (log.isDebugEnabled() && !taskStatus.getState().isDone()) {
            log.debug("Remote task " + taskStatus.getSelf() + " failed", cause);
        }
        abort(failWith(getTaskStatus(), FAILED, ImmutableList.of(Failures.toFailure(cause))));
    }

    private class UpdateResponseHandler
        implements SimpleHttpResponseCallback<TaskInfo> {
        private final List<TaskSource> sources;

        private UpdateResponseHandler(List<TaskSource> sources) {
            this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        }

        @Override
        public void success(TaskInfo value) {
            //try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
            try {
                long currentRequestStartMills;
                synchronized (HttpRemoteTask.this) {
                    currentRequest = null;
                    sendPlan.set(value.isNeedsPlan());
                    currentRequestStartMills = HttpRemoteTask.this.currentRequestStartMills;
                }
                updateStats(currentRequestStartMills);
                processTaskUpdate(value, sources);
                updateErrorTracker.requestSucceeded();
            } finally {
                sendUpdate();
            }
            //}
        }

        @Override
        public void failed(Throwable cause) {
            //try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
            try {
                long currentRequestStartMills;
                synchronized (HttpRemoteTask.this) {
                    currentRequest = null;
                    currentRequestStartMills = HttpRemoteTask.this.currentRequestStartMills;
                }
                updateStats(currentRequestStartMills);

                // on failure assume we need to update again
                needsUpdate.set(true);

                boolean connectionRefused =
                    (cause instanceof UncheckedIOException && cause.getMessage() != null && cause.getMessage()
                        .startsWith("Server refused connection")) || Failures.isConnectionRefused(cause);

                if (connectionRefused && getTaskStatus().getState() == PLANNED) {
                    log.warn(
                        "remove node for RPC_REFUSED_CONNECTION_ERROR:" + getTaskStatus().getSelf().getNodeServer());
                    ((MppServer) ServiceProvider.getInstance().getServer()).getNodeManager()
                        .removeNode(getTaskStatus().getSelf().getNodeServer(), session.getSchema());
                    TddlRuntimeException exception =
                        new TddlRuntimeException(ErrorCode.ERR_EXECUTE_MPP, cause, "Rpc refused connection error");
                    failTask(exception);
                    throw exception;
                }

                // if task not already done, record error
                TaskStatus taskStatus = getTaskStatus();
                if (!taskStatus.getState().isDone()) {
                    updateErrorTracker.requestFailed(cause);
                }
            } catch (Error e) {
                failTask(e);
                throw e;
            } catch (RuntimeException e) {
                failTask(e);
            } finally {
                sendUpdate();
            }
            //}
        }

        @Override
        public void fatal(Throwable cause) {
            failTask(cause);
        }

        private void updateStats(long currentRequestStartNanos) {
        }
    }

    private class UpdateBloomFilterResponseHandler implements SimpleHttpResponseCallback<TaskInfo> {

        private UpdateBloomFilterResponseHandler() {
        }

        @Override
        public void success(TaskInfo value) {

        }

        @Override
        public void failed(Throwable cause) {
            failTask(cause);
        }

        @Override
        public void fatal(Throwable cause) {
            failTask(cause);
        }
    }
}

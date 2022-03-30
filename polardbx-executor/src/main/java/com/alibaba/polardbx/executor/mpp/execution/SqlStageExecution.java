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

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.planner.RemoteSourceNode;
import com.alibaba.polardbx.executor.mpp.server.remotetask.HttpRemoteTask;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.executor.mpp.split.RemoteSplit;
import com.alibaba.polardbx.executor.mpp.util.ImmutableCollectors;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.util.Objects.requireNonNull;

public class SqlStageExecution {

    private static final Logger log = LoggerFactory.getLogger(SqlStageExecution.class);

    private final StageStateMachine stateMachine;
    private final Session session;
    private final RemoteTaskFactory remoteTaskFactory;
    private final NodeTaskMap nodeTaskMap;
    private StageId stageId;
    private final boolean summarizeTaskInfo;
    @GuardedBy("this")
    private final Set<TaskId> allTasks = newConcurrentHashSet();
    @GuardedBy("this")
    private final Set<TaskId> finishedTasks = newConcurrentHashSet();
    private final Set<TaskId> flushingTasks = newConcurrentHashSet();
    private final Map<Node, Set<RemoteTask>> tasks = new ConcurrentHashMap<>();
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final AtomicInteger nextTaskId = new AtomicInteger();

    private final Multimap<Integer, TaskLocation> exchangeLocations = HashMultimap.create();
    private final Set<Integer> completeSources = newConcurrentHashSet();
    private final Set<Integer> completeSourceFragments = newConcurrentHashSet();

    private final Predicate<StageId> needStageIdPredicate;
    private Boolean needStageId;

    private final Multimap<Integer, RemoteSourceNode> exchangeSources;
    private final AtomicBoolean splitsScheduled = new AtomicBoolean();
    private final ListenableFuture<List<BloomFilterInfo>> waitBloomFuture;

    public SqlStageExecution(
        StageId stageId,
        URI location,
        PlanFragment fragment,
        RemoteTaskFactory remoteTaskFactory,
        Session session,
        boolean summarizeTaskInfo,
        NodeTaskMap nodeTaskMap,
        ExecutorService executor,
        Predicate<StageId> needStageIdPredicate,
        QueryBloomFilter bloomFilterManager) {
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.stageId = stageId;
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.session = requireNonNull(session, "session is null");
        this.stateMachine = new StageStateMachine(
            requireNonNull(stageId, "stageId is null"),
            requireNonNull(location, "location is null"),
            session,
            requireNonNull(fragment, "fragment is null"),
            requireNonNull(executor, "executor is null"));

        this.needStageIdPredicate = needStageIdPredicate;

        ImmutableMultimap.Builder<Integer, RemoteSourceNode> fragmentToExchangeSource = ImmutableMultimap.builder();
        for (RemoteSourceNode remoteSourceNode : stateMachine.getFragment().getRemoteSourceNodes()) {
            for (Integer planFragmentId : remoteSourceNode.getSourceFragmentIds()) {
                fragmentToExchangeSource.put(planFragmentId, remoteSourceNode);
            }
        }
        this.exchangeSources = fragmentToExchangeSource.build();
        if (fragment.getConsumeFilterIds().size() > 0) {
            this.waitBloomFuture = Futures.allAsList(
                fragment.getConsumeFilterIds().stream().map(bloomFilterManager::getFuture)
                    .collect(Collectors.toList()));
        } else {
            this.waitBloomFuture = null;
        }
    }

    public void beginScheduling() {
        stateMachine.transitionToScheduling();
    }

    public synchronized void transitionToSchedulingSplits() {
        stateMachine.transitionToSchedulingSplits();
    }

    public StageId getStageId() {
        return stageId;
    }

    public void addStateChangeListener(StateMachine.StateChangeListener<StageState> stateChangeListener) {
        stateMachine.addStateChangeListener(stateChangeListener::stateChanged);
    }

    public StageInfo getStageInfo() {
        return stateMachine.getStageInfo(
            () -> getAllTasks().stream()
                .map(RemoteTask::getTaskInfo)
                .collect(ImmutableCollectors.toImmutableList()),
            ImmutableList::of);
    }

    public synchronized void setOutputBuffers(OutputBuffers outputBuffers) {
        requireNonNull(outputBuffers, "outputBuffers is null");

        while (true) {
            OutputBuffers currentOutputBuffers = this.outputBuffers.get();
            if (currentOutputBuffers != null) {
                if (outputBuffers.getVersion() <= currentOutputBuffers.getVersion()) {
                    return;
                }
                currentOutputBuffers.checkValidTransition(outputBuffers);
            }

            if (this.outputBuffers.compareAndSet(currentOutputBuffers, outputBuffers)) {
                for (RemoteTask task : getAllTasks()) {
                    task.setOutputBuffers(outputBuffers);
                }
                return;
            }
        }
    }

    public synchronized RemoteTask scheduleTask(Node node, int partition) {

        return scheduleTask(node, partition, true);
    }

    public synchronized RemoteTask scheduleTask(Node node, int partition, boolean startImmediately) {
        //requireNonNull(node, "node is null");

        checkState(!splitsScheduled.get(), "scheduleTask can not be called once splits have been scheduled");
        return scheduleTask(node, new TaskId(stateMachine.getStageId(), partition), ImmutableMultimap.of(), true,
            startImmediately);
    }

    private static Split createRemoteSplitFor(TaskId taskId, TaskLocation taskLocation, boolean needSetStageId) {
        // Fetch the results from the buffer assigned to the task based on id
        // set stageId in outputBuffer depends on needSetStageId for cte compatibility
        int outputBufferId = OutputBuffers.OutputBufferId
            .formatOutputBufferId(needSetStageId ? taskId.getStageId().getId() : 0, taskId.getId());
        TaskLocation location =
            new TaskLocation(taskLocation.getNodeServer(), taskLocation.getTaskId(), outputBufferId);
        return new Split(true, new RemoteSplit(location));
    }

    private synchronized RemoteTask scheduleTask(Node node, TaskId taskId, Multimap<Integer, Split> sourceSplits,
                                                 boolean noMoreSplits, boolean startImmediately) {
        ensureNeedSetStageId();

        ImmutableMultimap.Builder<Integer, Split> initialSplits = ImmutableMultimap.builder();
        initialSplits.putAll(sourceSplits);
        for (Map.Entry<Integer, TaskLocation> entry : exchangeLocations.entries()) {
            initialSplits.put(entry.getKey(), createRemoteSplitFor(taskId, entry.getValue(), needStageId));
        }

        RemoteTask task = remoteTaskFactory.createRemoteTask(
            stateMachine.getSession(),
            taskId,
            node,
            stateMachine.getFragment(),
            noMoreSplits,
            outputBuffers.get(),
            nodeTaskMap.createPartitionedSplitCountTracker(node, taskId),
            summarizeTaskInfo,
            initialSplits.build(),
            this);

        completeSources.forEach(task::noMoreSplits);

        allTasks.add(taskId);
        tasks.computeIfAbsent(node, key -> newConcurrentHashSet()).add(task);
        nodeTaskMap.addTask(node, task);

        task.addStateChangeListener(new StageTaskListener());

        if (!stateMachine.getState().isDone()) {
            task.start();
        } else {
            // stage finished while we were scheduling this task
            task.abort();
        }

        if (waitBloomFuture != null) {
            waitBloomFuture.addListener(new Runnable() {
                @Override
                public void run() {
                    try {
                        List<BloomFilterInfo> filterInfos = waitBloomFuture.get();
                        if (filterInfos != null && filterInfos.size() > 0) {
                            ((HttpRemoteTask) task).sendBloomFilters(filterInfos);
                        }
                    } catch (Exception e) {
                        log.warn("Send Bloom Filters failed!", e);
                    }

                }
            }, remoteTaskFactory.getExecutor());
        }

        return task;
    }

    public synchronized void clearExchangeLocations() {
        this.exchangeLocations.clear();
        this.completeSourceFragments.clear();
        this.completeSources.clear();
    }

    public boolean hasTasks() {
        return !tasks.isEmpty();
    }

    public long getMemoryReservation() {
        return 0;
    }

    public synchronized Duration getTotalCpuTime() {
        return new Duration(0, TimeUnit.MILLISECONDS);
    }

    public synchronized void schedulingComplete() {
        if (!stateMachine.transitionToScheduled()) {
            return;
        }

        if (getAllTasks().stream().anyMatch(task -> task.getTaskStatus().getState() == TaskState.RUNNING)) {
            stateMachine.transitionToRunning();
        }
        if (finishedTasks.containsAll(allTasks)) {
            stateMachine.transitionToFinished();
        }

        for (Integer partitionedSource : stateMachine.getFragment().getPartitionedSources()) {
            for (RemoteTask task : getAllTasks()) {
                task.noMoreSplits(partitionedSource);
            }
            completeSources.add(partitionedSource);
        }
    }

    public Set<Node> getScheduledNodes() {
        return ImmutableSet.copyOf(tasks.keySet());
    }

    public synchronized Set<RemoteTask> scheduleSplits(Node node, Multimap<Integer, Split> splits,
                                                       boolean noMoreSplits) {
        splitsScheduled.set(true);
        checkArgument(stateMachine.getFragment().getPartitionedSources().containsAll(splits.keySet()),
            "Invalid splits");

        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();
        Collection<RemoteTask> tasks = this.tasks.get(node);
        if (tasks == null) {
            // The output buffer depends on the task id starting from 0 and being sequential, since each
            // task is assigned a private buffer based on task id.
            TaskId taskId = new TaskId(stateMachine.getStageId(), nextTaskId.getAndIncrement());
            newTasks.add(scheduleTask(node, taskId, splits, noMoreSplits, true));
        } else {
            RemoteTask task = tasks.iterator().next();
            task.addSplits(splits, noMoreSplits);
            if (!task.isStarted()) {
                task.start();
            }
        }

        return newTasks.build();
    }

    public TaskLocation getTaskLocation(Node node, int partition) {
        TaskId taskId = new TaskId(stateMachine.getStageId(), partition);
        return remoteTaskFactory.createLocation(node, taskId);
    }

    public synchronized void addExchangeLocations(Integer fragmentId, Set<TaskLocation> exchangeLocations,
                                                  boolean noMoreExchangeLocations) {
        requireNonNull(fragmentId, "fragmentId is null");
        requireNonNull(exchangeLocations, "exchangeLocations is null");
        checkArgument(exchangeSources.get(fragmentId) != null,
            "Unknown remote source %s. Known sources are %s", fragmentId, exchangeSources.keys());

        ensureNeedSetStageId();

        for (RemoteSourceNode remoteSource : exchangeSources.get(fragmentId)) {
            checkArgument(remoteSource != null, "Unknown remote source %s. Known sources are %s", fragmentId,
                exchangeSources.keySet());

            this.exchangeLocations.putAll(remoteSource.getRelatedId(), exchangeLocations);

            boolean isNoMoreSplits = false;
            if (noMoreExchangeLocations) {
                completeSourceFragments.add(fragmentId);
                // is the source now complete?
                if (completeSourceFragments.containsAll(remoteSource.getSourceFragmentIds())) {
                    isNoMoreSplits = true;
                    completeSources.add(remoteSource.getRelatedId());
                }
            }

            for (RemoteTask task : getAllTasks()) {
                ImmutableMultimap.Builder<Integer, Split> newSplits = ImmutableMultimap.builder();
                for (TaskLocation exchangeLocation : exchangeLocations) {
                    newSplits.put(remoteSource.getRelatedId(),
                        createRemoteSplitFor(task.getTaskId(), exchangeLocation, needStageId));
                }
                task.addSplits(newSplits.build(), isNoMoreSplits);
            }
        }
    }

    public PlanFragment getFragment() {
        return stateMachine.getFragment();
    }

    public Session getSession() {
        return session;
    }

    public StageState getState() {
        return stateMachine.getState();
    }

    public synchronized void cancel() {
        stateMachine.transitionToCanceled();
        getAllTasks().forEach(RemoteTask::cancel);
    }

    public synchronized void abort() {
        stateMachine.transitionToAborted();
        getAllTasks().forEach(RemoteTask::abort);
    }

    public List<RemoteTask> getAllTasks() {
        return tasks.values().stream()
            .flatMap(Set::stream)
            .collect(toImmutableList());
    }

    @Override
    public String toString() {
        return stateMachine.toString();
    }

    private synchronized void ensureNeedSetStageId() {
        if (this.needStageId == null) {
            this.needStageId = this.needStageIdPredicate.apply(stateMachine.getStageId());
        }
    }

    private class StageTaskListener
        implements StateMachine.StateChangeListener<TaskStatus> {
        private long previousMemory;

        @Override
        public void stateChanged(TaskStatus taskStatus) {
            updateMemoryUsage(taskStatus);
            StageState stageState = getState();
            if (stageState.isDone()) {
                return;
            }

            TaskState taskState = taskStatus.getState();
            if (taskState == TaskState.FAILED) {
                Throwable failure = taskStatus.getFailures().stream()
                    .findFirst()
                    .map(ExecutionFailureInfo::toException)
                    .orElse(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_MPP, "A task failed for an unknown reason"));
                stateMachine.transitionToFailed(failure);
            } else if (taskState == TaskState.ABORTED) {
                // A task should only be in the aborted state if the STAGE is done (ABORTED or FAILED)
                stateMachine.transitionToFailed(new TddlRuntimeException(ErrorCode.ERR_EXECUTE_MPP,
                    "A task is in the ABORTED state but stage is " + stageState));
            } else if (taskState == TaskState.FINISHED) {
                if (log.isDebugEnabled()) {
                    log.debug("finish task " + taskStatus.getTaskId());
                }
                finishedTasks.add(taskStatus.getTaskId());
                flushingTasks.remove(taskStatus.getTaskId());
            } else if (taskState == TaskState.FLUSHING) {
                flushingTasks.add(taskStatus.getTaskId());
            } else if (taskState == TaskState.RUNNING && stageState == StageState.SCHEDULED) {
                stateMachine.transitionToRunning();
            }

            if (finishedTasks.containsAll(allTasks)) {
                stateMachine.transitionToFinished();
            } else if (Sets.union(finishedTasks, flushingTasks).containsAll(allTasks)) {
                stateMachine.transitionToFlushing();
            }
        }

        private synchronized void updateMemoryUsage(TaskStatus taskStatus) {
            long currentMemory = taskStatus.getMemoryReservation();
            long deltaMemoryInBytes = currentMemory - previousMemory;
            previousMemory = currentMemory;
            stateMachine.updateMemoryUsage(deltaMemoryInBytes);
        }

    }

}

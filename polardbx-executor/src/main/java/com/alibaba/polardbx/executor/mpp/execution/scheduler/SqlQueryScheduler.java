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
package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.LocationFactory;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.mpp.execution.QueryBloomFilter;
import com.alibaba.polardbx.executor.mpp.execution.QueryState;
import com.alibaba.polardbx.executor.mpp.execution.QueryStateMachine;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTaskFactory;
import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.execution.StageId;
import com.alibaba.polardbx.executor.mpp.execution.StageInfo;
import com.alibaba.polardbx.executor.mpp.execution.StageState;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.planner.NodePartitioningManager;
import com.alibaba.polardbx.executor.mpp.planner.PartitionHandle;
import com.alibaba.polardbx.executor.mpp.planner.PartitionShuffleHandle;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.planner.StageExecutionPlan;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import com.alibaba.polardbx.executor.mpp.util.ImmutableCollectors;
import com.alibaba.polardbx.gms.node.Node;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.mpp.execution.StageState.ABORTED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.CANCELED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.FAILED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.FINISHED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.FLUSHING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.RUNNING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.SCHEDULED;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.firstCompletedFuture;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;

public class SqlQueryScheduler {
    private static final Logger log = LoggerFactory.getLogger(SqlQueryScheduler.class);

    private final QueryStateMachine queryStateMachine;
    private final ExecutorService executor;
    private final StageId rootStageId;

    private final Map<StageId, SqlStageExecution> stages = Maps.newConcurrentMap();
    private final Map<StageId, StageScheduler> stageSchedulers = Maps.newConcurrentMap();
    private final Multimap<StageId, StageLinkage> stageLinkages = LinkedHashMultimap.create();

    private final Map<StageId, Set<SqlStageExecution>> stage2childStages = Maps.newConcurrentMap();
    private final Map<StageId, OutputBufferManager> outputBufferManagers = Maps.newConcurrentMap();
    private final Multimap<StageId, StageId> c2pLinkages = LinkedHashMultimap.create();

    private final boolean summarizeTaskInfo;
    private final AtomicBoolean started = new AtomicBoolean();

    private final StageExecutionPlan plan;
    private final boolean phasedExecutionSchedule;
    private final NodeSelector nodeSelector;
    private final QueryBloomFilter bloomFilterManager;

    public SqlQueryScheduler(QueryStateMachine queryStateMachine, LocationFactory locationFactory,
                             StageExecutionPlan plan, NodeSelector nodeSelector, RemoteTaskFactory remoteTaskFactory,
                             Session session,
                             boolean summarizeTaskInfo, ExecutorService executor, OutputBuffers rootOutputBuffers,
                             NodeTaskMap nodeTaskMap, NodePartitioningManager nodePartitioningManager,
                             QueryBloomFilter bloomFilterManager) {
        this.bloomFilterManager = bloomFilterManager;
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.summarizeTaskInfo = summarizeTaskInfo;

        requireNonNull(locationFactory, "locationFactory is null");
        this.plan = requireNonNull(plan, "stageExecutionPlan is null");
        requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.phasedExecutionSchedule = session.getClientContext().getParamManager()
            .getBoolean(ConnectionParams.MPP_QUERY_PHASED_EXEC_SCHEDULE_ENABLE);

        ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers = ImmutableMap.builder();
        ImmutableMultimap.Builder<StageId, StageLinkage> stageLinkages = ImmutableMultimap.builder();
        // Only fetch a distribution once per query to assure all stages see the same machine assignments
        Map<Integer, StageId> stageIdMapping = new HashMap<>();
        initStageIds(Optional.empty(), plan, queryStateMachine.getQueryId(), stageIdMapping);

        this.nodeSelector = nodeSelector;
        Map<StageId, SqlStageExecution> initedStages = new HashMap<>();
        List<SqlStageExecution> stages =
            createStages(Optional.empty(), stageIdMapping, initedStages, locationFactory, plan,
                partitioningHandler -> nodePartitioningManager
                    .getNodePartitioningMap(this.nodeSelector, partitioningHandler),
                remoteTaskFactory, session, executor, nodeTaskMap, stageSchedulers, stageLinkages
            );

        SqlStageExecution rootStage = stages.get(0);
        //root stage's partitionCnt should be 1
        rootStage.getFragment().getPartitioningScheme().setPartitionCount(1);
        rootStage.setOutputBuffers(rootOutputBuffers);
        this.rootStageId = rootStage.getStageId();
        this.stageSchedulers.putAll(stageSchedulers.build());
        this.stageLinkages.putAll(stageLinkages.build());
        this.stages.putAll(stages.stream().collect(
            ImmutableCollectors.toImmutableMap(SqlStageExecution::getStageId, identity())));
        this.executor = executor;

        rootStage.addStateChangeListener(state -> {
            if (state == FINISHED) {
                queryStateMachine.transitionToFinishing();
            } else if (state == CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToCanceled();
            }
        });

        for (SqlStageExecution stage : stages) {
            stage.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }

                if (state == FAILED) {
                    queryStateMachine.transitionToFailed(stage.getStageInfo().getFailureCause().toException());
                } else if (state == ABORTED) {
                    // this should never happen, since abort can only be triggered in query clean up after the query is finished
                    queryStateMachine
                        .transitionToFailed(
                            new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Query stage was aborted"));
                } else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (stage.hasTasks()) {
                        queryStateMachine.transitionToRunning();
                    }
                } else if (state.isDone()) {
                    bloomFilterManager.finishStage(stage.getStageId().getId());
                }
            });
        }
    }

    public QueryBloomFilter getBloomFilterManager() {
        return bloomFilterManager;
    }

    private void initStageIds(Optional<StageExecutionPlan> parent, StageExecutionPlan plan,
                              String queryId, Map<Integer, StageId> stageIdMapping) {

        boolean visitBefore = stageIdMapping.containsKey(plan.getFragment().getId());
        if (!visitBefore) {
            StageId curStageId = new StageId(queryId, plan.getFragment().getId());
            stageIdMapping.put(plan.getFragment().getId(), curStageId);
        }

        StageId stageId = stageIdMapping.get(plan.getFragment().getId());
        if (parent.isPresent()) {
            StageId parentId = stageIdMapping.get(parent.get().getFragment().getId());
            c2pLinkages.put(stageId, parentId);
        }

        if (!visitBefore) {
            for (StageExecutionPlan subStage : plan.getSubStages()) {
                initStageIds(Optional.of(plan), subStage, queryId, stageIdMapping
                );
            }
        }
    }

    private List<SqlStageExecution> createStages(Optional<SqlStageExecution> parent,
                                                 Map<Integer, StageId> stageIdMapping,
                                                 Map<StageId, SqlStageExecution> initedStages,
                                                 LocationFactory locationFactory, StageExecutionPlan plan,
                                                 Function<PartitionHandle, Map<Integer, Node>> partitioningCache,
                                                 RemoteTaskFactory remoteTaskFactory,
                                                 Session session, ExecutorService executor,
                                                 NodeTaskMap nodeTaskMap,
                                                 ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers,
                                                 ImmutableMultimap.Builder<StageId, StageLinkage> stageLinkages) {

        StageId stageId = stageIdMapping.get(plan.getFragment().getId());
        checkArgument(stageId != null, "stageId of fragment %s is null", plan.getFragment().getId());
        ImmutableList.Builder<SqlStageExecution> stages = ImmutableList.builder();

        if (initedStages.containsKey(stageId)) {
            StageLinkage linkage =
                new StageLinkage(plan.getFragment(), parent, stage2childStages.get(stageId), c2pLinkages
                );
            stageLinkages.put(stageId, linkage);
            stages.add(initedStages.get(stageId));
            return stages.build();
        }
        if (log.isDebugEnabled()) {
            log.debug("create stage: " + stageId + " for fragment: " + plan.getFragment().getId());
        }
        SqlStageExecution stage =
            new SqlStageExecution(stageId, locationFactory.createStageLocation(stageId), plan.getFragment(),
                remoteTaskFactory, session, summarizeTaskInfo, nodeTaskMap, executor, sId -> {
                // to predicate if the stage (specified by $sId) exists a childStage that has more than one parent stage
                if (!stage2childStages.containsKey(sId)) {
                    return false;
                }
                for (SqlStageExecution childSSE : stage2childStages.get(sId)) {
                    if (c2pLinkages.get(childSSE.getStageId()).size() > 1) {
                        return true;
                    }
                }
                return false;
            }, bloomFilterManager,
                plan.getFragment().isRemotePairWise() ?
                    nodeSelector.getOrderedNode().stream().map(Node::getNodeIdentifier).collect(
                        Collectors.toList()) : null
            );

        stages.add(stage);

        StageScheduler scheduler;

        if (plan.getFragment().getPartitionedSources().isEmpty()) {
            // nodes are pre determined by the nodePartitionMap
            Map<Integer, Node> partitionToNode = partitioningCache.apply(plan.getFragment().getPartitioning());
            Failures.checkCondition(
                !partitionToNode.isEmpty(), ErrorCode.ERR_NO_NODES_AVAILABLE, "No worker nodes available");
            scheduler = new FixedCountScheduler(stage, partitionToNode);
        } else if (plan.getFragment().getExpandSources().isEmpty()) {
            // only contain logicalView
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stage::getAllTasks);
            scheduler = new SourcePartitionedScheduler(
                stage, placementPolicy, plan.getSplitInfos(), null, session.getClientContext(),
                nodeSelector.getOrderedNode());
        } else {
            if (plan.getFragment().getPartitionedSources().size() == plan.getFragment().getExpandSources().size()) {
                Map<Integer, Node> partitionToNode = partitioningCache.apply(plan.getFragment().getPartitioning());
                scheduler = new FixedExpandSourceScheduler(
                    stage, partitionToNode, plan.getExpandInfo(),
                    session.getClientContext());
            } else {
                if (plan.getFragment().isRemotePairWise()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "not support bka join under partition wise now");
                }
                //contain logicalView&&expandView
                SplitPlacementPolicy placementPolicy =
                    new DynamicSplitPlacementPolicy(nodeSelector, stage::getAllTasks);
                scheduler = new SourcePartitionedScheduler(
                    stage, placementPolicy, plan.getSplitInfos(), plan.getExpandInfo(), session.getClientContext(),
                    nodeSelector.getOrderedNode());
            }
        }

        stageSchedulers.put(stageId, scheduler);

        int taskNum = scheduler.getTaskNum();
        int driverParallelism = scheduler.getDriverParallelism();
        plan.getFragment().setDriverParallelism(driverParallelism);
        plan.getFragment().getPartitioning().setPartitionCount(taskNum);
        bloomFilterManager.updateTaskParallelism(stageId.getId(), taskNum);

        Preconditions.checkArgument(taskNum > 0, "node count of this fragment must be greater than 0!");

        Set<StageId> childStageIds = new HashSet<>();

        int requireChildOutputNum = scheduler.requireChildOutputNum();
        Preconditions.checkArgument(requireChildOutputNum > 0, "node count of child fragment must be greater than 0!");

        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
            subStagePlan.getFragment().getPartitioningScheme().setPartitionCount(requireChildOutputNum);
            subStagePlan.getFragment().getPartitioningScheme().setPrunePartitions(scheduler.getPrunePartitions());
            List<SqlStageExecution> subTree =
                createStages(Optional.of(stage), stageIdMapping, initedStages, locationFactory, subStagePlan,
                    partitioningCache, remoteTaskFactory, session,
                    executor, nodeTaskMap, stageSchedulers, stageLinkages
                );

            for (SqlStageExecution child : subTree) {
                if (!childStageIds.contains(child.getStageId())) {
                    // avoid duplicate stage for cte
                    stages.add(child);
                    childStageIds.add(child.getStageId());
                }
            }

            SqlStageExecution childStage = subTree.get(0);
            childStagesBuilder.add(childStage);
        }
        Set<SqlStageExecution> childStages = childStagesBuilder.build();
        stage.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                for (SqlStageExecution childStage : childStages) {
                    // only cancel stage that all of it's parent is done
                    Optional<StageId> notFinishedParent = c2pLinkages.get(childStage.getStageId()).stream()
                        .filter(parentId -> !this.stages.get(parentId).getState().isDone()).findAny();
                    if (!notFinishedParent.isPresent()) {
                        childStage.cancel();
                    }
                }
            }
        });

        StageLinkage linkage =
            new StageLinkage(plan.getFragment(), parent, childStages, c2pLinkages);
        stageLinkages.put(stageId, linkage);
        stage2childStages.put(stageId, childStages);
        initedStages.put(stageId, stage);

        return stages.build();
    }

    public StageExecutionPlan getPlan() {
        return plan;
    }

    public synchronized StageInfo getStageInfo() {
        Map<StageId, StageInfo> stageInfos = stages.values().stream().map(SqlStageExecution::getStageInfo)
            .collect(ImmutableCollectors.toImmutableMap(StageInfo::getStageId, identity()));

        return buildStageInfo(new HashSet<>(), rootStageId, stageInfos);
    }

    private StageInfo buildStageInfo(Set<StageId> visitedStages, StageId stageId, Map<StageId, StageInfo> stageInfos) {
        StageInfo parent = stageInfos.get(stageId);
        if (visitedStages.contains(stageId)) {
            return null;
        }
        visitedStages.add(stageId);
        checkArgument(parent != null, "No stageInfo for %s", parent);
        List<StageInfo> childStages = stageLinkages.get(stageId).stream().limit(1)
            .flatMap(stageLinkage -> stageLinkage.getChildStageIds().stream())
            .collect(ImmutableCollectors.toImmutableSet()).stream()
            .map(childStageId -> buildStageInfo(visitedStages, childStageId, stageInfos)).filter(
                stageInfo -> stageInfo != null).collect(ImmutableCollectors.toImmutableList());
        if (childStages.isEmpty()) {
            return parent;
        }
        return new StageInfo(parent.getStageId(), parent.getState(), parent.getSelf(), parent.getPlan(),
            parent.getTypes(), parent.getStageStats(), parent.getTasks(), childStages, parent.getFailureCause()
        );
    }

    public long getTotalMemoryReservation() {
        return stages.values().stream().mapToLong(SqlStageExecution::getMemoryReservation).sum();
    }

    public Duration getTotalCpuTime() {
        long millis = stages.values().stream().mapToLong(stage -> stage.getTotalCpuTime().toMillis()).sum();
        return new Duration(millis, MILLISECONDS);
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            executor.submit(this::schedule);
        }
    }

    private void schedule() {
        try {
            Set<StageId> completedStages = new HashSet<>();
            ExecutionSchedule executionSchedule;
            if (phasedExecutionSchedule) {
                executionSchedule = new PhasedExecutionSchedule(stages.values());
            } else {
                executionSchedule = new AllAtOnceExecutionSchedule(stages.values());
            }
            while (!executionSchedule.isFinished()) {
                List<CompletableFuture<?>> blockedStages = new ArrayList<>();
                for (SqlStageExecution stage : executionSchedule.getStagesToSchedule()) {
                    stage.beginScheduling();
                    // perform some scheduling work
                    ScheduleResult result = stageSchedulers.get(stage.getStageId()).schedule();

                    // modify parent and children based on the results of the scheduling
                    if (result.isFinished()) {
                        stage.schedulingComplete();
                        if (log.isDebugEnabled()) {
                            log.debug(
                                "schedule stage:" + stage.getStageId() + " fragment: " + stage.getFragment().getId() +
                                    " tasks: " + result.getNewTasks().size());
                        }
                    } else if (!result.getBlocked().isDone()) {
                        blockedStages.add(result.getBlocked());
                    }
                    stageLinkages.get(stage.getStageId()).stream().forEach(stageLinkage -> stageLinkage
                        .processScheduleResults(stage.getStageId(), stage.getState(), result.getNewTasks(), true));
                    if (result.getBlockedReason().isPresent()) {
                        switch (result.getBlockedReason().get()) {
                        case WAITING_FOR_SOURCE:
                        case SPLIT_QUEUES_FULL:
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                "Unknown blocked reason: " + result.getBlockedReason().get());
                        }
                    }
                }

                // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
                for (SqlStageExecution stage : stages.values()) {
                    if (!completedStages.contains(stage.getStageId()) && stage.getState().isDone()) {
                        stageLinkages.get(stage.getStageId()).stream().forEach(stageLinkage -> stageLinkage
                            .processScheduleResults(stage.getStageId(), stage.getState(), ImmutableSet.of(), true));
                        completedStages.add(stage.getStageId());
                    }
                }

                // wait for a state change and then schedule again
                if (!blockedStages.isEmpty()) {
                    tryGetFutureValue(firstCompletedFuture(blockedStages), 1, SECONDS);
                    for (CompletableFuture<?> blockedStage : blockedStages) {
                        blockedStage.cancel(true);
                    }
                }
            }

            for (SqlStageExecution stage : stages.values()) {
                StageState state = stage.getState();
                if (state != SCHEDULED && state != RUNNING && state != FLUSHING && !state.isDone()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        format("Scheduling is complete, but stage %s is in state %s", stage.getStageId(), state)
                    );
                }
            }
        } catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
            throw Throwables.propagate(t);
        } finally {
            RuntimeException closeError = new RuntimeException();
            for (StageScheduler scheduler : stageSchedulers.values()) {
                try {
                    scheduler.close();
                } catch (Throwable t) {
                    queryStateMachine.transitionToFailed(t);
                    // Self-suppression not permitted
                    if (closeError != t) {
                        closeError.addSuppressed(t);
                    }
                }
            }
            if (closeError.getSuppressed().length > 0) {
                throw closeError;
            }
        }

        // TODO scheduling complete
        for (SqlStageExecution stage : stages.values()) {
            stage.schedulingComplete();
        }
    }

    public void cancelStage(StageId stageId) {
        //try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
        SqlStageExecution sqlStageExecution = stages.get(stageId);
        SqlStageExecution stage = requireNonNull(sqlStageExecution, () -> format("Stage %s does not exist", stageId));
        stage.cancel();
        //}
    }

    public void abort() {
        //try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
        stages.values().stream().forEach(SqlStageExecution::abort);
        this.bloomFilterManager.close();
        //}
    }

    private class StageLinkage {
        private final Integer currentStageFragmentId;
        private final Optional<SqlStageExecution> parent;
        private final Set<OutputBufferManager> childOutputBufferManagers;
        private final Set<StageId> childStageIds;
        private final boolean childStageHasMultiParent;

        public StageLinkage(PlanFragment fragment, Optional<SqlStageExecution> parent, Set<SqlStageExecution> children,
                            Multimap<StageId, StageId> linkages) {
            this.currentStageFragmentId = fragment.getId();
            this.parent = parent;
            this.childStageHasMultiParent =
                children.stream().filter(childStage -> linkages.get(childStage.getStageId()).size() > 1).findAny()
                    .isPresent();
            this.childOutputBufferManagers = children.stream().map(childStage -> {
                if (outputBufferManagers.containsKey(childStage.getStageId())) {
                    return outputBufferManagers.get(childStage.getStageId());
                }
                Set<StageId> childParents = new HashSet<>(linkages.get(childStage.getStageId()));
                PartitionShuffleHandle partitioningHandle =
                    childStage.getFragment().getPartitioningScheme().getShuffleHandle();
                OutputBufferManager outputBufferManager;
                if (partitioningHandle.getPartitionShuffleMode()
                    == PartitionShuffleHandle.PartitionShuffleMode.BROADCAST) {
                    ImmutableMap<Integer, Boolean> parentStageIds = childParents.stream()
                        .map(stageId -> stageId.getId()).collect(
                            ImmutableCollectors.toImmutableMap(stageId -> stageId, stageId -> false));
                    outputBufferManager =
                        new BroadcastOutputBufferManager(new HashMap<>(parentStageIds), childStage::setOutputBuffers);
                } else {
                    int partitionCount;
                    if (partitioningHandle.isRemotePairWise()) {
                        // task ID under partition-wise mode is determined by (physicalPartitionID % nodeCount)
                        // so we just assign a max count here
                        partitionCount = nodeSelector.getOrderedNode().size();
                        partitioningHandle.setPartitionCount(partitionCount);
                    } else {
                        partitionCount = partitioningHandle.getPartitionCount();
                    }
                    outputBufferManager =
                        new PartitionedOutputBufferManager(childParents, childStageHasMultiParent, partitionCount,
                            childStage::setOutputBuffers
                        );
                }
                outputBufferManagers.put(childStage.getStageId(), outputBufferManager);
                return outputBufferManager;
            }).collect(ImmutableCollectors.toImmutableSet());

            this.childStageIds =
                children.stream().map(SqlStageExecution::getStageId).collect(ImmutableCollectors.toImmutableSet());
        }

        public Set<StageId> getChildStageIds() {
            return childStageIds;
        }

        public void processScheduleResults(StageId stageId, StageState newState, Set<RemoteTask> newTasks,
                                           boolean doParent
        ) {
            boolean noMoreTasks = false;
            switch (newState) {
            case PLANNED:
            case SCHEDULING:
                // workers are still being added to the query
                break;
            case SCHEDULING_SPLITS:
            case SCHEDULED:
            case RUNNING:
            case FLUSHING:
            case FINISHED:
            case CANCELED:
                // no more workers will be added to the query
                noMoreTasks = true;
            case ABORTED:
            case FAILED:
                // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                // stage above to finish normally, which will result in a query
                // completing successfully when it should fail..
                break;
            }

            if (parent.isPresent() && doParent) {
                // Add an exchange location to the parent stage for each new task
                Set<TaskLocation> newExchangeLocations =
                    newTasks.stream().map(task -> task.getTaskStatus().getSelf())
                        .collect(ImmutableCollectors.toImmutableSet());
                parent.get().addExchangeLocations(currentStageFragmentId, newExchangeLocations, noMoreTasks);
            }

            if (!childOutputBufferManagers.isEmpty()) {
                // Add an output buffer to the child stages for each new task
                List<OutputBuffers.OutputBufferId> newOutputBuffers = null;
                if (childStageHasMultiParent) {
                    newOutputBuffers = newTasks.stream()
                        .map(task -> new OutputBuffers.OutputBufferId(task.getTaskId().getStageId().getId(),
                            task.getTaskId().getId()
                        )).collect(ImmutableCollectors.toImmutableList());
                } else {
                    // not specify stageId in OutputBufferId for cte compatibility
                    newOutputBuffers =
                        newTasks.stream().map(task -> new OutputBuffers.OutputBufferId(task.getTaskId().getId()))
                            .collect(ImmutableCollectors.toImmutableList());
                }

                for (OutputBufferManager child : childOutputBufferManagers) {
                    child.addOutputBuffers(stageId, newOutputBuffers, noMoreTasks);
                }
            }
        }
    }
}

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
package com.alibaba.polardbx.executor.mpp.execution;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.StateMachine.StateChangeListener;
import com.alibaba.polardbx.executor.mpp.operator.BlockedReason;
import com.alibaba.polardbx.executor.mpp.operator.OperatorStats;
import com.alibaba.polardbx.executor.mpp.operator.TaskStats;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.util.Failures;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.ABORTED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.CANCELED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.FAILED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.FINISHED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.FLUSHING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.PLANNED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.RUNNING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.SCHEDULED;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.SCHEDULING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.SCHEDULING_SPLITS;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.TERMINAL_STAGE_STATES;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class StageStateMachine {
    private static final Logger log = LoggerFactory.getLogger(StageStateMachine.class);

    private final StageId stageId;
    private final URI location;
    private final PlanFragment fragment;
    private final Session session;

    private final StateMachine<StageState> stageState;
    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<DateTime> schedulingComplete = new AtomicReference<>();

    private final AtomicLong peakMemory = new AtomicLong();
    private final AtomicLong currentMemory = new AtomicLong();

    public StageStateMachine(
        StageId stageId,
        URI location,
        Session session,
        PlanFragment fragment,
        ExecutorService executor) {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.location = requireNonNull(location, "location is null");
        this.session = requireNonNull(session, "session is null");
        this.fragment = requireNonNull(fragment, "fragment is null");

        stageState = new StateMachine<>("stage " + stageId, executor, PLANNED, TERMINAL_STAGE_STATES);
        if (log.isDebugEnabled()) {
            stageState.addStateChangeListener(state -> log.debug(String.format("Stage %s is %s", stageId, state)));
        }
    }

    public StageId getStageId() {
        return stageId;
    }

    public URI getLocation() {
        return location;
    }

    public Session getSession() {
        return session;
    }

    public StageState getState() {
        return stageState.get();
    }

    public PlanFragment getFragment() {
        return fragment;
    }

    public void addStateChangeListener(StateChangeListener<StageState> stateChangeListener) {
        stageState.addStateChangeListener(stateChangeListener);
    }

    public synchronized boolean transitionToScheduling() {
        return stageState.compareAndSet(PLANNED, SCHEDULING);
    }

    public synchronized boolean transitionToSchedulingSplits() {
        return stageState
            .setIf(SCHEDULING_SPLITS, currentState -> currentState == PLANNED || currentState == SCHEDULING);
    }

    public synchronized boolean transitionToScheduled() {
        schedulingComplete.compareAndSet(null, DateTime.now());
        return stageState.setIf(SCHEDULED,
            currentState -> currentState == PLANNED || currentState == SCHEDULING || currentState == SCHEDULING_SPLITS);
    }

    public boolean transitionToRunning() {
        return stageState.setIf(RUNNING, currentState -> currentState != RUNNING && !currentState.isDone());
    }

    public boolean transitionToFlushing() {
        return stageState.setIf(FLUSHING, currentState -> currentState != FLUSHING && !currentState.isDone());
    }

    public boolean transitionToFinished() {
        if (log.isDebugEnabled()) {
            log.debug("stage: " + stageId + " finished");
        }
        return stageState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToCanceled() {
        return stageState.setIf(CANCELED, currentState -> !currentState.isDone());
    }

    public boolean transitionToAborted() {
        return stageState.setIf(ABORTED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable) {
        requireNonNull(throwable, "throwable is null");

        failureCause.compareAndSet(null, Failures.toFailure(throwable));
        boolean failed = stageState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            log.error("Stage " + stageId + " failed", throwable);
        } else if (log.isDebugEnabled()) {
            log.debug("Failure after stage " + stageId + " finished", throwable);
        }
        return failed;
    }

    public long getPeakMemoryInBytes() {
        return peakMemory.get();
    }

    public long getMemoryReservation() {
        return currentMemory.get();
    }

    public void updateMemoryUsage(long deltaMemoryInBytes) {
        long currentMemoryValue = currentMemory.addAndGet(deltaMemoryInBytes);
        if (currentMemoryValue > peakMemory.get()) {
            peakMemory.updateAndGet(x -> currentMemoryValue > x ? currentMemoryValue : x);
        }
    }

    public StageInfo getStageInfo(Supplier<Iterable<TaskInfo>> taskInfosSupplier,
                                  Supplier<Iterable<StageInfo>> subStageInfosSupplier) {
        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageState state = stageState.get();

        List<TaskInfo> taskInfos = ImmutableList.copyOf(taskInfosSupplier.get());
        List<StageInfo> subStageInfos = ImmutableList.copyOf(subStageInfosSupplier.get());

        int totalTasks = taskInfos.size();
        int runningTasks = 0;
        int completedTasks = 0;

        int totalPipelineExecs = 0;
        int queuedPipelineExecs = 0;
        int runningPipelineExecs = 0;
        int completedPipelineExecs = 0;

        long cumulativeMemory = 0;
        long totalMemoryReservation = 0;
        long peakMemoryReservation = getPeakMemoryInBytes();

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long totalUserTime = 0;
        long totalBlockedTime = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();
        List<OperatorStats> operators = new ArrayList<>();

        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.getTaskStatus().getState();
            if (taskState.isDone()) {
                completedTasks++;
            } else {
                runningTasks++;
            }

            TaskStats taskStats = taskInfo.getStats();

            if (taskStats != null) {
                totalPipelineExecs += taskStats.getTotalPipelineExecs();
                queuedPipelineExecs += taskStats.getQueuedPipelineExecs();
                runningPipelineExecs += taskStats.getRunningPipelineExecs();
                completedPipelineExecs += taskStats.getCompletedPipelineExecs();

                cumulativeMemory += taskStats.getCumulativeMemory();
                totalMemoryReservation += taskStats.getMemoryReservation();

                totalScheduledTime += taskStats.getTotalScheduledTime();
                totalCpuTime += taskStats.getTotalCpuTime();
                totalUserTime += taskStats.getTotalUserTime();
                totalBlockedTime += taskStats.getTotalBlockedTime();
                if (!taskState.isDone()) {
                    fullyBlocked &= taskStats.isFullyBlocked();
                    blockedReasons.addAll(taskStats.getBlockedReasons());
                }

                processedInputDataSize += taskStats.getProcessedInputDataSize();
                processedInputPositions += taskStats.getProcessedInputPositions();

                outputDataSize += taskStats.getOutputDataSize();
                outputPositions += taskStats.getOutputPositions();

                if (taskState.isDone()) {
                    if (operators.size() == 0) {
                        for (int i = 0; i < taskStats.getOperatorStats().size(); i++) {
                            OperatorStats operator = taskStats.getOperatorStats().get(i);
                            operators.add(new OperatorStats(Optional.of(stageId), operator.getPipelineId(),
                                operator.getOperatorType(),
                                operator.getOperatorId(), operator.getOutputRowCount(), operator.getOutputBytes(),
                                operator.getStartupDuration(), operator.getDuration(), operator.getMemory(),
                                operator.getInstances(), operator.getSpillCnt()));
                        }
                    } else {
                        if (taskStats.getOperatorStats() != null) {
                            if (operators.size() == taskStats.getOperatorStats().size()) {
                                //ready
                                for (int i = 0; i < operators.size(); i++) {
                                    if (taskStats.getOperatorStats().get(i) != null) {
                                        operators.set(i, operators.get(i).add(taskStats.getOperatorStats().get(i)));
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                totalPipelineExecs += taskInfo.getTotalPipelineExecs();
                completedPipelineExecs += taskInfo.getCompletedPipelineExecs();
                cumulativeMemory += taskInfo.getCumulativeMemory();
                totalMemoryReservation += taskInfo.getMemoryReservation();
            }
        }

        StageStats stageStats = new StageStats(
            schedulingComplete.get(),
            totalTasks,
            runningTasks,
            completedTasks,

            totalPipelineExecs,
            queuedPipelineExecs,
            runningPipelineExecs,
            completedPipelineExecs,
            cumulativeMemory,
            totalMemoryReservation > 0 ? succinctBytes(totalMemoryReservation) : succinctBytes(0),
            peakMemoryReservation > 0 ? succinctBytes(peakMemoryReservation) : succinctBytes(0),
            totalScheduledTime,
            totalCpuTime,
            totalUserTime,
            totalBlockedTime,
            fullyBlocked && runningTasks > 0,
            blockedReasons,
            processedInputDataSize > 0 ? succinctBytes(processedInputDataSize) : succinctBytes(0),
            processedInputPositions,
            outputDataSize > 0 ? succinctBytes(outputDataSize) : succinctBytes(0),
            outputPositions,
            operators);

        ExecutionFailureInfo failureInfo = null;
        if (state == FAILED) {
            failureInfo = failureCause.get();
        }
        return new StageInfo(stageId,
            state,
            location,
            new PlanInfo(fragment.getId(), fragment.getRootNode(), fragment.getRootId(),
                fragment.getPartitionedSources().size() > 0),
            fragment.getTypes(),
            stageStats,
            taskInfos,
            subStageInfos,
            failureInfo);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("stageId", stageId)
            .add("stageState", stageState)
            .toString();
    }
}

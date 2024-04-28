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

import com.alibaba.polardbx.executor.mpp.operator.BlockedReason;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.OperatorStats;
import com.alibaba.polardbx.executor.mpp.operator.TaskStats;
import com.alibaba.polardbx.executor.mpp.planner.PipelineProperties;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.rel.RelNode;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.properties.MetricLevel.isSQLMetricEnabled;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PipelineContext {

    @GuardedBy("driverLock")
    private static final AtomicIntegerFieldUpdater<PipelineContext> driverIdUpdater =
        AtomicIntegerFieldUpdater.newUpdater(PipelineContext.class, "driverIdInt");

    @GuardedBy("driverLock")
    private static final AtomicIntegerFieldUpdater<PipelineContext> driverFinishedUpdater =
        AtomicIntegerFieldUpdater.newUpdater(PipelineContext.class, "driverFinishedInt");

    private volatile int driverIdInt = 0;
    private volatile int driverFinishedInt = 0;

    private final TaskContext taskContext;
    private final int pipelineId;

    private final List<DriverContext> drivers = new CopyOnWriteArrayList<>();
    private final Object driverLock = new Object();

    private final AtomicLong totalScheduledTime = new AtomicLong(0);
    private final AtomicLong totalCpuTime = new AtomicLong(0);
    private final AtomicLong totalUserTime = new AtomicLong(0);
    private final AtomicLong totalBlockedTime = new AtomicLong(0);

    @Nullable
    private PipelineProperties pipelineProperties;

    private final DateTime startTime = DateTime.now();

    public PipelineContext(int pipelineId, TaskContext taskContext) {
        this.pipelineId = pipelineId;
        this.taskContext = requireNonNull(taskContext, "taskContext is null");
    }

    public TaskContext getTaskContext() {
        return taskContext;
    }

    public TaskId getTaskId() {
        return taskContext.getTaskId();
    }

    public int getPipelineId() {
        return pipelineId;
    }

    public DriverContext addDriverContext(boolean partitioned) {
        int driverId;
        synchronized (driverLock) {
            driverId = driverIdUpdater.incrementAndGet(this);
        }

        DriverContext driverContext = new DriverContext(this, partitioned, driverId);
        drivers.add(driverContext);
        return driverContext;
    }

    public int getMetricLevel() {
        return taskContext.getMetricLevel();
    }

    public void driverFinished(DriverContext driverContext) {
        requireNonNull(driverContext, "driverContext is null");

        if (!drivers.contains(driverContext)) {
            throw new IllegalArgumentException("Unknown driver " + driverContext);
        }

        // counter the driver, finish pipeline if satisfied
        synchronized (driverLock) {
            driverFinishedUpdater.incrementAndGet(this);
            if (driverFinishedUpdater.get(this) == driverIdUpdater.get(this)) {
                taskContext.pipelineFinished(this);
            }
        }
        if (isSQLMetricEnabled(taskContext.getMetricLevel())) {
            totalCpuTime.getAndAdd(driverContext.getTotalCpuTime());
            totalScheduledTime.getAndAdd(driverContext.getTotalScheduledTime());
            totalUserTime.getAndAdd(driverContext.getTotalUserTime());
            totalBlockedTime.getAndAdd(driverContext.getTotalBlockedTime());
        }
    }

    public AtomicLong getTotalScheduledTime() {
        return totalScheduledTime;
    }

    public AtomicLong getTotalCpuTime() {
        return totalCpuTime;
    }

    public AtomicLong getTotalUserTime() {
        return totalUserTime;
    }

    public AtomicLong getTotalBlockedTime() {
        return totalBlockedTime;
    }

    public void start() {
        taskContext.start();
    }

    public void failed(Throwable cause) {
        taskContext.failed(cause);
    }

    public boolean isDone() {
        return taskContext.isDone();
    }

    public boolean isCpuTimerEnabled() {
        return taskContext.isCpuTimerEnabled();
    }

    public List<DriverContext> getDriverContexts() {
        return drivers;
    }

    public StageInfo buildLocalModeStageInfo(
        String queryId, URI self,
        Map<StageId, StageInfo> stageInfoMap,
        boolean finished) {
        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageId stageId = new StageId(queryId, pipelineId);
        StageInfo stageInfo = stageInfoMap.get(stageId);
        if (stageInfo != null) {
            return stageInfo;
        }

        StageState state = getState(finished);
        List<StageInfo> subStageInfos = new ArrayList<>();

        if (pipelineProperties != null) {
            for (PipelineProperties child : pipelineProperties.getChildren()) {
                PipelineContext childContext = null;
                List<PipelineContext> pipelineContexts = taskContext.getPipelineContexts();
                for (PipelineContext pipelineContext : pipelineContexts) {
                    if (pipelineContext.getPipelineId() == child.getPipelineId()) {
                        childContext = pipelineContext;
                        break;
                    }
                }
                if (childContext != null) {
                    StageInfo childStage = childContext.buildLocalModeStageInfo(queryId, self, stageInfoMap, finished);
                    subStageInfos.add(childStage);
                }
            }
        }

        List<TaskInfo> taskInfos = new ArrayList<>();
        for (DriverContext context : drivers) {
            TaskInfo taskInfo = context.buildLocalModeTaskInfo(queryId);
            taskInfos.add(taskInfo);
        }

        int totalTasks = taskInfos.size();
        int runningTasks = 0;
        int completedTasks = 0;

        int totalPipelineExecs = 0;
        int queuedPipelineExecs = 0;
        int runningPipelineExecs = 0;
        int completedPipelineExecs = 0;

        long cumulativeMemory = 0;
        long totalMemoryReservation = 0;
        long peakMemoryReservation = 0;

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

            TaskStats taskStats = taskInfo.getTaskStats();

            if (taskStats != null) {
                totalPipelineExecs += taskStats.getTotalPipelineExecs();
                queuedPipelineExecs += taskStats.getQueuedPipelineExecs();
                runningPipelineExecs += taskStats.getRunningPipelineExecs();
                completedPipelineExecs += taskStats.getCompletedPipelineExecs();

                cumulativeMemory += taskStats.getCumulativeMemory();
                totalMemoryReservation += taskStats.getMemoryReservation();

                totalScheduledTime += taskStats.getTotalScheduledTimeNanos();
                totalCpuTime += taskStats.getTotalCpuTimeNanos();
                totalUserTime += taskStats.getTotalUserTimeNanos();
                totalBlockedTime += taskStats.getTotalBlockedTimeNanos();
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
                                operator.getOperatorId(), operator.getOutputRowCount(),
                                operator.getRuntimeFilteredCount(),
                                operator.getOutputBytes(), operator.getStartupDuration(), operator.getDuration(),
                                operator.getMemory(), operator.getInstances(), operator.getSpillCnt()));
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
            startTime,
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

        RelNode physicalPlan = null;
        if (pipelineProperties != null) {
            physicalPlan = pipelineProperties.getRelNode();
        }
        StageInfo ret = new StageInfo(stageId,
            state,
            self,
            new PlanInfo(pipelineId, physicalPlan, physicalPlan.getRelatedId(), false),
            CalciteUtils.getTypes(physicalPlan.getRowType()),
            stageStats,
            taskInfos,
            subStageInfos,
            null);
        stageInfoMap.put(stageId, ret);
        return ret;
    }

    private StageState getState(boolean finished) {
        if (driverFinishedUpdater.get(this) == driverIdUpdater.get(this)) {
            return StageState.FINISHED;
        } else if (finished) {
            return StageState.FAILED;
        } else {
            return StageState.RUNNING;
        }
    }

    public void setPipelineProperties(@Nullable PipelineProperties pipelineProperties) {
        this.pipelineProperties = pipelineProperties;
    }
}

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferState;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferInfo;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.mpp.operator.TaskStats;
import com.alibaba.polardbx.util.MoreObjects;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import java.util.Set;

import static com.alibaba.polardbx.executor.mpp.execution.TaskStatus.initialTaskStatus;
import static com.alibaba.polardbx.executor.mpp.operator.TaskStats.getEmptyTaskStats;
import static java.util.Objects.requireNonNull;

@Immutable
public class TaskInfo {

    private static final TaskInfo EMPTY_TASK_INFO = new TaskInfo(
        TaskStatus.getEmptyTaskStatus(),
        DateTime.now(),
        new OutputBufferInfo(BufferState.OPEN, 0),
        ImmutableSet.of(),
        null,
        true,
        false,
        getEmptyTaskStats().getCompletedPipelineExecs(),
        getEmptyTaskStats().getTotalPipelineExecs(),
        getEmptyTaskStats().getCumulativeMemory(),
        getEmptyTaskStats().getMemoryReservation(),
        getEmptyTaskStats().getElapsedTimeMillis(),
        getEmptyTaskStats().getTotalCpuTimeNanos(),
        0,
        0,
        0,
        0);

    public static TaskInfo getEmptyTaskInfo() {
        return EMPTY_TASK_INFO;
    }

    private final TaskStatus taskStatus;
    private final DateTime lastHeartbeat;
    private final OutputBufferInfo outputBuffers;
    private final Set<Integer> noMoreSplits;
    private final TaskStats taskStats;

    private final boolean needsPlan;
    private final boolean complete;

    private final int completedPipelineExecs;
    private final int totalPipelineExecs;
    private final double cumulativeMemory;
    private final long memoryReservation;
    private final long elapsedTimeMillis;
    private final long totalCpuTime;
    private final long processTimeMillis;
    private final long processWall;
    private final long pullDataTimeMillis;
    private final long deliveryTimeMillis;

    @JsonCreator
    public TaskInfo(@JsonProperty("taskStatus") TaskStatus taskStatus,
                    @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
                    @JsonProperty("outputBuffers") OutputBufferInfo outputBuffers,
                    @JsonProperty("noMoreSplits") Set<Integer> noMoreSplits,
                    @JsonProperty("taskStats") TaskStats taskStats,
                    @JsonProperty("needsPlan") boolean needsPlan,
                    @JsonProperty("complete") boolean complete,
                    @JsonProperty("completedPipelineExecs") int completedPipelineExecs,
                    @JsonProperty("totalPipelineExecs") int totalPipelineExecs,
                    @JsonProperty("cumulativeMemory") double cumulativeMemory,
                    @JsonProperty("memoryReservation") long memoryReservation,
                    @JsonProperty("elapsedTimeMillis") long elapsedTimeMillis,
                    @JsonProperty("totalCpuTime") long totalCpuTime,
                    @JsonProperty("processTimeMillis") long processTimeMillis,
                    @JsonProperty("processWall") long processWall,
                    @JsonProperty("pullDataTimeMillis") long pullDataTimeMillis,
                    @JsonProperty("deliveryTimeMillis") long deliveryTimeMillis) {
        this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        this.lastHeartbeat = requireNonNull(lastHeartbeat, "lastHeartbeat is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.noMoreSplits = requireNonNull(noMoreSplits, "noMoreSplits is null");
        this.taskStats = taskStats;

        this.needsPlan = needsPlan;
        this.complete = complete;

        this.completedPipelineExecs = completedPipelineExecs;
        this.totalPipelineExecs = totalPipelineExecs;

        this.cumulativeMemory = cumulativeMemory;
        this.memoryReservation = memoryReservation;

        this.elapsedTimeMillis = elapsedTimeMillis;
        this.totalCpuTime = totalCpuTime;

        this.processTimeMillis = processTimeMillis;
        this.processWall = processWall;
        this.pullDataTimeMillis = pullDataTimeMillis;
        this.deliveryTimeMillis = deliveryTimeMillis;
    }

    @JsonProperty
    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    @JsonProperty
    public DateTime getLastHeartbeat() {
        return lastHeartbeat;
    }

    @JsonProperty
    public OutputBufferInfo getOutputBuffers() {
        return outputBuffers;
    }

    @JsonProperty
    public Set<Integer> getNoMoreSplits() {
        return noMoreSplits;
    }

    @JsonProperty
    public TaskStats getTaskStats() {
        return taskStats;
    }

    @JsonProperty
    public boolean isNeedsPlan() {
        return needsPlan;
    }

    @JsonProperty
    public boolean isComplete() {
        return complete;
    }

    @JsonProperty
    public int getCompletedPipelineExecs() {
        return completedPipelineExecs;
    }

    @JsonProperty
    public int getTotalPipelineExecs() {
        return totalPipelineExecs;
    }

    @JsonProperty
    public double getCumulativeMemory() {
        return cumulativeMemory;
    }

    @JsonProperty
    public long getMemoryReservation() {
        return memoryReservation;
    }

    @JsonProperty
    public long getElapsedTimeMillis() {
        return elapsedTimeMillis;
    }

    @JsonProperty
    public long getTotalCpuTime() {
        return totalCpuTime;
    }

    @JsonProperty
    public long getProcessTimeMillis() {
        return processTimeMillis;
    }

    @JsonProperty
    public long getProcessWall() {
        return processWall;
    }

    @JsonProperty
    public long getPullDataTimeMillis() {
        return pullDataTimeMillis;
    }

    @JsonProperty
    public long getDeliveryTimeMillis() {
        return deliveryTimeMillis;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("taskId", taskStatus.getTaskId())
            .add("state", taskStatus.getState())
            .toString();
    }

    public static TaskInfo createInitialTask(
        String nodeId, TaskId taskId, TaskLocation location, TaskStats taskStats) {
        return new TaskInfo(
            initialTaskStatus(taskId, location, nodeId),
            DateTime.now(),
            new OutputBufferInfo(BufferState.OPEN, 0),
            ImmutableSet.of(),
            null,
            true,
            false,
            taskStats.getCompletedPipelineExecs(),
            taskStats.getTotalPipelineExecs(),
            taskStats.getCumulativeMemory(),
            taskStats.getMemoryReservation(),
            taskStats.getElapsedTimeMillis(),
            taskStats.getTotalCpuTimeNanos(),
            0,
            0,
            0,
            0);
    }

    public TaskInfo withTaskStatus(TaskStatus newTaskStatus) {
        return new TaskInfo(
            newTaskStatus,
            lastHeartbeat,
            outputBuffers,
            noMoreSplits,
            taskStats,
            needsPlan,
            complete, completedPipelineExecs, totalPipelineExecs,
            cumulativeMemory,
            memoryReservation,
            elapsedTimeMillis,
            totalCpuTime,
            processTimeMillis,
            processWall,
            pullDataTimeMillis,
            deliveryTimeMillis);
    }

    public String toTaskString() {
        MoreObjects.ToStringHelper toString = MoreObjects.toStringHelper(this);
        toString.add("task", getTaskStatus().getTaskId());
        toString.add("elapsedTimeMillis", elapsedTimeMillis);
        toString.add("processTimeMillis", processTimeMillis);
        toString.add("processWall", processWall);
        toString.add("pullDataTime", pullDataTimeMillis);
        toString.add("deliveryTime", deliveryTimeMillis);
        TaskLocation taskLocation = getTaskStatus().getSelf();
        toString.add("host", taskLocation.getNodeServer().getHost() + ":" + taskLocation.getNodeServer().getHttpPort());
        return toString.toString();
    }
}

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
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.util.MoreObjects;
import com.alibaba.polardbx.optimizer.statis.TaskMemoryStatisticsGroup;
import com.alibaba.polardbx.statistics.ExecuteSQLOperation;
import com.alibaba.polardbx.statistics.RuntimeStatistics;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.alibaba.polardbx.executor.mpp.execution.TaskId.getEmptyTask;
import static com.alibaba.polardbx.executor.mpp.metadata.TaskLocation.getEmptyTaskLocation;
import static java.util.Objects.requireNonNull;

public class TaskStatus {

    private static final TaskStatus EMPTY_STATUS =
        new TaskStatus("", getEmptyTask(), "", 0, TaskState.PLANNED, getEmptyTaskLocation(),
            ImmutableList.of(), null, null, null, 0, 0, 0);

    public static TaskStatus getEmptyTaskStatus() {
        return EMPTY_STATUS;
    }

    /**
     * The first valid version that will be returned for a remote task.
     */
    public static final long STARTING_VERSION = 1;

    /**
     * A value lower than {@link #STARTING_VERSION}. This value can be used to
     * create an initial local task that is always older than any remote task.
     */
    private static final long MIN_VERSION = 0;

    /**
     * A value larger than any valid value. This value can be used to create
     * a final local task that is always newer than any remote task.
     */
    private static final long MAX_VERSION = Long.MAX_VALUE;

    private final TaskId taskId;
    private final String taskInstanceId;
    private final long version;
    private final TaskState state;
    private final TaskLocation self;
    private final String nodeId;

    private final int queuedPartitionedDrivers;
    private final int runningPartitionedDrivers;
    private final long memoryReservation;

    private final List<ExecutionFailureInfo> failures;

    private final List<ExecuteSQLOperation> sqlTracer;

    //算子视角的统计
    private final Map<Integer, RuntimeStatistics.OperatorStatisticsGroup> runtimeStatistics;

    //memoryPool视角的统计
    private final TaskMemoryStatisticsGroup memoryStatistics;

    @JsonCreator
    public TaskStatus(
        @JsonProperty("nodeId") String nodeId,
        @JsonProperty("taskId") TaskId taskId,
        @JsonProperty("taskInstanceId") String taskInstanceId,
        @JsonProperty("version") long version,
        @JsonProperty("state") TaskState state,
        @JsonProperty("self") TaskLocation self,
        @JsonProperty("failures") List<ExecutionFailureInfo> failures,
        @JsonProperty("sqlTracer") List<ExecuteSQLOperation> sqlTracer,
        @JsonProperty("runtimeStatistics") Map<Integer, RuntimeStatistics.OperatorStatisticsGroup> runtimeStatistics,
        @JsonProperty("memoryStatistics") TaskMemoryStatisticsGroup memoryStatistics,
        @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
        @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
        @JsonProperty("memoryReservation") long memoryReservation) {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");

        checkState(version >= MIN_VERSION, "version must be >= MIN_VERSION");
        this.version = version;
        this.state = requireNonNull(state, "state is null");
        this.self = requireNonNull(self, "self is null");

        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers must be positive");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;

        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers must be positive");
        this.runningPartitionedDrivers = runningPartitionedDrivers;

        this.memoryReservation = memoryReservation;
        this.failures = ImmutableList.copyOf(requireNonNull(failures, "failures is null"));
        this.sqlTracer = sqlTracer;
        this.runtimeStatistics = runtimeStatistics;
        this.memoryStatistics = memoryStatistics;
    }

    @JsonProperty
    public TaskId getTaskId() {
        return taskId;
    }

    @JsonProperty
    public String getTaskInstanceId() {
        return taskInstanceId;
    }

    @JsonProperty
    public long getVersion() {
        return version;
    }

    @JsonProperty
    public TaskState getState() {
        return state;
    }

    @JsonProperty
    public TaskLocation getSelf() {
        return self;
    }

    @JsonProperty
    public List<ExecutionFailureInfo> getFailures() {
        return failures;
    }

    @JsonProperty
    public int getQueuedPartitionedDrivers() {
        return queuedPartitionedDrivers;
    }

    @JsonProperty
    public int getRunningPartitionedDrivers() {
        return runningPartitionedDrivers;
    }

    @JsonProperty
    public long getMemoryReservation() {
        return memoryReservation;
    }

    @JsonProperty
    public String getNodeId() {
        return nodeId;
    }

    @JsonProperty
    public List<ExecuteSQLOperation> getSqlTracer() {
        return sqlTracer;
    }

    @JsonProperty
    public Map<Integer, RuntimeStatistics.OperatorStatisticsGroup> getRuntimeStatistics() {
        return runtimeStatistics;
    }

    @JsonProperty
    public TaskMemoryStatisticsGroup getMemoryStatistics() {
        return memoryStatistics;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("taskId", taskId)
            .add("state", state)
            .toString();
    }

    public static TaskStatus initialTaskStatus(TaskId taskId, TaskLocation location, String nodeId) {
        return new TaskStatus(nodeId, taskId, "", MIN_VERSION, TaskState.PLANNED, location, ImmutableList.of(), null,
            null, null, 0, 0, 0);
    }

    public static TaskStatus failWith(TaskStatus taskStatus, TaskState state, List<ExecutionFailureInfo> exceptions) {
        return new TaskStatus(
            taskStatus.getNodeId(),
            taskStatus.getTaskId(),
            taskStatus.getTaskInstanceId(),
            MAX_VERSION,
            state,
            taskStatus.getSelf(),
            exceptions,
            null,
            null,
            null,
            taskStatus.getQueuedPartitionedDrivers(),
            taskStatus.getRunningPartitionedDrivers(),
            taskStatus.getMemoryReservation());
    }

    public TaskStatus forceAbort() {
        return new TaskStatus(
            getNodeId(),
            getTaskId(),
            getTaskInstanceId(),
            MAX_VERSION,
            state,
            getSelf(),
            ImmutableList.of(),
            null,
            null,
            null,
            getQueuedPartitionedDrivers(),
            getRunningPartitionedDrivers(),
            getMemoryReservation());
    }
}

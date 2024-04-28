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
import com.alibaba.polardbx.executor.mpp.operator.OperatorStats;
import com.alibaba.polardbx.util.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class QueryStats {
    private final DateTime createTime;

    private final DateTime executionStartTime;
    private final DateTime lastHeartbeat;
    private final DateTime endTime;

    private final Duration elapsedTime;
    private final Duration queuedTime;
    private final Duration distributedPlanningTime;
    private final Duration totalPlanningTime;
    private final Duration finishingTime;

    private final int totalTasks;
    private final int runningTasks;
    private final int completedTasks;

    private final int totalPipelinExecs;
    private final int queuedPipelinExecs;
    private final int runningPipelinExecs;
    private final int completedPipelinExecs;

    private final double cumulativeMemory;
    private final DataSize totalMemoryReservation;
    private final DataSize peakMemoryReservation;

    private final Duration totalScheduledTime;
    private final Duration totalCpuTime;
    private final Duration totalUserTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize processedInputDataSize;
    private final long processedInputPositions;

    private final DataSize outputDataSize;
    private final long outputPositions;

    private final List<OperatorStats> operatorSummaries;

    @JsonCreator
    public QueryStats(
        @JsonProperty("createTime") DateTime createTime,
        @JsonProperty("executionStartTime") DateTime executionStartTime,
        @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
        @JsonProperty("endTime") DateTime endTime,

        @JsonProperty("elapsedTime") Duration elapsedTime,
        @JsonProperty("queuedTime") Duration queuedTime,
        @JsonProperty("distributedPlanningTime") Duration distributedPlanningTime,
        @JsonProperty("totalPlanningTime") Duration totalPlanningTime,
        @JsonProperty("finishingTime") Duration finishingTime,

        @JsonProperty("totalTasks") int totalTasks,
        @JsonProperty("runningTasks") int runningTasks,
        @JsonProperty("completedTasks") int completedTasks,

        @JsonProperty("totalPipelinExecs") int totalPipelinExecs,
        @JsonProperty("queuedPipelinExecs") int queuedPipelinExecs,
        @JsonProperty("runningPipelinExecs") int runningPipelinExecs,
        @JsonProperty("completedPipelinExecs") int completedPipelinExecs,

        @JsonProperty("cumulativeMemory") double cumulativeMemory,
        @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
        @JsonProperty("peakMemoryReservation") DataSize peakMemoryReservation,

        @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
        @JsonProperty("totalCpuTime") Duration totalCpuTime,
        @JsonProperty("totalUserTime") Duration totalUserTime,
        @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
        @JsonProperty("fullyBlocked") boolean fullyBlocked,
        @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,
        @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
        @JsonProperty("processedInputPositions") long processedInputPositions,

        @JsonProperty("outputDataSize") DataSize outputDataSize,
        @JsonProperty("outputPositions") long outputPositions,
        @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries) {
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.executionStartTime = executionStartTime;
        this.lastHeartbeat = requireNonNull(lastHeartbeat, "lastHeartbeat is null");
        this.endTime = endTime;

        this.elapsedTime = elapsedTime;
        this.queuedTime = queuedTime;
        this.distributedPlanningTime = distributedPlanningTime;
        this.totalPlanningTime = totalPlanningTime;
        this.finishingTime = finishingTime;

        checkArgument(totalTasks >= 0, "totalTasks is negative");
        this.totalTasks = totalTasks;
        checkArgument(runningTasks >= 0, "runningTasks is negative");
        this.runningTasks = runningTasks;
        checkArgument(completedTasks >= 0, "completedTasks is negative");
        this.completedTasks = completedTasks;

        checkArgument(totalPipelinExecs >= 0, "totalPipelinExecs is negative");
        this.totalPipelinExecs = totalPipelinExecs;
        checkArgument(queuedPipelinExecs >= 0, "queuedPipelinExecs is negative");
        this.queuedPipelinExecs = queuedPipelinExecs;
        checkArgument(runningPipelinExecs >= 0, "runningPipelinExecs is negative");
        this.runningPipelinExecs = runningPipelinExecs;
        checkArgument(completedPipelinExecs >= 0, "completedPipelinExecs is negative");
        this.completedPipelinExecs = completedPipelinExecs;

        this.cumulativeMemory = requireNonNull(cumulativeMemory, "cumulativeMemory is null");
        this.totalMemoryReservation = requireNonNull(totalMemoryReservation, "totalMemoryReservation is null");
        this.peakMemoryReservation = requireNonNull(peakMemoryReservation, "peakMemoryReservation is null");
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.totalUserTime = requireNonNull(totalUserTime, "totalUserTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.processedInputDataSize = requireNonNull(processedInputDataSize, "processedInputDataSize is null");
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));
    }

    @JsonProperty
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS", timezone = "GMT+8")
    public DateTime getCreateTime() {
        return createTime;
    }

    @JsonProperty
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS", timezone = "GMT+8")
    public DateTime getExecutionStartTime() {
        return executionStartTime;
    }

    @JsonProperty
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS", timezone = "GMT+8")
    public DateTime getLastHeartbeat() {
        return lastHeartbeat;
    }

    @JsonProperty
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS", timezone = "GMT+8")
    public DateTime getEndTime() {
        return endTime;
    }

    @JsonProperty
    public Duration getElapsedTime() {
        return elapsedTime;
    }

    @JsonProperty
    public Duration getQueuedTime() {
        return queuedTime;
    }

    @JsonProperty
    public Duration getExecutionTime() {
        if (queuedTime == null) {
            // counter-intuitively, this means that the query is still queued
            return new Duration(0, NANOSECONDS);
        }
        return succinctNanos((long) elapsedTime.getValue(NANOSECONDS) - (long) queuedTime.getValue(NANOSECONDS));
    }

    @JsonProperty
    public Duration getDistributedPlanningTime() {
        return distributedPlanningTime;
    }

    @JsonProperty
    public Duration getTotalPlanningTime() {
        return totalPlanningTime;
    }

    @JsonProperty

    public Duration getFinishingTime() {
        return finishingTime;
    }

    @JsonProperty
    public int getTotalTasks() {
        return totalTasks;
    }

    @JsonProperty
    public int getRunningTasks() {
        return runningTasks;
    }

    @JsonProperty
    public int getCompletedTasks() {
        return completedTasks;
    }

    @JsonProperty
    public int getTotalPipelinExecs() {
        return totalPipelinExecs;
    }

    @JsonProperty
    public int getQueuedPipelinExecs() {
        return queuedPipelinExecs;
    }

    @JsonProperty
    public int getRunningPipelinExecs() {
        return runningPipelinExecs;
    }

    @JsonProperty
    public int getCompletedPipelinExecs() {
        return completedPipelinExecs;
    }

    @JsonProperty
    public double getCumulativeMemory() {
        return cumulativeMemory;
    }

    @JsonProperty
    public DataSize getTotalMemoryReservation() {
        return totalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakMemoryReservation() {
        return peakMemoryReservation;
    }

    @JsonProperty
    public Duration getTotalScheduledTime() {
        return totalScheduledTime;
    }

    @JsonProperty
    public Duration getTotalCpuTime() {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getTotalUserTime() {
        return totalUserTime;
    }

    @JsonProperty
    public Duration getTotalBlockedTime() {
        return totalBlockedTime;
    }

    @JsonProperty
    public boolean isFullyBlocked() {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons() {
        return blockedReasons;
    }

    @JsonProperty
    public DataSize getProcessedInputDataSize() {
        return processedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions() {
        return processedInputPositions;
    }

    @JsonProperty
    public DataSize getOutputDataSize() {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions() {
        return outputPositions;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorSummaries() {
        return operatorSummaries;
    }

    public String toPlanString() {
        return MoreObjects.toStringHelper(this)
            .add("elapsedTime", elapsedTime)
            .add("totalPlanningTime", totalPlanningTime)
            .toString();
    }
}

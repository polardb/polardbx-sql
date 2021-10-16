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
import com.alibaba.polardbx.executor.mpp.operator.BlockedReason;
import com.alibaba.polardbx.executor.mpp.operator.OperatorStats;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class StageStats {
    private final DateTime schedulingComplete;

    private final int totalTasks;
    private final int runningTasks;
    private final int completedTasks;

    private final int totalPipelineExecs;
    private final int queuedPipelineExecs;
    private final int runningPipelineExecs;
    private final int completedPipelineExecs;

    private final double cumulativeMemory;
    private final DataSize totalMemoryReservation;
    private final DataSize peakMemoryReservation;

    private final long totalScheduledTime;
    private final long totalCpuTime;
    private final long totalUserTime;
    private final long totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize processedInputDataSize;
    private final long processedInputPositions;

    private final DataSize outputDataSize;
    private final long outputPositions;

    private final List<OperatorStats> operatorSummaries;

    @JsonCreator
    public StageStats(
        @JsonProperty("schedulingComplete") DateTime schedulingComplete,
        @JsonProperty("totalTasks") int totalTasks,
        @JsonProperty("runningTasks") int runningTasks,
        @JsonProperty("completedTasks") int completedTasks,

        @JsonProperty("totalPipelineExecs") int totalPipelineExecs,
        @JsonProperty("queuedPipelineExecs") int queuedPipelineExecs,
        @JsonProperty("runningPipelineExecs") int runningPipelineExecs,
        @JsonProperty("completedPipelineExecs") int completedPipelineExecs,

        @JsonProperty("cumulativeMemory") double cumulativeMemory,
        @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
        @JsonProperty("peakMemoryReservation") DataSize peakMemoryReservation,

        @JsonProperty("totalScheduledTime") long totalScheduledTime,
        @JsonProperty("totalCpuTime") long totalCpuTime,
        @JsonProperty("totalUserTime") long totalUserTime,
        @JsonProperty("totalBlockedTime") long totalBlockedTime,
        @JsonProperty("fullyBlocked") boolean fullyBlocked,
        @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

        @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
        @JsonProperty("processedInputPositions") long processedInputPositions,

        @JsonProperty("outputDataSize") DataSize outputDataSize,
        @JsonProperty("outputPositions") long outputPositions,
        @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries) {
        this.schedulingComplete = schedulingComplete;

        checkArgument(totalTasks >= 0, "totalTasks is negative");
        this.totalTasks = totalTasks;
        checkArgument(runningTasks >= 0, "runningTasks is negative");
        this.runningTasks = runningTasks;
        checkArgument(completedTasks >= 0, "completedTasks is negative");
        this.completedTasks = completedTasks;

        checkArgument(totalPipelineExecs >= 0, "totalPipelineExecs is negative");
        this.totalPipelineExecs = totalPipelineExecs;
        checkArgument(queuedPipelineExecs >= 0, "queuedPipelineExecs is negative");
        this.queuedPipelineExecs = queuedPipelineExecs;
        checkArgument(runningPipelineExecs >= 0, "runningPipelineExecs is negative");
        this.runningPipelineExecs = runningPipelineExecs;
        checkArgument(completedPipelineExecs >= 0, "completedPipelineExecs is negative");
        this.completedPipelineExecs = completedPipelineExecs;

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
        this.operatorSummaries = operatorSummaries;
    }

    @JsonProperty
    public DateTime getSchedulingComplete() {
        return schedulingComplete;
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
    public int getTotalPipelineExecs() {
        return totalPipelineExecs;
    }

    @JsonProperty
    public int getQueuedPipelineExecs() {
        return queuedPipelineExecs;
    }

    @JsonProperty
    public int getRunningPipelineExecs() {
        return runningPipelineExecs;
    }

    @JsonProperty
    public int getCompletedPipelineExecs() {
        return completedPipelineExecs;
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
    public long getTotalScheduledTime() {
        return totalScheduledTime;
    }

    @JsonProperty
    public long getTotalCpuTime() {
        return totalCpuTime;
    }

    @JsonProperty
    public long getTotalUserTime() {
        return totalUserTime;
    }

    @JsonProperty
    public long getTotalBlockedTime() {
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
}

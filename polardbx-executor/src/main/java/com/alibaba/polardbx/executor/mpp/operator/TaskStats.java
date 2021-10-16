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
package com.alibaba.polardbx.executor.mpp.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TaskStats {
    private final static TaskStats EMPTY_STATS = new TaskStats(DateTime.now(), DateTime.now(), -1);

    public static TaskStats getEmptyTaskStats() {
        return EMPTY_STATS;
    }

    private final DateTime createTime;
    private final DateTime firstStartTime;
    private final DateTime endTime;

    private final long elapsedTime;
    private final long queuedTime;
    private final long deliveryTime;

    private final int totalPipelineExecs;
    private final int queuedPipelineExecs;
    private final int runningPipelineExecs;
    private final int completedPipelineExecs;

    private final double cumulativeMemory;
    private final long memoryReservation;
    private final long peakMemory;

    private final long totalScheduledTime;
    private final long totalCpuTime;
    private final long totalUserTime;
    private final long totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final long processedInputDataSize;
    private final long processedInputPositions;

    private final long outputDataSize;
    private final long outputPositions;

    private final List<OperatorStats> operatorStats;

    public TaskStats(DateTime createTime, DateTime endTime, long size) {
        this(createTime,
            null,
            endTime,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0.0,
            size,
            size,
            0,
            size,
            0,
            0,
            false,
            ImmutableSet.of(),
            size,
            0,
            size,
            0,
            ImmutableList.of());
    }

    public TaskStats(DateTime createTime, DateTime endTime) {
        this(createTime,
            null,
            endTime,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0.0,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            ImmutableSet.of(),
            0,
            0,
            0,
            0,
            ImmutableList.of());
    }

    @JsonCreator
    public TaskStats(
        @JsonProperty("createTime")
            DateTime createTime,
        @JsonProperty("firstStartTime")
            DateTime firstStartTime,
        @JsonProperty("endTime")
            DateTime endTime,
        @JsonProperty("elapsedTime")
            long elapsedTime,
        @JsonProperty("queuedTime")
            long queuedTime,
        @JsonProperty("deliveryTime")
            long deliveryTime,
        @JsonProperty("totalPipelineExecs")
            int totalPipelineExecs,
        @JsonProperty("queuedPipelineExecs")
            int queuedPipelineExecs,
        @JsonProperty("runningPipelineExecs")
            int runningPipelineExecs,
        @JsonProperty("completedPipelineExecs")
            int completedPipelineExecs,
        @JsonProperty("cumulativeMemory")
            double cumulativeMemory,
        @JsonProperty("memoryReservation")
            long memoryReservation,
        @JsonProperty("peakMemory")
            long peakMemory,
        @JsonProperty("totalScheduledTime")
            long totalScheduledTime,
        @JsonProperty("totalCpuTime")
            long totalCpuTime,
        @JsonProperty("totalUserTime")
            long totalUserTime,
        @JsonProperty("totalBlockedTime")
            long totalBlockedTime,
        @JsonProperty("fullyBlocked")
            boolean fullyBlocked,
        @JsonProperty("blockedReasons")
            Set<BlockedReason> blockedReasons,
        @JsonProperty("processedInputDataSize")
            long processedInputDataSize,
        @JsonProperty("processedInputPositions")
            long processedInputPositions,
        @JsonProperty("outputDataSize")
            long outputDataSize,
        @JsonProperty("outputPositions")
            long outputPositions,
        @JsonProperty("operatorStats")
            List<OperatorStats> operatorStats) {
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.firstStartTime = firstStartTime;
        this.endTime = endTime;
        this.elapsedTime = elapsedTime;
        this.queuedTime = queuedTime;
        this.deliveryTime = deliveryTime;

        checkArgument(totalPipelineExecs >= 0, "totalPipelineExecs is negative");
        this.totalPipelineExecs = totalPipelineExecs;
        checkArgument(queuedPipelineExecs >= 0, "queuedPipelineExecs is negative");
        this.queuedPipelineExecs = queuedPipelineExecs;

        checkArgument(runningPipelineExecs >= 0, "runningPipelineExecs is negative");
        this.runningPipelineExecs = runningPipelineExecs;

        checkArgument(completedPipelineExecs >= 0, "completedPipelineExecs is negative");
        this.completedPipelineExecs = completedPipelineExecs;

        this.cumulativeMemory = cumulativeMemory;
        this.memoryReservation = memoryReservation;
        this.peakMemory = peakMemory;

        this.totalScheduledTime = totalScheduledTime;
        this.totalCpuTime = totalCpuTime;
        this.totalUserTime = totalUserTime;
        this.totalBlockedTime = totalBlockedTime;
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = requireNonNull(blockedReasons, "blockedReasons is null");

        this.processedInputDataSize = processedInputDataSize;
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSize = outputDataSize;
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;
        this.operatorStats = operatorStats;
    }

    @JsonProperty
    public DateTime getCreateTime() {
        return createTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getFirstStartTime() {
        return firstStartTime;
    }

    @Nullable
    @JsonProperty
    public DateTime getEndTime() {
        return endTime;
    }

    @JsonProperty
    public long getElapsedTime() {
        return elapsedTime;
    }

    @JsonProperty
    public long getQueuedTime() {
        return queuedTime;
    }

    @JsonProperty
    public long getDeliveryTime() {
        return deliveryTime;
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
    public long getMemoryReservation() {
        return memoryReservation;
    }

    @JsonProperty
    public long getPeakMemory() {
        return peakMemory;
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
    public long getProcessedInputDataSize() {
        return processedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions() {
        return processedInputPositions;
    }

    @JsonProperty
    public long getOutputDataSize() {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions() {
        return outputPositions;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorStats() {
        return operatorStats;
    }
}

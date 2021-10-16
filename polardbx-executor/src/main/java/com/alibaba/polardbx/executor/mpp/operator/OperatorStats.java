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

package com.alibaba.polardbx.executor.mpp.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.mpp.execution.StageId;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;

import javax.annotation.concurrent.Immutable;
import java.util.Optional;

@Immutable
public class OperatorStats {
    private final Optional<StageId> stageId;
    private final int pipelineId;
    private final Optional<String> operatorType;
    private final int operatorId;
    private final long outputRowCount;
    private final long outputBytes;
    private final double startupDuration;
    private final double duration;
    private final long memory;
    private final int instances;
    private final int spillCnt;

    @JsonCreator
    public OperatorStats(
        @JsonProperty("stageId")
            Optional<StageId> stageId,
        @JsonProperty("pipelineId")
            int pipelineId,
        @JsonProperty("operatorType")
            Optional<String> operatorType,
        @JsonProperty("operatorId")
            int operatorId,
        @JsonProperty("outputRowCount")
            long outputRowCount,
        @JsonProperty("outputBytes")
            long outputBytes,
        @JsonProperty("startupDuration")
            double startupDuration,
        @JsonProperty("duration")
            double duration,
        @JsonProperty("memory")
            long memory,
        @JsonProperty("instances")
            int instances,
        @JsonProperty("spillCnt")
            int spillCnt) {
        this.operatorType = operatorType;
        this.stageId = stageId;
        this.pipelineId = pipelineId;
        this.operatorId = operatorId;
        this.outputRowCount = outputRowCount;
        this.outputBytes = outputBytes;
        this.startupDuration = startupDuration;
        this.duration = duration;
        this.memory = memory;
        this.instances = instances;
        this.spillCnt = spillCnt;
    }

    @JsonProperty
    public int getPipelineId() {
        return pipelineId;
    }

    @JsonProperty
    public int getOperatorId() {
        return operatorId;
    }

    @JsonProperty
    public long getOutputRowCount() {
        return outputRowCount;
    }

    @JsonProperty
    public long getOutputBytes() {
        return outputBytes;
    }

    @JsonProperty
    public double getStartupDuration() {
        return startupDuration;
    }

    @JsonProperty
    public double getDuration() {
        return duration;
    }

    @JsonProperty
    public long getMemory() {
        return memory;
    }

    @JsonProperty
    public int getInstances() {
        return instances;
    }

    @JsonProperty
    public int getSpillCnt() {
        return spillCnt;
    }

    @JsonProperty
    public Optional<StageId> getStageId() {
        return stageId;
    }

    @JsonProperty
    public Optional<String> getOperatorType() {
        return operatorType;
    }

    public OperatorStats add(OperatorStats... operators) {
        return add(ImmutableList.copyOf(operators));
    }

    public OperatorStats add(Iterable<OperatorStats> operators) {
        long outputRowCount = this.outputRowCount;
        long outputBytes = this.outputBytes;
        double startupDuration = this.startupDuration;
        double duration = this.duration;
        long memory = this.memory;
        int instances = this.instances;
        int spillCnt = 0;
        for (OperatorStats operator : operators) {
            outputRowCount += operator.outputRowCount;
            outputBytes += operator.outputBytes;
            startupDuration += operator.startupDuration;
            duration += operator.duration;
            memory += operator.memory;
            instances += operator.instances;
            spillCnt += operator.spillCnt;
        }
        return new OperatorStats(stageId, pipelineId, operatorType, operatorId, outputRowCount, outputBytes,
            startupDuration, duration, memory, instances, spillCnt);
    }

    public RuntimeStatisticsSketch toSketch() {
        return new RuntimeStatisticsSketch(startupDuration, duration, 0, outputRowCount, outputBytes,
            memory, instances, spillCnt);
    }
}
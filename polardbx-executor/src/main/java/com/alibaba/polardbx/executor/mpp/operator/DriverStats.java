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

public class DriverStats {

    private final String driverId;
    private final long inputDataSize;
    private final long inputPositions;
    private final long outputDataSize;
    private final long outputPositions;
    private final long startMillis;
    private final long endMillis;
    private final long processNanos;

    private final long blockedNanos;
    private final DriverContext.DriverRuntimeStatistics driverRuntimeStatistics;

    @JsonCreator
    public DriverStats(
        @JsonProperty("driverId")
        String driverId,
        @JsonProperty("inputDataSize")
        long inputDataSize,
        @JsonProperty("inputPositions")
        long inputPositions,
        @JsonProperty("outputDataSize")
        long outputDataSize,
        @JsonProperty("outputPositions")
        long outputPositions,
        @JsonProperty("startMillis")
        long startMillis,
        @JsonProperty("endMillis")
        long endMillis,
        @JsonProperty("processNanos")
        long processNanos,
        @JsonProperty("blockedNanos")
        long blockedNanos,
        @JsonProperty("driverRuntimeStatistics")
        DriverContext.DriverRuntimeStatistics driverRuntimeStatistics
    ) {
        this.driverId = driverId;
        this.inputDataSize = inputDataSize;
        this.inputPositions = inputPositions;
        this.outputDataSize = outputDataSize;
        this.outputPositions = outputPositions;
        this.startMillis = startMillis;
        this.endMillis = endMillis;
        this.processNanos = processNanos;
        this.blockedNanos = blockedNanos;
        this.driverRuntimeStatistics = driverRuntimeStatistics;
    }

    @JsonProperty
    public String getDriverId() {
        return driverId;
    }

    public long getInputDataSize() {
        return inputDataSize;
    }

    @JsonProperty
    public DriverContext.DriverRuntimeStatistics getDriverRuntimeStatistics() {
        return driverRuntimeStatistics;
    }

    @JsonProperty
    public long getInputPositions() {
        return inputPositions;
    }

    public long getOutputDataSize() {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions() {
        return outputPositions;
    }

    @JsonProperty
    public long getStartMillis() {
        return startMillis;
    }

    @JsonProperty
    public long getEndMillis() {
        return endMillis;
    }

    @JsonProperty
    public long getProcessNanos() {
        return processNanos;
    }

    @JsonProperty
    public long getBlockedNanos() {
        return blockedNanos;
    }

    @Override
    public String toString() {
        return "DriverStats{" +
            "driverId='" + driverId + '\'' +
            ", inputDataSize=" + inputDataSize +
            ", inputPositions=" + inputPositions +
            ", outputDataSize=" + outputDataSize +
            ", outputPositions=" + outputPositions +
            ", startMillis=" + startMillis +
            ", endMillis=" + endMillis +
            ", processNanos=" + processNanos +
            ", blockedNanos=" + blockedNanos +
            '}';
    }
}

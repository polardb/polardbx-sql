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

package com.alibaba.polardbx.optimizer.statis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 *
 **/
public class MemoryStatisticsGroup {

    protected long memoryUsage;
    protected long maxMemoryUsage;
    protected long maxDiskUsage;
    protected Map<String, MemoryStatisticsGroup> memoryStatistics;

    public MemoryStatisticsGroup() {

    }

    public MemoryStatisticsGroup(@JsonProperty("memoryUsage") long memoryUsage,
                                 @JsonProperty("maxMemoryUsage") long maxMemoryUsage,
                                 @JsonProperty("maxDiskUsage") long maxDiskUsage) {
        this.memoryUsage = memoryUsage;
        this.maxMemoryUsage = maxMemoryUsage;
        this.maxDiskUsage = maxDiskUsage;
    }

    @JsonCreator
    public MemoryStatisticsGroup(@JsonProperty("memoryUsage") long memoryUsage,
                                 @JsonProperty("maxMemoryUsage") long maxMemoryUsage,
                                 @JsonProperty("maxDiskUsage") long maxDiskUsage,
                                 @JsonProperty("memoryStatistics") Map<String, MemoryStatisticsGroup> memoryStatistics) {
        this.memoryUsage = memoryUsage;
        this.maxMemoryUsage = maxMemoryUsage;
        this.maxDiskUsage = maxDiskUsage;
        this.memoryStatistics = memoryStatistics;
    }

    @JsonProperty
    public long getMemoryUsage() {
        return memoryUsage;
    }

    @JsonProperty
    public long getMaxMemoryUsage() {
        return maxMemoryUsage;
    }

    public void setMaxMemoryUsage(long maxMemoryUsage) {
        this.maxMemoryUsage = maxMemoryUsage;
    }

    @JsonProperty
    public long getMaxDiskUsage() {
        return maxDiskUsage;
    }

    @JsonProperty
    public Map<String, MemoryStatisticsGroup> getMemoryStatistics() {
        return memoryStatistics;
    }

    public void setMemoryStatistics(
        Map<String, MemoryStatisticsGroup> memoryStatistics) {
        this.memoryStatistics = memoryStatistics;
    }

    @Override
    public String toString() {
        return "MemoryStatisticsGroup{" +
            "memoryUsage=" + memoryUsage +
            ", maxMemoryUsage=" + maxMemoryUsage +
            ", maxDiskUsage=" + maxDiskUsage +
            '}';
    }
}

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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 *
 **/
public class TaskMemoryStatisticsGroup extends MemoryStatisticsGroup{

    protected long queryMemoryUsage;
    protected long queryMaxMemoryUsage;
    protected String desc;

    @JsonCreator
    public TaskMemoryStatisticsGroup(@JsonProperty("memoryUsage") long memoryUsage,
                                     @JsonProperty("maxMemoryUsage") long maxMemoryUsage,
                                     @JsonProperty("maxDiskUsage") long maxDiskUsage,
                                     @JsonProperty("queryMemoryUsage") long queryMemoryUsage,
                                     @JsonProperty("queryMaxMemoryUsage") long queryMaxMemoryUsage,
                                     @JsonProperty("memoryStatistics") Map<String, MemoryStatisticsGroup> memoryStatistics) {
        this.memoryUsage = memoryUsage;
        this.maxMemoryUsage = maxMemoryUsage;
        this.maxDiskUsage = maxDiskUsage;
        this.queryMemoryUsage = queryMemoryUsage;
        this.queryMaxMemoryUsage = queryMaxMemoryUsage;
        this.memoryStatistics = memoryStatistics;
    }

    @JsonProperty
    public long getQueryMemoryUsage() {
        return queryMemoryUsage;
    }

    @JsonProperty
    public long getQueryMaxMemoryUsage() {
        return queryMaxMemoryUsage;
    }

    @JsonIgnore
    public String getDesc() {
        return desc;
    }

    @Override
    public String toString() {
        return "TaskMemoryStatisticsGroup{" +
            "queryMemoryUsage=" + queryMemoryUsage +
            ", queryMaxMemoryUsage=" + queryMaxMemoryUsage +
            ", desc='" + desc + '\'' +
            ", memoryUsage=" + memoryUsage +
            ", maxMemoryUsage=" + maxMemoryUsage +
            ", maxDiskUsage=" + maxDiskUsage +
            ", memoryStatistics=" + memoryStatistics +
            '}';
    }
}

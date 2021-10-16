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

package com.alibaba.polardbx.executor.mpp.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.executor.utils.OrderByOption;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PartitioningScheme {

    private final List<Integer> partChannels;  //shuffle key 下标
    private final List<OrderByOption> orderByOptions;
    private final PartitionShuffleHandle shuffleHandle;  //下游节点本身的handle属性

    @JsonCreator
    public PartitioningScheme(
        @JsonProperty("partChannels") List<Integer> partChannels,
        @JsonProperty("orderByOptions") List<OrderByOption> orderByOptions,
        @JsonProperty("shuffleHandle")
            PartitionShuffleHandle shuffleHandle) {
        this.partChannels = partChannels;
        this.orderByOptions = orderByOptions;
        this.shuffleHandle = shuffleHandle;
    }

    @JsonProperty
    public List<Integer> getPartChannels() {
        return partChannels;
    }

    @JsonProperty
    public List<OrderByOption> getOrderByOptions() {
        return orderByOptions;
    }

    @JsonProperty
    public PartitionShuffleHandle getShuffleHandle() {
        return shuffleHandle;
    }

    public int getPartitionCount() {
        return shuffleHandle.getPartitionCount();
    }

    public void setPartitionCount(int partitionCount) {
        shuffleHandle.setPartitionCount(partitionCount);
    }

    public PartitionShuffleHandle.PartitionShuffleMode getPartitionMode() {
        return shuffleHandle.getPartitionShuffleMode();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("shuffleHandle", shuffleHandle)
            .add("partChannels", partChannels)
            .add("orderByOptions", orderByOptions)
            .toString();
    }
}

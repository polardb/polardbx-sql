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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PartitionShuffleHandle {

    private int partitionCount;
    private final PartitionShuffleMode partitionShuffleMode;
    private final boolean mergeSort;

    public enum PartitionShuffleMode {
        SINGLE,
        FIXED,
        BROADCAST
    }

    public PartitionShuffleHandle(
        PartitionShuffleMode partitionShuffleMode,
        boolean mergeSort) {
        this.mergeSort = mergeSort;
        this.partitionShuffleMode = partitionShuffleMode;
    }

    @JsonCreator
    public PartitionShuffleHandle(
        @JsonProperty("partitionShuffleMode")
            PartitionShuffleMode partitionShuffleMode,
        @JsonProperty("mergeSort")
            boolean mergeSort,
        @JsonProperty("partitionCount") int partitionCount) {
        this.partitionCount = partitionCount;
        this.mergeSort = mergeSort;
        this.partitionShuffleMode = partitionShuffleMode;
    }

    @JsonProperty
    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @JsonProperty
    public PartitionShuffleMode getPartitionShuffleMode() {
        return partitionShuffleMode;
    }

    public boolean isSinglePartition() {
        return partitionCount == 1;
    }

    @JsonProperty
    public boolean isMergeSort() {
        return mergeSort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionShuffleHandle)) {
            return false;
        }
        PartitionShuffleHandle that = (PartitionShuffleHandle) o;
        return partitionCount == that.partitionCount &&
            mergeSort == that.mergeSort &&
            partitionShuffleMode == that.partitionShuffleMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionCount, partitionShuffleMode, mergeSort);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("partitionShuffleMode", partitionShuffleMode)
            .add("mergeSort", mergeSort)
            .add("partitionCount", partitionCount)
            .toString();
    }
}

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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PartitionShuffleHandle {

    private final PartitionShuffleMode partitionShuffleMode;
    private final boolean mergeSort;
    private int fullPartCount = -1;
    private int partitionCount;
    private List<Integer> prunePartitions;

    private boolean remotePairWise = false;

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
        @JsonProperty("remotePairWise") boolean remotePairWise,
        @JsonProperty("partitionCount") int partitionCount,
        @JsonProperty("fullPartCount") int fullPartCount,
        @JsonProperty("prunePartitions") List<Integer> prunePartitions) {
        this.partitionCount = partitionCount;
        this.mergeSort = mergeSort;
        this.remotePairWise = remotePairWise;
        this.partitionShuffleMode = partitionShuffleMode;
        this.fullPartCount = fullPartCount;
        this.prunePartitions = prunePartitions;
    }

    @JsonProperty
    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @JsonProperty
    public int getFullPartCount() {
        return fullPartCount;
    }

    public void setFullPartCount(int fullPartCount) {
        this.fullPartCount = fullPartCount;
    }

    @JsonProperty
    public boolean isRemotePairWise() {
        return remotePairWise;
    }

    public void setRemotePairWise(boolean remotePairWise) {
        this.remotePairWise = remotePairWise;
    }

    @JsonProperty
    public List<Integer> getPrunePartitions() {
        return prunePartitions;
    }

    public void setPrunePartitions(List<Integer> prunePartitions) {
        this.prunePartitions = prunePartitions;
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
        return fullPartCount == that.fullPartCount &&
            partitionCount == that.partitionCount &&
            mergeSort == that.mergeSort &&
            partitionShuffleMode == that.partitionShuffleMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullPartCount, partitionCount, partitionShuffleMode, mergeSort);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("partitionShuffleMode", partitionShuffleMode)
            .add("mergeSort", mergeSort)
            .add("partitionCount", partitionCount)
            .add("fullPartCount", fullPartCount)
            .add("remotePairWise", remotePairWise)
            .toString();
    }

    public enum PartitionShuffleMode {
        SINGLE,
        FIXED,
        BROADCAST
    }
}

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
import com.google.common.base.Preconditions;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PartitionHandle {

    private int partitionCount;
    private final PartitionMode partitionMode;

    public enum PartitionMode {
        SOURCE,
        SINGLETON_SOURCE,
        COORDINATOR_ONLY,
        DEFAULT,
        SINGLETON
    }

    public static final PartitionHandle SINGLETON = new PartitionHandle(PartitionMode.SINGLETON, 1);
    public static final PartitionHandle SINGLETON_SOURCE = new PartitionHandle(PartitionMode.SINGLETON_SOURCE, 1);

    public PartitionHandle(
        PartitionMode partitionMode) {
        this.partitionMode = partitionMode;
    }

    @JsonCreator
    public PartitionHandle(
        @JsonProperty("partitionMode") PartitionMode partitionMode,
        @JsonProperty("partitionCount") int partitionCount) {
        this.partitionCount = partitionCount;
        this.partitionMode = partitionMode;
    }

    @JsonProperty
    public int getPartitionCount() {
        return partitionCount;
    }

    public PartitionHandle setPartitionCount(int partitionCount) {
        if (isSingleTon()) {
            Preconditions.checkArgument(partitionCount == 1, "SingleTon's partitionCount must be 1!");
        }
        this.partitionCount = partitionCount;
        return this;
    }

    @JsonProperty
    public PartitionMode getPartitionMode() {
        return partitionMode;
    }

    public boolean isSourceNode() {
        return this.partitionMode.equals(PartitionMode.SOURCE);
    }

    public boolean isSingletonSourceNode() {
        return this.partitionMode.equals(PartitionMode.SINGLETON_SOURCE);
    }

    public boolean isSingleTon() {
        return this.partitionMode.equals(PartitionMode.SINGLETON) || this.partitionMode.equals(
            PartitionMode.SINGLETON_SOURCE);
    }

    public boolean isDefaultNode() {
        return this.partitionMode.equals(PartitionMode.DEFAULT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionHandle that = (PartitionHandle) o;

        return Objects.equals(partitionCount, that.partitionCount) &&
            Objects.equals(partitionMode, that.partitionMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionCount);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("partitionMode", partitionMode)
            .add("partitionCount", partitionCount)
            .toString();
    }
}

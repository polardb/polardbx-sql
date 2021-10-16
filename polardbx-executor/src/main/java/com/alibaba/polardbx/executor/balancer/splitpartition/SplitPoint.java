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

package com.alibaba.polardbx.executor.balancer.splitpartition;

import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;

import java.util.Objects;

/**
 * A SplitPoint describes in which key the partition is split into two parts.
 * Eg.
 * p1 is defined by range [100, 200).
 * After insert a split-point 50, it becomes p1_1 [100, 150), p1_2 [150, 200)
 * TODO(moyi) refactor separate split-point value and name
 *
 * @author moyi
 * @since 2021/03
 */
public class SplitPoint {

    /**
     * The value of this split-point
     */
    public SearchDatumInfo value;

    /**
     * Left part of this split-point
     */
    public String leftPartition;

    /**
     * Right part of this split-point
     */
    public String rightPartition;

    public SplitPoint() {
    }

    public SplitPoint(SearchDatumInfo value) {
        this.value = value;
    }

    public SearchDatumInfo getValue() {
        return this.value;
    }

    public String getLeftPartition() {
        return leftPartition;
    }

    public String getRightPartition() {
        return rightPartition;
    }

    @Override
    public String toString() {
        return "SplitPoint{" +
            "value=" + value +
            ", leftPartition='" + leftPartition + '\'' +
            ", rightPartition='" + rightPartition + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SplitPoint)) {
            return false;
        }
        SplitPoint that = (SplitPoint) o;
        return Objects.equals(leftPartition, that.leftPartition) &&
            Objects.equals(value, that.value) &&
            Objects.equals(rightPartition, that.rightPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftPartition, value, rightPartition);
    }

    @Override
    public SplitPoint clone() {
        SplitPoint result = new SplitPoint();
        result.leftPartition = this.leftPartition;
        result.value = this.value.copy();
        result.rightPartition = this.rightPartition;
        return result;
    }
}

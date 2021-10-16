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

package org.apache.calcite.rel.logical;

public class AggOptimizationContext {
    private boolean isSortPushed = false;
    private boolean isAggPushed = false;
    private int transposeJoinNum = 0;
    private boolean isFromDistinctAgg = false;

    public AggOptimizationContext() {}

    void copyFrom(AggOptimizationContext aggOptimizationContext) {
        this.isSortPushed = aggOptimizationContext.isSortPushed;
        this.isAggPushed = aggOptimizationContext.isAggPushed;
        this.transposeJoinNum = aggOptimizationContext.transposeJoinNum;
        this.isFromDistinctAgg = aggOptimizationContext.isFromDistinctAgg;
    }

    public boolean isSortPushed() {
        return isSortPushed;
    }

    public void setSortPushed(boolean sortPushed) {
        isSortPushed = sortPushed;
    }

    public boolean isAggPushed() {
        return isAggPushed;
    }

    public void setAggPushed(boolean aggPushed) {
        isAggPushed = aggPushed;
    }

    public int getTransposeJoinNum() {
        return transposeJoinNum;
    }

    public void setTransposeJoinNum(int transposeJoinNum) {
        this.transposeJoinNum = transposeJoinNum;
    }

    public boolean isFromDistinctAgg() {
        return isFromDistinctAgg;
    }

    public void setFromDistinctAgg(boolean fromDistinctAgg) {
        isFromDistinctAgg = fromDistinctAgg;
    }
}

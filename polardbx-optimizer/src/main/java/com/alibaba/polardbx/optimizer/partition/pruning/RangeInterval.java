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

package com.alibaba.polardbx.optimizer.partition.pruning;

/**
 * @author chenghui.lch
 */
public class RangeInterval {

    /**
     * flag that current RangeInterval is near the left of bndValue
     * , such  b < col < a , then the col is near the left of a
     */
    public static final int NEAR_TYPE_LEFT = -1;
    /**
     * flag that current RangeInterval is near the right of bndValue
     * , such  b < col < a , then the col is near the right of b
     */
    public static final int NEAR_TYPE_RIGHT = 1;
    /**
     * flag that current RangeInterval is near the right of bndValue
     * , such  const <= col <= const , then the col is near middle of const
     */
    public static final int NEAR_TYPE_MIDDLE = 0;

    protected SearchDatumInfo bndValue;
    protected boolean includedBndValue;
    protected ComparisonKind cmpKind;
    protected int exprIndex;
    protected boolean maxInf = false;
    protected boolean minInf = false;
    protected int nearType = NEAR_TYPE_MIDDLE;

    private RangeInterval() {
    }

    protected void initNearType() {
        switch (cmpKind) {
        case LESS_THAN:
            // <:bndVal -> (minInf, bndVal)
            nearType = NEAR_TYPE_LEFT;
            break;
        case GREATER_THAN:
            // >:bndVal -> (bndVal, maxInf)
            nearType = NEAR_TYPE_RIGHT;
            break;
        default:
            break;
        }
    }

    public static RangeInterval buildRangeInterval(SearchDatumInfo bndValue,
                                                   ComparisonKind cmpKind,
                                                   boolean includedBndValue,
                                                   int exprIndex,
                                                   boolean maxInf,
                                                   boolean minInf) {
        RangeInterval rng = new RangeInterval();
        rng.setBndValue(bndValue);
        rng.setCmpKind(cmpKind);
        rng.setIncludedBndValue(includedBndValue);
        rng.setExprIndex(exprIndex);
        rng.setMaxInf(maxInf);
        rng.setMinInf(minInf);
        rng.initNearType();
        return rng;
    }

    public SearchDatumInfo getBndValue() {
        return bndValue;
    }

    public void setBndValue(SearchDatumInfo bndValue) {
        this.bndValue = bndValue;
    }

    public boolean isIncludedBndValue() {
        return includedBndValue;
    }

    public void setIncludedBndValue(boolean includedBndValue) {
        this.includedBndValue = includedBndValue;
    }

    public ComparisonKind getCmpKind() {
        return cmpKind;
    }

    public void setCmpKind(ComparisonKind cmpKind) {
        this.cmpKind = cmpKind;
    }

    public int getExprIndex() {
        return exprIndex;
    }

    public void setExprIndex(int exprIndex) {
        this.exprIndex = exprIndex;
    }

    public boolean isMaxInf() {
        return maxInf;
    }

    public void setMaxInf(boolean maxInf) {
        this.maxInf = maxInf;
    }

    public boolean isMinInf() {
        return minInf;
    }

    public void setMinInf(boolean minInf) {
        this.minInf = minInf;
    }

    public int getNearType() {
        return nearType;
    }

    public void setNearType(int nearType) {
        this.nearType = nearType;
    }

    public RangeInterval copy() {
        RangeInterval newInterval = new RangeInterval();
        newInterval.setBndValue(bndValue);
        newInterval.setIncludedBndValue(includedBndValue);
        newInterval.setCmpKind(cmpKind);
        newInterval.setMaxInf(maxInf);
        newInterval.setMinInf(minInf);
        newInterval.setExprIndex(exprIndex);
        newInterval.setNearType(nearType);
        return newInterval;
    }
}

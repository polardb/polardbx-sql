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

import java.util.ArrayList;
import java.util.List;

/**
 * A range representation
 *
 * @author chenghui.lch
 */
public class StepIntervalInfo {

    protected RangeInterval minVal;
    protected PartitionPruneStep minValStep;

    protected RangeInterval maxVal;
    protected PartitionPruneStep maxValStep;

    protected RangeIntervalType rangeType;

    /**
     * Label if current interval is build from a single-point interval,
     * such as pk=1 ==> p1 >= 1 and pk1 <= 1
     */
    protected boolean buildFromSingePointInterval = false;

    /**
     * Label if current interval is a single-point interval,
     * ( such  minVal <= x <= maVal && minVal == maxVal )
     */
    protected boolean isSinglePointInterval = false;

    /**
     * Label if current interval cannot be merged further
     * and the finalStep is the pruneStep that is generated after interval merging
     */
    protected boolean forbidMerging = false;
    protected PartitionPruneStep finalStep;

    protected boolean isStepIntervalInfoCombine= false;
    protected PartPruneStepType stepCombineType;
    protected List<StepIntervalInfo> subStepIntervalInfos = new ArrayList<>();

    public StepIntervalInfo() {
    }

    public StepIntervalInfo(StepIntervalInfo rng) {
        this.minVal = rng.getMinVal();
        this.minValStep = rng.getMinValStep();
        this.maxVal = rng.getMaxVal();
        this.maxValStep = rng.getMaxValStep();
        this.rangeType = rng.getRangeType();
        this.buildFromSingePointInterval = rng.isBuildFromSinglePointInterval();
        this.isSinglePointInterval = rng.isSinglePointInterval();
    }

    public PartitionPruneStep getMinValStep() {
        return minValStep;
    }

    public void setMinValStep(PartitionPruneStep minValStep) {
        this.minValStep = minValStep;
    }

    public PartitionPruneStep getMaxValStep() {
        return maxValStep;
    }

    public void setMaxValStep(PartitionPruneStep maxValStep) {
        this.maxValStep = maxValStep;
    }

    public RangeIntervalType getRangeType() {
        return rangeType;
    }

    public void setRangeType(
        RangeIntervalType rangeType) {
        this.rangeType = rangeType;
    }

    public RangeInterval getMinVal() {
        return minVal;
    }

    public void setMinVal(RangeInterval minVal) {
        this.minVal = minVal;
    }

    public RangeInterval getMaxVal() {
        return maxVal;
    }

    public void setMaxVal(RangeInterval maxVal) {
        this.maxVal = maxVal;
    }

    public StepIntervalInfo copy() {
        StepIntervalInfo newRng = new StepIntervalInfo();
        newRng.setMinVal(minVal.copy());
        newRng.setMinValStep(minValStep);
        newRng.setMaxVal(maxVal.copy());
        newRng.setMaxValStep(maxValStep);
        newRng.setRangeType(rangeType);
        newRng.setBuildFromSingePointInterval(this.buildFromSingePointInterval);
        newRng.setSinglePointInterval(this.isSinglePointInterval);
        return newRng;
    }

    public boolean isBuildFromSinglePointInterval() {
        return buildFromSingePointInterval;
    }

    public void setBuildFromSingePointInterval(boolean buildFromSingePointInterval) {
        this.buildFromSingePointInterval = buildFromSingePointInterval;
    }

    public boolean isSinglePointInterval() {
        return isSinglePointInterval;
    }

    public void setSinglePointInterval(boolean singlePointInterval) {
        this.isSinglePointInterval = singlePointInterval;
    }

    public boolean isForbidMerging() {
        return forbidMerging;
    }

    public void setForbidMerging(boolean stopMerging) {
        this.forbidMerging = stopMerging;
    }

    public PartitionPruneStep getFinalStep() {
        return finalStep;
    }

    public void setFinalStep(PartitionPruneStep finalStep) {
        this.finalStep = finalStep;
    }

    public boolean isStepIntervalInfoCombine() {
        return isStepIntervalInfoCombine;
    }

    public void setStepIntervalInfoCombine(boolean stepIntervalInfoCombine) {
        isStepIntervalInfoCombine = stepIntervalInfoCombine;
    }

    public List<StepIntervalInfo> getSubStepIntervalInfos() {
        return subStepIntervalInfos;
    }

    public void setSubStepIntervalInfos(
        List<StepIntervalInfo> subStepIntervalInfos) {
        this.subStepIntervalInfos = subStepIntervalInfos;
    }


    public PartPruneStepType getStepCombineType() {
        return stepCombineType;
    }

    public void setStepCombineType(PartPruneStepType stepCombineType) {
        this.stepCombineType = stepCombineType;
    }

}

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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class IntersectStepIntervalMerger implements StepIntervalMerger {

    public static final int LESS_THEN_OR_EQUALS_STEPS = 0;
    public static final int GREATER_THEN_OR_EQUALS_STEPS = 1;

    protected PartitionPruneStepCombine intersectStep;
    protected PartitionInfo partInfo;
    protected List<List<SearchExprInfo>> rangeBoundDatumCollection;
    protected List<List<PartitionPruneStep>> rangeStepCollection;

    public IntersectStepIntervalMerger(PartitionPruneStepCombine intersectStep) {
        this.intersectStep = intersectStep;
    }

    @Override
    public List<StepIntervalInfo> mergeIntervals(ExecutionContext context, PartPruneStepPruningContext pruningCtx) {
        List<StepIntervalInfo> resultRngs = new ArrayList<>();
        StepIntervalInfo mergedRng = PartitionPruneStepIntervalAnalyzer
            .mergeIntervalsForIntersectStep(this.partInfo, context, pruningCtx, this.rangeBoundDatumCollection,
                this.rangeStepCollection);
        resultRngs.add(mergedRng);
        return resultRngs;
    }

    @Override
    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public List<List<SearchExprInfo>> getRangeBoundDatumCollection() {
        return rangeBoundDatumCollection;
    }

    public void setRangeBoundDatumCollection(
        List<List<SearchExprInfo>> rangeBoundDatumCollection) {
        this.rangeBoundDatumCollection = rangeBoundDatumCollection;
    }

    public List<List<PartitionPruneStep>> getRangeStepCollection() {
        return rangeStepCollection;
    }

    public void setRangeStepCollection(
        List<List<PartitionPruneStep>> rangeStepCollection) {
        this.rangeStepCollection = rangeStepCollection;
    }

}

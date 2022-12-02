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
public class UnionStepIntervalMerger implements StepIntervalMerger {

    protected PartitionInfo partInfo;
    protected PartitionPruneStepCombine unionStep;

    public UnionStepIntervalMerger(PartitionPruneStepCombine unionStep, PartitionInfo partInfo) {
        this.unionStep = unionStep;
        this.partInfo = partInfo;
    }

    @Override
    public List<StepIntervalInfo> mergeIntervals(ExecutionContext context, PartPruneStepPruningContext pruningCtx) {
        List<StepIntervalInfo> allowedMergingRngListOfCurrUnionStep = new ArrayList<>();
        List<StepIntervalInfo> forbidMergingRngListOfCurrentUnionStep = new ArrayList<>();

        for (int i = 0; i < unionStep.getSubSteps().size(); i++) {
            PartitionPruneStep subPruneStep = unionStep.getSubSteps().get(i);

            List<StepIntervalInfo> subAllMergingRngList;
            subAllMergingRngList = subPruneStep.getIntervalMerger().mergeIntervals(context, pruningCtx);

            List<StepIntervalInfo> subForbidMergingRngList = new ArrayList<>();
            List<StepIntervalInfo> subAllowedMergingRngList = new ArrayList<>();
            for (int j = 0; j < subAllMergingRngList.size(); j++) {
                StepIntervalInfo mergedRng = subAllMergingRngList.get(j);
                if (mergedRng.isForbidMerging()) {
                    subForbidMergingRngList.add(mergedRng);
                } else {
                    subAllowedMergingRngList.add(mergedRng);
                }
            }

            if (subPruneStep.getStepType() == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
                // subStep is a AND Step
                if (!subForbidMergingRngList.isEmpty()) {
                    /**
                     * If there are some merged intervals which does NOT need any intervals merging(such as pk in (subQuery) ),
                     * then should keey the And logical relations for these intervals
                     */
                    StepIntervalInfo stepIntervalInfoCombine = new StepIntervalInfo();
                    stepIntervalInfoCombine.setStepCombineType(PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT);
                    stepIntervalInfoCombine.setStepIntervalInfoCombine(true);
                    stepIntervalInfoCombine.setForbidMerging(true);
                    stepIntervalInfoCombine.setSubStepIntervalInfos(subAllMergingRngList);
                    forbidMergingRngListOfCurrentUnionStep.add(stepIntervalInfoCombine);
                } else {
                    allowedMergingRngListOfCurrUnionStep.addAll(subAllowedMergingRngList);
                }
            } else {
                // subStep is a OP Step
                allowedMergingRngListOfCurrUnionStep.addAll(subAllowedMergingRngList);
                if (!subForbidMergingRngList.isEmpty()) {
                    forbidMergingRngListOfCurrentUnionStep.addAll(subForbidMergingRngList);
                }
            }
        }

        List<StepIntervalInfo> resultRanges = PartitionPruneStepIntervalAnalyzer
            .mergeIntervalsForUnionStep(partInfo, allowedMergingRngListOfCurrUnionStep);
        resultRanges.addAll(forbidMergingRngListOfCurrentUnionStep);

        return resultRanges;
    }

    @Override
    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }
}

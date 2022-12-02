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
import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionPruneStepCombine implements PartitionPruneStep {

    protected PartPruneStepType combineType;
    protected List<PartitionPruneStep> subSteps = new ArrayList<>();
    /**
     * the merger that is used to do range merge for step(both stepOp or stepCombine)
     */
    protected StepIntervalMerger intervalMerger;

    /**
     * The merged intervalInfo of PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT
     * <pre>
     *     this intervalInfo will be not null
     *     only if combineType=PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT
     *     and its range type cannot be RangeIntervalType.TAUTOLOGY_RANGE or RangeIntervalType.CONFLICT_RANGE
     * </pre>
     */
    protected StepIntervalInfo intervalInfo;

    public PartitionPruneStepCombine(PartPruneStepType combineType) {
        this.combineType = combineType;
    }

    @Override
    public PartitionInfo getPartitionInfo() {
        PartitionPruneStepOp step = (PartitionPruneStepOp) findFirstOpStep(this);
        if (step != null) {
            return step.getPartitionInfo();
        }
        return null;
    }

    protected PartitionPruneStep findFirstOpStep(PartitionPruneStep rootStep) {

        if (rootStep instanceof PartitionPruneStepCombine) {
            List<PartitionPruneStep> steps = ((PartitionPruneStepCombine) rootStep).getSubSteps();
            for (int i = 0; i < steps.size(); ++i) {
                PartitionPruneStep step = steps.get(i);
                if (step instanceof PartitionPruneStepOp) {
                    return step;
                }
                PartitionPruneStep rs = findFirstOpStep(step);
                if (rs != null) {
                    return rs;
                }
            }
            return null;
        } else {
            return rootStep;
        }
    }

    @Override
    public PartPruneStepType getStepType() {
        return combineType;
    }

    @Override
    public PartPrunedResult prunePartitions(ExecutionContext context, PartPruneStepPruningContext pruningCtx) {

        if (needMergeRanges(pruningCtx)) {
            /**
             * Do the auto merge intervals and build a new prune step from new merged intervals
             */
            PartitionPruneStep rngMergedStepInfo =
                PartitionPruneStepBuilder.mergePruneStepsForStepCombine(this, context, pruningCtx);

            /**
             * Do pruning by the new prune step from new merged intervals
             */
            PartPrunedResult rs = rngMergedStepInfo.prunePartitions(context, pruningCtx);
            pruningCtx.setRootStep(rngMergedStepInfo);
            return rs;
        }

        List<PartitionPruneStep> subStepList = this.subSteps;
        PartitionPruneStep step = subStepList.get(0);
        PartPrunedResult prunedRs = step.prunePartitions(context, pruningCtx);
        BitSet prunedPartBitSet = prunedRs.partBitSet;
        for (int i = 1; i < subStepList.size(); i++) {
            PartitionPruneStep tmpStep = subStepList.get(i);
            PartPrunedResult tmpPrunedRs = tmpStep.prunePartitions(context, pruningCtx);
            if (this.combineType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
                prunedPartBitSet.and(tmpPrunedRs.partBitSet);
            } else if (this.combineType == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
                prunedPartBitSet.or(tmpPrunedRs.partBitSet);
            }
        }
        PartitionPrunerUtils.collateStepExplainInfo(this, context, prunedRs, pruningCtx);
        return prunedRs;
    }

    public List<PartitionPruneStep> getSubSteps() {
        return subSteps;
    }

    @Override
    public String getStepDigest() {

        String andOrExprStr = this.combineType.getSymbol();
        StringBuilder digestBuilder = new StringBuilder(andOrExprStr);
        digestBuilder.append("(");
        for (int i = 0; i < subSteps.size(); i++) {
            PartitionPruneStep step = subSteps.get(i);
            if (i > 0) {
                digestBuilder.append(",");
            }
            digestBuilder.append(step.getStepDigest());
        }
        digestBuilder.append(")");
        return digestBuilder.toString();
    }

    @Override
    public String toString() {
        return getStepDigest();
    }

    public void setSubSteps(List<PartitionPruneStep> subSteps) {
        this.subSteps = subSteps;
    }

    @Override
    public StepIntervalMerger getIntervalMerger() {
        return intervalMerger;
    }

    @Override
    public boolean needMergeRanges(PartPruneStepPruningContext pruningCtx) {
        boolean enableAutoMergeIntervals = pruningCtx == null ? false : pruningCtx.isEnableAutoMergeIntervals();
        if (enableAutoMergeIntervals) {
            return this.intervalMerger != null;
        }
        return false;
    }

    @Override
    public int getOpStepCount() {
        int allOpStepCount = 0;
        for (int i = 0; i < this.getSubSteps().size(); i++) {
            allOpStepCount += this.getSubSteps().get(i).getOpStepCount();
        }
        return allOpStepCount;
    }

    public void setIntervalMerger(StepIntervalMerger intervalMerger) {
        this.intervalMerger = intervalMerger;
    }

    public StepIntervalInfo getIntervalInfo() {
        return intervalInfo;
    }

    public void setIntervalInfo(StepIntervalInfo intervalInfo) {
        this.intervalInfo = intervalInfo;
    }

    public PartPruneStepType getCombineType() {
        return combineType;
    }

    public void setCombineType(PartPruneStepType combineType) {
        this.combineType = combineType;
    }
}

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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.util.StepExplainItem;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionPruneSubPartStepAnd extends PartitionPruneStepCombine {

    protected PartitionPruneStep partStep;
    protected PartitionPruneSubPartStepOr subPartStepOr;

    public PartitionPruneSubPartStepAnd(PartitionPruneStep partStep,
                                        PartitionPruneSubPartStepOr subPartStepOr) {
        super(PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT);
        this.partStep = partStep;
        this.subPartStepOr = subPartStepOr;
        this.subSteps = new ArrayList<>();
        this.subSteps.add(partStep);
        this.subSteps.add(subPartStepOr);
    }

    protected PartitionPruneStep getSubStepByPartLevel(PartKeyLevel level) {
        if (level == PartKeyLevel.SUBPARTITION_KEY) {
            return subPartStepOr.getSubSteps().get(0);
        }
        return partStep;
    }

    @Override
    public PartPrunedResult prunePartitions(ExecutionContext context,
                                            PartPruneStepPruningContext pruningCtx,
                                            List<Integer> parentPartPosiSet) {

        PartitionPruneStep partStepAfterRangeMerging = partStep;
        if (partStepAfterRangeMerging.needMergeRanges(pruningCtx)) {
            if (partStepAfterRangeMerging instanceof PartitionPruneStepCombine) {
                partStepAfterRangeMerging =
                    ((PartitionPruneStepCombine) partStepAfterRangeMerging).mergeIntervalAndRebuildPruneStepIfNeed(
                        context, pruningCtx, parentPartPosiSet);
            }
        }
        PartPrunedResult rsOfPartByRs = partStepAfterRangeMerging.prunePartitions(context, pruningCtx, null);
        BitSet partByBitSet = rsOfPartByRs.getPartBitSet();
        List<Integer> prunePartPosiSet = new ArrayList<>();
        for (int i = partByBitSet.nextSetBit(0); i >= 0; i = partByBitSet.nextSetBit(i + 1)) {
            prunePartPosiSet.add(i + 1);
        }
        PartPrunedResult rsOfSubPartByRs = subPartStepOr.prunePartitions(context, pruningCtx, prunePartPosiSet);
        BitSet allPhyBitSetOfSubPart = rsOfSubPartByRs.getPhysicalPartBitSet();
        BitSet allPhyBitSetOfPart = rsOfPartByRs.getPhysicalPartBitSet();
        allPhyBitSetOfPart.and(allPhyBitSetOfSubPart);
        PartPrunedResult finalRs =
            PartPrunedResult.buildPartPrunedResult(rsOfPartByRs.getPartInfo(), allPhyBitSetOfPart,
                PartKeyLevel.SUBPARTITION_KEY, null, true);

        /**
         * collate result for explain prune step
         */
        StepExplainItem item = PartitionPrunerUtils.collateStepExplainInfo(this, context, finalRs, pruningCtx);
        if (item != null && pruningCtx.isEnableLogPruning()) {
            item.targetSubSteps.add(partStepAfterRangeMerging);
            item.targetSubSteps.add(subPartStepOr);
        }
        pruningCtx.setRootStep(this);

        return finalRs;
    }

    @Override
    public StepIntervalMerger getIntervalMerger() {
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Not support this method");
    }

    @Override
    public PartKeyLevel getPartLevel() {
        return PartKeyLevel.BOTH_PART_SUBPART_KEY;
    }

    @Override
    protected String getCombineSymbol() {
        return "SP_" + this.combineType.getSymbol();
    }

    public PartitionPruneStep getPartStep() {
        return partStep;
    }

    public PartitionPruneSubPartStepOr getSubPartStepOr() {
        return subPartStepOr;
    }
}

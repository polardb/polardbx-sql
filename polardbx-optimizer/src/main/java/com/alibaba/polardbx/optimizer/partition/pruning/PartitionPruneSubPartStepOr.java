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
import com.alibaba.polardbx.optimizer.parse.util.Pair;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.util.StepExplainItem;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartitionPruneSubPartStepOr extends PartitionPruneStepCombine {

    protected PartitionByDefinition parentPartByDef;
    protected PartitionPruneStep subPartStepTemp;
    protected boolean useSubPartByTemp = false;
    protected boolean removeDuplicatedPruning = false;

    public PartitionPruneSubPartStepOr(PartitionByDefinition parentPartByDef, PartitionPruneStep subPartStepTemp) {
        super(PartPruneStepType.PARTPRUNE_COMBINE_UNION);
        this.parentPartByDef = parentPartByDef;
        this.subPartStepTemp = subPartStepTemp;
        this.subSteps.add(subPartStepTemp);
        if (parentPartByDef.getSubPartitionBy() != null) {
            this.useSubPartByTemp = parentPartByDef.getSubPartitionBy().isUseSubPartTemplate();
            if (!useSubPartByTemp) {
                PartitionStrategy strategy = parentPartByDef.getSubPartitionBy().getStrategy();
                if (strategy != PartitionStrategy.LIST && strategy != PartitionStrategy.LIST_COLUMNS) {
                    removeDuplicatedPruning = true;
                }
            }
        }
    }

    @Override
    public PartPrunedResult prunePartitions(ExecutionContext context,
                                            PartPruneStepPruningContext pruningCtx,
                                            List<Integer> parentPartPosiSet) {

        PartitionInfo partInfo = subPartStepTemp.getPartitionInfo();
        BitSet allPhyPartBitSet = PartitionPrunerUtils.buildEmptyPhysicalPartitionsBitSet(partInfo);

        PartitionPruneStep stepAfterRangeMerging = subPartStepTemp;
        if (subPartStepTemp.needMergeRanges(pruningCtx)) {
            if (subPartStepTemp instanceof PartitionPruneStepCombine) {
                stepAfterRangeMerging =
                    ((PartitionPruneStepCombine) subPartStepTemp).mergeIntervalAndRebuildPruneStepIfNeed(context,
                        pruningCtx, parentPartPosiSet);
            }
        }
        if (useSubPartByTemp) {
            if (!parentPartPosiSet.isEmpty()) {
                List<Integer> tmpParentPostSet = new ArrayList<>();
                tmpParentPostSet.add(parentPartPosiSet.get(0));
                PartPrunedResult rsOfSubPartByRs =
                    stepAfterRangeMerging.prunePartitions(context, pruningCtx, tmpParentPostSet);
                PartitionPrunerUtils.collateStepExplainInfo(stepAfterRangeMerging, context, rsOfSubPartByRs,
                    pruningCtx);
                PartPrunedResult tmpSubPartPruneRs = rsOfSubPartByRs.copy();
                for (int i = 0; i < parentPartPosiSet.size(); i++) {
                    tmpSubPartPruneRs.setParentSpecPosi(parentPartPosiSet.get(i));
                    allPhyPartBitSet.or(tmpSubPartPruneRs.getPhysicalPartBitSet());
                }
            }
        } else {
            Map<String, Pair<RangePartRouter, PartPrunedResult>> subPartPruneRsMap = new HashMap<>();
            PartKeyLevel partLevel = getPartLevel();
            for (int i = 0; i < parentPartPosiSet.size(); i++) {
                Integer parentPartPost = parentPartPosiSet.get(i);
                PartitionRouter router = PartRouteFunction.getRouterByPartInfo(partLevel, parentPartPost, partInfo);
                List<Integer> tmpParentPostSet = new ArrayList<>();
                tmpParentPostSet.add(parentPartPost);
                PartPrunedResult rsOfSubPartByRs = null;
                if (removeDuplicatedPruning) {
                    String routeDigest = router.getDigest();
                    Pair<RangePartRouter, PartPrunedResult> tmpRouterAndPruneRs = subPartPruneRsMap.get(routeDigest);
                    if (tmpRouterAndPruneRs == null) {
                        rsOfSubPartByRs = stepAfterRangeMerging.prunePartitions(context, pruningCtx, tmpParentPostSet);
                        Pair<RangePartRouter, PartPrunedResult> routerAndRs = new Pair(router, rsOfSubPartByRs);
                        subPartPruneRsMap.put(router.getDigest(), routerAndRs);
                    } else {
                        rsOfSubPartByRs = tmpRouterAndPruneRs.getValue().copy();
                        rsOfSubPartByRs.setParentSpecPosi(parentPartPost);
                    }
                } else {
                    rsOfSubPartByRs = stepAfterRangeMerging.prunePartitions(context, pruningCtx, tmpParentPostSet);
                }
                BitSet phyPartBitSetOfOnePart = rsOfSubPartByRs.getPhysicalPartBitSet();
                allPhyPartBitSet.or(phyPartBitSetOfOnePart);
                PartitionPrunerUtils.collateStepExplainInfo(stepAfterRangeMerging, context, rsOfSubPartByRs,
                    pruningCtx);
            }
        }
        PartPrunedResult finalRs =
            PartPrunedResult.buildPartPrunedResult(partInfo, allPhyPartBitSet, PartKeyLevel.SUBPARTITION_KEY, null,
                true);

        /**
         * collate result for explain prune step
         */
        StepExplainItem item = PartitionPrunerUtils.collateStepExplainInfo(this, context, finalRs, pruningCtx);
        if (item != null && pruningCtx.isEnableLogPruning()) {
            item.targetSubSteps.add(stepAfterRangeMerging);
            item.prunePartSpecPosiList.addAll(parentPartPosiSet);
            item.isSubPartStepOr = true;
            item.useSubPartByTemp = useSubPartByTemp;
        }
        return finalRs;
    }

    @Override
    public PartKeyLevel getPartLevel() {
        return PartKeyLevel.SUBPARTITION_KEY;
    }

    @Override
    protected String getCombineSymbol() {
        return "SP_" + this.combineType.getSymbol();
    }

}

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

import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionPruneStepUtil {

    public static boolean onlyContainEqualCondition(List<String> partColList, PartitionPruneStep stepInfo,
                                                    boolean allowFalseCond) {

        return onlyContainEqualConditionInner(allowFalseCond, stepInfo);
    }

    private static boolean onlyContainEqualConditionInner(boolean allowFalseCond,
                                                          PartitionPruneStep stepInfo) {

        PartPruneStepType stepType = stepInfo.getStepType();
        if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
            return false;
        } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
            boolean onlyContainEqualCond = true;
            PartitionPruneStepCombine stepCombine = (PartitionPruneStepCombine) stepInfo;
            List<PartitionPruneStep> subSteps = stepCombine.getSubSteps();
            for (int i = 0; i < subSteps.size(); i++) {
                onlyContainEqualCond = onlyContainEqualConditionInner(allowFalseCond, subSteps.get(i));
                if (!onlyContainEqualCond) {
                    return false;
                }
            }
        } else if (stepType == PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
            return false;
        } else {

            PartitionPruneStepOp stepOp = (PartitionPruneStepOp) stepInfo;
            PartitionInfo partInfo = stepOp.getPartInfo();
            // Not support guess the partition count for table with subpartitions
            if (partInfo.getSubPartitionBy() != null) {
                return false;
            }

            int partColCnt = partInfo.getPartitionBy().getPartitionColumnNameList().size();
            if (stepOp.isDynamicSubQueryInStep()) {
                return ((PartitionPruneStepInSubQuery)stepOp).getEqPredPrefixPartColCount() == partColCnt;
            }

            int maxPartKexIdxOfEqualCond = stepOp.getPartPredPathInfo().getPartKeyEnd();
            ComparisonKind cmpKind = stepOp.getComparisonKind();
            if (cmpKind == ComparisonKind.EQUAL && maxPartKexIdxOfEqualCond + 1 == partColCnt) {
                return true;
            }
        }
        return false;
    }

    public static int guessShardCount(List<String> shardColumns, PartitionPruneStep stepInfo, int totalUpperBound) {

        if (shardColumns == null || shardColumns.isEmpty()) {
            return totalUpperBound;
        }
        if (stepInfo == null) {
            return totalUpperBound;
        }

        // FIXME subpartition
        int calUpperBound = guessShardCountInner(stepInfo, totalUpperBound);
        return calUpperBound;
    }

    private static int guessShardCountInner(PartitionPruneStep stepInfo, int upperBound) {

        PartPruneStepType stepType = stepInfo.getStepType();
        if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
            PartitionPruneStepCombine stepCombine = (PartitionPruneStepCombine) stepInfo;
            int orCount = 0;
            List<PartitionPruneStep> subSteps = stepCombine.getSubSteps();
            for (int i = 0; i < subSteps.size(); i++) {
                orCount += guessShardCountInner(subSteps.get(i), upperBound);
            }
            return Math.min(orCount, upperBound);
        } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
            PartitionPruneStepCombine stepCombine = (PartitionPruneStepCombine) stepInfo;
            int andCount = upperBound;
            List<PartitionPruneStep> subSteps = stepCombine.getSubSteps();
            for (int i = 0; i < subSteps.size(); i++) {
                upperBound = Math.min(andCount, guessShardCountInner(subSteps.get(i), upperBound));
            }
            return andCount;
        } else if (stepType == PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
            return upperBound;
        } else {
            PartitionPruneStepOp stepOp = (PartitionPruneStepOp) stepInfo;
            PartitionInfo partInfo = stepOp.getPartInfo();

            // FIXME subpartition
            // Not support guess the partition count for table with subpartitions
            if (partInfo.getSubPartitionBy() != null) {
                return upperBound;
            }

            if (stepOp.isDynamicSubQueryInStep()) {
                return upperBound;
            }

            ComparisonKind cmpKind = stepOp.getComparisonKind();

            if (cmpKind == ComparisonKind.EQUAL) {
                return 1;
            } else {
                return upperBound;
            }
        }

    }

}

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

import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionPruneStepUtil {

    public static boolean onlyContainEqualCondition(List<String> partColList,
                                                    PartitionPruneStep stepInfo,
                                                    boolean allowFalseCond) {

        return onlyContainEqualConditionInner(allowFalseCond, stepInfo);
    }

    protected static boolean onlyContainEqualConditionInner(boolean allowFalseCond,
                                                            PartitionPruneStep stepInfo) {

        PartPruneStepType stepType = stepInfo.getStepType();
        if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
            return false;
        } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
            PartitionPruneStepCombine stepCombine = (PartitionPruneStepCombine) stepInfo;

            if (stepCombine instanceof PartitionPruneSubPartStepAnd) {
                PartitionPruneSubPartStepAnd subPartStepAnd = (PartitionPruneSubPartStepAnd) stepCombine;
                PartitionPruneStep partLevelStep = subPartStepAnd.getSubStepByPartLevel(PartKeyLevel.PARTITION_KEY);
                PartitionPruneStep subPartLevelStep =
                    subPartStepAnd.getSubStepByPartLevel(PartKeyLevel.SUBPARTITION_KEY);

                boolean partLevelOnlyContainEqCond = onlyContainEqualConditionInner(allowFalseCond, partLevelStep);
                if (!partLevelOnlyContainEqCond) {
                    return false;
                }
                boolean subPartLevelOnlyContainEqCond =
                    onlyContainEqualConditionInner(allowFalseCond, subPartLevelStep);
                if (subPartLevelOnlyContainEqCond) {
                    return true;
                }
            }

//            int[] prefixEqPredColCnt = new int[1];
//            if (checkIfSinglePointIntervalQuery(stepCombine, prefixEqPredColCnt)) {
//                PartKeyLevel partLevel = stepCombine.getPartLevel();
//                PartitionInfo partInfo = stepCombine.getPartitionInfo();
//                PartitionByDefinition partByDef = null;
//                int actualPartColCnt = 0;
//                List<Integer> allLevelActualPartColCnts = partInfo.getAllLevelActualPartColCounts();
//                if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
//                    actualPartColCnt = allLevelActualPartColCnts.get(1);
//                    if (actualPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
//                        partByDef = partInfo.getPartitionBy().getSubPartitionBy();
//                        actualPartColCnt = partByDef.getPartitionColumnNameList().size();
//                    }
//                } else {
//                    actualPartColCnt = allLevelActualPartColCnts.get(0);
//                    if (actualPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
//                        partByDef = partInfo.getPartitionBy();
//                        actualPartColCnt = partByDef.getPartitionColumnNameList().size();
//                    }
//                }
//                if (actualPartColCnt == prefixEqPredColCnt[0] + 1) {
//                    return true;
//                }
//            }

            boolean onlyContainEqualCond = true;
            if (checkIfPrefixPartColPointSelect(allowFalseCond, stepInfo)) {
                return true;
            }

            List<PartitionPruneStep> subSteps = stepCombine.getSubSteps();
            for (int i = 0; i < subSteps.size(); i++) {
                onlyContainEqualCond &= onlyContainEqualConditionInner(allowFalseCond, subSteps.get(i));
                if (!onlyContainEqualCond) {
                    return false;
                }
            }
        } else if (stepType == PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
            return false;
        } else {

            PartitionPruneStepOp stepOp = (PartitionPruneStepOp) stepInfo;
            PartitionInfo partInfo = stepOp.getPartInfo();

            PartKeyLevel partLevel = stepOp.getPartLevel();
            PartitionByDefinition partByDef = null;
            int fullPartColCnt = 0;
            List<Integer> allLevelFullPartColCnts = partInfo.getAllLevelFullPartColCounts();
            if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
                fullPartColCnt = allLevelFullPartColCnts.get(1);
                if (fullPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                    partByDef = partInfo.getPartitionBy().getSubPartitionBy();
                    fullPartColCnt = partByDef.getPartitionColumnNameList().size();
                }
            } else {
                fullPartColCnt = allLevelFullPartColCnts.get(0);
                if (fullPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                    partByDef = partInfo.getPartitionBy();
                    fullPartColCnt = partByDef.getPartitionColumnNameList().size();
                }
            }

            if (stepOp.isDynamicSubQueryInStep()) {
                return false;
            }

            int maxPartKexIdxOfEqualCond = stepOp.getPartPredPathInfo().getPartKeyEnd();
            ComparisonKind cmpKind = stepOp.getComparisonKind();
            if (cmpKind == ComparisonKind.EQUAL && maxPartKexIdxOfEqualCond + 1 == fullPartColCnt) {
                return true;
            }
        }
        return false;
    }

    private static boolean checkIfSinglePointIntervalQuery(PartitionPruneStepCombine stepCombine,
                                                           int[] prefixEqPredColCnt) {

        if (stepCombine.getStepType() == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
            return false;
        }

        if (stepCombine.getSubSteps().size() != 2) {
            return false;
        }

        List<PartitionPruneStep> subSteps = stepCombine.getSubSteps();
        for (int i = 0; i < subSteps.size(); i++) {
            PartPruneStepType stepType = subSteps.get(i).getStepType();
            if (stepType != PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
                return false;
            }
        }

        PartitionPruneStepOp stepOp1 = (PartitionPruneStepOp) subSteps.get(0);
        PartitionPruneStepOp stepOp2 = (PartitionPruneStepOp) subSteps.get(1);
        PartPredPathInfo predPathInfo1 = stepOp1.getPartPredPathInfo();
        PartPredPathInfo predPathInfo2 = stepOp2.getPartPredPathInfo();
        if (!predPathInfo1.equals(predPathInfo2) || !predPathInfo1.getDigest().equals(predPathInfo2.getDigest())) {
            return false;
        }

        if (predPathInfo1.getCmpKind() != ComparisonKind.EQUAL) {
            return false;
        }

        int maxPartKeyIdxOfEq = predPathInfo1.getPartKeyEnd();
        if (prefixEqPredColCnt != null) {
            prefixEqPredColCnt[0] = maxPartKeyIdxOfEq;
        }
        return true;
    }

    private static boolean checkIfPrefixPartColPointSelect(boolean allowFalseCond,
                                                           PartitionPruneStep stepInfo) {

        PartitionPruneStepCombine stepCombine = (PartitionPruneStepCombine) stepInfo;
        List<PartitionPruneStep> subSteps = stepCombine.getSubSteps();

        /**
         * For the point select of prefix part cols,
         * such as (c1,c2,c3) are full part cols, and query conditions is (c1,c2)=(a,b)
         *  then it must be (c1,c2,c3) >= (a,b,min) and (c1,c2,c3) <= (a,b,max)
         */
        int subStepCnt = subSteps.size();
        if (subStepCnt != 2) {
            return false;
        }

        for (int i = 0; i < subStepCnt; i++) {
            if (!(subSteps.get(i) instanceof PartitionPruneStepOp)) {
                return false;
            }
        }

        PartitionPruneStepOp oneStepOp = (PartitionPruneStepOp) subSteps.get(0);
        PartitionPruneStepOp twoStepOp = (PartitionPruneStepOp) subSteps.get(1);
        PartKeyLevel partLevel = oneStepOp.getPartLevel();

        ComparisonKind cmpKind1 = oneStepOp.getComparisonKind();
        ComparisonKind cmpKind2 = twoStepOp.getComparisonKind();

        if (!(
            (cmpKind1 == ComparisonKind.GREATER_THAN_OR_EQUAL && cmpKind2 == ComparisonKind.LESS_THAN_OR_EQUAL)
                || (cmpKind2 == ComparisonKind.GREATER_THAN_OR_EQUAL && cmpKind1 == ComparisonKind.LESS_THAN_OR_EQUAL)
        )) {
            return false;
        }

        PartPredPathInfo predPath1 = oneStepOp.getPartPredPathInfo();
        PartPredPathInfo predPath2 = twoStepOp.getPartPredPathInfo();

        String predPart1Digest = predPath1.getDigest(true);
        String predPart2Digest = predPath2.getDigest(true);
        if (!predPart1Digest.equalsIgnoreCase(predPart2Digest)) {
            return false;
        }

        PartitionInfo partInfo = stepInfo.getPartitionInfo();
        List<Integer> actPartColCnts = partInfo.getAllLevelActualPartColCounts();
        Integer actPartColCnt =
            partLevel == PartKeyLevel.SUBPARTITION_KEY ? actPartColCnts.get(1) : actPartColCnts.get(0);
        if (actPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
            PartitionByDefinition partBy = null;
            if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
                partBy = partInfo.getPartitionBy().getSubPartitionBy();
            } else {
                partBy = partInfo.getPartitionBy();
            }
            actPartColCnt = partBy.getPartitionColumnNameList().size();
        }

        int maxPrefixColIdx = predPath1.getPartKeyEnd();
        if ((maxPrefixColIdx + 1) < actPartColCnt) {
            return false;
        }

        return true;
    }

    public static int guessShardCount(List<String> shardColumns, PartitionPruneStep stepInfo, int totalUpperBound) {

        if (shardColumns == null || shardColumns.isEmpty()) {
            return totalUpperBound;
        }
        if (stepInfo == null) {
            return totalUpperBound;
        }

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
            if (stepCombine instanceof PartitionPruneSubPartStepAnd) {
                PartitionPruneSubPartStepAnd subPartStepAnd = (PartitionPruneSubPartStepAnd) stepCombine;
                PartitionPruneStep partLevelStep = subPartStepAnd.getSubStepByPartLevel(PartKeyLevel.PARTITION_KEY);
                PartitionPruneStep subPartLevelStep =
                    subPartStepAnd.getSubStepByPartLevel(PartKeyLevel.SUBPARTITION_KEY);
                int guessShardCntOfPartLevel = guessShardCountInner(partLevelStep, upperBound);
                if (guessShardCntOfPartLevel != 1) {
                    return upperBound;
                }
                int guessShardCntOfSubPartLevel = guessShardCountInner(subPartLevelStep, upperBound);
                return Math.min(guessShardCntOfPartLevel * guessShardCntOfSubPartLevel, upperBound);
            }
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

//            PartitionInfo partInfo = stepOp.getPartInfo();
//            // FIXME subpartition
//            // Not support guess the partition count for table with subpartitions
//            if (partInfo.getPartitionBy().getSubPartitionBy() != null) {
//                return upperBound;
//            }

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

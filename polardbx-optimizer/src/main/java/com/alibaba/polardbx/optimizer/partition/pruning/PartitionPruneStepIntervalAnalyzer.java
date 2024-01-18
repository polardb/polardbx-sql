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
import com.alibaba.polardbx.optimizer.partition.exception.InvalidTypeConversionException;
import com.alibaba.polardbx.optimizer.partition.exception.SubQueryDynamicValueNotReadyException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Analyze and merge range for partition predicate
 *
 * @author chenghui.lch
 */
public class PartitionPruneStepIntervalAnalyzer {

    protected static StepIntervalMerger buildOpStepRangeMerger(PartitionInfo partInfo,
                                                               PartitionPruneStepOp stepOp) {
        return new OpStepIntervalMerger(stepOp, partInfo);
    }

    protected static StepIntervalMerger buildUnionStepRangeMerger(PartitionInfo partInfo,
                                                                  PartitionPruneStepCombine stepCombine,
                                                                  PartPruneStepBuildingContext buildContext) {
        return new UnionStepIntervalMerger(stepCombine, partInfo);
    }

    protected static StepIntervalMerger buildIntersectStepRangeMerger(PartitionInfo partInfo,
                                                                      PartitionPruneStepCombine stepCombine,
                                                                      PartPruneStepBuildingContext buildContext) {

        /**
         * 0： < & <=
         * 1:  > & >=
         */
        List<List<SearchExprInfo>> rangeBoundDatumCollection = new ArrayList<>(2);
        List<List<PartitionPruneStep>> rangeStepCollection = new ArrayList<>(2);

        boolean useFastSinglePointIntervalMerging = true;
        if (buildContext != null) {
            useFastSinglePointIntervalMerging = buildContext.isUseFastSinglePointIntervalMerging();
        }

        for (int i = 0; i < 2; i++) {
            rangeBoundDatumCollection.add(new ArrayList<>());
            rangeStepCollection.add(new ArrayList<>());
        }

        List<PartitionPruneStep> stopMergingSteps = new ArrayList<>();
        List<PartitionPruneStep> toBeMergedSteps = new ArrayList<>();
        for (int i = 0; i < stepCombine.getSubSteps().size(); i++) {
            PartitionPruneStep step = stepCombine.getSubSteps().get(i);
            PartitionPruneStepOp stepOp = (PartitionPruneStepOp) step;
            if (stepOp.isDynamicSubQueryInStep()) {
                stopMergingSteps.add(step);
            } else {
                toBeMergedSteps.add(step);
            }
        }

        if (useFastSinglePointIntervalMerging) {
            boolean containEqSteps = false;
            PartitionPruneStepOp firstEqStepOp = null;
            for (int i = 0; i < toBeMergedSteps.size(); i++) {
                PartitionPruneStep step = toBeMergedSteps.get(i);
                PartitionPruneStepOp stepOp = (PartitionPruneStepOp) step;
                PartPredicateRouteFunction routeFunction = (PartPredicateRouteFunction) stepOp.getPredRouteFunc();
                ComparisonKind cmpKind = routeFunction.getSearchExprInfo().getCmpKind();
                if (cmpKind == ComparisonKind.EQUAL) {
                    containEqSteps = true;
                    firstEqStepOp = stepOp;
                    break;
                }
            }
            if (containEqSteps) {
                return firstEqStepOp.getIntervalMerger();
            }
        }

        /**
         * classify all the predicates into three kinds according to cmpKind: < & <=, = , > & >=
         */
        for (int i = 0; i < toBeMergedSteps.size(); i++) {
            PartitionPruneStep step = toBeMergedSteps.get(i);

            PartitionPruneStepOp stepOp = (PartitionPruneStepOp) step;
            PartPredicateRouteFunction routeFunction = (PartPredicateRouteFunction) stepOp.getPredRouteFunc();
            SearchExprInfo searchExprInfo = routeFunction.getSearchExprInfo();
            ComparisonKind cmpKind = routeFunction.getSearchExprInfo().getCmpKind();
            if (cmpKind == ComparisonKind.LESS_THAN || cmpKind == ComparisonKind.LESS_THAN_OR_EQUAL) {
                rangeBoundDatumCollection.get(IntersectStepIntervalMerger.LESS_THEN_OR_EQUALS_STEPS)
                    .add(searchExprInfo);
                rangeStepCollection.get(IntersectStepIntervalMerger.LESS_THEN_OR_EQUALS_STEPS).add(step);
            } else if (cmpKind == ComparisonKind.GREATER_THAN || cmpKind == ComparisonKind.GREATER_THAN_OR_EQUAL) {
                rangeBoundDatumCollection.get(IntersectStepIntervalMerger.GREATER_THEN_OR_EQUALS_STEPS)
                    .add(searchExprInfo);
                rangeStepCollection.get(IntersectStepIntervalMerger.GREATER_THEN_OR_EQUALS_STEPS).add(step);
            } else {

                /**
                 *  col = const =>  col >= const and col <= const
                 */
                OpStepIntervalMerger opStepRangeMerger = (OpStepIntervalMerger) step.getIntervalMerger();
                PartitionPruneStepOp leStep = opStepRangeMerger.getMaxValStep();
                SearchExprInfo leExprInfo =
                    ((PartPredicateRouteFunction) leStep.getPredRouteFunc()).getSearchExprInfo();
                rangeBoundDatumCollection.get(IntersectStepIntervalMerger.LESS_THEN_OR_EQUALS_STEPS).add(leExprInfo);
                rangeStepCollection.get(IntersectStepIntervalMerger.LESS_THEN_OR_EQUALS_STEPS).add(leStep);

                PartitionPruneStepOp geStep = opStepRangeMerger.getMinValStep();
                SearchExprInfo geExprInfo =
                    ((PartPredicateRouteFunction) geStep.getPredRouteFunc()).getSearchExprInfo();
                rangeBoundDatumCollection.get(IntersectStepIntervalMerger.GREATER_THEN_OR_EQUALS_STEPS).add(geExprInfo);
                rangeStepCollection.get(IntersectStepIntervalMerger.GREATER_THEN_OR_EQUALS_STEPS).add(geStep);

            }
        }

        List<StepIntervalInfo> stopMergingIntervalInfos = new ArrayList<>();
        if (!stopMergingSteps.isEmpty()) {
            for (int i = 0; i < stopMergingSteps.size(); i++) {
                PartitionPruneStep step = stopMergingSteps.get(i);
                StepIntervalInfo intervalStopMerging = new StepIntervalInfo();
                intervalStopMerging.setForbidMerging(true);
                intervalStopMerging.setFinalStep(step);
                intervalStopMerging.setPartLevel(step.getPartLevel());
                stopMergingIntervalInfos.add(intervalStopMerging);
            }
        }

        IntersectStepIntervalMerger stepRangeMerger = new IntersectStepIntervalMerger(stepCombine);
        stepRangeMerger.setPartInfo(partInfo);
        stepRangeMerger.setRangeBoundDatumCollection(rangeBoundDatumCollection);
        stepRangeMerger.setRangeStepCollection(rangeStepCollection);
        stepRangeMerger.setStopMergingIntervalList(stopMergingIntervalInfos);

        return stepRangeMerger;
    }

    protected static class StepRangeIntervalInfoComparator implements Comparator<StepIntervalInfo> {
        protected RangeEndPointComparator rngEndPointComparator;

        public StepRangeIntervalInfoComparator(SearchDatumComparator querySpaceComparator) {
            this.rngEndPointComparator = new RangeEndPointComparator(querySpaceComparator);
        }

        @Override
        public int compare(StepIntervalInfo o1, StepIntervalInfo o2) {
            return this.rngEndPointComparator.compare(o1.getMinVal(), o2.getMinVal());
        }
    }

    /**
     * The Comparator for the endPoint of a interval
     *
     * <pre>
     *  we define "<A" as the virtual endpoint "A-" and define ">A" as the virtual endpoint "A+":
     *      <=A: A]
     *      <A:  A-]
     *      >=A: [A
     *      >A: [A+
     *   and we has the order:
     *      A- < A < A+
     *
     *  </pre>
     */
    protected static class RangeEndPointComparator implements Comparator<RangeInterval> {
        protected SearchDatumComparator querySpaceComparator;

        public RangeEndPointComparator(SearchDatumComparator querySpaceComparator) {
            this.querySpaceComparator = querySpaceComparator;
        }

        @Override
        public int compare(RangeInterval o1Rng, RangeInterval o2Rng) {
            int cmpRs = 0;
            if (o1Rng.isMaxInf()) {
                if (o2Rng.isMaxInf()) {
                    cmpRs = 0;
                } else if (o2Rng.isMinInf()) {
                    cmpRs = 1;
                } else {
                    cmpRs = 1;
                }
            } else if (o1Rng.isMinInf()) {
                if (o2Rng.isMaxInf()) {
                    cmpRs = -1;
                } else if (o2Rng.isMinInf()) {
                    cmpRs = 0;
                } else {
                    cmpRs = -1;
                }
            } else {
                if (o2Rng.isMaxInf()) {
                    cmpRs = -1;
                } else if (o2Rng.isMinInf()) {
                    cmpRs = 1;
                } else {
                    cmpRs = this.querySpaceComparator.compare(o1Rng.getBndValue(), o2Rng.getBndValue());
                    if (cmpRs == 0) {
                        if (o1Rng.getNearType() > o2Rng.getNearType()) {
                            cmpRs = 1;
                        } else if (o1Rng.getNearType() < o2Rng.getNearType()) {
                            cmpRs = -1;
                        }
                    }
                }
            }
            return cmpRs;
        }
    }

    protected static boolean isNeighbour(SearchDatumComparator querySpaceComparator, RangeInterval o1,
                                         RangeInterval o2) {
        boolean checkRs = false;
        int cmpRs = querySpaceComparator.compare(o1.getBndValue(), o2.getBndValue());
        if (cmpRs == 0) {
            if (o1.getNearType() == RangeInterval.NEAR_TYPE_LEFT) {
                if (o2.getNearType() == RangeInterval.NEAR_TYPE_LEFT) {
                    checkRs = false;
                } else if (o2.getNearType() == RangeInterval.NEAR_TYPE_RIGHT) {
                    checkRs = false;
                } else {
                    // o2.getNearType() == RangeInterval.NEAR_TYPE_MIDDLE
                    checkRs = true;
                }
            } else if (o1.getNearType() == RangeInterval.NEAR_TYPE_RIGHT) {
                if (o2.getNearType() == RangeInterval.NEAR_TYPE_LEFT) {
                    checkRs = false;
                } else if (o2.getNearType() == RangeInterval.NEAR_TYPE_RIGHT) {
                    checkRs = false;
                } else {
                    // o2.getNearType() == RangeInterval.NEAR_TYPE_MIDDLE
                    checkRs = true;
                }
            } else {
                // o1.getNearType() == RangeInterval.NEAR_TYPE_MIDDLE
                checkRs = true;
            }
        }
        return checkRs;

    }

    protected static List<StepIntervalInfo> mergeIntervalsForUnionStep(PartitionInfo partInfo,
                                                                       PartKeyLevel partLevel,
                                                                       List<StepIntervalInfo> ranges) {

        List<StepIntervalInfo> resultRanges = new ArrayList<>();
        PartitionByDefinition partBy = null;
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partBy = partInfo.getPartitionBy().getSubPartitionBy();
        } else {
            partBy = partInfo.getPartitionBy();
        }

        // Sort all the start point for each ranges
        SearchDatumComparator querySpaceComparator = partBy.getQuerySpaceComparator();
        RangeEndPointComparator rangeEndPointComparator = new RangeEndPointComparator(querySpaceComparator);

        StepRangeIntervalInfoComparator intervalInfoComparator =
            new StepRangeIntervalInfoComparator(querySpaceComparator);
        List<StepIntervalInfo> sortedRangeInfos = new ArrayList<>();
        for (int i = 0; i < ranges.size(); i++) {
            StepIntervalInfo range = ranges.get(i);
            RangeIntervalType intervalType = range.getRangeType();
            if (intervalType == RangeIntervalType.SATISFIABLE_RANGE) {
                sortedRangeInfos.add(range);
            } else if (intervalType == RangeIntervalType.TAUTOLOGY_RANGE) {
                resultRanges.add(range);
                return resultRanges;
            } else {
                //intervalType == RangeIntervalType.CONFLICT_RANGE
                // Ignore the rng of RangeIntervalType.CONFLICT_RANGE
            }
        }
        sortedRangeInfos.sort(intervalInfoComparator);

        StepIntervalInfo lastRange = null;
        for (int i = 0; i < sortedRangeInfos.size(); ++i) {

            StepIntervalInfo rng = sortedRangeInfos.get(i);
            RangeInterval rngMinPoint = rng.getMinVal();
            RangeInterval rngMaxPoint = rng.getMaxVal();
            if (resultRanges.isEmpty()) {
                StepIntervalInfo newRng = new StepIntervalInfo(rng);
                newRng.setPartLevel(partLevel);
                resultRanges.add(newRng);
                labelAsSinglePointIntervalIfFound(partInfo, newRng);
                lastRange = newRng;
            } else {

                int cmpRs = rangeEndPointComparator.compare(lastRange.getMaxVal(), rngMinPoint);
                if (cmpRs > 0) {

                    /**
                     *      rng_{i-1}:  [a,    b]
                     *      rng_{i}:         [e,   c]
                     *
                     * range[i-1].start <= range[i].start < range[i-1].end ,
                     * so range[i-1] and range[i] must be overlap
                     *
                     */
                    int cmpEndRs = rangeEndPointComparator.compare(lastRange.getMaxVal(), rngMaxPoint);
                    if (cmpEndRs < 0) {

                        /**
                         *      rng_{i-1}:  [a,      b]
                         *      rng_{i}:         [e,   c]
                         *
                         * Merge range[i-1] and range[i],
                         * so use range[i].end = max( range[i-1].end, range[i].end )
                         *
                         *  case1:
                         *      rng_{i-1}:  (a,   b]
                         *      rng_{i}:       (e,  c)
                         *      s.t.
                         *          b > e
                         *          b < c
                         *
                         *      then
                         *          range[i-1] and range[i] are overlap, but can merge
                         *
                         */
                        lastRange.setMaxVal(rng.getMaxVal());
                        lastRange.setMaxValStep(rng.getMaxValStep());
                        lastRange.setBuildFromSingePointInterval(false);
                        labelAsSinglePointIntervalIfFound(partInfo, lastRange);
                    } else {
                        /**
                         *    rng_{i-1}:  [a,          b]
                         *    rng_{i}:        [e,   c]
                         *    range[i-1].end >= range[i-1].end
                         *    ,
                         *    so no need to update the bound of last lastRanges
                         *
                         *  case1:
                         *      rng_{i-1}:  [a,         b)
                         *      rng_{i}:         [e, c]
                         *      s.t.
                         *          b > c
                         *
                         *  case2:
                         *      rng_{i-1}:  [a,         b]
                         *      rng_{i}:         [e,    b)
                         *
                         *      s.t.
                         *          range[i-1].end is closed & range[i].end is open
                         *
                         *  case3:
                         *      rng_{i-1}:  (a,     b)
                         *      rng_{i}:       (e,  b)
                         *      s.t.
                         *          b > e
                         *          range[i-1] and range[i] are overlap, but can merge
                         *
                         *  case4:
                         *      rng_{i-1}:  (a,     b]
                         *      rng_{i}:       (e,  b]
                         *      s.t.
                         *          b > e
                         *
                         *      then
                         *          range[i-1] and range[i] are overlap, but can merge
                         *
                         */
                    }
                } else if (cmpRs == 0) {

                    /**
                     * range[i-1].start <= range[i].start = range[i-1].end,
                     *
                     * All cases:
                     *  case1:
                     *      rng_{i-1}:  (a, b)
                     *      rng_{i}:         (b, c)
                     *
                     *      if range[i-1].end is open & range[i].start is open,
                     *      then
                     *          range[i-1] and range[i] are be NOT overlap, so can NOT merge
                     *
                     *  case2***:
                     *      rng_{i-1}:  (a, b]
                     *      rng_{i}:         [b, c)
                     *
                     *      if range[i-1].end is closed & range[i].start is  close,
                     *      then
                     *          range[i-1] and range[i] are overlap, but can merge
                     *
                     */

                    if (rng.getMinVal().isIncludedBndValue()) {
                        /**
                         * Only handle range merge for case2 :
                         *
                         *      rng_{i-1}:  (a, b]
                         *      rng_{i}:         [b, c)
                         */
                        lastRange.setMaxVal(rng.getMaxVal());
                        lastRange.setMaxValStep(rng.getMaxValStep());
                        lastRange.setBuildFromSingePointInterval(false);
                        labelAsSinglePointIntervalIfFound(partInfo, lastRange);
                    } else {
                        /**
                         * Handle for case1, create new range:
                         */
                        StepIntervalInfo newRng = new StepIntervalInfo(rng);
                        newRng.setPartLevel(partLevel);
                        labelAsSinglePointIntervalIfFound(partInfo, newRng);
                        resultRanges.add(newRng);
                        lastRange = newRng;
                    }

                } else {
                    /**
                     * range[i-1].start  <= range[i-1].end < range[i].start ,
                     * so range[i-1] and range[i] must be NOT overlap,
                     * so range[i-1] and range[i] are independent ranges
                     *
                     *      rng_{i-1}:    [a,    b]
                     *      rng_{i}:                 [e,   c]
                     *
                     * All cases:
                     *  case1:
                     *      rng_{i-1}:  (a, b)
                     *      rng_{i}:         (b, c)
                     *
                     *      if range[i-1].end is open & range[i].start is open,
                     *      then
                     *          range[i-1] and range[i] are be NOT overlap, so can NOT merge
                     *
                     *  case2***:
                     *      rng_{i-1}:  (a, b)
                     *      rng_{i}:         [b, c)
                     *
                     *      if range[i-1].end is open & range[i].start is closed
                     *      then
                     *          range[i-1] and range[i] are NOT overlap, but can merge
                     *  case3***:
                     *      rng_{i-1}:  (a, b]
                     *      rng_{i}:         (b, c)
                     *
                     *      if range[i-1].end is close & range[i].start is open
                     *      then
                     *          range[i-1] and range[i] are NOT overlap, but can merge
                     *
                     *  case4:
                     *      rng_{i-1}:  (a, b]
                     *      rng_{i}:           (e, c)
                     *      s.t.
                     *        b < e
                     *          range[i-1] and range[i] are NOT overlap, but can NOT merge
                     *
                     */

                    if (isNeighbour(querySpaceComparator, lastRange.getMaxVal(), rngMinPoint)) {
                        /**
                         * Only handle range merge for case2 & case3
                         */
                        lastRange.setMaxVal(rng.getMaxVal());
                        lastRange.setMaxValStep(rng.getMaxValStep());
                        lastRange.setBuildFromSingePointInterval(false);
                        labelAsSinglePointIntervalIfFound(partInfo, lastRange);
                    } else {
                        /**
                         * Only handle range merge for case1 & case4
                         */
                        StepIntervalInfo newRng = new StepIntervalInfo(rng);
                        newRng.setPartLevel(partLevel);
                        labelAsSinglePointIntervalIfFound(partInfo, newRng);
                        resultRanges.add(newRng);
                        lastRange = newRng;
                    }
                }
            }
        }
        return resultRanges;
    }

    public static StepIntervalInfo mergeIntervalsForIntersectStep(PartitionInfo partInfo,
                                                                  ExecutionContext context,
                                                                  PartPruneStepPruningContext pruningCtx,
                                                                  List<List<SearchExprInfo>> rangeBoundDataumCollection,
                                                                  List<List<PartitionPruneStep>> rangeStepCollection,
                                                                  PartitionPruneStepCombine targetIntersectStep) {

        List<SearchExprInfo> allLessThanOrEqualsRngBndInfos =
            rangeBoundDataumCollection.get(IntersectStepIntervalMerger.LESS_THEN_OR_EQUALS_STEPS);
        List<SearchExprInfo> allGreaterThanOrEqualsRngBndInfos =
            rangeBoundDataumCollection.get(IntersectStepIntervalMerger.GREATER_THEN_OR_EQUALS_STEPS);

        // ----------------------
        PartKeyLevel partLevel = targetIntersectStep.getPartLevel();
        PartitionByDefinition partBy = null;
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partBy = partInfo.getPartitionBy().getSubPartitionBy();
        } else {
            partBy = partInfo.getPartitionBy();
        }
        int partColCnt = partBy.getPartitionColumnNameList().size();

        // process all range predicates with < & <=, find the range predicate with the min value 
        RangeInterval maxVal = null;
        PartitionPruneStep maxValStep = null;
        if (!allLessThanOrEqualsRngBndInfos.isEmpty()) {
            // find min value
            maxVal = findMostSearchDatumInfo(partInfo, partLevel, context, pruningCtx, false,
                allLessThanOrEqualsRngBndInfos);
            if (maxVal != null) {
                maxValStep =
                    rangeStepCollection.get(IntersectStepIntervalMerger.LESS_THEN_OR_EQUALS_STEPS)
                        .get(maxVal.getExprIndex());
            }

        }
        if (maxVal == null) {
            // no any range of < & <= ,so maxVal is +inf
            maxVal = RangeInterval.buildRangeInterval(SearchDatumInfo.createMaxValDatumInfo(partColCnt),
                ComparisonKind.LESS_THAN, false, 0, true, false);
        }

        // process all range predicates with > & >=, find the range predicate with the max value 
        RangeInterval minVal = null;
        PartitionPruneStep minValStep = null;
        if (!allGreaterThanOrEqualsRngBndInfos.isEmpty()) {
            // find max value
            minVal = findMostSearchDatumInfo(partInfo, partLevel, context, pruningCtx, true,
                allGreaterThanOrEqualsRngBndInfos);
            if (minVal != null) {
                minValStep = rangeStepCollection.get(IntersectStepIntervalMerger.GREATER_THEN_OR_EQUALS_STEPS)
                    .get(minVal.getExprIndex());
            }
        }
        if (minVal == null) {
            // no any range of > & >= ,so minVal is -inf
            minVal = RangeInterval.buildRangeInterval(SearchDatumInfo.createMinValDatumInfo(partColCnt),
                ComparisonKind.GREATER_THAN, false, 0, false, true);
        }

        SearchDatumComparator querySpaceComparator = partBy.getQuerySpaceComparator();
        boolean isRangeConflict = checkIfRangeConflict(querySpaceComparator, minVal, maxVal);
        boolean isEqualValuesConflict = false;
        StepIntervalInfo stepRngInfo = new StepIntervalInfo();
        if (isRangeConflict || isEqualValuesConflict) {
            stepRngInfo = buildConflictRange();
            stepRngInfo.setPartLevel(partLevel);
        } else {
            stepRngInfo.setRangeType(RangeIntervalType.SATISFIABLE_RANGE);
            stepRngInfo.setMaxVal(maxVal);
            stepRngInfo.setMaxValStep(maxValStep);
            stepRngInfo.setMinVal(minVal);
            stepRngInfo.setMinValStep(minValStep);
            stepRngInfo.setPartLevel(partLevel);
            if (maxVal.isMaxInf() && minVal.isMinInf()) {
                // Range:  [-inf, +inf]
                stepRngInfo.setRangeType(RangeIntervalType.TAUTOLOGY_RANGE);
            }
            labelAsSinglePointIntervalIfFound(partInfo, stepRngInfo);
        }
        return stepRngInfo;
    }

    protected static void labelAsSinglePointIntervalIfFound(PartitionInfo partInfo, StepIntervalInfo stepRngInfo) {
        RangeInterval maxVal = stepRngInfo.getMaxVal();
        RangeInterval minVal = stepRngInfo.getMinVal();
        if (!maxVal.isMaxInf() && !minVal.isMinInf() && maxVal.isIncludedBndValue() && minVal.isIncludedBndValue()) {
            SearchDatumComparator queryComparator = partInfo.getPartitionBy().getQuerySpaceComparator();
            int comRs = queryComparator.compare(maxVal.getBndValue(), minVal.getBndValue());
            if (comRs == 0) {
                stepRngInfo.setSinglePointInterval(true);
                return;
            }
        }
        stepRngInfo.setSinglePointInterval(false);
    }

    protected static boolean checkIfRangeConflict(SearchDatumComparator querySpaceComparator,
                                                  RangeInterval minVal,
                                                  RangeInterval maxVal) {

        if (maxVal.isMaxInf() || minVal.isMinInf()) {
            return false;
        }

        int comRs = querySpaceComparator.compare(minVal.getBndValue(), maxVal.getBndValue());
        if (comRs > 0) {
            // minVal > maxVal
            return true;
        } else if (comRs == 0) {
            // minVal = maxVal, but maxVal or minVal is NOT included
            if (!minVal.isIncludedBndValue() || !maxVal.isIncludedBndValue()) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param isFindMax isFindMax=false: findMin, isFindMax=true: findMax
     */
    protected static RangeInterval findMostSearchDatumInfo(PartitionInfo partInfo,
                                                           PartKeyLevel partLevel,
                                                           ExecutionContext context,
                                                           PartPruneStepPruningContext pruningCtx,
                                                           boolean isFindMax,
                                                           List<SearchExprInfo> searchExprInfos) {

        List<Pair<SearchExprEvalResult, Integer>> searchExprEvalRsInfo =
            computeSearchDatumInfosForIntersect(searchExprInfos, context, pruningCtx);
        if (searchExprEvalRsInfo.isEmpty()) {
            return null;
        }

        PartitionByDefinition partBy = null;
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partBy = partInfo.getPartitionBy().getSubPartitionBy();
        } else {
            partBy = partInfo.getPartitionBy();
        }

        SearchDatumComparator queryComparator = partBy.getQuerySpaceComparator();
        SearchExprEvalResult mostExprRs = searchExprEvalRsInfo.get(0).getKey();
        Integer mostExprIdx = searchExprEvalRsInfo.get(0).getValue();
        ComparisonKind mostExprRsCmpKind = mostExprRs.getComparisonKind();
        int exprIndex = mostExprIdx;
        for (int i = 1; i < searchExprEvalRsInfo.size(); i++) {
            Pair<SearchExprEvalResult, Integer> exprRsAndExprIdx = searchExprEvalRsInfo.get(i);
            SearchExprEvalResult nextExprEvalRs = exprRsAndExprIdx.getKey();
            Integer nextExprIdx = exprRsAndExprIdx.getValue();
            ComparisonKind nextCmpKind = nextExprEvalRs.getComparisonKind();
            int comRs = queryComparator.compare(mostExprRs.getSearchDatumInfo(), nextExprEvalRs.getSearchDatumInfo());
            if (isFindMax) {
                // Find the max value from searchDatumInfos
                if (comRs == -1) {
                    // mostDatumInfo < nextDatumInfo
                    mostExprRs = nextExprEvalRs;
                    mostExprRsCmpKind = nextCmpKind;
                    exprIndex = nextExprIdx;
                } else if (comRs == 0) {
                    // mostDatumInfo = nextDatumInfo

                    int rs = mostExprRsCmpKind.getComparison() - nextCmpKind.getComparison();
                    if (rs == -1) {
                        // mostDatumInfoCmpKind : <=:3
                        // nextCmpKind : <:4
                        mostExprRs = nextExprEvalRs;
                        mostExprRsCmpKind = nextCmpKind;
                        exprIndex = nextExprIdx;
                    }
                }
            } else {
                // Find the min value from searchDatumInfos
                if (comRs == 1) {
                    // mostDatumInfo > nextDatumInfo

                    // Find max value
                    mostExprRs = nextExprEvalRs;
                    mostExprRsCmpKind = nextCmpKind;
                    exprIndex = nextExprIdx;
                } else if (comRs == 0) {
                    int rs = mostExprRsCmpKind.getComparison() - nextCmpKind.getComparison();
                    if (rs == 1) {
                        // mostDatumInfoCmpKind : >=:1
                        // nextCmpKind : >:0
                        mostExprRs = nextExprEvalRs;
                        mostExprRsCmpKind = nextCmpKind;
                        exprIndex = nextExprIdx;
                    }
                }
            }
        }
        RangeInterval rangeInterval = RangeInterval.buildRangeInterval(mostExprRs.getSearchDatumInfo(),
            mostExprRsCmpKind, mostExprRsCmpKind.containEqual(), exprIndex, false, false);
        return rangeInterval;
    }

    protected static List<Pair<SearchExprEvalResult, Integer>> computeSearchDatumInfosForIntersect(
        List<SearchExprInfo> searchExprInfos,
        ExecutionContext context,
        PartPruneStepPruningContext pruningCtx) {

        List<Pair<SearchExprEvalResult, Integer>> allDatumInfos = new ArrayList<>();
        for (int i = 0; i < searchExprInfos.size(); i++) {
            SearchExprInfo exprInfo = searchExprInfos.get(i);
            try {
                SearchExprEvalResult datumInfo =
                    PartitionPrunerUtils.evalExprValsAndBuildOneDatum(context, pruningCtx, exprInfo);
                Pair rsAndExprIdxPair = new Pair(datumInfo, i);
                allDatumInfos.add(rsAndExprIdxPair);
            } catch (Throwable ex) {
                if (ex instanceof InvalidTypeConversionException) {
                    /**
                     *  As all the searchExprInfos are computed for Intersection, 
                     *  so when a SearchExprInfo is failed to compute its SearchDatumInfo because of 
                     *  unsupported type conversion, 
                     *  the SearchExprInfo should be treated as Always-True expr
                     */
                    continue;
                } else if (ex instanceof SubQueryDynamicValueNotReadyException) {
                    /**
                     *  when it is failed to compute its SearchDatumInfo because of
                     *  the not-ready subquery-dyanamic value,
                     *  the SearchExprInfo should be ignore and
                     *  so generate a full scan bitset
                     */
                    continue;
                } else {
                    throw ex;
                }
            }
        }
        return allDatumInfos;
    }

    protected static StepIntervalInfo buildTautologyRange() {
        StepIntervalInfo range = new StepIntervalInfo();
        range.setRangeType(RangeIntervalType.TAUTOLOGY_RANGE);
        return range;
    }

    protected static StepIntervalInfo buildConflictRange() {
        StepIntervalInfo range = new StepIntervalInfo();
        range.setRangeType(RangeIntervalType.CONFLICT_RANGE);
        return range;
    }

}

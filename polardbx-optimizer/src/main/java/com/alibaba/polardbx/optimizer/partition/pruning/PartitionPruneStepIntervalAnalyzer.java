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
import com.alibaba.polardbx.optimizer.partition.exception.InvalidTypeConversionException;

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
        
        if(useFastSinglePointIntervalMerging) {
            boolean containEqSteps = false;
            PartitionPruneStepOp firstEqStepOp = null;
            for (int i = 0; i < stepCombine.getSubSteps().size(); i++) {
                PartitionPruneStepOp stepOp = (PartitionPruneStepOp) stepCombine.getSubSteps().get(i);
                PartPredicateRouteFunction routeFunction = (PartPredicateRouteFunction) stepOp.getPredRouteFunc();
                ComparisonKind cmpKind = routeFunction.getSearchExprInfo().getCmpKind();
                if (cmpKind == ComparisonKind.EQUAL ) {
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
        for (int i = 0; i < stepCombine.getSubSteps().size(); i++) {
            PartitionPruneStep step = stepCombine.getSubSteps().get(i);

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
                 *  col op const =>  col >= const and col <= const
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

        IntersectStepIntervalMerger stepRangeMerger = new IntersectStepIntervalMerger(stepCombine);
        stepRangeMerger.setPartInfo(partInfo);
        stepRangeMerger.setRangeBoundDatumCollection(rangeBoundDatumCollection);
        stepRangeMerger.setRangeStepCollection(rangeStepCollection);
        return stepRangeMerger;
    }

    protected static class StepRangeIntervalInfoComparator implements Comparator<StepIntervalInfo> {
        protected SearchDatumComparator querySpaceComparator;

        public StepRangeIntervalInfoComparator(SearchDatumComparator querySpaceComparator) {
            this.querySpaceComparator = querySpaceComparator;
        }

        @Override
        public int compare(StepIntervalInfo o1, StepIntervalInfo o2) {
            return this.querySpaceComparator.compare(o1.getMinVal().getBndValue(), o2.getMinVal().getBndValue());
        }
    }

    protected static List<StepIntervalInfo> mergeIntervalsForUnionStep(PartitionInfo partInfo,
                                                                       List<StepIntervalInfo> ranges) {

        List<StepIntervalInfo> resultRanges = new ArrayList<>();

        // Sort all the start point for each ranges
        SearchDatumComparator querySpaceComparator = partInfo.getPartitionBy().getQuerySpaceComparator();

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
            SearchDatumInfo rngStart = rng.getMinVal().getBndValue();
            SearchDatumInfo rngEnd = rng.getMaxVal().getBndValue();
            if (resultRanges.isEmpty()) {
                StepIntervalInfo newRng = new StepIntervalInfo(rng);
                resultRanges.add(newRng);
                labelAsSinglePointIntervalIfFound(partInfo, newRng);
                lastRange = newRng;
            } else {

                int cmpRs = querySpaceComparator.compare(lastRange.getMaxVal().getBndValue(), rngStart);
                if (cmpRs > 0) {

                    /**
                     *      rng_{i-1}:  [a,    b]
                     *      rng_{i}:         [e,   c]  
                     *
                     * range[i-1].start <= range[i].start < range[i-1].end ,
                     * so range[i-1] and range[i] must be overlap
                     */
                    int cmpEndRs = querySpaceComparator.compare(lastRange.getMaxVal().getBndValue(), rngEnd);
                    if (cmpEndRs < 0) {

                        /**
                         *      rng_{i-1}:  [a,      b]
                         *      rng_{i}:         [e,   c] 
                         *
                         * Merge range[i-1] and range[i], 
                         * so use range[i].end = max( range[i-1].end, range[i].end )
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
                         */
                    }
                } else if (cmpRs == 0) {

                    /**
                     * range[i-1].start <= range[i].start = range[i-1].end,
                     *
                     * Four cases:
                     *  case1:
                     *      rng_{i-1}:  (a, b)
                     *      rng_{i}:         (b, c)   
                     *
                     *      if range[i-1].end is open & range[i].start is open,
                     *      then
                     *          range[i-1] and range[i] are be NOT overlap, so can NOT merge
                     *
                     *  case2:
                     *      rng_{i-1}:  (a, b]
                     *      rng_{i}:         (b, c)  
                     *
                     *      if range[i-1].end is closed & range[i].start is open,
                     *      then
                     *          range[i-1] and range[i] are NOT overlap, but can merge
                     *
                     *  case3:
                     *      rng_{i-1}:  (a, b)
                     *      rng_{i}:         [b, c)  
                     *
                     *      if range[i-1].end is open & range[i].start is closed 
                     *      then
                     *          range[i-1] and range[i] are NOT overlap, but can merge
                     *  case4:
                     *      rng_{i-1}:  (a, b]
                     *      rng_{i}:         [b, c)  
                     *
                     *      range[i-1].end is closed & range[i].start is closed      
                     *      then
                     *          range[i-1] and range[i] are overlap, need merge
                     */
                    boolean lastRngEndIncluded = lastRange.getMaxVal().isIncludedBndValue();
                    boolean rngStartIncluded = rng.getMinVal().isIncludedBndValue();
                    if (!lastRngEndIncluded && !rngStartIncluded) {
                        /**
                         * Handle for case1
                         */
                        //  range[i-1] and range[i] are be NOT overlap, so can NOT merge
                        StepIntervalInfo newRng = new StepIntervalInfo(rng);
                        labelAsSinglePointIntervalIfFound(partInfo, newRng);
                        resultRanges.add(newRng);
                        lastRange = newRng;

                    } else {
                        /**
                         * Handle for case2 case3 case4
                         */
                        //  range[i-1] and range[i] are be maybe overlap, can merge
                        lastRange.setMaxVal(rng.getMaxVal());
                        lastRange.setMaxValStep(rng.getMaxValStep());
                        lastRange.setBuildFromSingePointInterval(false);
                        labelAsSinglePointIntervalIfFound(partInfo, lastRange);
                    }
                } else {
                    /**
                     * range[i-1].start  <= range[i-1].end < range[i].start ,
                     * so range[i-1] and range[i] must be NOT overlap, 
                     * so range[i-1] and range[i] are independent ranges
                     */
                    StepIntervalInfo newRng = new StepIntervalInfo(rng);
                    labelAsSinglePointIntervalIfFound(partInfo, newRng);
                    resultRanges.add(newRng);
                    lastRange = newRng;
                }
            }
        }
        return resultRanges;
    }

    public static StepIntervalInfo mergeIntervalsForIntersectStep(PartitionInfo partInfo,
                                                                  ExecutionContext context,
                                                                  PartPruneStepPruningContext pruningCtx,
                                                                  List<List<SearchExprInfo>> rangeBoundDataumCollection,
                                                                  List<List<PartitionPruneStep>> rangeStepCollection) {

        List<SearchExprInfo> allLessThanOrEqualsRngBndInfos =
            rangeBoundDataumCollection.get(IntersectStepIntervalMerger.LESS_THEN_OR_EQUALS_STEPS);
        List<SearchExprInfo> allGreaterThanOrEqualsRngBndInfos =
            rangeBoundDataumCollection.get(IntersectStepIntervalMerger.GREATER_THEN_OR_EQUALS_STEPS);

        // ----------------------
        int partColCnt = partInfo.getPartitionBy().getPartitionColumnNameList().size();

        // process all range predicates with < & <=, find the range predicate with the min value 
        RangeInterval maxVal = null;
        PartitionPruneStep maxValStep = null;
        if (!allLessThanOrEqualsRngBndInfos.isEmpty()) {
            // find min value
            maxVal = findMostSearchDatumInfo(partInfo, context, pruningCtx, false, allLessThanOrEqualsRngBndInfos);
            if (maxVal != null) {
                maxValStep =
                    rangeStepCollection.get(IntersectStepIntervalMerger.LESS_THEN_OR_EQUALS_STEPS)
                        .get(maxVal.getExprIndex());
            }

        }
        if (maxVal == null) {
            // no any range of < & <= ,so maxVal is +inf 
            maxVal = new RangeInterval();
            maxVal.setBndValue(SearchDatumInfo.createMaxValDatumInfo(partColCnt));
            maxVal.setCmpKind(ComparisonKind.LESS_THAN);
            maxVal.setMaxInf(true);
        }

        // process all range predicates with > & >=, find the range predicate with the max value 
        RangeInterval minVal = null;
        PartitionPruneStep minValStep = null;
        if (!allGreaterThanOrEqualsRngBndInfos.isEmpty()) {
            // find max value
            minVal = findMostSearchDatumInfo(partInfo, context, pruningCtx, true, allGreaterThanOrEqualsRngBndInfos);
            if (minVal != null) {
                minValStep = rangeStepCollection.get(IntersectStepIntervalMerger.GREATER_THEN_OR_EQUALS_STEPS)
                    .get(minVal.getExprIndex());
            }
        }
        if (minVal == null) {
            // no any range of > & >= ,so minVal is -inf 
            minVal = new RangeInterval();
            minVal.setBndValue(SearchDatumInfo.createMinValDatumInfo(partColCnt));
            minVal.setCmpKind(ComparisonKind.GREATER_THAN);
            minVal.setMinInf(true);
        }

        SearchDatumComparator querySpaceComparator = partInfo.getPartitionBy().getQuerySpaceComparator();
        boolean isRangeConflict = checkIfRangeConflict(querySpaceComparator, minVal, maxVal);
        boolean isEqualValuesConflict = false;
        StepIntervalInfo stepRngInfo = new StepIntervalInfo();
        if (isRangeConflict || isEqualValuesConflict) {
            stepRngInfo.setRangeType(RangeIntervalType.CONFLICT_RANGE);
        } else {
            stepRngInfo.setRangeType(RangeIntervalType.SATISFIABLE_RANGE);
            stepRngInfo.setMaxVal(maxVal);
            stepRngInfo.setMaxValStep(maxValStep);
            stepRngInfo.setMinVal(minVal);
            stepRngInfo.setMinValStep(minValStep);
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

    protected static boolean checkIfEqualValuesConflict(SearchDatumComparator querySpaceComparator,
                                                        RangeInterval minVal,
                                                        RangeInterval maxVal,
                                                        List<SearchDatumInfo> equalValues) {

        if (equalValues.isEmpty()) {
            return false;
        }

        // Check if all equal values is same
        SearchDatumInfo lastEqVal = null;
        for (int i = 0; i < equalValues.size(); i++) {
            SearchDatumInfo eqVal = equalValues.get(i);
            if (lastEqVal == null) {
                lastEqVal = eqVal;
            } else {
                int cmpEqRs = querySpaceComparator.compare(lastEqVal, eqVal);
                if (cmpEqRs != 0) {
                    return true;
                }
            }
        }

        if (!maxVal.isMaxInf()) {
            int cmpMaxRs = querySpaceComparator.compare(lastEqVal, maxVal.getBndValue());
            if (cmpMaxRs > 0) {
                // eqVal > maxVal
                return true;
            } else if (cmpMaxRs == 0 && !maxVal.isIncludedBndValue()) {
                // eqVal = maxVal, but maxVal is NOT included
                return true;
            }
        }

        if (!minVal.isMinInf()) {
            int cmpMinRs = querySpaceComparator.compare(lastEqVal, minVal.getBndValue());
            if (cmpMinRs < 0) {
                // eqVal < minVal
                return true;
            } else if (cmpMinRs == 0 && !minVal.isIncludedBndValue()) {
                // eqVal = minVal, but minVal is NOT included
                return true;
            }
        }
        return false;
    }

    /**
     * @param isFindMax isFindMax=false: findMin, isFindMax=true: findMax
     */
    protected static RangeInterval findMostSearchDatumInfo(PartitionInfo partInfo,
                                                           ExecutionContext context,
                                                           PartPruneStepPruningContext pruningCtx,
                                                           boolean isFindMax,
                                                           List<SearchExprInfo> searchExprInfos) {

        List<SearchExprEvalResult> searchExprEvalRsInfo =
            computeSearchDatumInfosForIntersect(searchExprInfos, context, pruningCtx);
        if (searchExprEvalRsInfo.isEmpty()) {
            return null;
        }

        SearchDatumComparator queryComparator = partInfo.getPartitionBy().getQuerySpaceComparator();
        SearchExprEvalResult mostExprRs = searchExprEvalRsInfo.get(0);
        ComparisonKind mostExprRsCmpKind = mostExprRs.getComparisonKind();
        int exprIndex = 0;
        for (int i = 1; i < searchExprEvalRsInfo.size(); i++) {
            SearchExprEvalResult nextExprEvalRs = searchExprEvalRsInfo.get(i);
            ComparisonKind nextCmpKind = nextExprEvalRs.getComparisonKind();
            int comRs = queryComparator.compare(mostExprRs.getSearchDatumInfo(), nextExprEvalRs.getSearchDatumInfo());
            if (isFindMax) {
                // Find the max value from searchDatumInfos
                if (comRs == -1) {
                    // mostDatumInfo < nextDatumInfo
                    mostExprRs = nextExprEvalRs;
                    mostExprRsCmpKind = nextCmpKind;
                    exprIndex = i;
                } else if (comRs == 0) {
                    // mostDatumInfo = nextDatumInfo

                    int rs = mostExprRsCmpKind.getComparison() - nextCmpKind.getComparison();
                    if (rs == -1) {
                        // mostDatumInfoCmpKind : <=:3
                        // nextCmpKind : <:4
                        mostExprRs = nextExprEvalRs;
                        mostExprRsCmpKind = nextCmpKind;
                        exprIndex = i;
                    }
                }
            } else {
                // Find the min value from searchDatumInfos
                if (comRs == 1) {
                    // mostDatumInfo > nextDatumInfo

                    // Find max value
                    mostExprRs = nextExprEvalRs;
                    mostExprRsCmpKind = nextCmpKind;
                    exprIndex = i;
                } else if (comRs == 0) {
                    int rs = mostExprRsCmpKind.getComparison() - nextCmpKind.getComparison();
                    if (rs == 1) {
                        // mostDatumInfoCmpKind : >=:1
                        // nextCmpKind : >:0
                        mostExprRs = nextExprEvalRs;
                        mostExprRsCmpKind = nextCmpKind;
                        exprIndex = i;
                    }
                }
            }
        }
        RangeInterval rangeInterval = new RangeInterval();
        rangeInterval.setBndValue(mostExprRs.getSearchDatumInfo());
        rangeInterval.setCmpKind(mostExprRsCmpKind);
        rangeInterval.setIncludedBndValue(mostExprRsCmpKind.containEqual());
        rangeInterval.setExprIndex(exprIndex);
        return rangeInterval;
    }

    protected static List<SearchExprEvalResult> computeSearchDatumInfosForIntersect(
        List<SearchExprInfo> searchExprInfos,
        ExecutionContext context,
        PartPruneStepPruningContext pruningCtx) {

        List<SearchExprEvalResult> allDatumInfos = new ArrayList<>();
        for (int i = 0; i < searchExprInfos.size(); i++) {
            SearchExprInfo exprInfo = searchExprInfos.get(i);
            try {
                SearchExprEvalResult datumInfo =
                    PartitionPrunerUtils.evalExprValsAndBuildOneDatum(context, pruningCtx, exprInfo);
                allDatumInfos.add(datumInfo);
            } catch (Throwable ex) {
                if (ex instanceof InvalidTypeConversionException) {
                    /**
                     *  As all the searchExprInfos are computed for Intersection, 
                     *  so when a SearchExprInfo is failed to compute its SearchDatumInfo because of 
                     *  unsupported type conversion, 
                     *  the SearchExprInfo should be treated as Always-True expr
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

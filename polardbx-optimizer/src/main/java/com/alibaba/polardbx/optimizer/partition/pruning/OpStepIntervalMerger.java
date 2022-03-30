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
public class OpStepIntervalMerger implements StepIntervalMerger {

    protected PartitionInfo partInfo;
    protected PartitionPruneStepOp opStep;

    protected SearchExprInfo exprInfo = null;
    protected StepIntervalInfo opRawRange = null;

    protected PartitionPruneStepOp minValStep;
    protected PartitionPruneStepOp maxValStep;

    public OpStepIntervalMerger(PartitionPruneStepOp opStep, PartitionInfo partInfo) {
        this.opStep = opStep;
        this.partInfo = partInfo;
        this.exprInfo = ((PartPredicateRouteFunction) opStep.getPredRouteFunc()).getSearchExprInfo();
        initOpRawRange();
    }

    protected void initOpRawRange() {

        int partCol = partInfo.getPartitionBy().getPartitionColumnNameList().size();
        ComparisonKind cmpKind = exprInfo.getCmpKind();
        if (cmpKind == ComparisonKind.LESS_THAN || cmpKind == ComparisonKind.LESS_THAN_OR_EQUAL) {
            StepIntervalInfo range = new StepIntervalInfo();
            range.setRangeType(RangeIntervalType.SATISFIABLE_RANGE);


            /**
             * Convert stepOp <(=):stepOp   to (minInf,stepOp) or  (minInf,stepOp]
             */
            RangeInterval rangeInterval = RangeInterval.buildRangeInterval(null,
                cmpKind, cmpKind.containEqual(), 0, false, false);

            /**
             * col <  const | col <= const
             */
            range.setMaxVal(rangeInterval);
            range.setMaxValStep(opStep);
            this.maxValStep = opStep;

            RangeInterval minVal = RangeInterval.buildRangeInterval(SearchDatumInfo.createMinValDatumInfo(partCol),
                ComparisonKind.GREATER_THAN, false, 0, false, true);

            range.setMinVal(minVal);

            this.opRawRange = range;

        } else if (cmpKind == ComparisonKind.GREATER_THAN || cmpKind == ComparisonKind.GREATER_THAN_OR_EQUAL) {

            StepIntervalInfo range = new StepIntervalInfo();
            range.setRangeType(RangeIntervalType.SATISFIABLE_RANGE);

            /**
             * Convert >(=):stepOp to (step,maxInf) or  [stepOp,maxInf)
             */

            RangeInterval rangeInterval = RangeInterval.buildRangeInterval(null,
                cmpKind, cmpKind.containEqual(), 0, false, false);


            /**
             * col <  const | col <= const
             */

            range.setMinVal(rangeInterval);
            range.setMinValStep(opStep);
            this.minValStep = opStep;

            RangeInterval maxVal = RangeInterval.buildRangeInterval(SearchDatumInfo.createMaxValDatumInfo(partCol),
                ComparisonKind.LESS_THAN, false, 0, true, false);


            range.setMaxVal(maxVal);

            this.opRawRange = range;
        } else {

            StepIntervalInfo range = new StepIntervalInfo();
            range.setRangeType(RangeIntervalType.SATISFIABLE_RANGE);

            /**
             * Convert =:stepOp to [stepOp,stepOp]
             */

            /**
             * col = const ==>  
             *      col >= const  (minValStep)
             *  and col <= const  (maxValStep) 
             */

            // col <= const  (maxValStep)
            ComparisonKind maxValCmpKind = ComparisonKind.LESS_THAN_OR_EQUAL;
            RangeInterval maxValRng = RangeInterval.buildRangeInterval(null,
                maxValCmpKind, maxValCmpKind.containEqual(), 0, false, false);
            range.setMaxVal(maxValRng);

            PartitionPruneStepOp newMaxValStep = opStep.copy();
            newMaxValStep.adjustComparisonKind(maxValCmpKind);
            newMaxValStep.setOriginalStepOp(this.opStep);
            this.maxValStep = newMaxValStep;
            range.setMaxValStep(newMaxValStep);

            // col >= const  (minValStep)
            ComparisonKind minValCmpKind = ComparisonKind.GREATER_THAN_OR_EQUAL;
            RangeInterval minValRng = RangeInterval.buildRangeInterval(null,
                minValCmpKind, minValCmpKind.containEqual(), 0, false, false);
            range.setMinVal(minValRng);

            PartitionPruneStepOp newMinValStep = opStep.copy();
            newMinValStep.adjustComparisonKind(minValCmpKind);
            newMinValStep.setOriginalStepOp(this.opStep);
            this.minValStep = newMinValStep;
            range.setMinValStep(newMinValStep);

            range.setBuildFromSingePointInterval(true);
            this.opRawRange = range;
        }
    }

    @Override
    public List<StepIntervalInfo> mergeIntervals(ExecutionContext context, PartPruneStepPruningContext pruningCtx) {

        List<SearchExprEvalResult> exprEvalRsInfos = null;
        List<StepIntervalInfo> ranges = new ArrayList<>();

        List<SearchExprInfo> exprRsInfos = new ArrayList<>();
        exprRsInfos.add(this.exprInfo);
        exprEvalRsInfos =
            PartitionPruneStepIntervalAnalyzer.computeSearchDatumInfosForIntersect(exprRsInfos, context, pruningCtx);
        if (exprEvalRsInfos.isEmpty()) {
            /**
             * When datumInfos is empty, its SearchDatumInfo computing must be failed because of UnsupportedTypeConversionException,
             * so current opStep should be treated as Always-True expr and generate a Always-True range
             */
            StepIntervalInfo range = PartitionPruneStepIntervalAnalyzer.buildTautologyRange();
            ranges.add(range);
            return ranges;
        }

        /**
         * Because current OpStepRangeMerger and its opStep are cached in PlanCache, so a new StepRangeIntervalInfo must be copy  
         * and make sure that current OpStepRangeMerger and its opStep cannot be do any modification. 
         */
        StepIntervalInfo range = opRawRange.copy();

        if (!range.isBuildFromSinglePointInterval()) {
            if (!range.getMaxVal().isMaxInf()) {
                /**
                 * Save the computed result datumInfos into the new copied StepRangeIntervalInfo
                 */
                range.getMaxVal().setBndValue(exprEvalRsInfos.get(0).getSearchDatumInfo());
            }

            if (!range.getMinVal().isMinInf()) {
                /**
                 * Save the computed result datumInfos into the new copied StepRangeIntervalInfo
                 */
                range.getMinVal().setBndValue(exprEvalRsInfos.get(0).getSearchDatumInfo());
            }
        } else {
            range.getMaxVal().setBndValue(exprEvalRsInfos.get(0).getSearchDatumInfo());
            range.getMinVal().setBndValue(exprEvalRsInfos.get(0).getSearchDatumInfo());
        }

        ranges.add(range);
        return ranges;
    }

    @Override
    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public PartitionPruneStepOp getMinValStep() {
        return minValStep;
    }

    public PartitionPruneStepOp getMaxValStep() {
        return maxValStep;
    }
}

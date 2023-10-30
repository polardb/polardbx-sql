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
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.util.Rex2ExprStringVisitor;

import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionPruneStepOp implements PartitionPruneStep {

    /**
     * The part info of current prune step
     */
    protected PartitionInfo partInfo;
    /**
     * the part level of partition key
     */
    protected PartKeyLevel partKeyMatchLevel;

    /**
     * the predicate info of condition that contains one partition columns
     */
    protected PartPredPathInfo partPredPathInfo;

    /**
     * the route function of prediate
     */
    protected PartRouteFunction predRouteFunc;

    /**
     * the merger that is used to do range merge for step(both stepOp or stepCombine)
     */
    protected StepIntervalMerger rangeMerger;

    /**
     * label current step is conflict step and just return empty partition when do partition pruning
     */
    protected boolean isConflict = false;

    /**
     * label if current step use force scan and ignoring any partition predicate
     * <pre>
     *     if forceFullScan=true and isConflict=false:
     *          do full scan
     *     if forceFullScan=true and isConflict=true:
     *          do zero scan
     * </pre>
     */
    protected boolean forceFullScan = false;

    /**
     * Label if scan all subpartitions cross all partitions when do full scan (forceFullScan=true)
     */
    protected boolean fullScanSubPartsCrossAllParts;

    /**
     * label that is current step only to scan first partition, only used for single tbl / broadcast tbl
     */
    protected boolean isScanFirstPartOnly = false;

    /**
     * label if current stepOp need do range enum for hash
     * <pre>
     *
     * </pre>
     */
    protected boolean needDoRangeEnumForHash = false;

    /**
     * the digest cache of curr op step
     */
    protected String stepOpDigestCache;

    /**
     * orginal single point step op, only used by intervalMerger to build single-point stepOp from merged interval
     */
    protected PartitionPruneStepOp originalStepOp;

    /**
     * Label if current step is a in expr with dynamicSubQuery
     */
    protected boolean dynamicSubQueryInStep = false;

    public PartitionPruneStepOp(BuildStepOpParams buildStepOpParams) {

        PartitionInfo partInfo = buildStepOpParams.getPartInfo();
        PartPredPathInfo partPredPathInfo = buildStepOpParams.getPartPredPathInfo();
        PartRouteFunction predRouteFunc = buildStepOpParams.getPredRouteFunc();
        PartKeyLevel partKeyMatchLevel = buildStepOpParams.getPartKeyMatchLevel();
        boolean isConflict = buildStepOpParams.isConflict();
        boolean forceFullScan = buildStepOpParams.isForceFullScan();
        boolean fullScanSubPartsCrossAllParts = buildStepOpParams.isFullScanSubPartsCrossAllParts();
        boolean isScanFirstPartOnly = buildStepOpParams.isScanFirstPartOnly();
        boolean enableRangeMerge = buildStepOpParams.isEnableRangeMerge();

        this.partInfo = partInfo;
        this.partKeyMatchLevel = partKeyMatchLevel;
        this.partPredPathInfo = partPredPathInfo;
        this.predRouteFunc = predRouteFunc;
        if (predRouteFunc != null) {
            this.needDoRangeEnumForHash = predRouteFunc instanceof PartEnumRouteFunction;
        }
        this.isConflict = isConflict;
        this.forceFullScan = forceFullScan;
        this.fullScanSubPartsCrossAllParts = fullScanSubPartsCrossAllParts;
        this.isScanFirstPartOnly = isScanFirstPartOnly;
        if (forceFullScan || isScanFirstPartOnly) {
            this.rangeMerger = null;
        } else {
            if (enableRangeMerge) {
                this.rangeMerger = this.needDoRangeEnumForHash ? null :
                    PartitionPruneStepIntervalAnalyzer.buildOpStepRangeMerger(partInfo, this);
            }
        }
    }

    public PartitionPruneStepOp(PartitionInfo partInfo,
                                PartPredPathInfo partPredPathInfo,
                                PartRouteFunction predRouteFunc,
                                PartKeyLevel partKeyMatchLevel,
                                boolean enableRangeMerge,
                                boolean isConflict,
                                boolean forceFullScan,
                                boolean isScanFirstPartOnly) {
        this.partInfo = partInfo;
        this.partKeyMatchLevel = partKeyMatchLevel;
        this.partPredPathInfo = partPredPathInfo;
        this.predRouteFunc = predRouteFunc;
        if (predRouteFunc != null) {
            this.needDoRangeEnumForHash = predRouteFunc instanceof PartEnumRouteFunction;
        }
        this.isConflict = isConflict;
        this.forceFullScan = forceFullScan;
        this.isScanFirstPartOnly = isScanFirstPartOnly;
        if (forceFullScan || isScanFirstPartOnly) {
            this.rangeMerger = null;
        } else {
            if (enableRangeMerge) {
                this.rangeMerger = this.needDoRangeEnumForHash ? null :
                    PartitionPruneStepIntervalAnalyzer.buildOpStepRangeMerger(partInfo, this);
            }
        }
    }

    /**
     * Only used by copy
     */
    protected PartitionPruneStepOp() {
    }

    protected void adjustComparisonKind(ComparisonKind newCmpKind) {
        if (!needDoRangeEnumForHash) {
            ((PartPredicateRouteFunction) this.predRouteFunc).getSearchExprInfo().setCmpKind(newCmpKind);
            this.predRouteFunc.setCmpKind(newCmpKind);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Failed to adjust comparision kind for PartitionPruneStepOp after range merge");
        }
    }

    @Override
    public String getStepDigest() {
        if (this.stepOpDigestCache == null) {
            this.stepOpDigestCache = buildStepDigest(null);
        }
        return this.stepOpDigestCache;
    }

    @Override
    public PartKeyLevel getPartLevel() {
        return this.partKeyMatchLevel;
    }

    @Override
    public PartitionInfo getPartitionInfo() {
        return this.partInfo;
    }

    @Override
    public PartPruneStepType getStepType() {

        if (forceFullScan || isScanFirstPartOnly) {
            return PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY;
        } else {
            return PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY;
        }
    }

    @Override
    public PartPrunedResult prunePartitions(ExecutionContext context, PartPruneStepPruningContext pruningCtx,
                                            List<Integer> parentPartPosiSet) {

        PartitionInfo partInfo = this.partInfo;

        Integer parentPartPosi =
            parentPartPosiSet == null || parentPartPosiSet.isEmpty() ? null : parentPartPosiSet.get(0);
        if (isScanFirstPartOnly) {
            PartitionRouter router =
                PartRouteFunction.getRouterByPartInfo(this.partKeyMatchLevel, parentPartPosi, this.partInfo);
            BitSet partBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSetByPartRouter(router);
            partBitSet.set(0, 1, true);
            PartPrunedResult rs =
                PartPrunedResult.buildPartPrunedResult(partInfo, partBitSet, this.partKeyMatchLevel, parentPartPosi,
                    false);
            PartitionPrunerUtils.collateStepExplainInfo(this, context, rs, pruningCtx);
            return rs;
        }

        if (forceFullScan) {
            BitSet allPartBitSet = null;

            boolean fullScanAllPhyPart = this.fullScanSubPartsCrossAllParts;
            boolean useSubPartBy = this.partInfo.getPartitionBy().getSubPartitionBy() != null;
            if (fullScanAllPhyPart && useSubPartBy) {
                if (!this.isConflict) {
                    allPartBitSet = PartitionPrunerUtils.buildFullPhysicalPartitionsBitSet(partInfo);
                } else {
                    allPartBitSet = PartitionPrunerUtils.buildEmptyPhysicalPartitionsBitSet(partInfo);
                }
                PartPrunedResult rs =
                    PartPrunedResult.buildPartPrunedResult(partInfo, allPartBitSet, this.partKeyMatchLevel,
                        parentPartPosi, true);
                PartitionPrunerUtils.collateStepExplainInfo(this, context, rs, pruningCtx);
                return rs;
            }

            PartitionRouter router =
                PartRouteFunction.getRouterByPartInfo(this.partKeyMatchLevel, parentPartPosi, this.partInfo);
            if (!this.isConflict) {
                allPartBitSet = PartitionPrunerUtils.buildFullScanPartitionsBitSetByPartRouter(router);
            } else {
                allPartBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSetByPartRouter(router);
            }
            PartPrunedResult rs =
                PartPrunedResult.buildPartPrunedResult(partInfo, allPartBitSet, this.partKeyMatchLevel, parentPartPosi,
                    false);
            PartitionPrunerUtils.collateStepExplainInfo(this, context, rs, pruningCtx);
            return rs;
        }

        PartPrunedResult prunedResult = predRouteFunc.routePartitions(context, pruningCtx, parentPartPosiSet);
        PartitionPrunerUtils.collateStepExplainInfo(this, context, prunedResult, pruningCtx);
        return prunedResult;
    }

    @Override
    public StepIntervalMerger getIntervalMerger() {
        return rangeMerger;
    }

    @Override
    public boolean needMergeRanges(PartPruneStepPruningContext pruningCtx) {
        return false;
    }

    @Override
    public int getOpStepCount() {
        return 1;
    }

    public StepOpPartPredExprInfo getPartColToPredExprInfo() {

        if (getStepType() == PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
            /**
             * For all the special StepOp of fullScan/zeroScan/firstScan, return null of PartPredExprInfo
             */
//            if (!isConflict) {
//                //cmpExpr = "(noPartKeyOp,fullScan)";
//            } else {
//                if (isScanFirstPartOnly) {
//                    //cmpExpr = "(firstPartScanOnly)";
//                } else {
//                    //cmpExpr = "(noPartKeyOp,zeroScan)";
//                }
//
//            }
            return null;
        }

        // ((%s) %s (%s)) or // ((%s1,%s2) %s (%s1,%s2))
        StepOpPartPredExprInfo colAndExprInfo = new StepOpPartPredExprInfo();
        PartitionByDefinition partBy = this.partInfo.getPartitionBy();
        List<String> partColList = partBy.getPartitionColumnNameList();
        Integer partColCnt = partColList.size();
        colAndExprInfo.setCmpKind(this.getComparisonKind());
        if (!needDoRangeEnumForHash) {
            Integer keyEndIdx = this.partPredPathInfo.partKeyEnd;
            PartPredicateRouteFunction partPredicateRouteFunction = (PartPredicateRouteFunction) this.predRouteFunc;
            for (int i = 0; i < partColCnt; i++) {
                PartClauseExprExec clauseInfoExec = partPredicateRouteFunction.getSearchExprInfo().getExprExecArr()[i];
                if (i <= keyEndIdx) {
                    if (clauseInfoExec.getValueKind() == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                        PartClauseInfo clauseInfo =
                            partPredicateRouteFunction.getSearchExprInfo().getExprExecArr()[i].getClauseInfo();
                        colAndExprInfo.getPartPredExprList().add(clauseInfo);
                    }
                }
            }
            return colAndExprInfo;
        } else {
            /**
             * Impossible here
             */
            return null;
        }
    }

    public String buildStepDigest(ExecutionContext ec) {
        StringBuilder digestBuilder = new StringBuilder("");
        if (isScanFirstPartOnly) {
            digestBuilder.append("(firstPartScanOnly)");
        } else {
            if (forceFullScan) {
                String fullScanType = "fullScan";
                if (!isConflict) {
                    //cmpExpr = "(noPartKeyOp,fullScan)";
                    if (!fullScanSubPartsCrossAllParts) {
                        digestBuilder.append("(noPartKeyOp,fullScan)");
                    } else {
                        digestBuilder.append("(noPartKeyOp,fullScanPhyParts)");
                    }

                } else {
                    //cmpExpr = "(noPartKeyOp,zeroScan)";
                    if (!fullScanSubPartsCrossAllParts) {
                        digestBuilder.append("(noPartKeyOp,zeroScan)");
                    } else {
                        digestBuilder.append("(noPartKeyOp,zeroScanPhyParts)");
                    }
                }
                return digestBuilder.toString();
            }
        }

        PartitionByDefinition partByDef = null;
        if (this.partKeyMatchLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partByDef = this.partInfo.getPartitionBy().getSubPartitionBy();
        } else {
            partByDef = this.partInfo.getPartitionBy();
        }

        // ((%s) %s (%s)) or // ((%s1,%s2) %s (%s1,%s2))
        List<String> partColList = partByDef.getPartitionColumnNameList();
        Integer partColCnt = partColList.size();
        if (!needDoRangeEnumForHash) {
            StringBuilder inputExprBuilder = new StringBuilder("(");
            StringBuilder constExprBuilder = new StringBuilder("(");

            Integer keyEndIdx = this.partPredPathInfo.partKeyEnd;
            PartPredicateRouteFunction partPredicateRouteFunction = (PartPredicateRouteFunction) this.predRouteFunc;
            for (int i = 0; i < partColCnt; i++) {
                PartClauseExprExec clauseInfoExec = partPredicateRouteFunction.getSearchExprInfo().getExprExecArr()[i];
                if (i > 0) {
                    inputExprBuilder.append(",");
                    constExprBuilder.append(",");
                }
                inputExprBuilder.append(partColList.get(i));
                if (i <= keyEndIdx) {
                    if (clauseInfoExec.getValueKind() == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                        PartClauseInfo clauseInfo =
                            partPredicateRouteFunction.getSearchExprInfo().getExprExecArr()[i].getClauseInfo();
                        if (clauseInfoExec.isAlwaysNullValue()) {
                            constExprBuilder.append("null");
                        } else {
                            constExprBuilder.append(
                                Rex2ExprStringVisitor.convertRexToExprString(clauseInfo.getConstExpr(), ec));
                        }
                    }
                }
                if (clauseInfoExec.getValueKind() == PartitionBoundValueKind.DATUM_MIN_VALUE) {
                    constExprBuilder.append("min");
                } else if (clauseInfoExec.getValueKind() == PartitionBoundValueKind.DATUM_MAX_VALUE) {
                    constExprBuilder.append("max");
                }
            }
            inputExprBuilder.append(")");
            constExprBuilder.append(")");

            ComparisonKind cmpKind = this.getComparisonKind();
            digestBuilder.append("(").append(inputExprBuilder).append(cmpKind.getComparisionSymbol())
                .append(constExprBuilder).append(")");
            return digestBuilder.toString();
        } else {

            /**
             * when needDoRangeEnumForHash = true, partInfo must has only one partition column
             */

            PartEnumRouteFunction partHashEnumRouteFunction = (PartEnumRouteFunction) this.predRouteFunc;
            StepIntervalInfo rngInfo = partHashEnumRouteFunction.getRangeIntervalInfo();
            String partColName = partByDef.getPartitionColumnNameList().get(0);
            StringBuilder minExprBuilder = new StringBuilder();
            RangeInterval minRng = rngInfo.getMinVal();
            if (minRng != null) {
                if (minRng.isMinInf()) {
                    minExprBuilder.append("(").append(partColName).append(">").append("min").append(")");
                } else {
                    SearchDatumInfo min = minRng.getBndValue();
                    boolean inclBnd = minRng.isIncludedBndValue();
                    minExprBuilder.append("(").append(partColName).append(inclBnd ? ">=" : ">")
                        .append(min.getSingletonValue().getValue().stringValue().toStringUtf8()).append(")");
                }
            }

            StringBuilder maxExprBuilder = new StringBuilder();
            RangeInterval maxRng = rngInfo.getMaxVal();
            if (maxRng != null) {
                if (maxRng.isMaxInf()) {
                    maxExprBuilder.append("(").append(partColName).append("<").append("max").append(")");
                } else {
                    SearchDatumInfo max = maxRng.getBndValue();
                    boolean inclBnd = maxRng.isIncludedBndValue();
                    maxExprBuilder.append("(").append(partColName).append(inclBnd ? "<=" : "<")
                        .append(max.getSingletonValue().getValue().stringValue().toStringUtf8()).append(")");
                }
            }
            digestBuilder.append("(").append(minExprBuilder).append(")").append(" and ").append("(")
                .append(maxExprBuilder)
                .append(")");
            return digestBuilder.toString();
        }

    }

    @Override
    public String toString() {
        return buildStepDigest(null);
    }

    public PartKeyLevel getPartKeyMatchLevel() {
        return partKeyMatchLevel;
    }

    private void setPartKeyMatchLevel(PartKeyLevel partKeyMatchLevel) {
        this.partKeyMatchLevel = partKeyMatchLevel;
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }

    public ComparisonKind getComparisonKind() {
        return this.predRouteFunc.getCmpKind();
    }

    private void setPartInfo(PartitionInfo partInfo) {
        this.partInfo = partInfo;
    }

    public PartPredPathInfo getPartPredPathInfo() {
        return partPredPathInfo;
    }

    public PartRouteFunction getPredRouteFunc() {
        return predRouteFunc;
    }

    public boolean isConflict() {
        return isConflict;
    }

    private void setConflict(boolean conflict) {
        isConflict = conflict;
    }

    public boolean isNeedDoRangeEnumForHash() {
        return needDoRangeEnumForHash;
    }

    private void setNeedDoRangeEnumForHash(boolean needDoRangeEnumForHash) {
        this.needDoRangeEnumForHash = needDoRangeEnumForHash;
    }

    public boolean isScanFirstPartOnly() {
        return isScanFirstPartOnly;
    }

    private void setScanFirstPartOnly(boolean scanFirstPartOnly) {
        isScanFirstPartOnly = scanFirstPartOnly;
    }

    private void setPartPredPathInfo(PartPredPathInfo partPredPathInfo) {
        this.partPredPathInfo = partPredPathInfo;
    }

    private void setPredRouteFunc(PartRouteFunction predRouteFunc) {
        this.predRouteFunc = predRouteFunc;
    }

    private void setRangeMerger(StepIntervalMerger rangeMerger) {
        this.rangeMerger = rangeMerger;
    }

    public PartitionPruneStepOp getOriginalStepOp() {
        return originalStepOp;
    }

    protected void setOriginalStepOp(PartitionPruneStepOp originalStepOp) {
        this.originalStepOp = originalStepOp;
    }

    protected PartitionPruneStepOp copy() {
        PartitionPruneStepOp stepOp = new PartitionPruneStepOp();
        stepOp.setPartInfo(this.partInfo);
        stepOp.setConflict(this.isConflict);
        stepOp.setNeedDoRangeEnumForHash(this.needDoRangeEnumForHash);
        stepOp.setPartKeyMatchLevel(this.partKeyMatchLevel);
        stepOp.setScanFirstPartOnly(this.isScanFirstPartOnly);
        stepOp.setRangeMerger(this.rangeMerger);
        stepOp.setPartPredPathInfo(this.partPredPathInfo);
        stepOp.setPredRouteFunc(this.predRouteFunc.copy());
        stepOp.setDynamicSubQueryInStep(this.dynamicSubQueryInStep);
        return stepOp;
    }

    public boolean isDynamicSubQueryInStep() {
        return dynamicSubQueryInStep;
    }

    public void setDynamicSubQueryInStep(boolean dynamicSubQueryInStep) {
        this.dynamicSubQueryInStep = dynamicSubQueryInStep;
    }

    public boolean isForceFullScan() {
        return forceFullScan;
    }
}

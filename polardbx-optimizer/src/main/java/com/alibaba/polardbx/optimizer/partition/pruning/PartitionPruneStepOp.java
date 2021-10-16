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
import com.alibaba.polardbx.optimizer.partition.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

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

    public PartitionPruneStepOp(PartitionInfo partInfo,
                                PartPredPathInfo partPredPathInfo,
                                PartRouteFunction predRouteFunc,
                                PartKeyLevel partKeyMatchLevel,
                                boolean enableRangeMerge,
                                boolean isConflict,
                                boolean isScanFirstPartOnly) {
        this.partInfo = partInfo;
        this.partKeyMatchLevel = partKeyMatchLevel;
        this.partPredPathInfo = partPredPathInfo;
        this.predRouteFunc = predRouteFunc;
        if (predRouteFunc != null) {
            this.needDoRangeEnumForHash = predRouteFunc instanceof PartEnumRouteFunction;
        }
        this.isConflict = isConflict;
        this.isScanFirstPartOnly = isScanFirstPartOnly;
        //this.stepOpDigest = buildStepDigest();
        if (this.partKeyMatchLevel == PartKeyLevel.NO_PARTITION_KEY) {
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
    private PartitionPruneStepOp() {
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
            this.stepOpDigestCache = buildStepDigest();
        }
        return this.stepOpDigestCache;
    }

    @Override
    public PartitionInfo getPartitionInfo() {
        return this.partInfo;
    }

    @Override
    public PartPruneStepType getStepType() {
        return partKeyMatchLevel == PartKeyLevel.NO_PARTITION_KEY ?
            PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY : PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY;
    }

    @Override
    public PartPrunedResult prunePartitions(ExecutionContext context, PartPruneStepPruningContext pruningCtx) {

        PartitionInfo partInfo = this.partInfo;

        if (isScanFirstPartOnly) {
            PartPrunedResult rs = new PartPrunedResult();
            BitSet partBitSet = null;
            partBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSet(partInfo);
            partBitSet.set(0, 1, true);
            rs.partBitSet = partBitSet;
            rs.partInfo = partInfo;
            return rs;
        }

        if (partKeyMatchLevel == PartKeyLevel.NO_PARTITION_KEY) {
            PartPrunedResult rs = new PartPrunedResult();
            BitSet allPartBitSet = null;
            if (!this.isConflict) {
                allPartBitSet = PartitionPrunerUtils.buildFullScanPartitionsBitSet(partInfo);
            } else {
                allPartBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSet(partInfo);
            }
            rs.partBitSet = allPartBitSet;
            rs.partInfo = partInfo;
            return rs;
        }

        BitSet finalPartBitSet = predRouteFunc.routePartitions(context, pruningCtx);
        PartPrunedResult result = new PartPrunedResult();
        result.partBitSet = finalPartBitSet;
        result.partInfo = partInfo;
        return result;
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

    private String buildStepDigest() {

        String cmpExpr = null;
        StringBuilder digestBuilder = new StringBuilder("");
        if (getStepType() == PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
            if (!isConflict) {
                //cmpExpr = "(noPartKeyOp,fullScan)";
                digestBuilder.append("(noPartKeyOp,fullScan)");
            } else {
                if (isScanFirstPartOnly) {
                    //cmpExpr = "(firstPartScanOnly)";
                    digestBuilder.append("(firstPartScanOnly)");
                } else {
                    //cmpExpr = "(noPartKeyOp,zeroScan)";
                    digestBuilder.append("(noPartKeyOp,zeroScan)");
                }

            }
            return digestBuilder.toString();
        }

        // ((%s) %s (%s)) or // ((%s1,%s2) %s (%s1,%s2))
        List<String> partColList = this.partInfo.getPartitionBy().getPartitionColumnNameList();
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
                            constExprBuilder.append(clauseInfo.getConstExpr().toString());
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
            String partColName = partInfo.getPartitionBy().getPartitionColumnNameList().get(0);
            
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
            digestBuilder.append("(").append(minExprBuilder).append(")").append(" and ").append("(").append(maxExprBuilder)
                .append(")");
            return digestBuilder.toString();
        }

    }

    @Override
    public String toString() {
        return buildStepDigest();
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
        return stepOp;
    }

}

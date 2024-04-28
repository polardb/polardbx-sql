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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.ScalarSubQueryExecContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.BitSetLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.util.Rex2ExprStringVisitor;
import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import org.apache.calcite.rex.RexDynamicParam;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionPruneStepInSubQuery extends PartitionPruneStepOp {

    protected List<RexDynamicParam> eqExprDynamicParams;
    protected RexDynamicParam subQueryRexDynamicParam;
    protected PartitionPruneStep eqExprFinalStep = null;
    protected int eqPredPrefixPartColCount = 1;

    public PartitionPruneStepInSubQuery(PartitionInfo partInfo, PartKeyLevel level) {
        this.partInfo = partInfo;
        this.partKeyMatchLevel = level;
        this.dynamicSubQueryInStep = true;
        this.rangeMerger = PartitionPruneStepIntervalAnalyzer.buildOpStepRangeMerger(partInfo, this);
    }

    @Override
    public PartPrunedResult prunePartitions(ExecutionContext context,
                                            PartPruneStepPruningContext pruningCtx,
                                            List<Integer> parentPartPosiSet) {

        Object subQueryVal = null;
        Object[] result = new Object[1];
        List valList = null;
        int valCnt = 0;
        boolean returnFullScan = false;
        int partCnt = partInfo.getPartitionBy().getPartitions().size();
        boolean fetchSucc = SubQueryDynamicParamUtils.fetchScalarSubQueryConstantValue(subQueryRexDynamicParam,
            context.getScalarSubqueryCtxMap(), false, result);
        Integer parentPartPosi =
            parentPartPosiSet == null || parentPartPosiSet.isEmpty() ? null : parentPartPosiSet.get(0);
        if (!fetchSucc) {
            returnFullScan = true;
        } else {
            subQueryVal = result[0];
            if (subQueryVal instanceof List) {
                valList = (List) subQueryVal;
            } else {
                valList = new ArrayList();
                valList.add(subQueryVal);
            }
            valCnt = valList.size();
            if (valCnt >= pruningCtx.getMaxInSubQueryPruningSize()) {
                returnFullScan = true;
            } else {
                if (valCnt >= partCnt) {
                    returnFullScan = true;
                }
            }
        }
        if (returnFullScan) {
            // subquery is not ready
            PartitionRouter router =
                PartRouteFunction.getRouterByPartInfo(this.partKeyMatchLevel, parentPartPosi, this.partInfo);
            BitSet partBitSet = PartitionPrunerUtils.buildFullScanPartitionsBitSetByPartRouter(router);
            PartPrunedResult rs =
                PartPrunedResult.buildPartPrunedResult(partInfo, partBitSet, this.partKeyMatchLevel, parentPartPosi,
                    false);
            PartitionPrunerUtils.collateStepExplainInfo(this, context, rs, pruningCtx);
            return rs;
        }

        int relatedId = subQueryRexDynamicParam.getRel().getRelatedId();
        PartPrunedResult prunedResult = null;
        for (int i = 0; i < valCnt; i++) {
            ScalarSubQueryExecContext sbExecRsCtx = context.getScalarSubqueryCtxMap().get(relatedId);
            if (sbExecRsCtx != null) {
                sbExecRsCtx.setSubQueryInExpr(true);
                sbExecRsCtx.setNextPruningRowNum(i);
            }
            PartPrunedResult tmpPrunedResult = eqExprFinalStep.prunePartitions(context, pruningCtx, parentPartPosiSet);
            if (prunedResult == null) {
                prunedResult = tmpPrunedResult;
            } else {
                prunedResult.getPartBitSet().or(tmpPrunedResult.getPartBitSet());
            }
        }
        PartitionPrunerUtils.collateStepExplainInfo(this, context, prunedResult, pruningCtx);
        return prunedResult;
    }

    @Override
    public String buildStepDigest(ExecutionContext ec) {
        StringBuilder digestSb = new StringBuilder("InSubQuery(");
        if (ec == null) {
            digestSb.append("query=").append(subQueryRexDynamicParam.toString()).append(",");
            digestSb.append("eqExpr=").append(eqExprFinalStep.getStepDigest());
        } else {
            String sbRexDynamicStr = Rex2ExprStringVisitor.convertRexToExprString(subQueryRexDynamicParam, ec);
            digestSb.append("query=").append(sbRexDynamicStr).append(",");
            digestSb.append("eqExpr=").append(eqExprFinalStep.getStepDigest());
        }
        digestSb.append(")");
        return digestSb.toString();
    }

    public void setSubQueryRexDynamicParam(RexDynamicParam subQueryRexDynamicParam) {
        this.subQueryRexDynamicParam = subQueryRexDynamicParam;
    }

    public void setEqExprDynamicParams(List<RexDynamicParam> eqExprDynamicParams) {
        this.eqExprDynamicParams = eqExprDynamicParams;
    }

    public void setEqExprFinalStep(PartitionPruneStep eqExprFinalStep) {
        this.eqExprFinalStep = eqExprFinalStep;
    }

    public int getEqPredPrefixPartColCount() {
        return eqPredPrefixPartColCount;
    }

    public void setEqPredPrefixPartColCount(int eqPredPrefixPartColCount) {
        this.eqPredPrefixPartColCount = eqPredPrefixPartColCount;
    }

    @Override
    public ComparisonKind getComparisonKind() {
        return ComparisonKind.EQUAL;
    }
}

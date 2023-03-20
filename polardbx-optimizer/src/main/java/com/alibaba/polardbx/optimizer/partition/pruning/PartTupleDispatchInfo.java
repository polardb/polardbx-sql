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
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.util.Rex2ExprStringVisitor;

import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartTupleDispatchInfo implements PartitionPruneBase {

    protected PartitionTupleRouteInfo tupleRouteInfo;

    protected int dispatchInfoIndex;

    /**
     * The tuple route info for partition
     */
    protected PartTupleRouteFunction partDispatchFunc;

    /**
     * The tuple route info for subpartition
     */
    protected PartTupleRouteFunction subPartDispatchFunc;

    public PartTupleDispatchInfo(PartitionTupleRouteInfo tupleRouteInfo, int dispatchInfoIndex) {
        this.tupleRouteInfo = tupleRouteInfo;
        this.dispatchInfoIndex = dispatchInfoIndex;
    }

    public PartPrunedResult routeTuple(ExecutionContext ec, PartPruneStepPruningContext pruningCtx) {

        PartitionInfo partInfo = this.tupleRouteInfo.getPartInfo();

        PartitionTableType tblType = partInfo.getTableType();
        if (tblType == PartitionTableType.SINGLE_TABLE || tblType == PartitionTableType.GSI_SINGLE_TABLE) {

            PartPrunedResult rs = new PartPrunedResult();
            BitSet partBitSet = null;
            partBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSet(partInfo);
            partBitSet.set(0, 1, true);
            rs.partBitSet = partBitSet;
            rs.partInfo = partInfo;
            PartitionPrunerUtils.collateTupleRouteExplainInfo(this, ec, rs, pruningCtx);
            return rs;

        } else if (tblType == PartitionTableType.BROADCAST_TABLE || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {

            PartPrunedResult rs = new PartPrunedResult();
            BitSet partBitSet = null;
            partBitSet = PartitionPrunerUtils.buildFullScanPartitionsBitSet(partInfo);
            rs.partBitSet = partBitSet;
            rs.partInfo = partInfo;
            PartitionPrunerUtils.collateTupleRouteExplainInfo(this, ec, rs, pruningCtx);
            return rs;
        }

        // Do pruning for partition-keys
        BitSet finalBitSet = partDispatchFunc.routePartitions(ec, pruningCtx);
        PartPrunedResult rs = new PartPrunedResult();
        rs.partBitSet = finalBitSet;
        rs.partInfo = partInfo;
        PartitionPrunerUtils.collateTupleRouteExplainInfo(this, ec, rs, pruningCtx);
        return rs;
    }

    public SearchDatumInfo calcSearchDatum(ExecutionContext ec, PartPruneStepPruningContext pruningCtx) {
        return partDispatchFunc.calcSearchDatum(ec, pruningCtx);
    }

    public PartTupleRouteFunction getPartDispatchFunc() {
        return partDispatchFunc;
    }

    public void setPartDispatchFunc(PartTupleRouteFunction partDispatchFunc) {
        this.partDispatchFunc = partDispatchFunc;
    }

    public PartTupleRouteFunction getSubPartDispatchFunc() {
        return subPartDispatchFunc;
    }

    public void setSubPartDispatchFunc(PartTupleRouteFunction subPartDispatchFunc) {
        this.subPartDispatchFunc = subPartDispatchFunc;
    }

    public String buildStepDigest(ExecutionContext ec) {
        StringBuilder digestBuilder = new StringBuilder("");
        PartitionInfo partInfo = this.tupleRouteInfo.getPartInfo();

        // ((%s) %s (%s)) or // ((%s1,%s2) %s (%s1,%s2))
        List<String> partColList = partInfo.getPartitionBy().getPartitionColumnNameList();
        Integer partColCnt = partColList.size();

        digestBuilder.append("TupleRoute#").append(this.dispatchInfoIndex).append("(");
        StringBuilder inputExprBuilder = new StringBuilder("(");
        StringBuilder constExprBuilder = new StringBuilder("(");

        PartTupleRouteFunction partTupleRouteFunc = this.partDispatchFunc;
        for (int i = 0; i < partColCnt; i++) {
            PartClauseExprExec clauseInfoExec = partTupleRouteFunc.partClauseExprExecArr[i];
            if (i > 0) {
                inputExprBuilder.append(",");
                constExprBuilder.append(",");
            }
            inputExprBuilder.append(partColList.get(i));
            PartClauseInfo clauseInfo = clauseInfoExec.getClauseInfo();
            if (clauseInfoExec.isAlwaysNullValue()) {
                constExprBuilder.append("null");
            } else {
                constExprBuilder.append(Rex2ExprStringVisitor.convertRexToExprString(clauseInfo.getConstExpr(), ec));
            }
        }
        inputExprBuilder.append(")");
        constExprBuilder.append(")");

        ComparisonKind cmpKind = ComparisonKind.EQUAL;
        digestBuilder.append("(").append(inputExprBuilder).append(cmpKind.getComparisionSymbol())
            .append(constExprBuilder).append(")");
        digestBuilder.append(")");
        return digestBuilder.toString();
    }

    @Override
    public String toString() {
        return buildStepDigest(null);
    }

    public int getDispatchInfoIndex() {
        return dispatchInfoIndex;
    }
}

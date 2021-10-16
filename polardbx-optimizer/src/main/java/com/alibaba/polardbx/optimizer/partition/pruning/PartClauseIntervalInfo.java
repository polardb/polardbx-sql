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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 * The interval info of a partition predicate expr and its execution form
 *
 * @author chenghui.lch
 */
public class PartClauseIntervalInfo extends AbstractLifecycle {

    protected PartitionInfo partInfo;
    protected PartClauseInfo partClause;
    protected PartClauseExprExec partClauseExec;
    protected ExprContextProvider exprCtxHolder;
    protected ComparisonKind cmpKind;

    public PartClauseIntervalInfo(PartClauseInfo partClause, PartitionInfo partInfo,
                                  ExprContextProvider exprCtxHolder) {
        this.partClause = partClause;
        this.exprCtxHolder = exprCtxHolder;
        this.partInfo = partInfo;
        this.cmpKind = PartitionPruneStepBuilder.getComparisonBySqlKind(partClause.getOpKind());
        init();
    }

    @Override
    protected void doInit() {
        this.partClauseExec = initExprExec(this.partInfo, this.partClause, this.exprCtxHolder);
    }

    public static PartClauseExprExec initExprExec(PartitionInfo partInfo, PartClauseInfo partPredClause,
                                                  ExprContextProvider exprCtxHolder) {
        PartClauseExprExec partClauseExec = new PartClauseExprExec(PartitionBoundValueKind.DATUM_NORMAL_VALUE);

        int partKeyIndex = partPredClause.getPartKeyIndex();
        SqlOperator partFuncOp = PartitionPruneStepBuilder.getPartFuncSqlOperation(partPredClause, partInfo);
        boolean isNull = partPredClause.isNull();
        RexNode partPredExpr = partPredClause.getConstExpr();
        PartitionIntFunction partFunc = null;
        if (partFuncOp != null) {
            partFunc = PartitionPrunerUtils.getPartitionIntFunction(partFuncOp.getName());
        }
        IExpression exprExec = RexUtils.getEvalFuncExec(partPredExpr, exprCtxHolder);
        partClauseExec.setExprCtxHolder(exprCtxHolder);
        partClauseExec.setExprExec(exprExec);
        partClauseExec.setPartIntFunc(partFunc);
        partClauseExec.setPredExprReturnType(isNull ? null : DataTypeUtil.calciteToDrdsType(partPredExpr.getType()));
        partClauseExec.setPartColDataType(
            partInfo.getPartitionBy().getPartitionFieldList().get(partKeyIndex).getField().getDataType());
        partClauseExec.setValueKind(PartitionBoundValueKind.DATUM_NORMAL_VALUE);
        partClauseExec.setAlwaysNullValue(isNull);
        partClauseExec.setClauseInfo(partPredClause);
        partClauseExec.setDynamicConstExprOnly(partPredClause.isDynamicConstOnly());
        partClauseExec.setPartFldAccessType(PartFieldAccessType.QUERY_PRUNING);
        return partClauseExec;
    }

    public PartClauseInfo getPartClause() {
        return partClause;
    }

    public PartClauseExprExec getPartClauseExec() {
        return partClauseExec;
    }

    public ComparisonKind getCmpKind() {
        return cmpKind;
    }
}



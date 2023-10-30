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
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
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

    private static PartClauseExprExec initExprExec(PartitionInfo partInfo,
                                                   PartClauseInfo partPredClause,
                                                   ExprContextProvider exprCtxHolder) {
        PartClauseExprExec partClauseExec = new PartClauseExprExec(PartitionBoundValueKind.DATUM_NORMAL_VALUE);

        PartKeyLevel partLevel = partPredClause.getPartKeyLevel();

        PartitionByDefinition partByDef = null;
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partByDef = partInfo.getPartitionBy().getSubPartitionBy();
        } else {
            partByDef = partInfo.getPartitionBy();
        }

        int partKeyIndex = partPredClause.getPartKeyIndex();
        boolean isNull = partPredClause.isNull();
        RexNode partPredExpr = partPredClause.getConstExpr();
        PartitionIntFunction partFunc = null;

//        SqlOperator partFuncOp = PartitionPruneStepBuilder.getPartFuncSqlOperation(partLevel, partKeyIndex, partInfo);
//        if (partFuncOp != null) {
//            partFunc = PartitionPrunerUtils.getPartitionIntFunction(partFuncOp, partLevel, partInfo);
//        }

        SqlCall partFuncCall = PartitionFunctionBuilder.getPartFuncCall(partLevel, partKeyIndex, partInfo);
        if (partFuncCall != null) {
            partFunc = PartitionFunctionBuilder.createPartFuncByPartFuncCal(partFuncCall);
        }

        IExpression exprExec = RexUtils.getEvalFuncExec(partPredExpr, exprCtxHolder);
        partClauseExec.setExprCtxHolder(exprCtxHolder);
        partClauseExec.setExprExec(exprExec);
        partClauseExec.setPartIntFunc(partFunc);
        partClauseExec.setPredExprReturnType(isNull ? null : DataTypeUtil.calciteToDrdsType(partPredExpr.getType()));

        ColumnMeta partFldColMeta = partByDef.getPartitionFieldList().get(partKeyIndex);
        partClauseExec.setPartColDataType(partFldColMeta.getField().getDataType());
        partClauseExec.setPartColMeta(partFldColMeta);

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



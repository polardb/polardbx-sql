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

package com.alibaba.polardbx.optimizer.core.expression.calc;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.exception.SubQueryDynamicValueNotReadyException;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by chuanqin on 17/7/13.
 */
public class DynamicParamExpression extends AbstractExpression {

    private int index;

    // index for IN expr
    private int subIndex = -1;

    // the col index of ROW expr
    private int skIndex = -1;
    //private Map<Integer, ParameterContext> param;

    //
    private ExprContextProvider contextProvider;

    // Fields for subquery
    private RelNode relNode;

    // RexDynamic For scalar subqery
    private RexDynamicParam sbExprRex;

    /**
     * label if DynamicParamExpression contain valueObj itself
     */
    private boolean flag = false;
    private SqlKind subqueryKind;
    private List<IExpression> subqueryOperands;
    private SqlOperator subqueryOp;

    public DynamicParamExpression(int index, ExprContextProvider contextProvider, int subIndex,
                                  int skIndex) {
        this.index = index;
        this.contextProvider = contextProvider;
        this.subIndex = subIndex;
        this.skIndex = skIndex;
    }

    public DynamicParamExpression(RelNode rel) {
        this.index = PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX;
        this.relNode = rel;
    }

    public DynamicParamExpression(RelNode rel, SqlKind subqueryKind, SqlOperator subqueryOp,
                                  List<IExpression> subqueryOperands) {
        this.index = PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX;
        this.relNode = rel;
        this.subqueryKind = subqueryKind;
        this.subqueryOp = subqueryOp;
        this.subqueryOperands = subqueryOperands;
    }

    public DynamicParamExpression(RelNode rel, RexDynamicParam rex, SqlKind subqueryKind, SqlOperator subqueryOp) {
        this.index = PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX;
        this.relNode = rel;
        this.sbExprRex = rex;
        this.subqueryKind = subqueryKind;
        this.subqueryOp = subqueryOp;

    }

    @Override
    public Object eval(Row row, ExecutionContext ec) {
        if (index == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
            Object[] valRs = new Object[1];
            boolean fetchSucc =
                SubQueryDynamicParamUtils.fetchScalarSubQueryConstantValue(this.sbExprRex, ec.getScalarSubqueryCtxMap(),
                    true, valRs);
            if (fetchSucc) {
                Object scalarSbVal = valRs[0];
                if (scalarSbVal == RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY) {
                    return null;
                }
                return convertParameterType(scalarSbVal);
            } else {
                throw new SubQueryDynamicValueNotReadyException();
            }
        }

        Map<Integer, ParameterContext> param = ec.getParams() == null ? new HashMap<>() :
            ec.getParams().getCurrentParameter();
        ParameterContext pc = param.get(index + 1);
        if (pc == null) {
            return null;
        }
        Object value = pc.getValue();
        if (value instanceof RawString) {
            RawString rawString = (RawString) value;
            return convertParameterType(rawString.acquireObject(subIndex, skIndex));
        }
        return convertParameterType(value);
    }

    @Override
    public Object evalEndPoint(Row row, ExecutionContext ec, Boolean cmpDirection, AtomicBoolean inclEndp) {
        return eval(row, ec);
    }

    @Override
    public Object eval(Row row) {
        return eval(row, contextProvider.getContext());
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public void setRelNode(RelNode relNode) {
        this.relNode = relNode;
    }

    public SqlKind getSubqueryKind() {
        return subqueryKind;
    }

    public List<IExpression> getSubqueryOperands() {
        return subqueryOperands;
    }

    public SqlOperator getSubqueryOp() {
        return subqueryOp;
    }

    /**
     * Convert parameters from FastSQL to DRDS data types, e.g. BigDecimal to Decimal
     */
    private static Object convertParameterType(Object in) {
        if (in instanceof BigDecimal) {
            return Decimal.fromBigDecimal((BigDecimal) in);
        }
        return in;
    }
}

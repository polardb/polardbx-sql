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

package com.alibaba.polardbx.optimizer.partition.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.ScalarSubQueryExecContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlBinaryOperator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Rex2ExprStringVisitor extends RexExplainVisitor {

    protected ExecutionContext ec;
    public Rex2ExprStringVisitor(ExecutionContext ec) {
        super(null);
        this.ec = ec;
    }

    @Override
    public void visit(SqlBinaryOperator operator, RexCall call) {
        switch (operator.getKind()) {
        case AND:
        case OR:
            Iterator<RexNode> iterator = call.getOperands().iterator();
            while (iterator.hasNext()) {
                final RexNode next = iterator.next();
                switch (next.getKind()) {
                case AND:
                case OR:
                    sb.append("(");
                    next.accept(this);
                    sb.append(")");
                    break;
                default:
                    next.accept(this);
                } // end of switch

                if (iterator.hasNext()) {
                    sb.append(" ").append(operator.getName()).append(" ");
                }
            } // end of while
            break;
        default:
            sb.append("(");
            call.getOperands().get(0).accept(this);
            sb.append(" ").append(operator.getName()).append(" ");
            call.getOperands().get(1).accept(this);
            sb.append(")");
        } // end of switch
    }

    @Override
    public Object visitDynamicParam(RexDynamicParam dynamicParam) {
        int dynamicIndex = dynamicParam.getIndex();
        if (ec != null) {
            if (dynamicIndex >= 0) {
                Map<Integer, ParameterContext> param = ec.getParams() == null ? new HashMap<>() :
                    ec.getParams().getCurrentParameter();
                ParameterContext pc = param.get(dynamicIndex + 1);
                Object value = null;
                if (pc != null) {
                    value = pc.getValue();
                }
                sb.append(dynamicParam).append("[").append(value == null ? "null" : value).append("]");
                return sb;
            } else if (dynamicIndex == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
                Map<Integer, ScalarSubQueryExecContext> scalarSbCtxMap = ec.getScalarSubqueryCtxMap();
                RelNode sbRel = dynamicParam.getRel();
                ScalarSubQueryExecContext ctx = scalarSbCtxMap.get(sbRel.getId());
                if (ctx == null) {
                    sb.append(dynamicParam);
                    return sb;
                }
                Object value = ctx.getSubQueryResult();
                sb.append(dynamicParam).append("[").append(value == null ? "null" : String.valueOf(value)).append("]");
                return sb;
            } else {
                sb.append(dynamicParam);
                return sb;
            }
        } else {
            sb.append(dynamicParam);
            return sb;
        }
    }

    public String getExprString() {
        return sb.toString();
    }

    public static String convertRexToExprString(RexNode expr, ExecutionContext ec) {
        if (ec == null) {
            return expr.toString();
        }
        Rex2ExprStringVisitor visitor = new Rex2ExprStringVisitor(ec);
        expr.accept(visitor);
        return visitor.getExprString();
    }

}

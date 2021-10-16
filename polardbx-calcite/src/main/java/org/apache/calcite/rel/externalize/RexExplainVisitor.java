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

package org.apache.calcite.rel.externalize;

import java.util.Iterator;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexSystemVar;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUserVar;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.omg.CORBA.OBJ_ADAPTER;

public class RexExplainVisitor implements RexVisitor {

    private RelNode       parent;
    private StringBuilder sb = new StringBuilder();

    public RexExplainVisitor(RelNode parent){
        this.parent = parent;
    }

    @Override
    public StringBuilder visitInputRef(RexInputRef inputRef) {
        int index = inputRef.getIndex();
        RelDataTypeField field = getField(index);
        if (field != null) {
            sb.append(field.getKey().toString());
        }
        return sb;
    }

    @Override
    public Object visitLocalRef(RexLocalRef localRef) {
        // TODO Auto-generated method stub
        return sb;
    }

    @Override
    public Object visitLiteral(RexLiteral literal) {
        sb.append(literal.getValue());
        return sb;
    }

    @Override
    public StringBuilder visitCall(RexCall call) {
        SqlOperator operator = call.getOperator();
        operator.accept(this, call);
        return sb;
    }

    @Override
    public void visit(SqlOperator operator, RexCall call) {
        if (operator.getKind() == SqlKind.ROW) {
            specialOperator(operator, call);
        } else {
            sb.append(call.toString());
        }
    }

    @Override
    public void visit(SqlPrefixOperator operator, RexCall call) {
        specialOperator(operator, call);
    }

    @Override
    public void visit(SqlPostfixOperator operator, RexCall call) {
        specialOperator(operator, call);
    }

    @Override
    public void visit(SqlLikeOperator operator, RexCall call) {
        call.getOperands().get(0).accept(this);
        sb.append(" ").append(operator.getName()).append(" ");
        call.getOperands().get(1).accept(this);
    }

    public void specialOperator(SqlOperator operator, RexCall call) {
        sb.append(operator.getName()).append("(");
        int i = -1;
        for (RexNode operand : call.getOperands()) {
            if (++i > 0) {
                sb.append(", ");
            }
            operand.accept(this);
        }
        sb.append(")");
    }

    @Override
    public void visit(SqlFunction operator, RexCall call) {
        sb.append(operator.getName()).append("(");
        int i = -1;
        for (RexNode operand : call.getOperands()) {
            if (++i > 0) {
                sb.append(", ");
            }
            operand.accept(this);
        }
        sb.append(")");
    }

    @Override
    public void visit(SqlCaseOperator operator, RexCall call) {
        sb.append(call.toString());
    }

    @Override
    public Object visitTableInputRef(RexTableInputRef fieldRef) {
        return null;
    }

    @Override
    public Object visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return null;
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
                call.getOperands().get(0).accept(this);
                sb.append(" ").append(operator.getName()).append(" ");
                call.getOperands().get(1).accept(this);
        } // end of switch
    }

    public Object visitCall(SqlFunctionalOperator call) {
        sb.append(call.toString());
        return sb;
    }

    @Override
    public Object visitOver(RexOver over) {
        sb.append(over.toString());
        return sb;
    }

    @Override
    public Object visitCorrelVariable(RexCorrelVariable correlVariable) {
        sb.append(correlVariable.toString());
        return sb;
    }

    @Override
    public Object visitDynamicParam(RexDynamicParam dynamicParam) {
        sb.append(dynamicParam.toString());
        return sb;
    }

    @Override
    public Object visitRangeRef(RexRangeRef rangeRef) {
        sb.append(rangeRef.toString());
        return sb;
    }

    @Override
    public Object visitFieldAccess(RexFieldAccess fieldAccess) {
        sb.append(fieldAccess.toString());
        return sb;
    }

    @Override
    public Object visitSubQuery(RexSubQuery subQuery) {
        sb.append(subQuery.toString());
        return sb;
    }

    @Override
    public Object visitSystemVar(RexSystemVar systemVar) {
        sb.append(systemVar.toString());
        return sb;
    }

    @Override
    public Object visitUserVar(RexUserVar userVar) {
        sb.append(userVar.toString());
        return sb;
    }

    public String toSqlString() {
        return sb.toString();
    }

    public void visit(AggregateCall aggregateCall) {
        sb.append(aggregateCall.getAggregation().getName());
        sb.append("(");
        if (aggregateCall.isDistinct()) {
            sb.append((aggregateCall.getArgList().size() == 0) ? "DISTINCT" : "DISTINCT ");
        }
        int i = -1;
        for (Integer arg : aggregateCall.getArgList()) {
            if (++i > 0) {
                sb.append(", ");
            }
            sb.append(getField(arg).getKey());
        }
        sb.append(")");
        if (aggregateCall.hasFilter()) {
            sb.append(" FILTER $");
            sb.append(aggregateCall.filterArg);
        }
    }

    public void visit(GroupConcatAggregateCall groupConcatAggregateCall) {
        sb.append(groupConcatAggregateCall.getAggregation().getName());
        sb.append("(");
        if (groupConcatAggregateCall.isDistinct()) {
            sb.append((groupConcatAggregateCall.getArgList().size() == 0) ? "DISTINCT" : "DISTINCT ");
        }
        int i = -1;
        for (Integer arg : groupConcatAggregateCall.getArgList()) {
            if (++i > 0) {
                sb.append(", ");
            }
            sb.append(getField(arg).getKey());
        }
        if (groupConcatAggregateCall.getOrderList() != null && groupConcatAggregateCall.getOrderList().size() != 0) {
            sb.append(" ORDER BY ");
            boolean first = true;
            for (i = 0; i < groupConcatAggregateCall.getOrderList().size(); i++) {
                Integer arg = groupConcatAggregateCall.getOrderList().get(i);
                if (!first) {
                    sb.append(", ");
                } else {
                    first = false;
                }
                sb.append(getField(arg).getKey());
                sb.append(" ");
                sb.append(groupConcatAggregateCall.getAscOrDescList().get(i));
            }
        }

        if (groupConcatAggregateCall.getSeparator() != null) {
            sb.append(" SEPARATOR ");
            sb.append("'");
            sb.append(groupConcatAggregateCall.getSeparator());
            sb.append("'");
        }
        sb.append(")");
        if (groupConcatAggregateCall.hasFilter()) {
            sb.append(" FILTER $");
            sb.append(groupConcatAggregateCall.filterArg);
        }
    }

    public RelDataTypeField getField(int index) {
        if (parent instanceof MultiJoin) {
            return parent.getRowType().getFieldList().get(index);
        }
        for (RelNode input : parent.getInputs()) {
            if (index < input.getRowType().getFieldCount()) {
                RelDataTypeField field = input.getRowType().getFieldList().get(index);
                return field;
            } else {
                index -= input.getRowType().getFieldCount();
            }
        }
        assert false;
        return null;
    }

}

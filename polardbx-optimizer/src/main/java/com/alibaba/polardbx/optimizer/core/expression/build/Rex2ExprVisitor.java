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

package com.alibaba.polardbx.optimizer.core.expression.build;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.expression.calc.CorrelateExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.LiteralExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.ScalarFunctionExpression;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSystemVar;
import org.apache.calcite.rex.RexUserVar;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.util.IntervalString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.BINARY;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

/**
 * @author Eric Fu
 */
public class Rex2ExprVisitor extends RexVisitorImpl<IExpression> {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    protected List<DynamicParamExpression> dynamicExpressions = null;
    protected List<RexCall> bloomFilters = null;
    protected ExprContextProvider contextProvider;

    public Rex2ExprVisitor(ExecutionContext executionContext) {
        super(false);
        this.contextProvider = new ExprContextProvider(executionContext);
    }

    public Rex2ExprVisitor(ExprContextProvider contextHolder) {
        super(false);
        this.contextProvider = contextHolder;
    }

    public void setDynamicExpressions(List<DynamicParamExpression> dynamicExpressions) {
        this.dynamicExpressions = dynamicExpressions;
    }

    public void setBloomFilters(List<RexCall> bloomFilters) {
        this.bloomFilters = bloomFilters;
    }

    @Override
    public IExpression visitLiteral(RexLiteral literal) {
        if (literal.getType() instanceof IntervalSqlType) {
            return buildIntervalFunction(literal, contextProvider);
        } else {
            return new LiteralExpression(literal.getValue3());
        }
    }

    @Override
    public IExpression visitInputRef(RexInputRef inputRef) {
        return new InputRefExpression(inputRef.getIndex());
    }

    @Override
    public IExpression visitDynamicParam(RexDynamicParam dynamicParam) {
        /**
         * When
         *  index >= 0, it means the content of RexDynamicParam is the params value can be fetched from ExecutionContext
         *  index = -1, it means the content of RexDynamicParam is phy table name;
         *  index = -2, it means the content of RexDynamicParam is scalar subquery;
         *  index = -3, it means the content of RexDynamicParam is apply subquery.
         */
        if (dynamicParam.getIndex() == -3) {
            DynamicParamExpression dynamicParamExpression = null;
            if (dynamicParam.getSubqueryKind() == null) {
                dynamicParamExpression = new DynamicParamExpression(dynamicParam.getRel());
            } else {
                List<IExpression> iExpressionList = Lists.newArrayList();
                for (RexNode r : dynamicParam.getSubqueryOperands()) {
                    iExpressionList.add(r.accept(this));
                }
                dynamicParamExpression =
                    new DynamicParamExpression(dynamicParam.getRel(), dynamicParam.getSubqueryKind(),
                        dynamicParam.getSubqueryOp(), iExpressionList);
            }
            if (dynamicExpressions != null) {
                dynamicExpressions.add(dynamicParamExpression);
            }
            return dynamicParamExpression;
        }
        return new DynamicParamExpression(dynamicParam.getIndex(), contextProvider);
    }

    @Override
    public IExpression visitFieldAccess(RexFieldAccess fieldAccess) {
        RexCorrelVariable rexCorrelVariable = (RexCorrelVariable) fieldAccess.getReferenceExpr();
        CorrelateExpression ce = new CorrelateExpression(fieldAccess.getField().getIndex());
        Row row = contextProvider.getContext().getCorrelateRowMap().get(rexCorrelVariable.getId());
        if (row != null) {
            ce.setRow(row);
        }
        return ce;
    }

    @Override
    public IExpression visitCall(RexCall call) {
        call = rewriteRexNode(call);

        if (((RexCall) call).getOperator() instanceof SqlRuntimeFilterFunction) {
            if (bloomFilters != null) {
                bloomFilters.add(call);
            }
            // Always true
            return new LiteralExpression(true);
        }

        String functionName = call.getOperator().getName();

        if (call.getKind().equals(SqlKind.MINUS_PREFIX)) {
            functionName = "UNARY_MINUS";
        }
        if (call.getKind().equals(SqlKind.PLUS_PREFIX)) {
            functionName = "UNARY_PLUS";
        }

        List<RexNode> operands = call.getOperands();
        List<IExpression> args = new ArrayList<>(operands.size());
        for (int i = 0; i < operands.size(); i++) {
            args.add(operands.get(i).accept(this));
        }
        args.addAll(visitExtraParams(call));

        DataType resultType = DataTypeUtil.calciteToDrdsType(call.getType());
        List<DataType> operandTypes = operands.stream()
            .map(RexNode::getType)
            .map(type -> DataTypeUtil.calciteToDrdsType(type))
            .collect(Collectors.toList());
        AbstractScalarFunction scalarFunction = ExtraFunctionManager.getExtraFunction(functionName,
            operandTypes, resultType);

        if (scalarFunction instanceof AbstractCollationScalarFunction) {
            CollationName collation = null;
            if (contextProvider.getContext() != null) {
                collation = Rex2ExprUtil.fixCollation(call, contextProvider.getContext());
            }
            ((AbstractCollationScalarFunction) scalarFunction).setCollation(collation);
        }

        return ScalarFunctionExpression
            .getScalarFunctionExp(args, scalarFunction, contextProvider);
    }

    private List<IExpression> visitExtraParams(RexCall call) {
        String functionName = call.getOperator().getName();
        if (functionName.equalsIgnoreCase("CAST")) {
            RelDataType relDataType = call.getType();
            DataType dataType = DataTypeUtil.calciteToDrdsType(relDataType);
            if ((relDataType.getSqlTypeName() == CHAR || relDataType.getSqlTypeName() == BINARY)
                && relDataType.getPrecision() >= 0) {
                return ImmutableList.of(new LiteralExpression(dataType.getStringSqlType()),
                    new LiteralExpression(relDataType.getPrecision()));
            }

            if (relDataType.getSqlTypeName() == DECIMAL && relDataType.getPrecision() > 0) {
                return ImmutableList.of(new LiteralExpression(dataType.getStringSqlType()),
                    new LiteralExpression(relDataType.getPrecision()),
                    new LiteralExpression(relDataType.getScale() == -1 ? 0 : relDataType.getScale()));
            }

            // For fractional time type, preserve scale value.
            if (DataTypeUtil.isFractionalTimeType(dataType)) {
                return ImmutableList
                    .of(new LiteralExpression(dataType.getStringSqlType()),
                        new LiteralExpression(relDataType.getScale() < 0 ? 0 : relDataType.getScale()));
            }

            return ImmutableList.of(new LiteralExpression(dataType.getStringSqlType()));
        }
        if (functionName.equalsIgnoreCase("CONVERT")) {
            return ImmutableList.of(new LiteralExpression(1));
        }
        return Collections.emptyList();
    }

    private RexCall rewriteRexNode(RexCall call) {
        if (call.getOperator().getClass() == SqlStdOperatorTable.TRIM.getClass()) {
            ImmutableList<RexNode> operands = call.operands;
            List<RexNode> nodes = Arrays.asList(operands.get(2), operands.get(1), operands.get(0));
            return call.clone(call.type, nodes);
        } else if (call.op == TddlOperatorTable.DATE_ADD
            || call.op == TddlOperatorTable.ADDDATE
            || call.op == TddlOperatorTable.DATE_SUB
            || call.op == TddlOperatorTable.SUBDATE) {

            RexNode interval = call.getOperands().get(1);
            if (interval instanceof RexCall && ((RexCall) interval).op == TddlOperatorTable.INTERVAL_PRIMARY) {
                // flat the operands tree:
                //
                //       DATE_ADD                                 DATE_ADD
                //       /      \                               /    |     \
                //   TIME   INTERVAL_PRIMARY        =>      TIME    VALUE   TIME_UNIT
                //                 /    \
                //           VALUE        TIME_UNIT
                RexNode literal = ((RexCall) interval).getOperands().get(1);
                if (literal instanceof RexLiteral && ((RexLiteral) literal).getValue() != null) {
                    String unit = ((RexLiteral) literal).getValue().toString();
                    RexCall newCall = (RexCall) REX_BUILDER.makeCall(
                        call.getType(),
                        call.op,
                        ImmutableList.of(
                            call.getOperands().get(0),
                            ((RexCall) interval).getOperands().get(0),
                            REX_BUILDER.makeLiteral(unit)
                        ));
                    return newCall;
                }

            } else if (call.getOperands().size() == 2) {
                // flat the operands tree:
                //
                //       DATE_ADD                                   DATE_ADD
                //       /      \                               /     |         \
                //   TIME    DAY VALUE        =>           TIME    DAY VALUE   INTERVAL_DAY
                //
                RexCall newCall = (RexCall) REX_BUILDER.makeCall(
                    call.getType(),
                    call.op,
                    ImmutableList.of(
                        call.getOperands().get(0),
                        call.getOperands().get(1),
                        REX_BUILDER.makeLiteral(MySQLIntervalType.INTERVAL_DAY.name())
                    ));
                return newCall;
            }

        }
        return call;
    }

    public static IExpression buildIntervalFunction(RexLiteral literal, ExprContextProvider contextHolder) {
        assert literal.getType() instanceof IntervalSqlType;
        List<IExpression> args = new ArrayList<>(2);
        String unit = literal.getType().getIntervalQualifier().getUnit().name();
        String value = ((IntervalString) literal.getValue()).getIntervalStr();
        args.add(new LiteralExpression(value));
        args.add(new LiteralExpression(unit));
        AbstractScalarFunction scalarFunction = ExtraFunctionManager.getExtraFunction("INTERVAL_PRIMARY", null, null);
        return ScalarFunctionExpression.getScalarFunctionExp(args, scalarFunction, contextHolder);
    }

    @Override
    public IExpression visitSystemVar(RexSystemVar systemVar) {
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "System variables (not pushed down)");
    }

    @Override
    public IExpression visitUserVar(RexUserVar userVar) {
        List<IExpression> args = new ArrayList<>(1);
        args.add(new LiteralExpression(userVar.getName()));
        AbstractScalarFunction scalarFunction = ExtraFunctionManager.getExtraFunction("UserVAr", null, null);
        return ScalarFunctionExpression
            .getScalarFunctionExp(args, scalarFunction, contextProvider.getContext());
    }
}

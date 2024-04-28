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

package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

/**
 * Rewrite the expression tree to fit the vectorized framework.
 */
public class ExpressionRewriter {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private ExecutionContext executionContext;
    private final boolean fallback;
    private final boolean enableCSE;
    private ExprContextProvider contextProvider;

    public ExpressionRewriter(ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.fallback =
            !executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_EXPRESSION_VECTORIZATION);
        this.enableCSE =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE);
        this.contextProvider = new ExprContextProvider(executionContext);
    }

    public RexCall rewrite(RexCall call, boolean isScalar) {
        Preconditions.checkNotNull(call);
        if (TddlOperatorTable.CONTROL_FLOW_VECTORIZED_OPERATORS.contains(call.op)) {
            return rewriteControlFlowFunction(call);
        } else if (call.op == TddlOperatorTable.TRIM) {
            return rewriteTrim(call);
        } else if (call.op == TddlOperatorTable.DATE_ADD
            || call.op == TddlOperatorTable.ADDDATE
            || call.op == TddlOperatorTable.DATE_SUB
            || call.op == TddlOperatorTable.SUBDATE) {
            return rewriteTemporalCalc(call);
        } else if (!isScalar && !fallback && call.op == TddlOperatorTable.IN) {
            return rewriteIn(call);
        } else if (enableCSE && call.op == TddlOperatorTable.OR) {
            return rewriteOr(call);
        } else if (enableCSE && call.op == TddlOperatorTable.AND) {
            return rewriteAnd(call);
        }
        return call;
    }

    private RexCall rewriteTrim(RexCall call) {
        ImmutableList<RexNode> operands = call.operands;
        List<RexNode> nodes = Arrays.asList(operands.get(2), operands.get(1), operands.get(0));
        return call.clone(call.type, nodes);
    }

    private RexCall rewriteTemporalCalc(RexCall call) {
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
        return call;
    }

    private RexCall rewriteIn(RexCall call) {
        List<RexNode> operands = call.getOperands();
        List<RexNode> newOperandList = new ArrayList<>();
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);
        newOperandList.add(left);

        // expand the in(...) to operand list.
        boolean expanded = expandIn(newOperandList, right);
        if (!expanded) {
            // fail to expand the IN value list.
            return call;
        }
        RexCall newCall = (RexCall) REX_BUILDER.makeCall(
            call.getType(),
            call.op,
            newOperandList
        );
        return newCall;
    }

    private boolean expandIn(List<RexNode> newOperandList, RexNode right) {
        boolean expandable = right instanceof RexCall
            && ((RexCall) right).op == TddlOperatorTable.ROW
            && RexUtil.isConstant(right);
        if (!expandable) {
            return false;
        }
        // row expression optimized
        for (RexNode operand : ((RexCall) right).getOperands()) {
            if (operand instanceof RexDynamicParam) {
                // evaluate dynamic param
                Object value = extractDynamicValue((RexDynamicParam) operand);

                // check if list value
                if (value instanceof List) {
                    for (Object listItem : (List) value) {
                        if (listItem instanceof List) {
                            // cannot expand: row in format of ((1,2,3), (2,3,4))
                            return false;
                        }
                        SqlTypeName typeName = DataTypeUtil.typeNameOfParam(listItem);
                        RelDataType relDataType = TYPE_FACTORY.createSqlType(typeName);
                        DataType dataType = DataTypeUtil.calciteToDrdsType(relDataType);
                        RexNode literalNode = REX_BUILDER.makeLiteral(
                            dataType.convertFrom(listItem),
                            relDataType,
                            false);
                        newOperandList.add(literalNode);
                    }
                } else {
                    // ban prepare mode
                    return false;
                }
            } else {
                newOperandList.add(operand);
            }
        }

        return true;
    }

    private RexCall rewriteOr(RexCall call) {
        Preconditions.checkArgument(call.op == TddlOperatorTable.OR);

        if (isDNF(call)) {
            call = refineAndFromOr(call);
        }

        boolean needMerge = needMergeBetweenFromOr(call);
        if (needMerge) {
            call = mergeBetweenFromOr(call);
        }

        return call;
    }

    private boolean isDNF(RexCall call) {
        // check if or-expression is DNF
        return call.op == TddlOperatorTable.OR
            && call.getOperands().size() > 1
            && call.getOperands().stream()
            .allMatch(child -> child instanceof RexCall && ((RexCall) child).op == TddlOperatorTable.AND);
    }

    private boolean needMergeBetweenFromOr(RexCall call) {
        return call.op == TddlOperatorTable.OR
            && call.getOperands().size() == 3
            && call.getOperands().stream()
            .allMatch(
                child ->
                    child instanceof RexCall
                        && ((RexCall) child).op == TddlOperatorTable.BETWEEN
                        && ((RexCall) child).getOperands().size() == 3
                        && ((RexCall) child).getOperands().get(0) instanceof RexInputRef
                        && ((RexCall) child).getOperands().get(1).getType().getSqlTypeName() == BIGINT
                        && ((RexCall) child).getOperands().get(2).getType().getSqlTypeName() == BIGINT
                        && ((RexCall) child).getOperands().get(1).getClass() == ((RexCall) child).getOperands()
                        .get(2).getClass()
            );
    }

    private RexCall mergeBetweenFromOr(RexCall call) {
        RexInputRef inputRef = null;
        long lowerBound = Long.MAX_VALUE;
        long upperBound = Long.MIN_VALUE;

        for (int i = 0; i < call.getOperands().size(); i++) {
            RexCall child = (RexCall) call.getOperands().get(i);

            RexInputRef current = (RexInputRef) child.getOperands().get(0);
            if (inputRef == null) {
                inputRef = current;
            } else if (current.getIndex() != inputRef.getIndex()) {
                return call;
            }

            RexNode operandValue1 = child.getOperands().get(1);
            RexNode operandValue2 = child.getOperands().get(2);

            if (operandValue1 instanceof RexDynamicParam) {
                lowerBound =
                    Math.min(lowerBound,
                        DataTypes.LongType.convertFrom(extractDynamicValue((RexDynamicParam) operandValue1)));
                upperBound =
                    Math.max(upperBound,
                        DataTypes.LongType.convertFrom(extractDynamicValue((RexDynamicParam) operandValue2)));
            } else if (operandValue2 instanceof RexLiteral) {
                lowerBound =
                    Math.min(lowerBound, DataTypes.LongType.convertFrom(((RexLiteral) operandValue1).getValue3()));
                upperBound =
                    Math.max(upperBound, DataTypes.LongType.convertFrom(((RexLiteral) operandValue2).getValue3()));
            } else {
                return call;
            }
        }

        RexCall newBetween = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.BETWEEN,
            ImmutableList.of(
                // input ref
                inputRef,
                // lower value
                REX_BUILDER.makeLiteral(lowerBound, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), BIGINT),
                // upper value
                REX_BUILDER.makeLiteral(upperBound, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), BIGINT)
            )
        );

        return newBetween;
    }

    private RexCall refineAndFromOr(RexCall call) {
        // collect all the expression node
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
        final int andCount = call.getOperands().size();
        Set<CommonExpressionNode>[] nodeSets = new Set[andCount];
        for (int i = 0; i < andCount; i++) {
            nodeSets[i] = new HashSet<>();
            RexCall andExpression = (RexCall) call.getOperands().get(i);
            andExpression.getOperands().stream()
                .map(e -> new CommonExpressionNode(e, params))
                .forEach(nodeSets[i]::add);
        }

        // find intersection of all and-expressions
        Stream<CommonExpressionNode> stream = nodeSets[0].stream();
        for (int i = 1; i < andCount; i++) {
            stream = stream.filter(nodeSets[i]::contains);
        }
        Set<CommonExpressionNode> intersection = stream.collect(Collectors.toSet());
        if (intersection.isEmpty()) {
            return call;
        }

        // remove node from intersection for each and-expression,
        // and then construct new or-expression
        List<RexNode> newOrExpressionOperands = new ArrayList<>();
        for (int i = 0; i < andCount; i++) {
            RexCall andExpression = (RexCall) call.getOperands().get(i);

            List<RexNode> newOperandList = nodeSets[i].stream().filter(e -> !intersection.contains(e))
                .map(CommonExpressionNode::getRexNode).collect(Collectors.toList());

            RexCall newAndExpression = (RexCall) REX_BUILDER.makeCall(
                andExpression.type,
                andExpression.op,
                newOperandList
            );

            // merge between from and-expression
            newAndExpression = rewriteAnd(newAndExpression);

            newOrExpressionOperands.add(newAndExpression);
        }
        RexCall newOrExpression = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.OR,
            newOrExpressionOperands
        );

        // merge between from or-expression
        if (needMergeBetweenFromOr(newOrExpression)) {
            newOrExpression = mergeBetweenFromOr(newOrExpression);
        }

        // construct new and-expression.
        List<RexNode> refinedAnd =
            intersection.stream()
                .map(commonExpressionNode -> commonExpressionNode.getRexNode())
                .collect(Collectors.toList());
        RexCall newCall = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.AND,
            ImmutableList.<RexNode>builder().addAll(refinedAnd).add(newOrExpression).build()
        );

        return newCall;
    }

    // merge (>= and <=) to between
    private RexCall rewriteAnd(RexCall call) {
        Preconditions.checkArgument(call.op == TddlOperatorTable.AND);

        if (call.getOperands().size() < 2) {
            return call;
        }

        Set<Pair<Integer, Integer>> betweenPairSet = new HashSet<>();

        // collect lower && upper bounds call
        boolean[] unreachable = new boolean[call.getOperands().size()];
        boolean needMerge = false;
        for (int i = 0; i < call.getOperands().size(); i++) {
            for (int j = i + 1; j < call.getOperands().size(); j++) {
                if (call.getOperands().get(i) instanceof RexCall && call.getOperands().get(j) instanceof RexCall) {
                    RexCall call1 = (RexCall) call.getOperands().get(i);
                    RexCall call2 = (RexCall) call.getOperands().get(j);

                    if (!unreachable[i]
                        && !unreachable[j]
                        && call1.getOperands().size() == call2.getOperands().size()
                        && call1.getOperands().size() == 2
                        && call1.getOperands().get(0) instanceof RexInputRef
                        && call2.getOperands().get(0) instanceof RexInputRef
                        && (call1.getOperands().get(1) instanceof RexLiteral || call1.getOperands()
                        .get(1) instanceof RexDynamicParam)
                        && (call2.getOperands().get(1) instanceof RexLiteral || call2.getOperands()
                        .get(1) instanceof RexDynamicParam)
                        && ((RexInputRef) call1.getOperands().get(0)).getIndex() == ((RexInputRef) call2.getOperands()
                        .get(0)).getIndex()
                    ) {
                        if (call1.op == TddlOperatorTable.GREATER_THAN_OR_EQUAL
                            && call2.op == TddlOperatorTable.LESS_THAN_OR_EQUAL) {
                            betweenPairSet.add(Pair.of(i, j));
                            unreachable[i] = true;
                            unreachable[j] = true;
                            needMerge = true;
                        } else if (call2.op == TddlOperatorTable.GREATER_THAN_OR_EQUAL
                            && call1.op == TddlOperatorTable.LESS_THAN_OR_EQUAL) {
                            betweenPairSet.add(Pair.of(j, i));
                            unreachable[i] = true;
                            unreachable[j] = true;
                            needMerge = true;
                        }
                    }
                }
            }
        }

        if (!needMerge) {
            return call;
        }

        List<RexNode> reachableOperands = IntStream.range(0, call.getOperands().size())
            .filter(i -> !unreachable[i]).mapToObj(i -> call.getOperands().get(i)).collect(Collectors.toList());

        List<RexNode> betweenCalls = betweenPairSet.stream()
            .map(pair -> {
                RexCall lower = (RexCall) call.getOperands().get(pair.getKey());
                RexCall upper = (RexCall) call.getOperands().get(pair.getValue());
                RexCall newBetween = (RexCall) REX_BUILDER.makeCall(
                    TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
                    TddlOperatorTable.BETWEEN,
                    ImmutableList.of(
                        lower.getOperands().get(0), // input ref
                        lower.getOperands().get(1), // lower value
                        upper.getOperands().get(1) // upper value
                    )
                );
                return newBetween;
            })
            .collect(Collectors.toList());

        RexCall newAnd;
        if (reachableOperands.isEmpty() && betweenCalls.size() == 1) {
            // and-expression has only one operand
            return (RexCall) betweenCalls.get(0);
        } else {
            newAnd = (RexCall) REX_BUILDER.makeCall(
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
                TddlOperatorTable.AND,
                ImmutableList.<RexNode>builder().addAll(betweenCalls).addAll(reachableOperands).build()
            );
        }

        return newAnd;
    }

    private RexCall rewriteControlFlowFunction(RexCall rexCall) {
        final List<RexNode> exprList = rexCall.getOperands();
        final RelDataType returnType = rexCall.type;
        final RelDataType condType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
        if (rexCall.op == SqlStdOperatorTable.CASE) {
            List<RexNode> operands = IntStream.range(0, exprList.size())
                .mapToObj((i) -> {
                    RexNode node = exprList.get(i);
                    if (i % 2 == 0 && i != exprList.size() - 1) {
                        // Other expressions need to be rewrite to IS TRUE expression to be compatible.
                        while (!Rex2VectorizedExpressionVisitor
                            .canBindingToCommonFilterExpression(node)) {
                            node = REX_BUILDER.makeCall(TddlOperatorTable.IS_TRUE, node);
                        }
                    }
                    return node;
                }).collect(Collectors.toList());

            return rexCall.clone(rexCall.type, operands);
        } else if (rexCall.op == TddlOperatorTable.IFNULL) {
            // all IFNULL function has already been rewrite to COALESCE function.
            // IFNULL(expr1, expr2) => COALESCE(expr1, expr2)
            RexCall coalesceOperator =
                (RexCall) REX_BUILDER.makeCall(returnType, TddlOperatorTable.COALESCE, ImmutableList.copyOf(exprList));
            return coalesceOperator;
        } else if (rexCall.op == TddlOperatorTable.IF) {
            // IF(cond, thenExpr, elseExpr) => case when cond then thenExpr else elseExpr
            List<RexNode> exprs = ImmutableList.<RexNode>builder().addAll(rexCall.operands).build();
            RexCall caseOp = (RexCall) REX_BUILDER.makeCall(returnType, TddlOperatorTable.CASE, exprs);
            return rewriteControlFlowFunction(caseOp);
        }
        return rexCall;
    }

    /**
     * rewrite CASE operator to IF operator.
     * CASE WHEN cond1 THEN expr1
     * WHEN cond2 THEN expr2
     * ...
     * ELSE exprN
     * END
     * =>
     * IF = IF1
     * IF1 = (cond1, expr1, IF2)
     * IFI = (condI, exprI, IFI+1)
     * I < N
     */
    private RexNode makeCaseOperator(final RelDataType type, final List<RexNode> exprList, int startIndex) {
        RexNode elseNode = startIndex + 2 == exprList.size() - 1 ?
            exprList.get(startIndex + 2) :
            makeCaseOperator(type, exprList, startIndex + 2);

        List<RexNode> newExprList = ImmutableList.of(
            exprList.get(startIndex),
            exprList.get(startIndex + 1),
            elseNode
        );
        RexNode rexNode = REX_BUILDER.makeCall(type, TddlOperatorTable.IF, newExprList);
        return rexNode;
    }

    private Object extractDynamicValue(RexDynamicParam dynamicParam) {
        // pre-compute the dynamic value when binging expression.
        DynamicParamExpression dynamicParamExpression =
            new DynamicParamExpression(dynamicParam.getIndex(), contextProvider,
                dynamicParam.getSubIndex(), dynamicParam.getSkIndex());

        return dynamicParamExpression.eval(null, executionContext);
    }

}

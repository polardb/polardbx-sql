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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprVisitor;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.function.SqlSequenceFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlValuesFunction;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceSequenceWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSequenceParam;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.ALL;
import static org.apache.calcite.sql.SqlKind.LITERAL;
import static org.apache.calcite.sql.SqlKind.SOME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;

/**
 * Created by chuanqin on 17/8/7.
 */
public class RexUtils {
    private final static List<SqlOperator> UN_PUSHABLE_FUNCTION = ImmutableList.of(
        TddlOperatorTable.CAN_ACCESS_TABLE,
        TddlOperatorTable.GET_LOCK,
        TddlOperatorTable.RELEASE_LOCK,
        TddlOperatorTable.RELEASE_ALL_LOCKS,
        TddlOperatorTable.IS_FREE_LOCK,
        TddlOperatorTable.IS_USED_LOCK,
        TddlOperatorTable.ASSIGNMENT,
        TddlOperatorTable.VERSION,
        TddlOperatorTable.PART_ROUTE
    );

    private final static RelDataTypeFactory FACTORY = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(FACTORY);

    public static IExpression buildRexNode(RexNode rexNode, ExecutionContext executionContext) {
        if (rexNode == null) {
            return null;
        }
        Rex2ExprVisitor visitor = new Rex2ExprVisitor(new ExprContextProvider(executionContext));
        return rexNode.accept(visitor);
    }

    public static IExpression buildRexNode(RexNode rexNode, ExprContextProvider contextProvider) {
        if (rexNode == null) {
            return null;
        }
        Rex2ExprVisitor visitor = new Rex2ExprVisitor(contextProvider);
        return rexNode.accept(visitor);
    }

    public static IExpression buildRexNode(RexNode rexNode, ExecutionContext executionContext,
                                           List<DynamicParamExpression> dynamicExpressions) {
        if (rexNode == null) {
            return null;
        }
        Rex2ExprVisitor visitor = new Rex2ExprVisitor(new ExprContextProvider(executionContext));
        visitor.setDynamicExpressions(dynamicExpressions);
        return rexNode.accept(visitor);
    }

    public static IExpression buildRexNode(RexNode rexNode, ExprContextProvider contextHolder,
                                           List<DynamicParamExpression> dynamicExpressions) {
        if (rexNode == null) {
            return null;
        }
        Rex2ExprVisitor visitor = new Rex2ExprVisitor(contextHolder);
        visitor.setDynamicExpressions(dynamicExpressions);
        return rexNode.accept(visitor);
    }

    public static IExpression buildRexNode(RexNode rexNode, ExecutionContext executionContext,
                                           List<DynamicParamExpression> dynamicExpressions,
                                           List<RexCall> bloomfilters) {
        if (rexNode == null) {
            return null;
        }
        Rex2ExprVisitor visitor = new Rex2ExprVisitor(new ExprContextProvider(executionContext));
        visitor.setBloomFilters(bloomfilters);
        visitor.setDynamicExpressions(dynamicExpressions);
        return rexNode.accept(visitor);
    }

    public static IExpression buildRexNode(RexNode rexNode, ExprContextProvider contextHolder,
                                           List<DynamicParamExpression> dynamicExpressions,
                                           List<RexCall> bloomfilters) {
        if (rexNode == null) {
            return null;
        }
        Rex2ExprVisitor visitor = new Rex2ExprVisitor(contextHolder);
        visitor.setBloomFilters(bloomfilters);
        visitor.setDynamicExpressions(dynamicExpressions);
        return rexNode.accept(visitor);
    }

    private static Object getDynamicParam(RexDynamicParam rexDynamicParam, Map<Integer, ParameterContext> paramMap) {
        int index = rexDynamicParam.getIndex();
        return paramMap.get(index + 1).getValue();
    }

    public static Object getRexNodeValue(RexNode rexNode, ExecutionContext ec) {
        return getRexNodeValue(rexNode, ec.getParams().getCurrentParameter());
    }

    public static Object getRexNodeValue(RexNode rexNode, Map<Integer, ParameterContext> params) {
        Object value = null;
        if (rexNode instanceof RexDynamicParam) {
            RexDynamicParam rexDynamicParam = (RexDynamicParam) rexNode;

            value = getDynamicParam(rexDynamicParam, params);
        } else if (rexNode instanceof RexLiteral) {
            value = RexLiteralTypeUtils.getJavaObjectFromRexLiteral((RexLiteral) rexNode);
        } else {
            throw new UnsupportedOperationException("Get value from " + rexNode.getClass() + " is not supported");
        }
        return value;
    }

    /**
     * Replace expression in VALUES and implicit/explicit sequence call with a parameter of literal
     *
     * @param insert insert or replace
     * @param ec ExecutionContext
     * @param handlerParams Out param for sequence related values
     */
    public static void updateParam(LogicalInsert insert, ExecutionContext ec, boolean handleDuplicateKeyUpdateList,
                                   LogicalInsert.HandlerParams handlerParams) {
        if (OptimizerContext.getContext(insert.getSchemaName()).isSqlMock()) {
            return;
        }

        final Parameters params = ec.getParams();
        final boolean isBatch = params.isBatch();

        final LogicalDynamicValues values = RelUtils.getRelInput(insert);

        // Handle expression
        replaceExpressionWithLiteralParam(values, ec);
        if (insert.withDuplicateKeyUpdate() && handleDuplicateKeyUpdateList) {
            // Deterministic pushdown
            replaceExpressionForDuplicateKeyUpdate(insert, ec);
        }

        // Handle sequence
        final int seqColumnIndex = insert.getSeqColumnIndex();
        final boolean usingSequence = seqColumnIndex >= 0;

        final String schemaName = insert.getSchemaName();
        final String tableName = insert.getLogicalTableName();
        final boolean autoValueOnZero = SequenceAttribute.getAutoValueOnZero(ec.getSqlMode());

        if (null != handlerParams) {
            handlerParams.usingSequence = usingSequence;
        }

        // If it's not using sequence, and it's not using batch, which means
        // there won't be NEXTVAL, skip calculating sequence.
        if (!usingSequence && isBatch) {
            return;
        }

        // Explicit call to NEXTVAL may exist in single table INSERT
        ReplaceSequenceWithLiteralVisitor visitor = new ReplaceSequenceWithLiteralVisitor(params,
            seqColumnIndex,
            schemaName,
            tableName,
            autoValueOnZero);

        if (usingSequence) {
            boolean autoIncrementUsingSeq = replaceSequenceWithLiteralParam(insert, values, params, visitor);

            if (null != handlerParams) {
                handlerParams.autoIncrementUsingSeq = autoIncrementUsingSeq;
            }
        }

        final Long lastInsertId = visitor.getLastInsertId();
        final Long returnedLastInsertId = null != lastInsertId ? lastInsertId : visitor.getReturnedLastInsertId();

        if (null != handlerParams) {
            if (null != lastInsertId) {
                handlerParams.lastInsertId = lastInsertId;
            }

            if (null != returnedLastInsertId) {
                handlerParams.returnedLastInsertId = returnedLastInsertId;
            }
        }
    }

    public static void replaceExpressionWithLiteralParam(LogicalDynamicValues values, ExecutionContext ec) {
        for (List<RexNode> tuple : values.tuples) {
            for (RexNode rex : tuple) {
                if (!(rex instanceof RexCallParam)) {
                    continue;
                }

                evalRexCallParam((RexCallParam) rex, ec, RexCallParam::getRexCall);
            }
        }
    }

    public static void evalRexCallParam(RexCallParam callParam, ExecutionContext ec,
                                        Function<RexCallParam, RexNode> getRexCall) {
        final Parameters params = ec.getParams();
        final boolean isBatch = params.isBatch();
        if (isBatch) {
            final List<Map<Integer, ParameterContext>> batchParams = params.getBatchParameters();
            final ExecutionContext tmpEc = ec.copy((Parameters) null);

            for (Map<Integer, ParameterContext> param : batchParams) {
                tmpEc.setParams(new Parameters(param));
                // RexCall -> IExpr.eval()
                final Function<RexNode, Object> evalFunc = getEvalFunc(tmpEc);

                final int paramIndex = callParam.getIndex();

                // RexCallParams ->  RexCall (the real expr call) -> IExpr.eval() -> obj
                Object value = evalFunc.apply(getRexCall.apply(callParam));
                if (value instanceof Slice) {
                    value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
                } else if (value instanceof ByteString) {
                    value = ((ByteString) value).getBytes();
                }
                final ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    paramIndex + 1, value});

                param.put(paramIndex + 1, newPc);
            }

        } else {
            final Function<RexNode, Object> evalFunc = getEvalFunc(ec);
            final Map<Integer, ParameterContext> param = params.getCurrentParameter();

            final int paramIndex = callParam.getIndex();
            Object value = evalFunc.apply(getRexCall.apply(callParam));
            if (value instanceof Slice) {
                value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
            } else if (value instanceof ByteString) {
                value = ((ByteString) value).getBytes();
            }
            final ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                paramIndex + 1, value});

            param.put(paramIndex + 1, newPc);
        }
    }

    public static void replaceExpressionForDuplicateKeyUpdate(LogicalInsert upsert, ExecutionContext ec) {
        final LogicalDynamicValues input = RelUtils.getRelInput(upsert);
        final ImmutableList<RexNode> rexRow = input.getTuples().get(0);
        final List<String> insertColumns = input.getRowType().getFieldNames();
        final List<String> tableColumns = upsert.getTable().getRowType().getFieldNames();

        final Map<String, RexNode> columnValueMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Ord.zip(insertColumns).forEach(o -> columnValueMap.put(o.getValue(), rexRow.get(o.getKey())));
        final List<RexNode> columnValueMapping =
            tableColumns.stream().map(columnValueMap::get).collect(Collectors.toList());

        final ReplaceValuesCall replaceValuesCall = new ReplaceValuesCall(columnValueMapping);

        final RexShuttle visitor = new RexShuttle() {
            @Override
            public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                if (!(dynamicParam instanceof RexCallParam)) {
                    return super.visitDynamicParam(dynamicParam);
                }

                evalRexCallParam((RexCallParam) dynamicParam, ec, (r) -> r.getRexCall().accept(replaceValuesCall));
                return dynamicParam;
            }
        };

        final List<RexNode> rexNodes = upsert.getDuplicateKeyUpdateList();

        rexNodes.forEach(rex -> rex.accept(visitor));
    }

    public static boolean replaceSequenceWithLiteralParam(LogicalInsert insert, LogicalDynamicValues values,
                                                          Parameters params,
                                                          ReplaceSequenceWithLiteralVisitor visitor) {
        final boolean isBatch = params.isBatch();

        final Set<Integer> autoIncColumnIndex = new HashSet<>();
        final List<RexNode> mergeRow = new ArrayList<>();

        int offset = 0;
        for (List<RexNode> tuple : values.getTuples()) {
            autoIncColumnIndex.add(insert.getSeqColumnIndex() + offset);
            mergeRow.addAll(tuple);
            offset += tuple.size();
        }

        // Not in SPLIT mode
        if (params.getSequenceSize().get() == 0) {
            // Compute sequence size;
            int seqSize = computeImplicitSequenceSize(visitor, params, mergeRow, autoIncColumnIndex);

            final int expectedImplicitSeqSize =
                autoIncColumnIndex.size() * (params.isBatch() ? params.getBatchSize() : 1);
            final boolean isSimpleSeq = (SequenceAttribute.Type.SIMPLE == visitor.getSeqType());

            if (seqSize == expectedImplicitSeqSize || !isSimpleSeq) {
                // For SIMPLE sequence, If not all auto increment column are using implicit value,
                // we cannot get sequence value in batch, because SIMPLE might be updated by insert
                params.getSequenceSize().set(seqSize);
                params.getSequenceIndex().set(0);
            }
        }

        boolean autoIncrementUsingSeq = true;
        if (isBatch) {
            // The number of parameter value is larger than RexDynamicParam
            for (Map<Integer, ParameterContext> curParam : params.getBatchParameters()) {
                autoIncrementUsingSeq &= visitor.replaceDynamicParam(mergeRow, curParam, autoIncColumnIndex);
            }
        } else {
            // The number of parameter value is equal to RexDynamicParam
            autoIncrementUsingSeq =
                visitor.replaceDynamicParam(mergeRow, params.getCurrentParameter(), autoIncColumnIndex);
        }

        return autoIncrementUsingSeq;
    }

    public static int computeImplicitSequenceSize(ReplaceSequenceWithLiteralVisitor visitor, Parameters params,
                                                  List<RexNode> mergeRow, Set<Integer> autoIncColumnIndex) {
        int seqSize = 0;
        for (Map<Integer, ParameterContext> curParam : params.getBatchParameters()) {
            final int tmpSeqCount = visitor.getImplicitSequenceBatchSize(mergeRow, curParam, autoIncColumnIndex);

            if (tmpSeqCount > 0) {
                seqSize += tmpSeqCount;
            }
        }
        return seqSize;
    }

    public static Function<RexNode, Object> getEvalFunc(ExecutionContext executionContext) {
        return rexNode -> buildRexNode(rexNode, executionContext).eval(null);
    }

    public static IExpression getEvalFuncExec(RexNode input, ExprContextProvider ctxHolder) {
        return buildRexNode(input, ctxHolder);
    }

    public static Object getValueFromRexNode(RexNode rexNode, ExecutionContext executionContext,
                                             Map<Integer, ParameterContext> rowParameters) {
        final ExecutionContext copy = executionContext.copy(new Parameters(rowParameters));

        Object value = null;
        if (rexNode instanceof RexDynamicParam) {
            final int valueIndex = ((RexDynamicParam) rexNode).getIndex() + 1;
            value = rowParameters.get(valueIndex).getValue();
        } else if (rexNode instanceof RexLiteral) {
            value = RexLiteralTypeUtils.getJavaObjectFromRexLiteral((RexLiteral) rexNode, true);
        } else if (rexNode instanceof RexCall) {
            value = buildRexNode(rexNode, copy).eval(null);
            if (value instanceof Decimal) {
                value = ((Decimal) value).toBigDecimal();
            } else if (value instanceof Slice) {
                value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
            }
        } else {
            throw new UnsupportedOperationException("Get value from " + rexNode.getClass() + " is not supported");
        }
        return value;
    }

    public static Object getValueFromRexNode(RexNode rexNode, Row row, ExecutionContext ec) {
        final Map<Integer, ParameterContext> rowParameters = ec.getParams().getCurrentParameter();

        Object result = null;
        if (rexNode instanceof RexDynamicParam) {
            final int valueIndex = ((RexDynamicParam) rexNode).getIndex() + 1;
            result = rowParameters.get(valueIndex).getValue();
        } else if (rexNode instanceof RexLiteral) {
            result = RexLiteralTypeUtils.getJavaObjectFromRexLiteral((RexLiteral) rexNode, true);
        } else {
            final Object value = buildRexNode(rexNode, ec).eval(row);
            if (value instanceof Decimal) {
                result = ((Decimal) value).toBigDecimal();
            } else if (value instanceof Slice) {
                result = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
            } else {
                result = value;
            }
        }
        return result;
    }

    /**
     * Eval RexNode
     *
     * @param rexRow RexNode list to be eval
     * @param row Selected row, null for insert row without duplicate
     * @param param Insert parameter row
     * @return Value list foreach RexNode
     */
    public static List<Object> buildRowValue(List<RexNode> rexRow, Row row, Map<Integer, ParameterContext> param) {
        final ExecutionContext tmpEc = new ExecutionContext();
        tmpEc.setParams(new Parameters(param));

        final List<Object> result = new ArrayList<>();
        for (RexNode rex : rexRow) {
            final Object value = getValueFromRexNode(rex, row, tmpEc);
            result.add(value);
        }

        return result;
    }

    public static boolean isSimpleCondition(Join join, RexNode condition, int leftBound) {
        return isBatchKeysAccessCondition(join, condition, leftBound, RestrictType.BOTH, null, true);
    }

    public static boolean isBatchKeysAccessCondition(Join join, RexNode condition, int leftBound,
                                                     RestrictType restrictType,
                                                     Function<Pair<RelDataType, RelDataType>, Boolean> checkFunc) {
        return isBatchKeysAccessCondition(join, condition, leftBound, restrictType, checkFunc, false);
    }

    public static boolean isBatchKeysAccessCondition(Join join, RexNode condition, int leftBound,
                                                     RestrictType restrictType,
                                                     Function<Pair<RelDataType, RelDataType>, Boolean> checkFunc,
                                                     boolean isStrictSimple) {
        if (condition == null) {
            return false;
        }

        if (forceNLJoin(join)) {
            return false;
        }

        if (!(condition instanceof RexCall)) {
            return false;
        }
        final RexCall currentCondition = (RexCall) condition;
        switch (currentCondition.getKind()) {
        case EQUALS: {
            if (currentCondition.getOperands().size() != 2) {
                return false;
            }
            RexNode operand1 = currentCondition.getOperands().get(0);
            RexNode operand2 = currentCondition.getOperands().get(1);
            if (!(operand1 instanceof RexInputRef && operand2 instanceof RexInputRef)) {
                return false;
            }

            int indexOp1 = ((RexInputRef) operand1).getIndex();
            int indexOp2 = ((RexInputRef) operand2).getIndex();
            RelColumnOrigin relColumnOriginLeft = null;
            RelColumnOrigin relColumnOriginRight = null;
            RelDataType relDataTypeLeft;
            RelDataType relDataTypeRight;
            final boolean indexOp1IsLeft = indexOp1 < leftBound;
            final boolean indexOp2IsRight = indexOp2 >= leftBound;
            RelMetadataQuery mq = join.getCluster().getMetadataQuery();
            if (indexOp1IsLeft && indexOp2IsRight) {
                if (restrictType == RestrictType.LEFT) {
                    relColumnOriginLeft = mq.getColumnOrigin(join.getLeft(), indexOp1);
                } else if (restrictType == RestrictType.RIGHT) {
                    relColumnOriginRight = mq.getColumnOrigin(join.getRight(), indexOp2 - leftBound);
                } else if (restrictType == RestrictType.BOTH) {
                    relColumnOriginLeft = mq.getColumnOrigin(join.getLeft(), indexOp1);
                    relColumnOriginRight = mq.getColumnOrigin(join.getRight(), indexOp2 - leftBound);
                } else if (restrictType == RestrictType.NONE) {
                    // pass
                }
                relDataTypeLeft = join.getLeft().getRowType().getFieldList().get(indexOp1).getType();
                relDataTypeRight =
                    join.getRight().getRowType().getFieldList().get(indexOp2 - leftBound).getType();
            } else if (!indexOp1IsLeft && !indexOp2IsRight) {
                if (restrictType == RestrictType.LEFT) {
                    relColumnOriginLeft = mq.getColumnOrigin(join.getLeft(), indexOp2);
                } else if (restrictType == RestrictType.RIGHT) {
                    relColumnOriginRight = mq.getColumnOrigin(join.getRight(), indexOp1 - leftBound);
                } else if (restrictType == RestrictType.BOTH) {
                    relColumnOriginLeft = mq.getColumnOrigin(join.getLeft(), indexOp2);
                    relColumnOriginRight = mq.getColumnOrigin(join.getRight(), indexOp1 - leftBound);
                } else if (restrictType == RestrictType.NONE) {
                    // pass
                }
                relDataTypeLeft = join.getLeft().getRowType().getFieldList().get(indexOp2).getType();
                relDataTypeRight =
                    join.getRight().getRowType().getFieldList().get(indexOp1 - leftBound).getType();
            } else {
                return false;
            }

            if ((restrictType == RestrictType.LEFT && relColumnOriginLeft != null)
                || (restrictType == RestrictType.RIGHT && relColumnOriginRight != null)
                || (restrictType == RestrictType.BOTH && relColumnOriginLeft != null && relColumnOriginRight != null)
                || (restrictType == RestrictType.NONE)) {
                if (checkFunc != null) {
                    return checkFunc.apply(Pair.of(relDataTypeLeft, relDataTypeRight));
                } else {
                    return true;
                }

            } else {
                return false;
            }
        }
        case AND: {
            //isStrictSimple means all of conditions is simple.
            boolean simple = isStrictSimple;
            for (RexNode rexNode : ((RexCall) condition).getOperands()) {
                if (isStrictSimple) {
                    simple &= isBatchKeysAccessCondition(join, rexNode, leftBound, restrictType,
                        checkFunc, isStrictSimple);
                } else {
                    simple |= isBatchKeysAccessCondition(join, rexNode, leftBound, restrictType,
                        checkFunc, isStrictSimple);
                }
                // '&' need all is true, '|' find one and return.
                if (!isStrictSimple && simple) {
                    return true;
                }
            }
            return simple;
        }
        default: {
            return false;
        }
        } // end of switch other else return false;
    }

    public static boolean forceNLJoin(Join join) {
        if (join instanceof LogicalSemiJoin &&
            join.getJoinType() == JoinRelType.ANTI &&
            ((LogicalSemiJoin) join).getOperands() != null &&
            ((LogicalSemiJoin) join).getOperands().size() > 1) {
            return true;
        }
        return false;
    }

    public static boolean isBatchKeysAccessConditionRefIndexScan(RexNode condition, Join join, boolean lookupRightSide,
                                                                 LogicalTableLookup logicalTableLookup) {
        if (condition == null) {
            return false;
        }
        if (!(condition instanceof RexCall)) {
            return false;
        }

        int leftBound = join.getLeft().getRowType().getFieldCount();

        final RexCall currentCondition = (RexCall) condition;
        switch (currentCondition.getKind()) {
        case EQUALS: {
            if (currentCondition.getOperands().size() != 2) {
                return false;
            }
            RexNode operand1 = currentCondition.getOperands().get(0);
            RexNode operand2 = currentCondition.getOperands().get(1);
            if (!(operand1 instanceof RexInputRef && operand2 instanceof RexInputRef)) {
                return false;
            }

            final int indexOp1 = ((RexInputRef) operand1).getIndex();
            final int indexOp2 = ((RexInputRef) operand2).getIndex();
            final boolean indexOp1IsLeft = indexOp1 < leftBound;
            final boolean indexOp2IsRight = indexOp2 >= leftBound;
            RelMetadataQuery relMetadataQuery = join.getCluster().getMetadataQuery();
            final int idx;
            if (indexOp1IsLeft && indexOp2IsRight) {
                if (lookupRightSide) {
                    idx = indexOp2 - leftBound;
                } else {
                    idx = indexOp1;
                }
            } else if (!indexOp1IsLeft && !indexOp2IsRight) {
                if (lookupRightSide) {
                    idx = indexOp1 - leftBound;
                } else {
                    idx = indexOp2;
                }
            } else {
                return false;
            }

            RelColumnOrigin columnOrigin = relMetadataQuery.getColumnOrigin(logicalTableLookup, idx);
            if (columnOrigin != null && columnOrigin.getOriginTable() == logicalTableLookup.getIndexTable()) {
                return true;
            } else {
                return false;
            }
        }
        case AND: {
            boolean result = false;
            for (RexNode rexNode : ((RexCall) condition).getOperands()) {
                result |= isBatchKeysAccessConditionRefIndexScan(rexNode, join, lookupRightSide, logicalTableLookup);
            }
            return result;
        }
        default: {
            return false;
        }
        } // end of switch other else return false;
    }

    private static ImmutableBitSet getSimpleConditionInnerRef(Join join, RexNode condition, int leftBound,
                                                              List<Pair<RexInputRef, RexInputRef>> equalPairs) {
        if (condition == null) {
            return null;
        }
        //condition will never be RexInputRef
        if (condition instanceof RexCall) {
            final RexCall currentCondition = (RexCall) condition;
            switch (currentCondition.getKind()) {
            case EQUALS: {
                if (currentCondition.getOperands().size() == 2) {
                    final RexNode operand1 = currentCondition.getOperands().get(0);
                    final RexNode operand2 = currentCondition.getOperands().get(1);
                    if (operand1 instanceof RexInputRef && operand2 instanceof RexInputRef) {
                        final int indexOp1 = ((RexInputRef) operand1).getIndex();
                        final int indexOp2 = ((RexInputRef) operand2).getIndex();
                        if ((indexOp1 < leftBound && indexOp2 < leftBound) || (indexOp1 >= leftBound
                            && indexOp2 >= leftBound)) {
                            return null;
                        } else if (indexOp1 < leftBound) {
                            if (join.getJoinType().equals(JoinRelType.RIGHT)) {
                                equalPairs.add(new Pair<>((RexInputRef) operand2, (RexInputRef) operand1));
                                return ImmutableBitSet.of(indexOp1);
                            } else {
                                equalPairs.add(new Pair<>((RexInputRef) operand1, (RexInputRef) operand2));
                                return ImmutableBitSet.of(indexOp2 - leftBound);
                            }
                        } else {
                            if (join.getJoinType().equals(JoinRelType.RIGHT)) {
                                equalPairs.add(new Pair<>((RexInputRef) operand1, (RexInputRef) operand2));
                                return ImmutableBitSet.of(indexOp2);
                            } else {
                                equalPairs.add(new Pair<>((RexInputRef) operand2, (RexInputRef) operand1));
                                return ImmutableBitSet.of(indexOp1 - leftBound);
                            }
                        }
                    }
                }
                break;
            }

            case AND: {
                ImmutableBitSet bitSet = ImmutableBitSet.of();
                for (int i = 0; i < currentCondition.getOperands().size(); i++) {
                    final ImmutableBitSet sub =
                        getSimpleConditionInnerRef(join, currentCondition.getOperands().get(i), leftBound, equalPairs);
                    if (null == sub) {
                        return null;
                    }
                    bitSet = bitSet.union(sub);
                }
                return bitSet;
            }
            }
        }

        return null;
    }

    public static List<Pair<Integer, Integer>> getSimpleCondition(Join join, RexNode condition) {
        List<Pair<Integer, Integer>> indexRelations = new ArrayList<>();

        if (condition == null) {
            return indexRelations;
        }
        //condition will never be RexInputRef
        if (condition instanceof RexCall) {
            final RexCall currentCondition = (RexCall) condition;
            switch (currentCondition.getKind()) {
            case EQUALS: {
                if (currentCondition.getOperands().size() == 2) {
                    final RexNode operand1 = currentCondition.getOperands().get(0);
                    final RexNode operand2 = currentCondition.getOperands().get(1);
                    if (operand1 instanceof RexInputRef && operand2 instanceof RexInputRef) {
                        final int indexOp1 = ((RexInputRef) operand1).getIndex();
                        final int indexOp2 = ((RexInputRef) operand2).getIndex();
                        final int leftBound = join.getLeft().getRowType().getFieldCount();
                        if ((indexOp1 < leftBound && indexOp2 < leftBound) || (indexOp1 >= leftBound
                            && indexOp2 >= leftBound)) {
                            return indexRelations;
                        }
                        Pair<Integer, Integer> pair = new Pair<>(indexOp1, indexOp2);
                        indexRelations.add(pair);

                    }
                }
                break;
            }

            case AND: {
                for (int i = 0; i < currentCondition.getOperands().size(); i++) {
                    indexRelations.addAll(getSimpleCondition(join, currentCondition.getOperands().get(i)));
                }
            }
            case OR:
                return indexRelations;
            default:
                //do nothing
            }
        }

        return indexRelations;
    }

    public static RexNode convertPkToRowExpression(Join join, RexNode condition, int leftBound,
                                                   ImmutableBitSet innerPks) {
        if (condition == null) {
            return null;
        }

        if (condition instanceof RexCall) {
            final RexCall currentCondition = (RexCall) condition;

            // Test current condition.
            final List<Pair<RexInputRef, RexInputRef>> equalPairs = new ArrayList<>();
            final ImmutableBitSet bitSet = getSimpleConditionInnerRef(join, currentCondition, leftBound, equalPairs);
            if (null == bitSet) {
                return condition; // Bad rex.
            }
            if (bitSet.contains(innerPks)) {
                // Good rex to row expression and other equal conditions.
                final RexNode left = join.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.ROW,
                    equalPairs.stream()
                        .filter(pair -> join.getJoinType().equals(JoinRelType.RIGHT) ?
                            innerPks.get(pair.getValue().getIndex()) :
                            innerPks.get(pair.getValue().getIndex() - leftBound))
                        .map(Pair::getKey)
                        .collect(Collectors.toList()));
                final RexNode right = join.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.ROW,
                    equalPairs.stream()
                        .filter(pair -> join.getJoinType().equals(JoinRelType.RIGHT) ?
                            innerPks.get(pair.getValue().getIndex()) :
                            innerPks.get(pair.getValue().getIndex() - leftBound))
                        .map(Pair::getValue)
                        .collect(Collectors.toList()));
                final RexNode rowExpr = join.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.EQUALS,
                    left, right);
                condition = rowExpr;
                for (Pair<RexInputRef, RexInputRef> pair : equalPairs) {
                    if (join.getJoinType().equals(JoinRelType.RIGHT) ? innerPks.get(pair.getValue().getIndex()) :
                        innerPks.get(pair.getValue().getIndex() - leftBound)) {
                        continue;
                    }
                    condition = join.getCluster().getRexBuilder().makeCall(
                        SqlStdOperatorTable.AND,
                        condition,
                        join.getCluster().getRexBuilder().makeCall(
                            SqlStdOperatorTable.EQUALS, pair.getKey(), pair.getValue())
                    );
                }
            }
        }
        return condition;
    }

    /**
     * Returns whether a given tree contains any un-pushable function
     *
     * @param node a RexNode tree
     */
    public static boolean containsUnPushableFunction(
        RexNode node, boolean postPlanner) {
        // test if the root of RexNode tree is IMPLICIT_CAST.
        if (RexUtil.containsRootImplicitCast(node)) {
            return true;
        }

        try {
            RexVisitor<Void> visitor =
                new RexVisitorImpl<Void>(true) {
                    public Void visitCall(RexCall call) {
                        if (UN_PUSHABLE_FUNCTION.contains(call.op)) {
                            throw new Util.FoundOne(call);
                        }
                        if (!postPlanner) {
                            if (TddlOperatorTable.ASSIGNMENT == call.getOperator()) {
                                throw new Util.FoundOne(call);
                            }
                        }
                        super.visitCall(call);
                        return null;
                    }
                };
            node.accept(visitor);
            return false;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return true;
        }
    }

    public static void calculateAndUpdateAllRexCallParams(LogicalInsert relNode, ExecutionContext executionContext) {
        final LogicalDynamicValues oldInput = relNode.getUnOptimizedLogicalDynamicValues();
        final boolean noBatchParam = executionContext.getParams() == null || (executionContext.getParams().isBatch()
            && executionContext.getParams().getBatchSize() > 1);
        final boolean noNeedGenerateParam = null == oldInput || oldInput.getTuples().size() <= 1;
        if (noBatchParam || noNeedGenerateParam) {
            return;
        }

        Map<Integer, ParameterContext> oldParams;
        if (executionContext.getParams().isBatch()) {
            //only has one batch
            oldParams = executionContext.getParams().getFirstParameter();
        } else {
            oldParams = executionContext.getParams().getCurrentParameter();
        }
        if (oldParams == null) {
            return;
        }
        Map<Integer, ParameterContext> newParams = new HashMap<>();
        final List<RexNode> onDuplicatedUpdate = relNode.getUnOptimizedDuplicateKeyUpdateList();
        final AtomicInteger paraIndex = new AtomicInteger(0);
        final AtomicInteger seqParamCount = new AtomicInteger(0);

        //1、compute/move all the param to the newParams and update the index for each column param
        Ord.zip(oldInput.getTuples()).forEach(o -> {
            final ImmutableList<RexNode> row = o.getValue();
            Ord.zip(row).forEach(c -> {
                final RexNode oldColRex = c.getValue();

                if (oldColRex instanceof RexSequenceParam) {
                    // skip
                    seqParamCount.getAndIncrement();
                    return;
                }

                final int newIndex = paraIndex.getAndIncrement();

                if (oldColRex instanceof RexDynamicParam && !(oldColRex instanceof RexCallParam)) {
                    final int oldIndex = ((RexDynamicParam) oldColRex).getIndex() + 1;
                    newParams.put(newIndex + 1,
                        PlannerUtils.changeParameterContextIndex(oldParams.get(oldIndex), newIndex + 1));
                    return;
                }

                final Object value = RexUtils.getValueFromRexNode(
                    oldColRex instanceof RexCallParam ? ((RexCallParam) oldColRex).getRexCall() : oldColRex, null,
                    executionContext);

                final ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    (paraIndex.get() + 1), value});
                newParams.put(newIndex + 1, newPc);
            });
        });
        /**
         * 2、update the index of RexDynamicParam in onDuplicatedUpdate list recursively
         * how to update them? firstly, find out the minimum RexDynamicPara and compute the offset
         * between the maximum RexDynamicParam of the new param(the last column of the last row),
         * then update the index of RexDynamicParam in onDuplicatedUpdate list recursively by plus the offset
         */
        if (GeneralUtil.isNotEmpty(onDuplicatedUpdate)) {
            FindMinDynamicParam findMinDynamicParam = new FindMinDynamicParam();
            onDuplicatedUpdate.forEach(o -> ((RexCall) o).getOperands().get(1).accept(findMinDynamicParam));
            int minPara = findMinDynamicParam.getMinDynamicParam();
            if (minPara != Integer.MAX_VALUE) {
                int offset = newParams.size() + seqParamCount.get() - minPara;
                for (int index = minPara; index < oldParams.size(); index++) {
                    newParams.put(index + 1 + offset, oldParams.get(index + 1));
                }
            }

        }

        oldParams.clear();
        oldParams.putAll(newParams);
    }

    /**
     * Convert to long value, return zero for null
     */
    public static long valueOfObject(Object autoIncValue) {
        long newValue = 0;
        if (autoIncValue == null) {
            newValue = 0;
        } else if (autoIncValue instanceof Number) { // Integer/Long/BigDecimal
            newValue = ((Number) autoIncValue).longValue();
        } else if (autoIncValue instanceof String) {
            try {
                newValue = Long.parseLong((String) autoIncValue);
            } catch (NumberFormatException e) {
                // Round up for float or double type
                newValue = (long) Math.ceil(Double.parseDouble((String) autoIncValue));
            }
        }
        return newValue;
    }

    /**
     * Convert to long value, return null for null
     */
    public static Long valueOfObject1(Object autoIncValue) {
        if (null == autoIncValue) {
            return null;
        }
        return valueOfObject(autoIncValue);
    }

    public static boolean isSeqCall(RexNode value) {
        return value instanceof RexCall && ((RexCall) value).getOperator() instanceof SqlSequenceFunction;
    }

    public static class ParamFinder extends RexVisitorImpl {

        private final List<RexDynamicParam> params = new ArrayList<>();

        protected ParamFinder() {
            super(true);
        }

        public static List<RexDynamicParam> getParams(RexNode rex) {
            List<RexDynamicParam> result = new ArrayList<>();
            if (null == rex) {
                return result;
            }

            if (rex instanceof RexDynamicParam) {
                result.add((RexDynamicParam) rex);
                return result;
            }

            final ParamFinder finder = new ParamFinder();
            rex.accept(finder);

            result.addAll(finder.getParams());
            return result;
        }

        public List<RexDynamicParam> getParams() {
            return params;
        }

        @Override
        public Object visitDynamicParam(RexDynamicParam dynamicParam) {
            params.add(dynamicParam);
            return super.visitDynamicParam(dynamicParam);
        }

        @Override
        public Object visitSubQuery(RexSubQuery subQuery) {
            return super.visitSubQuery(subQuery);
        }
    }

    public static class FindMinDynamicParam extends RexShuttle {
        private int minDynamicParam = Integer.MAX_VALUE;

        public FindMinDynamicParam() {
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam call) {
            minDynamicParam = Math.min(minDynamicParam, call.getIndex());
            if (call instanceof RexCallParam) {
                ((RexCallParam) call).getRexCall().accept(this);
            } else {
                super.visitDynamicParam(call);
            }

            return call;
        }

        public int getMinDynamicParam() {
            return minDynamicParam;
        }
    }

    public static class ReplaceDynamicParam extends RexShuttle {

        private final int offset;
        private final Map<Integer, Integer> paramMapping = new HashMap<>();

        public ReplaceDynamicParam(int offset) {
            this.offset = offset;
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam call) {
            final int curIndex = call.getIndex();
            final int newIndex = curIndex + offset;

            if (call instanceof RexCallParam) {
                RexNode rexCall = ((RexCallParam) call).getRexCall();
                if (rexCall instanceof RexCall) {
                    rexCall = super.visitCall((RexCall) rexCall);
                }
                return new RexCallParam(call.getType(), newIndex, rexCall);
            } else {
                paramMapping.put(curIndex, newIndex);
                return new RexDynamicParam(call.getType(), newIndex);
            }
        }

        public Map<Integer, Integer> getParamMapping() {
            return paramMapping;
        }
    }

    public static class ReplaceValuesCall extends RexShuttle {

        private final List<RexNode> columnParamMapping;

        public ReplaceValuesCall(List<RexNode> columnValueMapping) {
            this.columnParamMapping = columnValueMapping;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (call.getOperator() instanceof SqlValuesFunction && "VALUES"
                .equalsIgnoreCase(call.getOperator().getName())) {
                final RexInputRef ref = (RexInputRef) call.getOperands().get(0);

                return columnParamMapping.get(ref.getIndex());
            }
            return super.visitCall(call);
        }
    }

    public enum RestrictType {
        /**
         * Left only
         */
        LEFT,

        /**
         * Right only
         */
        RIGHT,

        /**
         * Both direction
         */
        BOTH,

        /**
         * No restrict
         */
        NONE
    }

    public static SqlOperator buildSemiOperator(SqlOperator so) {
        SqlOperator rs = null;
        if (so.getKind() == ALL) {
            rs = RelOptUtil.op(((SqlQuantifyOperator) so).comparisonKind.negateNullSafe(),
                null);
        } else if (so.getKind() == SOME) {
            rs = RelOptUtil.op(((SqlQuantifyOperator) so).comparisonKind, null);
        } else {
            rs = EQUALS;
        }

        return rs;
    }

    /**
     * Returns a list of expressions that project the first {@code fieldCount}
     * fields of the top input on a {@link RelBuilder}'s stack.
     */
    public static List<RexNode> fields(RelBuilder builder, int fieldCount) {
        final List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            projects.add(builder.field(i));
        }
        return projects;
    }

    public static boolean allLiteral(List<RexNode> operands) {
        return operands.stream().allMatch(rex -> rex.isA(LITERAL));
    }

    public static boolean allParam(List<RexNode> operands) {
        return operands.stream()
            .allMatch(rex -> (rex instanceof RexDynamicParam && ((RexDynamicParam) rex).getIndex() >= 0));
    }
}

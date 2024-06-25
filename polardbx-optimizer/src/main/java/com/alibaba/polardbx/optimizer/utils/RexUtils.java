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

import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeVisitor;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprVisitor;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.function.SqlSequenceFunction;
import com.alibaba.polardbx.optimizer.core.function.SqlValuesFunction;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceSequenceWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.pruning.PartClauseInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepCombine;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepOp;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLongs;
import io.airlift.slice.Slice;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSequenceParam;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.math.BigInteger;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.ALL;
import static org.apache.calcite.sql.SqlKind.IS_NOT_DISTINCT_FROM;
import static org.apache.calcite.sql.SqlKind.LITERAL;
import static org.apache.calcite.sql.SqlKind.ROW;
import static org.apache.calcite.sql.SqlKind.SOME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;

/**
 * Created by chuanqin on 17/8/7.
 */
public class RexUtils {
    public static final Logger logger = LoggerFactory.getLogger(RexUtils.class);
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

        final String schemaName = insert.getSchemaName();
        final String tableName = insert.getLogicalTableName();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);

        // Handle default expr
        if (tableMeta.hasDefaultExprColumn() && InstanceVersion.isMYSQL80()) {
            final Function<RexNode, Object> evalFunc = getEvalFunc(ec);
            for (int i = 0; i < insert.getDefaultExprColRexNodes().size(); i++) {
                ColumnMeta columnMeta = insert.getDefaultExprColMetas().get(i);
                RexNode rexNode = insert.getDefaultExprColRexNodes().get(i);
                Object value = evalFunc.apply(rexNode);
                if (value instanceof Slice) {
                    if (isBinaryReturnType(rexNode)) {
                        value = ((Slice) value).getBytes();
                    } else {
                        value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
                    }
                } else if (value instanceof ByteString) {
                    value = ((ByteString) value).getBytes();
                }
                if (!columnMeta.isNullable() && value == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        String.format("Column `%s` cannot be null", columnMeta.getName()));
                }
            }
        }

        // Handle generated column
        if (tableMeta.hasLogicalGeneratedColumn()) {
            boolean strict = SQLMode.isStrictMode(ec.getSqlModeFlags()) && !insert.isInsertIgnore();
            final LogicalDynamicValues input = RelUtils.getRelInput(insert);
            List<Integer> inputToEvalFieldsMapping = insert.getInputToEvalFieldsMapping();
            List<ColumnMeta> evalColumnMetas = insert.getEvalRowColMetas();
            CursorMeta cursorMeta = CursorMeta.build(evalColumnMetas);

            int batchSize =
                ec.getParams().isBatch() ? ec.getParams().getBatchParameters().size() : input.getTuples().size();
            List<List<Object>> rows = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                List<RexNode> tuple;
                Map<Integer, ParameterContext> param;

                if (ec.getParams().isBatch()) {
                    tuple = input.getTuples().get(0);
                    param = ec.getParams().getBatchParameters().get(i);
                } else {
                    tuple = input.getTuples().get(i);
                    param = ec.getParams().getFirstParameter();
                }

                List<Object> row = new ArrayList<>();
                for (int j = 0; j < inputToEvalFieldsMapping.size(); j++) {
                    RexNode rexNode = tuple.get(inputToEvalFieldsMapping.get(j));
                    if (rexNode instanceof RexDynamicParam) {
                        // Should all be converted here
                        final int valueIndex = ((RexDynamicParam) rexNode).getIndex() + 1;
                        row.add(param.get(valueIndex).getValue());
                    } else {
                        throw new UnsupportedOperationException(
                            "Get value from " + rexNode.getClass() + " is not supported");
                    }
                }
                rows.add(row);
            }

            // Convert row type
            List<RexNode> rexNodes = insert.getGenColRexNodes();
            int refColCnt = inputToEvalFieldsMapping.size() - rexNodes.size();
            for (int i = 0; i < rows.size(); i++) {
                List<Object> row = rows.get(i);
                List<RexNode> tuple = input.getTuples().get(ec.getParams().isBatch() ? 0 : i);
                for (int j = 0; j < refColCnt; j++) {
                    row.set(j, convertValue(row.get(j), tuple.get(inputToEvalFieldsMapping.get(j)), strict,
                        evalColumnMetas.get(j), ec));
                }
            }

            // Eval generated columns
            for (List<Object> row : rows) {
                Row r = new ArrayRow(cursorMeta, row.toArray());
                for (int i = 0; i < rexNodes.size(); i++) {
                    Object value =
                        RexUtils.convertValue(RexUtils.getValueFromRexNode(rexNodes.get(i), r, ec), rexNodes.get(i),
                            strict, r.getParentCursorMeta().getColumnMeta(refColCnt + i), ec);
                    r.setObject(i + refColCnt, value);
                    row.set(i + refColCnt, value);
                }
            }

            // Finally we update Params
            for (int i = 0; i < batchSize; i++) {
                List<Object> row = rows.get(i);
                List<RexNode> tuple;
                Map<Integer, ParameterContext> param;

                if (ec.getParams().isBatch()) {
                    tuple = input.getTuples().get(0);
                    param = ec.getParams().getBatchParameters().get(i);
                } else {
                    tuple = input.getTuples().get(i);
                    param = ec.getParams().getFirstParameter();
                }

                for (int j = refColCnt; j < inputToEvalFieldsMapping.size(); j++) {
                    RexDynamicParam rexNode = (RexDynamicParam) tuple.get(inputToEvalFieldsMapping.get(j));
                    final int paramIndex = rexNode.getIndex();
                    Object value = row.get(j);
                    if (value instanceof Slice) {
                        value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
                    } else if (value instanceof ByteString) {
                        value = ((ByteString) value).getBytes();
                    }
                    final ParameterContext newPc =
                        new ParameterContext(ParameterMethod.setObject1, new Object[] {paramIndex + 1, value});
                    param.put(paramIndex + 1, newPc);
                }
            }
        }

        // Handle sequence
        final int seqColumnIndex = insert.getSeqColumnIndex();
        final boolean usingSequence = seqColumnIndex >= 0;

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

    public static Object convertValue(Object value, DataType dataType, boolean strict, ColumnMeta columnMeta,
                                      ExecutionContext ec) {
        if (value == null) {
            if (!columnMeta.isNullable()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("Can not store null value in column `%s`", columnMeta.getName()));
            }
            return null;
        }

        // If data is out of range, DN will throw error, so we ignore data truncation here
        PartFieldAccessType accessType = strict ? PartFieldAccessType.DDL_EXECUTION : PartFieldAccessType.DML_PRUNING;
        PartitionField partitionField =
            PartitionPrunerUtils.buildPartField(value, dataType, columnMeta.getDataType(), null, ec, accessType);
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(ec);
        switch (partitionField.dataType().fieldType()) {
        case MYSQL_TYPE_LONGLONG:
            if (partitionField.dataType().isUnsigned()) {
                return new BigInteger(UnsignedLongs.toString(partitionField.longValue()));
            } else {
                return partitionField.longValue();
            }
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_TINY:
            return partitionField.longValue(sessionProperties);
        case MYSQL_TYPE_NEWDECIMAL:
            return Decimal.fromString(partitionField.stringValue().toStringUtf8());
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
            return partitionField.stringValue(sessionProperties).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
            MySQLTimeVal mySQLTimeVal = partitionField.timestampValue(0, sessionProperties);
            boolean zeroTimestamp = mySQLTimeVal.getSeconds() == 0;
            MysqlDateTime mysqlDateTime = zeroTimestamp ? MysqlDateTime.zeroDateTime() :
                MySQLTimeConverter.convertTimestampToDatetime(mySQLTimeVal, sessionProperties.getTimezone());
            mysqlDateTime.setSqlType(Types.TIMESTAMP);
            return new OriginalTimestamp(mysqlDateTime);
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
            return new OriginalTimestamp(
                partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, sessionProperties));
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            return new OriginalDate(
                partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, sessionProperties));
        default:
            throw new UnsupportedOperationException("Value cast is not supported");
        }
    }

    public static Object convertValue(Object value, RexNode rex, boolean strict, ColumnMeta columnMeta,
                                      ExecutionContext ec) {
        return convertValue(value, getTypeFromRexNode(rex, value), strict, columnMeta, ec);
    }

    public static DataType getTypeFromRexNode(RexNode rex, Object value) {
        DataType type = DataTypeUtil.calciteToDrdsType(rex.getType());
        ExprContextProvider exprCxtProvider = new ExprContextProvider();
        IExpression evalFuncExec = RexUtils.getEvalFuncExec(rex, exprCxtProvider);
        if (evalFuncExec instanceof DynamicParamExpression) {
            type = DataTypeUtil.getTypeOfObject(value);
        }
        return type;
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

    public static boolean paramExists(int paramIndex, ExecutionContext ec) {
        final Parameters params = ec.getParams();
        final boolean isBatch = params.isBatch();
        if (isBatch) {
            final List<Map<Integer, ParameterContext>> batchParams = params.getBatchParameters();

            return batchParams.stream().anyMatch(m -> m.containsKey(paramIndex));
        } else {
            return params.getCurrentParameter().containsKey(paramIndex);
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
                RexNode rexNode = getRexCall.apply(callParam);
                Object value = evalFunc.apply(rexNode);
                if (value instanceof Slice) {
                    if (isBinaryReturnType(rexNode)) {
                        value = ((Slice) value).getBytes();
                    } else {
                        value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
                    }
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
            RexNode rexNode = getRexCall.apply(callParam);
            Object value = evalFunc.apply(rexNode);
            if (value instanceof Slice) {
                if (isBinaryReturnType(rexNode)) {
                    value = ((Slice) value).getBytes();
                } else {
                    value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
                }
            } else if (value instanceof ByteString) {
                value = ((ByteString) value).getBytes();
            }
            final ParameterContext newPc = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                paramIndex + 1, value});

            param.put(paramIndex + 1, newPc);
        }
    }

    public static boolean isBinaryReturnType(RexNode rexNode) {
        try {
            if ("BINARY".equalsIgnoreCase(rexNode.getType().getCharset().name())) {
                return true;
            }
        } catch (Throwable e) {
            // Ignore
        }
        return false;
    }

    public static Collection<RexDynamicParam> getDynamicParams(RexNode target) {
        Collection<RexDynamicParam> collection = Lists.newLinkedList();
        final RexShuttle visitor = new RexShuttle() {
            @Override
            public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                collection.add(dynamicParam);
                return dynamicParam;
            }
        };
        target.accept(visitor);
        return collection;
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
            final boolean isNewSeq = (SequenceAttribute.Type.NEW == visitor.getSeqType());
            final boolean isSimpleSeq = (SequenceAttribute.Type.SIMPLE == visitor.getSeqType());

            if (seqSize == expectedImplicitSeqSize || (!isNewSeq && !isSimpleSeq)) {
                // For NEW/SIMPLE sequence, If not all auto increment column are using implicit value,
                // we cannot get sequence value in batch, because NEW/SIMPLE might be updated by insert
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
                if (isBinaryReturnType(rexNode)) {
                    value = ((Slice) value).getBytes();
                } else {
                    value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
                }
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
                if (isBinaryReturnType(rexNode)) {
                    result = ((Slice) value).getBytes();
                } else {
                    result = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
                }
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
    public static List<Object> buildRowValue(List<RexNode> rexRow, Row row, Map<Integer, ParameterContext> param,
                                             ExecutionContext ec) {
        final ExecutionContext tmpEc = ec.copy(new Parameters(param));

        final List<Object> result = new ArrayList<>();
        for (RexNode rex : rexRow) {
            try {
                final Object value = getValueFromRexNode(rex, row, tmpEc);
                result.add(value);
            } catch (Exception e) {
                throw new UnsupportedOperationException(
                    "Get value from " + rex.getClass() + " is not supported");
            }
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
        case IS_NOT_DISTINCT_FROM:
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

            // For "<=>", nullable data type is not allowed for BKA join
            if (currentCondition.getKind() == IS_NOT_DISTINCT_FROM &&
                (relDataTypeLeft.isNullable() || relDataTypeRight.isNullable())) {
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
        if (node == null) {
            return false;
        }
        // test if the root of RexNode tree is IMPLICIT_CAST.
        if (RexUtil.containsRootImplicitCast(node)) {
            return true;
        }

        try {
            RexVisitor<Void> visitor =
                new RexVisitorImpl<Void>(true) {
                    public Void visitCall(RexCall call) {
                        if (!call.op.canPushDown()) {
                            throw new Util.FoundOne(call);
                        }
                        if (call instanceof RexOver) {
                            LoggerFactory.getLogger(Window.class).error("window should be expand before");
                            throw new Util.FoundOne(call);
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

        final String schemaName = relNode.getSchemaName();
        final String tableName = relNode.getLogicalTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

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

    public static void updateParam(Comparative comparative, ExecutionContext executionContext,
                                   Predicate<RexCallParam> before, Consumer<RexCallParam> after) {
        RexDynamicParamComparativeVisitor.analyze(
            comparative,
            (rexCallParam) -> {
                if (before.test(rexCallParam)) {
                    evalRexCallParam(rexCallParam, executionContext, RexCallParam::getRexCall);
                    after.accept(rexCallParam);
                }

                // Visit all RexCallParam
                return false;
            },
            RexCallParam.class);
    }

    public static void updateParam(PartitionPruneStep pruneStep, ExecutionContext executionContext,
                                   Predicate<RexCallParam> before, Consumer<RexCallParam> after) {
        RexNodePartitionPruneStepVisitor.analyze(
            pruneStep,
            (rexCallParam) -> {
                if (before.test(rexCallParam)) {
                    evalRexCallParam(rexCallParam, executionContext, RexCallParam::getRexCall);
                    after.accept(rexCallParam);
                }

                // Visit all RexCallParam
                return false;
            },
            RexCallParam.class
        );
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

    public static boolean isRowDynamic(RexNode rexNode) {
        if (rexNode == null) {
            return false;
        }
        if (!(rexNode instanceof RexCall) || rexNode.getKind() != ROW) {
            return false;
        }

        if (((RexCall) rexNode).getOperands() == null || ((RexCall) rexNode).getOperands().size() == 0) {
            return false;
        }

        if (((RexCall) rexNode).getOperands().size() != 1) {
            return false;
        }

        RexNode rex = ((RexCall) rexNode).getOperands().get(0);
        if (!(rex instanceof RexDynamicParam && ((RexDynamicParam) rex).literal())) {
            return false;
        }

        if (((RexDynamicParam) rex).getIndex() < 0) {
            return false;
        }
        return true;
    }

    public static String buildRexExprStringWithContext(RexNode expr, ExecutionContext ec) {
        return null;
    }

    public static class ColumnRefFinder extends RexVisitorImpl<Boolean> {

        public ColumnRefFinder() {
            super(true);
        }

        public boolean analyze(RexNode rex) {
            return Boolean.TRUE.equals(rex.accept(this));
        }

        @Override
        public Boolean visitInputRef(RexInputRef inputRef) {
            return true;
        }

        @Override
        public Boolean visitCall(RexCall call) {
            if ("VALUES".equalsIgnoreCase(call.getOperator().getName())) {
                return false;
            }

            Boolean r = null;
            for (RexNode operand : call.operands) {
                r = operand.accept(this);

                if (Boolean.TRUE.equals(r)) {
                    return true;
                }
            }
            return r;
        }
    }

    public static class RexDynamicParamShuttle extends RexShuttle {

        private final Predicate<RexDynamicParam> predicate;
        private RelShuttle relShuttle;
        private boolean found = false;

        public RexDynamicParamShuttle(Predicate<RexDynamicParam> predicate) {
            this.predicate = predicate;
        }

        /**
         * @return true if RexNode contains at least one RexCallParam
         */
        public static boolean analyze(RexNode rex, Predicate<RexDynamicParam> predicate) {
            final RexDynamicParamShuttle finder = new RexDynamicParamShuttle(predicate);

            rex.accept(finder);
            return finder.found;
        }

        /**
         * @return true if RelNode contains at least one RexCallParam
         */
        public static boolean analyze(RelNode rel, Predicate<RexDynamicParam> predicate) {
            final RexDynamicParamShuttle rexShuttle = new RexDynamicParamShuttle(predicate);
            final RexNodeRelShuttle relShuttle = new RexNodeRelShuttle(rexShuttle);
            rexShuttle.setRelShuttle(relShuttle);

            rel.accept(relShuttle);
            return rexShuttle.found;
        }

        public RexDynamicParamShuttle setRelShuttle(RelShuttle relShuttle) {
            this.relShuttle = relShuttle;
            return this;
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            if (predicate.test(dynamicParam)) {
                found = true;
                return dynamicParam;
            }
            return super.visitDynamicParam(dynamicParam);
        }

        @Override
        public RexNode visitCall(RexCall call) {
            for (RexNode operand : call.operands) {
                operand.accept(this);

                if (found) {
                    break;
                }
            }
            return call;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            if (found) {
                return subQuery;
            }

            if (null != relShuttle) {
                final RelNode newRel = subQuery.rel.accept(relShuttle);
                if (newRel != subQuery.rel) {
                    subQuery = subQuery.clone(newRel);
                }
            }
            return super.visitSubQuery(subQuery);
        }
    }

    /**
     * Traverse all RexDynamicParam in RexNode
     */
    public static class RexDynamicParamVisitor extends RexVisitorImpl<Boolean> {

        private final Predicate<RexDynamicParam> stopper;

        public RexDynamicParamVisitor(Predicate<RexDynamicParam> stopper) {
            super(true);
            this.stopper = stopper;
        }

        /**
         * @return true if stopper returns true
         */
        public static boolean analyze(RexNode rex, Predicate<RexDynamicParam> stopper) {
            final RexDynamicParamVisitor finder = new RexDynamicParamVisitor(stopper);
            return Boolean.TRUE.equals(rex.accept(finder));
        }

        @Override
        public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
            return stopper.test(dynamicParam);
        }

        @Override
        public Boolean visitCall(RexCall call) {
            Boolean stopped = null;
            for (RexNode operand : call.operands) {
                stopped = operand.accept(this);

                if (Boolean.TRUE.equals(stopped)) {
                    return true;
                }
            }
            return stopped;
        }
    }

    /**
     * Visit all RexNode int PartitionPruneStep
     *
     * @param <T> actual subclass of RexNode that we are looking for
     */
    public static class RexNodePartitionPruneStepVisitor<T extends RexNode> {

        private final AtomicBoolean found = new AtomicBoolean(false);
        private final Predicate<T> stopper;
        private final Class<T> paramType;

        public static <R extends RexNode> boolean analyze(PartitionPruneStep partitionPruneStep,
                                                          Predicate<R> predicate,
                                                          Class<R> paramType) {
            final RexNodePartitionPruneStepVisitor<R> visitor =
                new RexNodePartitionPruneStepVisitor<>(predicate, paramType);
            visitor.visit(partitionPruneStep);
            return visitor.found.get();
        }

        public RexNodePartitionPruneStepVisitor(Predicate<T> stopper, Class<T> paramType) {
            this.stopper = stopper;
            this.paramType = paramType;
        }

        public boolean visit(PartitionPruneStep partitionPruneStep) {
            if (partitionPruneStep instanceof PartitionPruneStepOp) {
                return visit((PartitionPruneStepOp) partitionPruneStep);
            } else if (partitionPruneStep instanceof PartitionPruneStepCombine) {
                return ((PartitionPruneStepCombine) partitionPruneStep)
                    .getSubSteps()
                    .stream()
                    .anyMatch(p -> visit((PartitionPruneStepOp) p));
            } else {
                return false;
            }
        }

        @SuppressWarnings("unchecked")
        public boolean visit(PartitionPruneStepOp partitionPruneStepOp) {
            try {
                return partitionPruneStepOp
                    .getPartColToPredExprInfo()
                    .getPartPredExprList()
                    .stream()
                    .map(PartClauseInfo::getOriginalPredicate)
                    .anyMatch(rex -> RexDynamicParamVisitor.analyze(
                        rex,
                        (r) -> {
                            if (paramType.isAssignableFrom(r.getClass())) {
                                found.compareAndSet(false, true);
                                return stopper.test((T) r);
                            }
                            return false;
                        })
                    );
            } catch (Exception e) {
                logger.error("Visit PartitionPruneStepOp failed!", e);
            }

            return false;
        }
    }

    public static class ReplaceScalarFunctionWithRexCallParamVisitor extends RexShuttle {
        private final AtomicInteger currentParamIndex;
        private final Deque<Boolean> topStack = new ArrayDeque<>();
        private final Deque<Boolean> computableStack = new ArrayDeque<>();

        private Predicate<RexNode> computable;
        private Consumer<RexNode> trigger;

        private RexNodeRelShuttle relShuttle;

        public ReplaceScalarFunctionWithRexCallParamVisitor(AtomicInteger currentParamIndex,
                                                            Predicate<RexNode> computable,
                                                            Consumer<RexNode> trigger) {
            this.currentParamIndex = currentParamIndex;
            this.topStack.push(true);
            this.computable = computable;
            this.trigger = trigger;
        }

        public ReplaceScalarFunctionWithRexCallParamVisitor setRelShuttle(RexNodeRelShuttle relShuttle) {
            this.relShuttle = relShuttle;
            return this;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            if (null != relShuttle) {
                final RelNode newRel = subQuery.rel.accept(relShuttle);
                if (newRel != subQuery.rel) {
                    subQuery = subQuery.clone(newRel);
                }
            }
            return super.visitSubQuery(subQuery);
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            RexNode visited = null;

            boolean outterOperatorIsScalarFunction = Optional.ofNullable(this.computableStack.peek()).orElse(false);
            if (!outterOperatorIsScalarFunction) {
                outterOperatorIsScalarFunction = this.computable.test(call);
            }

            if (outterOperatorIsScalarFunction) {
                // Check operands
                this.topStack.push(false);
                try {
                    visited = super.visitCall(call);
                } finally {
                    this.topStack.pop();
                }

                final boolean replaceCall = Optional.ofNullable(topStack.peek()).orElse(false);
                if (replaceCall) {
                    trigger.accept(call);
                    // Replace top RexNode with RexCallParam
                    return new RexCallParam(call.getType(), currentParamIndex.incrementAndGet(), call);
                }

                if (call.getOperator() == TddlOperatorTable.NEXTVAL) {
                    // If it's a nested seq.nextVal, we can't compute.
                    throw new TddlRuntimeException(ErrorCode.ERR_FUNCTION, "'" + call + "'");
                }

                return visited;
            } else {
                // Operands of logical operator (e.g. OR) might be computable,
                // and we still can use them for partitioning
                this.computableStack.push(false);
                try {
                    return super.visitCall(call);
                } finally {
                    this.computableStack.pop();
                }
            }
        }
    }

    /**
     * Traverse RelNode, visit each RelNode with {@link RexNodeRelShuttle#rexShuttle}
     */
    public static class RexNodeRelShuttle extends RelShuttleImpl {

        private final RexShuttle rexShuttle;

        public RexNodeRelShuttle(RexShuttle rexShuttle) {
            this.rexShuttle = rexShuttle;
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            return super.visit(aggregate).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalMatch match) {
            return super.visit(match).accept(rexShuttle);
        }

        @Override
        public RelNode visit(TableScan scan) {
            return super.visit(scan).accept(rexShuttle);
        }

        @Override
        public RelNode visit(TableFunctionScan scan) {
            return super.visit(scan).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalValues values) {
            return super.visit(values).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            return super.visit(filter).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            return super.visit(project).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalOutFile outFile) {
            return super.visit(outFile).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            return super.visit(join).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalSemiJoin semiJoin) {
            return super.visit(semiJoin).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            return super.visit(correlate).accept(rexShuttle);
        }

        @Override
        public RelNode visit(MultiJoin mjoin) {
            return super.visit(mjoin).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalUnion union) {
            return super.visit(union).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalIntersect intersect) {
            return super.visit(intersect).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalMinus minus) {
            return super.visit(minus).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalSort sort) {
            return super.visit(sort).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalExchange exchange) {
            return super.visit(exchange).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalTableLookup tableLookup) {
            return super.visit(tableLookup).accept(rexShuttle);
        }

        @Override
        public RelNode visit(LogicalExpand expand) {
            return super.visit(expand).accept(rexShuttle);
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof HepRelVertex) {
                RelNode relNode = ((HepRelVertex) other).getCurrentRel();
                return relNode.accept(this);
            }
            if (other instanceof RelSubset) {
                RelNode relNode = Util.first(((RelSubset) other).getBest(), ((RelSubset) other).getOriginal());
                return relNode.accept(this);
            }

            if (other instanceof LogicalJoin) {
                return visit((LogicalJoin) other);
            }
            if (other instanceof LogicalAggregate) {
                return visit((LogicalAggregate) other);
            }
            if (other instanceof LogicalProject) {
                return visit((LogicalProject) other);
            }
            if (other instanceof TableScan) {
                return visit((TableScan) other);
            }
            if (other instanceof TableFunctionScan) {
                return visit((TableFunctionScan) other);
            }
            if (other instanceof LogicalValues) {
                return visit((LogicalValues) other);
            }
            if (other instanceof LogicalFilter) {
                return visit((LogicalFilter) other);
            }
            if (other instanceof LogicalCorrelate) {
                return visit((LogicalCorrelate) other);
            }
            if (other instanceof LogicalUnion) {
                return visit((LogicalUnion) other);
            }
            if (other instanceof LogicalIntersect) {
                return visit((LogicalIntersect) other);
            }
            if (other instanceof LogicalMinus) {
                return visit((LogicalMinus) other);
            }
            if (other instanceof LogicalMatch) {
                return visit((LogicalMatch) other);
            }
            if (other instanceof LogicalSort) {
                return visit((LogicalSort) other);
            }
            if (other instanceof LogicalExchange) {
                return visit((LogicalExchange) other);
            }
            if (other instanceof LogicalTableLookup) {
                return visit((LogicalTableLookup) other);
            }
            if (other instanceof LogicalExpand) {
                return visit((LogicalExpand) other);
            }
            if (other instanceof BroadcastTableModify) {
                return other;
            }
            if (other instanceof VirtualView) {
                return other;
            }
            if (other instanceof LogicalOutFile) {
                return visit((LogicalOutFile) other);
            }

            return super.visit(other).accept(rexShuttle);
        }
    }

    /**
     * Visit all {@link RexDynamicParam} in a {@link Comparative}
     */
    public static class RexDynamicParamComparativeVisitor<T extends RexDynamicParam> extends ComparativeVisitor {

        private final AtomicBoolean found = new AtomicBoolean(false);
        /**
         * Process RexDynamicParam as needed in this Predicate,
         * return true when your visit is finished
         */
        private final Predicate<T> stopper;
        /**
         * Acceptable param type of {@link RexDynamicParamComparativeVisitor#stopper}
         */
        private final Class<T> paramType;

        public RexDynamicParamComparativeVisitor(Predicate<T> stopper, Class<T> paramType) {
            this.stopper = stopper;
            this.paramType = paramType;
        }

        /**
         * @return true if {@link RexDynamicParamComparativeVisitor#paramType} is found
         */
        public static <R extends RexDynamicParam> boolean analyze(Comparative comparative, Predicate<R> stopper,
                                                                  Class<R> paramType) {
            final RexDynamicParamComparativeVisitor<R> visitor =
                new RexDynamicParamComparativeVisitor<>(stopper, paramType);
            visitor.go(comparative);
            return visitor.found.get();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void visit(Comparative comp, int ordinal, Comparative parent) {
            if (comp.getValue() instanceof RexNode) {
                RexDynamicParamVisitor.analyze(
                    (RexNode) comp.getValue(),
                    (r) -> {
                        if (paramType.isAssignableFrom(r.getClass())) {
                            found.compareAndSet(false, true);
                            return stopper.test((T) r);
                        }

                        // find out all RexDynamicParam
                        return false;
                    });
            }

            super.visit(comp, ordinal, parent);
        }
    }

    /**
     * get all {@link RexDynamicParam} in a {@link Comparative}
     */
    public static class RexDynamicParamFromComparativeVisitor<T extends RexDynamicParam> extends ComparativeVisitor {

        private final List<RexDynamicParam> rexDynamicParams = Lists.newArrayList();

        public RexDynamicParamFromComparativeVisitor() {
        }

        @SuppressWarnings("unchecked")
        @Override
        public void visit(Comparative comp, int ordinal, Comparative parent) {
            if (comp.getValue() instanceof RexNode) {
                RexUtils.getDynamicParams((RexNode) comp.getValue()).forEach(rexDynamicParams::add);
            }

            super.visit(comp, ordinal, parent);
        }

        public List<RexDynamicParam> getRexDynamicParams() {
            return rexDynamicParams;
        }
    }
}

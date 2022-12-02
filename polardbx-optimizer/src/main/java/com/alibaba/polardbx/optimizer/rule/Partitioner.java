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

package com.alibaba.polardbx.optimizer.rule;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeBaseList;
import com.alibaba.polardbx.common.model.sqljep.ComparativeMapChoicer;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.model.sqljep.DynamicComparative;
import com.alibaba.polardbx.common.model.sqljep.ExtComparative;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.exception.RouteCompareDiffException;
import com.alibaba.polardbx.rule.model.MatcherResult;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.MapUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.IS_NULL;

/**
 * @author chenmo.cm
 */
public class Partitioner extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(Partitioner.class);

    private final TddlRule tddlRule;
    private final OptimizerContext context;
    private String schemaName;

    private InternalTimeZone shardRouterTimeZone;

    /**
     * Allow const expression as sharding condition?
     */
    private boolean enableConstExpr = false;

    private static final Map<SqlKind, Integer> COMPARATIVE_MAP = new HashMap<>(8);

    static {
        COMPARATIVE_MAP.put(SqlKind.EQUALS, Comparative.Equivalent);
        COMPARATIVE_MAP.put(SqlKind.NOT_EQUALS, Comparative.NotEquivalent);
        COMPARATIVE_MAP.put(SqlKind.GREATER_THAN, Comparative.GreaterThan);
        COMPARATIVE_MAP.put(SqlKind.GREATER_THAN_OR_EQUAL, Comparative.GreaterThanOrEqual);
        COMPARATIVE_MAP.put(SqlKind.LESS_THAN, Comparative.LessThan);
        COMPARATIVE_MAP.put(SqlKind.LESS_THAN_OR_EQUAL, Comparative.LessThanOrEqual);
    }

    public Partitioner(TddlRule tddlRule, OptimizerContext context) {
        this.tddlRule = tddlRule;
        this.context = context;
        this.schemaName = context.getSchemaName();
    }

    public Collection<TableRule> getTableRules() {
        return tddlRule.getTables();
    }

    @Override
    protected void doInit() {
        if (tddlRule != null && !tddlRule.isInited()) {
            tddlRule.init();
        }
    }

    /**
     * 为了可以让CostBaedOptimizer可以订阅tddlconfig的改变所以暴露
     */
    public TddlRule getTddlRule() {
        return tddlRule;
    }

    @Override
    protected void doDestroy() {
        if (tddlRule != null && tddlRule.isInited()) {
            tddlRule.destroy();
        }
    }

    public List<TargetDB> shard(String logicTable, boolean isWrite, boolean forceAllowFullTableScan,
                                Map<String, Comparative> comparatives, Map<Integer, ParameterContext> param,
                                Map<String, Object> calcParams, ExecutionContext ec) {
        List<TargetDB> targetDbs =
            shard(logicTable, isWrite, forceAllowFullTableScan, null, comparatives, param, calcParams, ec);
        if (targetDbs == null || targetDbs.isEmpty()) {
            throw new IllegalArgumentException("can't find target db. table is " + logicTable);
        }
        return targetDbs;
    }

    /**
     *
     */
    protected List<TargetDB> shard(final String logicTable, boolean isWrite, boolean forceAllowFullTableScan,
                                   List<TableRule> ruleList, final Map<String, Comparative> comparatives,
                                   final Map<Integer, ParameterContext> param, Map<String, Object> calcParams,
                                   ExecutionContext ec) {
        MatcherResult result;
        /**
         * column name from tddl rule could be upper case
         */
        final Map<String, DataType> dataTypeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final SchemaManager schemaManager = ec.getSchemaManager(context.getSchemaName());
        if (!MapUtils.isEmpty(comparatives)) {
            dataTypeMap.putAll(PlannerUtils.buildDataType(ImmutableList.copyOf(comparatives.keySet()),
                schemaManager.getTable(logicTable)));
        }

        Map<String, DataType> tmpDataTypeMap =
            (Map<String, DataType>) calcParams.get(CalcParamsAttribute.SHARD_DATATYPE_MAP);
        if (tmpDataTypeMap != null && dataTypeMap.isEmpty()) {
            dataTypeMap.putAll(tmpDataTypeMap);
        }

        calcParams.put(CalcParamsAttribute.SHARD_PARAMS, param);
        final Object o = calcParams.get(CalcParamsAttribute.COM_DB_TB);
        final Map<String, DataType> dataTypeMapFull = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (o != null) {
            final Map<String, Comparative> stringComparativeMap = (Map<String, Comparative>) ((Map) o).get(logicTable);
            if (!MapUtils.isEmpty(stringComparativeMap)) {
                dataTypeMapFull.putAll(PlannerUtils.buildDataType(ImmutableList.copyOf(stringComparativeMap.keySet()),
                    schemaManager.getTable(logicTable)));
            }
        }
        calcParams.put(CalcParamsAttribute.SHARD_DATATYPE_MAP, dataTypeMapFull);

        TableRule tbRule = context.getRuleManager().getTableRule(logicTable);
        calcParams.remove(CalcParamsAttribute.DB_SHARD_KEY_SET);
        calcParams.remove(CalcParamsAttribute.TB_SHARD_KEY_SET);

        ComparativeMapChoicer c = new ComparativeMapChoicer() {

            @Override
            public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> colNameSet) {
                Map<String, Comparative> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (String str : colNameSet) {
                    Comparative c = getColumnComparative(arguments, str);
                    if (c != null) {
                        map.put(str, c);
                    }

                }
                return map;
            }

            @Override
            public Comparative getColumnComparative(List<Object> arguments, String colName) {
                if (!((Map) o).containsKey(logicTable)) {
                    return null;
                }
                return getComparativeByFetcher(tbRule, (Map<String, Comparative>) ((Map) o).get(logicTable), colName, param,
                    dataTypeMapFull, calcParams);
            }
        };
        calcParams.put(CalcParamsAttribute.SHARD_CHOISER, c);
        try {
            result = tddlRule.routeMverAndCompare(!isWrite, logicTable, new ComparativeMapChoicer() {

                @Override
                public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> colNameSet) {
                    Map<String, Comparative> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for (String str : colNameSet) {
                        Comparative c = getColumnComparative(arguments, str);
                        if (c != null) {
                            map.put(str, c);
                        }
                    }
                    return map;
                }

                @Override
                public Comparative getColumnComparative(List<Object> arguments, String colName) {
                    return getComparativeByFetcher(tbRule, comparatives, colName, param, dataTypeMap, calcParams);
                }
            }, Lists.newArrayList(), forceAllowFullTableScan, ruleList, calcParams);
        } catch (RouteCompareDiffException e) {
            throw GeneralUtil.nestedException(e);
        }

        return result.getCalculationResult();
    }

    public Comparative getComparativeByFetcher(TableRule tableRule, Map<String, Comparative> comparatives, String colName,
                                      Map<Integer, ParameterContext> param, Map<String, DataType> dataTypeMap,
                                      Map<String, Object> calcParams) {
        ComparativeFetcher comparativeFetcher = new ComparativeFetcher(this.shardRouterTimeZone);
        return comparativeFetcher.getComparativeAndReplaceParams(tableRule, comparatives, colName, param, dataTypeMap, calcParams);
    }

//    public Comparative getComparative(TableRule tableRule,
//                                      Map<String, Comparative> comparatives,
//                                      String colName,
//                                      Map<Integer, ParameterContext> param,
//                                      Map<String, DataType> dataTypeMap,
//                                      Map<String, Object> calcParams) {
//
//        /**
//         *  filter中col与val的动态参数idx的map
//         */
//        Map<String, Integer> condColValIdxMap =
//            (Map<String, Integer>) calcParams.get(CalcParamsAttribute.COND_COL_IDX_MAP);
//
//        if (condColValIdxMap != null) {
//            // 如果指定了 filter中col与val的动态参数idx的映射关系，直接使用
//            // 通常简单的等值点查会有传这个参数
//
//            /**
//             * 没有参数
//             */
//            if (MapUtils.isEmpty(param)) {
//                return null;
//            }
//
//            int index = condColValIdxMap.get(colName);
//            Object paramVal = param.get(index + 1).getValue();
//            DataType dataType = dataTypeMap.get(colName);
//            // Only TIMESTAMP/DATETIME type need correct timezone.
//            if (dataType instanceof TimestampType) {
//                paramVal = correctTimeZoneForParamVal(tableRule, colName, dataType, calcParams, paramVal);
//            }
//            return new Comparative(Comparative.Equivalent, dataType.convertJavaFrom(paramVal));
//        }
//
//        if (MapUtils.isEmpty(comparatives)) {
//            return null;
//        }
//
//        /**
//         * 没有参数
//         */
//        if (MapUtils.isEmpty(param)) {
//            Comparative c = findComparativeIgnoreCase(comparatives, colName);
//            if (c == null) {
//                return null;
//            }
//            c = c.clone();
//            Object paramVal = c.getValue();
//            if (paramVal instanceof RexDynamicParam) {
//                throw new IllegalArgumentException(
//                    "RexDynamicParam should not be enter here, might cause by params missing.");
//            }
//            DataType dataType = dataTypeMap.get(colName);
//            // Only TIMESTAMP/DATETIME type need correct timezone.
//            if (dataType instanceof TimestampType) {
//                paramVal = correctTimeZoneForParamVal(tableRule, colName, dataType, calcParams, paramVal);
//            }
//            c.setValue(dataType.convertJavaFrom(paramVal));
//
//            return c;
//        } else {
//            /**
//             * 用实际值替换参数
//             */
//            final Comparative c = findComparativeIgnoreCase(comparatives, colName);
//            if (c != null) {
//                Comparative clone = (Comparative) c.clone();
//                replaceParamWithValue(tableRule, colName, clone, param, dataTypeMap, colName, calcParams);
//                return clone;
//            } else {
//                return null;
//            }
//        }
//    }
//
//    private Object correctTimeZoneForParamVal(TableRule tableRule, String colName,
//                                              DataType dataType,
//                                              Map<String, Object> calcParams,
//                                              Object paramVal) {
//        InternalTimeZone connTimeZoneInfo = (InternalTimeZone) calcParams.get(CalcParamsAttribute.CONN_TIME_ZONE);
//        TimeZone connTimeZone = null;
//        if (connTimeZoneInfo != null) {
//            connTimeZone = connTimeZoneInfo.getTimeZone();
//        }
//
//        TimeZoneCorrector timeZoneCorrector = new TimeZoneCorrector(shardRouterTimeZone, tableRule, connTimeZone);
//        paramVal = timeZoneCorrector.correctTimeZoneIfNeed(colName, dataType, paramVal, calcParams);
//        Object finalParamVal = dataType.convertJavaFrom(paramVal);
//        return finalParamVal;
//    }
//
//    private static Comparative findComparativeIgnoreCase(Map<String, Comparative> comparatives, String colName) {
//        for (Entry<String, Comparative> entry : comparatives.entrySet()) {
//            if (entry.getKey().equalsIgnoreCase(colName)) {
//                return entry.getValue();
//            }
//        }
//        return null;
//    }
//
//    private Comparative replaceParamWithValue(TableRule tableRule, String colName,
//                                       Comparative comparative,
//                                       Map<Integer, ParameterContext> param,
//                                       DataType dataType, Map<String, Object> calcParams) {
//        Object v = comparative.getValue();
//        Object paramVal = null;
//        Comparative newComp = comparative;
//        boolean isSucc = true;
//        if (v instanceof RexDynamicParam) {
//            int index = ((RexDynamicParam) v).getIndex();
//            if (index != PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX && index != PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX) {
//                paramVal = param.get(index + 1).getValue();
//            } else if ( index == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX ) {
//                ExecutionContext ec = (ExecutionContext) calcParams.get(CalcParamsAttribute.EXECUTION_CONTEXT);
//                Object[] val = new Object[1];
//                if (SubQueryDynamicParamUtils.fetchScalarSubQueryConstantValue(v, ec.getScalarSubqueryCtxMap(), true, val)) {
//                    paramVal = val[0];
//                } else {
//                    isSucc = false;
//                }
//            } else {
//                return comparative;
//            }
//
//            if (!isSucc) {
//                return null;
//            }
//
//        } else if (v instanceof RexNode) {
//            // Construct a temporary ExecutionContext to wrap parameters
//            ExecutionContext context = new ExecutionContext();
//            Parameters parameters = new Parameters();
//            parameters.setParams(param);
//            context.setParams(parameters);
//            context.setTimeZone(shardRouterTimeZone);
//
//            // Eval with null row (must be constant here)
//            IExpression expression = RexUtils.buildRexNode((RexNode) v, context);
//            paramVal = expression.eval(null);
//        } else {
//            /*
//             *  comparative.getValue() may be a Java Object ( such String/Date/Timestamp, ....)
//             *
//             *  e.g.
//             *  for the insert sql (  check_date is timestamp, check_date is shard key ):
//             *  insert into tb (id, check_date, is_freeze) values (1, '2019-12-12 23:00',1)
//             *  ,
//             *  this sql will be constructed a comparative of check_date='2019-12-12 23:00',
//             *  not a comparative of check_date=?.
//             *
//             *  so comparative.getValue() maybe occur a non-RexDynamicParam value
//             */
//            paramVal = v;
//        }
//
//        // Only TIMESTAMP/DATETIME type need correct timezone.
//        if (dataType instanceof TimestampType) {
//            paramVal = correctTimeZoneForParamVal(tableRule, colName, dataType, calcParams, paramVal);
//        }
//        comparative.setValue(dataType.convertJavaFrom(paramVal));
//        return comparative;
//    }
//
//    private Comparative replaceParamWithValue(TableRule tableRule,
//                                       String colName,
//                                       Comparative comparative,
//                                       Map<Integer, ParameterContext> param,
//                                       Map<String, DataType> dataTypeMap,
//                                       String name,
//                                       Map<String, Object> calcParams) {
//        if (comparative instanceof ComparativeAND || comparative instanceof ComparativeOR) {
//            List<Comparative> compList = ((ComparativeBaseList) comparative).getList();
//            List<Comparative> newCompList = new ArrayList<>();
//            boolean isCompOr = comparative instanceof ComparativeOR;
//            Comparative finalComp = comparative;
//            for (int i = 0; i < compList.size(); i++) {
//                Comparative c = compList.get(i);
//                Comparative newC = null;
//                if (c instanceof ComparativeAND || c instanceof ComparativeOR) {
//                    newC = replaceParamWithValue(tableRule, colName, c, param, dataTypeMap, name, calcParams);
//                } else if (c instanceof ExtComparative) {
//                    newC = replaceParamWithValue(tableRule,
//                        colName,
//                        c,
//                        param,
//                        dataTypeMap.get(((ExtComparative) c).getColumnName()),
//                        calcParams);
//                } else {
//                    newC = replaceParamWithValue(tableRule, colName, c, param, dataTypeMap.get(name), calcParams);
//                }
//
//                if (isCompOr) {
//                    if (newC == null) {
//                        finalComp = null;
//                        break;
//                    }
//                } else {
//                    if (newC != null) {
//                        newCompList.add(newC);
//                    }
//                }
//            }
//
//            if (isCompOr) {
//                return finalComp;
//            } else {
//                if (newCompList.size() > 1) {
//                    ((ComparativeAND) finalComp).setList(newCompList);
//                } else if (newCompList.size() == 1) {
//                    finalComp = newCompList.get(0);
//                } else {
//                    finalComp = null;
//                }
//                return finalComp;
//            }
////            for (Comparative c : ((ComparativeBaseList) comparative).getList()) {
////                if (c instanceof ComparativeAND || c instanceof ComparativeOR) {
////                    replaceParamWithValue(tableRule, colName, c, param, dataTypeMap, name, calcParams);
////                } else if (c instanceof ExtComparative) {
////                    replaceParamWithValue(tableRule,
////                        colName,
////                        c,
////                        param,
////                        dataTypeMap.get(((ExtComparative) c).getColumnName()),
////                        calcParams);
////                } else {
////                    replaceParamWithValue(tableRule, colName, c, param, dataTypeMap.get(name), calcParams);
////                }
////            }
//        } else if (comparative instanceof ExtComparative) {
//            return replaceParamWithValue(tableRule,
//                colName,
//                comparative,
//                param,
//                dataTypeMap.get(((ExtComparative) comparative).getColumnName()),
//                calcParams);
//        } else {
//            return replaceParamWithValue(tableRule, colName, comparative, param, dataTypeMap.get(name), calcParams);
//        }
//    }
//
    public static Map<String, Comparative> getComparatives(List<ColumnMeta> columns, List<Object> values,
                                                           List<String> names) {
        Map<String, Comparative> comparativeMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta meta = columns.get(i);
            String name = names.get(i);
            DataType dataType = meta.getDataType();
            Object value = values.get(i);
            Comparative comparative = new ExtComparative(name, Comparative.Equivalent, dataType.convertJavaFrom(value));
            comparativeMap.put(name, comparative);
        }
        return comparativeMap;
    }

    public static Map<String, Comparative> getComparativeORWithSingleColumn(ColumnMeta column, List<Object> values,
                                                                            String name) {
        Map<String, Comparative> comparativeMap = new HashMap<>();
        if (values.size() == 0) {
            return comparativeMap;
        } else if (values.size() == 1) {
            Comparative comparative =
                new ExtComparative(name, Comparative.Equivalent, column.getDataType().convertJavaFrom(values.get(0)));
            comparativeMap.put(name, comparative);
            return comparativeMap;
        }
        DataType dataType = column.getDataType();
        ComparativeOR outerOR;
        Comparative tmpComparative;
        outerOR = new ComparativeOR();
        comparativeMap.put(name, outerOR);
        for (Object value : values) {
            tmpComparative = new ExtComparative(name, Comparative.Equivalent, dataType.convertJavaFrom(value));
            outerOR.getList().add(tmpComparative);
        }
        return comparativeMap;
    }

    public static Map<String, Comparative> getLookupComparative(List<Object> shardingKeyValues,
                                                                List<ColumnMeta> shardingKeyMetas) {
        Map<String, Comparative> comparatives = new HashMap<>();
        for (int i = 0; i < shardingKeyValues.size(); i++) {
            final Object value = shardingKeyValues.get(i);
            final String shardingKeyName = shardingKeyMetas.get(i).getName();
            Object convertedValue = shardingKeyMetas.get(i).getDataType().convertJavaFrom(value);

            Comparative comparative = new ExtComparative(shardingKeyName, Comparative.Equivalent, convertedValue);
            comparatives.put(shardingKeyName, comparative);
        }
        return comparatives;
    }

    /**
     * 判断比较操作符能否/如何下推
     * <p>
     * 操作符：=, >=, <=, !=, >, <
     * </p>
     * <p>
     * 所有操作符均转换成列在左边的形式，如 1 = A 转换为 A = 1
     * </p>
     */
    public Comparative getComparativeComparison(RexCall rexNode, RelDataType rowType, String colName,
                                                Map<Integer, ParameterContext> param) {
        if (!isSupportedExpr(rexNode)) {
            return null;
        }

        SqlKind kind = rexNode.getKind();
        List<RexNode> operands = rexNode.getOperands();
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        return getComparative(rowType, colName, param, kind, left, right);

    }

    public Comparative getComparative(RelDataType rowType, String colName, Map<Integer, ParameterContext> param,
                                         SqlKind kind, RexNode left, RexNode right) {
        /**
         * 列名绑定
         */
        RexInputRef columnRef;
        RelDataTypeField columnInfo;
        RexNode constant;
        int comparisonOperator;
        if (left instanceof RexInputRef) {
            // 绑定列名
            columnRef = (RexInputRef) left;
            columnInfo = rowType.getFieldList().get(columnRef.getIndex());
            constant = right;
            comparisonOperator = COMPARATIVE_MAP.get(kind);
        } else if (right instanceof RexInputRef) {
            // 出现 1 = id 的写法
            columnRef = (RexInputRef) right;
            columnInfo = rowType.getFieldList().get(columnRef.getIndex());
            constant = left;
            comparisonOperator = Comparative.exchangeComparison(COMPARATIVE_MAP.get(kind));
        } else {
            // 出现 1 = 0 的写法
            return null;
        }

        if (colName.equalsIgnoreCase(columnInfo.getName())) {
            if (constant instanceof RexLiteral && ((RexLiteral) constant).isNull()
                && comparisonOperator == Comparative.Equivalent) {
                return new Comparative(comparisonOperator, null);
            }
            Object value = getValue(constant, columnInfo, param);
            if (value != null) {
                return new Comparative(comparisonOperator, value);
            }
        }

        return null;
    }

    public Comparative getComparativeIn(RexCall rexNode, RelDataType rowType, String colName,
                                        Map<Integer, ParameterContext> param) {
        if (rexNode instanceof RexSubQuery) {
            return null;
        }

        List<RexNode> operands = rexNode.getOperands();
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        int skIndex = -1;
        boolean rowExpression = false;
        boolean columnInValue = true;
        RexCall row = null;
        if (left instanceof RexInputRef && right.getKind() == SqlKind.ROW) {
            if (right instanceof RexCall && ((RexCall) right).getOperands().size() >= 1) {
                RexNode rightOperand = ((RexCall) right).getOperands().get(0);
                if (rightOperand != null && rightOperand instanceof RexDynamicParam
                    && ((RexDynamicParam) rightOperand).getIndex() == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
                    return null;
                }
            }

            // id in (1, 2)
            row = (RexCall) right;
        } else if (left.getKind() == SqlKind.ROW && right.getKind() == SqlKind.ROW) {
            final List<String> fieldNames = rowType.getFieldNames();
            final List<Ord<RexNode>> sk = Ord.zip(((RexCall) left).getOperands()).stream().filter(
                o -> o.getValue() instanceof RexInputRef && colName.equalsIgnoreCase(
                    fieldNames.get(((RexInputRef) o.getValue()).getIndex()))).collect(Collectors.toList());

            if (sk.size() != 1) {
                return null;
            }

            rowExpression = true;
            skIndex = sk.get(0).getKey();
            left = sk.get(0).getValue();
            row = (RexCall) right;
        } else if (right.getKind() == SqlKind.ROW) {
            if (right instanceof RexCall && ((RexCall) right).getOperands().size() >= 1) {
                RexNode rightOperand = ((RexCall) right).getOperands().get(0);
                if (rightOperand != null && rightOperand instanceof RexDynamicParam
                    && ((RexDynamicParam) rightOperand).getIndex() == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
                    return null;
                }
            }
            // maybe 1 in (id, 2)
            columnInValue = false;
            row = (RexCall) right;
        } else {
            // should not be here
            return null;
        }

        if (row.getOperands().size() <= 0) {
            // should not be here
            return null;
        }
        boolean rowDynamic = RexUtils.isRowDynamic(row);

        final int op = Comparative.Equivalent;
        ComparativeBaseList or = new ComparativeOR();
        for (RexNode rowValue : row.getOperands()) {
            RexNode column = columnInValue ? left : rowValue;
            RexNode valueNode = columnInValue ? rowValue : left;

            if (rowExpression) {
                if (valueNode instanceof RexDynamicParam) {
                    // do nothing
                } else {
                    valueNode = ((RexCall) valueNode).getOperands().get(skIndex);
                }
            }

            Object value = null;
            RelDataTypeField columnInfo = null;
            if (column instanceof RexInputRef) {
                columnInfo = rowType.getFieldList().get(((RexInputRef) column).getIndex());
                value = getValue(valueNode, columnInfo, param);
                if (value == null) {
                    // value is not a RexLiteral
                    return null;
                }
            }

            if (null != value && null != columnInfo && colName.equalsIgnoreCase(columnInfo.getName())) {
                if (rowDynamic) {
                    or.getList().add(new DynamicComparative(op, value, skIndex));
                } else {
                    or.getList().add(new Comparative(op, value));
                }
            } else {
                return null;
            }
        } // end of for
        return or;
    }

    /**
     * Support {@code InputRef OP Constant} or {@code Constant OP InputRef}
     */
    private boolean isSupportedExpr(RexCall rexNode) {
        List<RexNode> operands = rexNode.getOperands();
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);
        if (isInputRef(left) && (isConstant(right) || SubQueryDynamicParamUtils.isMaxOneRowScalarSubQueryConstant(right))) {
            return true;
        }

        if (isInputRef(right) && (isConstant(left) || SubQueryDynamicParamUtils.isMaxOneRowScalarSubQueryConstant(left))) {
            return true;
        }
        return false;
    }

    private static boolean isInputRef(RexNode rexNode) {
        return RexUtil.isReferenceOrAccess(rexNode, true);
    }

    private boolean isConstant(RexNode rexNode) {
        if (RexUtil.isLiteral(rexNode, true)) {
            return true;
        }

        if (rexNode instanceof RexDynamicParam) {
            if (((RexDynamicParam) rexNode).getIndex() >= 0) {
                return true;
            } else {
                return false;
            }
        }

        if (enableConstExpr) {
            return RexUtil.isConstant(rexNode);
        }

        return false;
    }

    public Comparative getComparativeAndOr(RexCall rexCall, RelDataType rowType, String colName,
                                           ComparativeBaseList comp, Map<Integer, ParameterContext> param) {

        boolean isExistInAllSubFilter = true;

        for (RexNode operand : rexCall.getOperands()) {
            if (!(operand instanceof RexCall)) {
                if (comp instanceof ComparativeAND) {
                    continue;
                } else {
                    return null;
                }
            }

            RexCall subFilter = (RexCall) operand;
            Comparative subComp = getComparative(subFilter, rowType, colName, param);
            if (subComp != null) {
                // Deduplication
                if (comp.getList() != null && !comp.getList().contains(subComp)) {
                    // Comparative only supports two operators.
                    if (comp.getList().size() == 2) {
                        ComparativeBaseList newComp;
                        if (comp instanceof ComparativeAND) {
                            newComp = new ComparativeAND();
                        } else {
                            newComp = new ComparativeOR();
                        }
                        newComp.addComparative(comp.getList().get(1));
                        newComp.addComparative(subComp);
                        comp.getList().set(1, newComp);
                    } else {
                        comp.addComparative(subComp);
                    }
                }
            }

            isExistInAllSubFilter &= (subComp != null);
        }

        if (comp == null || comp.getList() == null || comp.getList().isEmpty()) {
            return null;
        } else if (comp instanceof ComparativeOR && !isExistInAllSubFilter) {
            /**
             * <pre>
             * 针对or类型，必须所有的子条件都包含该列条件，否则就是一个全库扫描，返回null值
             * 比如分库键为id，如果条件是 id = 1 or id = 3，可以返回
             * 如果条件是id = 1 or name = 2，应该是个全表扫描
             * </pre>
             */
            return null;
        } else if (comp.getList().size() == 1) {
            return comp.getList().get(0);// 可能只有自己一个and
        }

        return comp;
    }

    /**
     * 将一个{@linkplain RexNode}表达式转化为Tddl Rule所需要的{@linkplain Comparative}对象
     *
     * @param colName @return
     */
    public Comparative getComparative(RexNode rexNode, RelDataType rowType, String colName,
                                      Map<Integer, ParameterContext> param) {
        // 前序遍历，找到所有符合要求的条件
        if (rexNode == null) {
            return null;
        }

        Comparative comp = null;

        if (rexNode instanceof RexCall) {
            SqlKind kind = rexNode.getKind();

            switch (kind) {
            case IN:
                /*
                 * Calcite 默认会将 IN 全部转成 OR，详见：
                 * org.apache.calcite.sql2rel.SqlToRelConverter#convertInToOr
                 */
                return getComparativeIn((RexCall) rexNode, rowType, colName, param);
            case LIKE:
            case NOT:
                return null;
            case AND:
                return getComparativeAndOr((RexCall) rexNode, rowType, colName, new ComparativeAND(), param);
            case OR:
                return getComparativeAndOr((RexCall) rexNode, rowType, colName, new ComparativeOR(), param);
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return getComparativeComparison((RexCall) rexNode, rowType, colName, param);
            case BETWEEN:
                return getComparativeBetween((RexCall) rexNode, rowType, colName, param);
            case IS_NOT_FALSE:
            case IS_NOT_TRUE:
            case IS_NOT_NULL:
            case IS_FALSE:
            case IS_TRUE:
                // 这些运算符不参与下推判断
                return null;
            case IS_NULL:
                return getComparativeIsNull((RexCall) rexNode, rowType, colName, param);
            case CAST:
                return getComparative(((RexCall) rexNode).getOperands().get(0), rowType, colName, param);
            default:
                return null;
            } // end of switch
        }

        return comp;
    }

    private static Comparative getComparativeIsNull(RexCall rexNode, RelDataType rowType, String colName,
                                                    Map<Integer, ParameterContext> param) {
        assert rexNode.isA(IS_NULL);
        List<RexNode> operands = rexNode.getOperands();
        RexNode input = operands.get(0);

        RexInputRef columnRef;
        RelDataTypeField columnInfo;

        if (!(input instanceof RexInputRef)) {
            return null;
        }

        columnRef = (RexInputRef) input;
        columnInfo = rowType.getFieldList().get(columnRef.getIndex());
        if (colName.equalsIgnoreCase(columnInfo.getName())) {
            return new Comparative(Comparative.Equivalent, null);
        } else {
            return null;
        }
    }

    private Comparative getComparativeBetween(RexCall rexNode, RelDataType rowType, String colName,
                                              Map<Integer, ParameterContext> param) {
        RexNode column = rexNode.getOperands().get(0);
        RexNode left = rexNode.getOperands().get(1);
        RexNode right = rexNode.getOperands().get(2);

        if (!isInputRef(column) || !isConstant(left) || !isConstant(right)) {
            return null;
        }

        Comparative leftComp = getComparative(rowType, colName, param, SqlKind.GREATER_THAN_OR_EQUAL, column, left);
        if (null == leftComp) {
            return null;
        }
        Comparative rightComp = getComparative(rowType, colName, param, SqlKind.LESS_THAN_OR_EQUAL, column, right);
        if (null == rightComp) {
            return null;
        }

        ComparativeAND result = new ComparativeAND();
        result.addComparative(leftComp);
        result.addComparative(rightComp);

        return result;
    }

    /**
     * Get comparative for one row
     *
     * @param rowValues values for one row
     * @param shardColumns column index and column info for each sharding column
     * @param sequenceValues computed sequence values for this row
     */
    public <T extends RexNode> Map<String, Comparative> getInsertComparative(ImmutableList<T> rowValues,
                                                                             List<Pair<Integer, RelDataTypeField>> shardColumns,
                                                                             Map<Integer, ParameterContext> param,
                                                                             Map<Integer, Long> sequenceValues,
                                                                             List<DataType> dataTypes) {
        Map<String, Comparative> comparatives = new HashMap<>();
        for (int i = 0; i < shardColumns.size(); i++) {
            Pair<Integer, RelDataTypeField> column = shardColumns.get(i);
            int fieldIndex = column.getKey();
            RelDataTypeField columnInfo = column.getValue();

            Long seqVal = sequenceValues == null ? null : sequenceValues.get(fieldIndex);
            Object value;
            if (seqVal != null) {
                value = seqVal;
            } else {
                T rexNode = rowValues.get(fieldIndex);
                value = getInsertValue(rexNode, param, dataTypes.get(i));
            }

            Comparative comparative =
                new ExtComparative(column.getValue().getKey(), COMPARATIVE_MAP.get(SqlKind.EQUALS), value);
            comparatives.put(columnInfo.getName(), comparative);
        }
        return comparatives;
    }

    /**
     * Used for hot key
     */
    public <T extends RexNode> Map<String, Comparative> getInsertFullComparative(
        Map<String, Comparative> insertComparative) {
        if (insertComparative.size() == 1) {
            return insertComparative;
        } else {
            Map<String, Comparative> comparativeHashMap = Maps.newHashMap();
            ComparativeAND comparativeAND = new ComparativeAND();
            for (String s : insertComparative.keySet()) {
                comparativeAND.addComparative(insertComparative.get(s));
                comparativeHashMap.put(s, comparativeAND);
            }
            return comparativeHashMap;
        }
    }

    /**
     *
     */
    private Object getValue(RexNode constant, RelDataTypeField type, Map<Integer, ParameterContext> param) {
        try {
            final DataType dataType = DataTypeUtil.calciteToDrdsType(type.getValue());
            return getValue(constant, param, dataType);
        } catch (Exception e) {
            logger.error("get value failed! ", e);
            throw e;
        }
    }

    /**
     * SELECT uses ColumnMeta.getDataType to convert the value, so INSERT should
     * use the same DataType.
     *
     * @param dataType ColumnMeta.getDataType
     */
    private Object getInsertValue(RexNode constant, Map<Integer, ParameterContext> param, DataType dataType) {
        try {
            return getValue(constant, param, dataType);
        } catch (Exception e) {
            logger.error("get value failed! ", e);
            throw e;
        }
    }

    private Object getValue(RexNode constant, Map<Integer, ParameterContext> param, DataType dataType) {
        if (constant instanceof RexLiteral) {
            RexLiteral value = (RexLiteral) constant;
            return dataType.convertJavaFrom(value.getValue3());
        }

        if (constant instanceof RexDynamicParam && MapUtils.isNotEmpty(param)) {
            RexDynamicParam rdm = (RexDynamicParam) constant;

            Object valueObj;
            // RexDynamicParam index start from 0, param index start from 1
            valueObj = param.get(rdm.getIndex() + 1).getValue();

            if (valueObj instanceof RawString) {
                valueObj = ((RawString) valueObj).getObj(rdm.getSubIndex(), -1);
            }
            return dataType.convertJavaFrom(valueObj);
        } else if (constant instanceof RexDynamicParam) {
            return constant;
        }

        if (constant.getKind() == SqlKind.CAST) {
            RexNode operand0 = ((RexCall) constant).getOperands().get(0);
            return getValue(operand0, param, dataType);
        }

        // scalar functions
        if (enableConstExpr && RexUtil.isConstant(constant)) {
            return constant;
        } else {
            return null;
        }
    }

    public void setShardRouterTimeZone(InternalTimeZone shardRouterTimeZone) {
        this.shardRouterTimeZone = shardRouterTimeZone;
    }

    public void setEnableConstExpr(boolean enableConstExpr) {
        this.enableConstExpr = enableConstExpr;
    }
}

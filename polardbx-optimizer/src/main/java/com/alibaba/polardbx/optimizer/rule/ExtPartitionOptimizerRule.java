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

import com.alibaba.polardbx.common.model.sqljep.DynamicComparative;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import com.alibaba.polardbx.optimizer.sharding.DataNodeChooser;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeBaseList;
import com.alibaba.polardbx.common.model.sqljep.ComparativeExtMapChoicer;
import com.alibaba.polardbx.common.model.sqljep.ComparativeMapChoicer;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.model.sqljep.ExtComparative;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.exception.RouteCompareDiffException;
import com.alibaba.polardbx.rule.model.MatcherResult;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * 将分库分表字段 提取到同一个comparative中，便于后面计算分库同时计算分表
 *
 * @author hongxi.chx
 */
public class ExtPartitionOptimizerRule extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(ExtPartitionOptimizerRule.class);

    private final TddlRule tddlRule;

    private static final Map<SqlKind, Integer> COMPARATIVE_MAP = new HashMap<>(8);

    static {
        COMPARATIVE_MAP.put(SqlKind.EQUALS, Comparative.Equivalent);
        COMPARATIVE_MAP.put(SqlKind.NOT_EQUALS, Comparative.NotEquivalent);
        COMPARATIVE_MAP.put(SqlKind.GREATER_THAN, Comparative.GreaterThan);
        COMPARATIVE_MAP.put(SqlKind.GREATER_THAN_OR_EQUAL, Comparative.GreaterThanOrEqual);
        COMPARATIVE_MAP.put(SqlKind.LESS_THAN, Comparative.LessThan);
        COMPARATIVE_MAP.put(SqlKind.LESS_THAN_OR_EQUAL, Comparative.LessThanOrEqual);
    }

    public ExtPartitionOptimizerRule(TddlRule tddlRule) {
        this.tddlRule = tddlRule;
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
                                Map<String, Comparative> comparatives, Map<Integer, ParameterContext> param) {
        return shard(logicTable, isWrite, forceAllowFullTableScan, comparatives, param, false);
    }

    /**
     * @param logicTable 计算切分的逻辑表
     * @param isWrite 是否写操作
     * @param forceAllowFullTableScan 是否允许全表扫描
     * @param comparatives 切分条件
     * @param param 参数化参数
     * @param shardForExtraDb 计算逻辑表的路由结果时，是否应用扩展库（即历史库、离线库之类）的拓扑
     */
    public List<TargetDB> shard(String logicTable, boolean isWrite, boolean forceAllowFullTableScan,
                                Map<String, Comparative> comparatives, Map<Integer, ParameterContext> param,
                                boolean shardForExtraDb) {

        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, shardForExtraDb);
        return shard(logicTable, isWrite, forceAllowFullTableScan, comparatives, param, calcParams);
    }

    public List<TargetDB> shard(String logicTable, boolean isWrite, boolean forceAllowFullTableScan,
                                Map<String, Comparative> comparatives, Map<Integer, ParameterContext> param,
                                Map<String, Object> calcParams) {
        List<TargetDB> targetDbs = shard(logicTable,
            isWrite,
            forceAllowFullTableScan,
            null,
            comparatives,
            param,
            calcParams);
        if (targetDbs == null || targetDbs.isEmpty()) {
            throw new IllegalArgumentException("can't find target db. table is " + logicTable);
        }
//        if (ConfigDataMode.isFastMock()) {
//            for (TargetDB targetDB : targetDbs) {
//                for (String tableName : targetDB.getTableNames()) {
//                    MockDataManager.phyTableToLogicalTableName.put(tableName, logicTable);
//                }
//            }
//        }
        return targetDbs;
    }

    /**
     *
     */
    public List<TargetDB> shard(final String logicTable, boolean isWrite, boolean forceAllowFullTableScan,
                                List<TableRule> ruleList, final Map<String, Comparative> comparatives,
                                final Map<Integer, ParameterContext> param, Map<String, Object> calcParams) {
        MatcherResult result;
        /**
         * column name from tddl rule could be upper case
         */
        final Map<String, DataType> dataTypeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (!MapUtils.isEmpty(comparatives)) {
            // getLatestSchemaManager is only for drds.
            SchemaManager schemaManager =
                OptimizerContext.getContext(tddlRule.getSchemaName()).getLatestSchemaManager();
            dataTypeMap.putAll(PlannerUtils.buildDataType(ImmutableList.copyOf(comparatives.keySet()),
                schemaManager.getTable(logicTable)));
        }
        try {
            result = tddlRule.routeMverAndCompare(!isWrite, logicTable, new ComparativeExtMapChoicer() {

                @Override
                public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> colNameSet) {
                    Map<String, Comparative> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for (String str : colNameSet) {
                        map.put(str, getColumnComparative(arguments, str));
                    }
                    return map;
                }

                @Override
                public Comparative getColumnComparative(List<Object> arguments, String colName) {
                    throw new UnsupportedOperationException("not support single colName, must be a Set");
                }

                @Override
                public Comparative getColumnComparative(List<Object> arguments, Set<String> colNames) {
                    return getColumnComparative(arguments, colNames);
                }

            }, Lists.newArrayList(), forceAllowFullTableScan, ruleList, calcParams);
        } catch (RouteCompareDiffException e) {
            throw GeneralUtil.nestedException(e);
        }

        return result.getCalculationResult();
    }

    public static Comparative getComparative(Map<String, Comparative> comparatives, Set<String> colNames,
                                             Map<Integer, ParameterContext> param, Map<String, DataType> dataTypeMap) {
        if (MapUtils.isEmpty(comparatives)) {
            return null;
        }

        /**
         * 没有参数
         */
        if (MapUtils.isEmpty(param)) {
            return findComparativeIgnoreCase(comparatives, colNames);
        } else {
            /**
             * 用实际值替换参数
             */
            final Comparative c = findComparativeIgnoreCase(comparatives, colNames);
            if (c != null) {
                Comparative clone = (Comparative) c.clone();
                for (String colName : colNames) {
                    replaceParamWithValue(clone, param, dataTypeMap.get(colName));
                }
                return clone;
            } else {
                return null;
            }
        }
    }

    private static Comparative findComparativeIgnoreCase(Map<String, Comparative> comparatives, Set<String> colNames) {
        for (Entry<String, Comparative> entry : comparatives.entrySet()) {
            for (String col : colNames) {
                if (col.equalsIgnoreCase(entry.getKey())) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    private static void replaceParamWithValue(Comparative comparative, Map<Integer, ParameterContext> param,
                                              DataType dataType) {
        if (comparative instanceof ComparativeAND || comparative instanceof ComparativeOR) {
            for (Comparative c : ((ComparativeBaseList) comparative).getList()) {
                replaceParamWithValue(c, param, dataType);
            }
            return;
        }

        Object v = comparative.getValue();
        if (v instanceof RexDynamicParam) {
            int index = ((RexDynamicParam) v).getIndex();
            if (index != PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
                comparative.setValue(dataType.convertJavaFrom(param.get(index + 1).getValue()));
            }
        }
    }

    /**
     * 根据列条件,表名以及参数计算所在分表
     */
    public List<TargetDB> shard(String logicTable, final Map<String, Integer> columnComp,
                                final Map<Integer, ParameterContext> param, TableRule tableRule,
                                final Map<String, DataType> dataType) {

        /**
         * column name from tddl rule could be upper case
         */
        final Map<String, DataType> dataTypeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        dataTypeMap.putAll(dataType);

        final Map<String, Integer> columnCompMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columnCompMap.putAll(columnComp);

        MatcherResult result;
        try {
            result = tddlRule.routeMverAndCompare(true, logicTable, new ComparativeMapChoicer() {

                @Override
                public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> colNameSet) {
                    Map<String, Comparative> map = new TreeMap<String, Comparative>(String.CASE_INSENSITIVE_ORDER);
                    for (String str : colNameSet) {
                        map.put(str, getColumnComparative(arguments, str));
                    }
                    return map;
                }

                @Override
                public Comparative getColumnComparative(List<Object> arguments, String colName) {
                    return getComparative(colName, columnCompMap, param, dataTypeMap);
                }
            }, Lists.newArrayList(), true, ImmutableList.of(tableRule));
        } catch (RouteCompareDiffException e) {
            throw GeneralUtil.nestedException(e);
        }
        return result.getCalculationResult();
    }

    public Comparative getComparative(String column, Map<String, Integer> columnComp,
                                      Map<Integer, ParameterContext> param, Map<String, DataType> dataType) {
        /**
         * 没有参数
         */
        if (MapUtils.isEmpty(param)) {
            return null;
        }

        int index = columnComp.get(column);
        Object v = param.get(index + 1).getValue();
        DataType dt = dataType.get(column);
        return new Comparative(Comparative.Equivalent, dt.convertJavaFrom(v));
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
    public static Comparative getComparativeComparison(RexCall rexNode, RelDataType rowType, Set<String> colName,
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

    protected static Comparative getComparative(RelDataType rowType, Set<String> colNames,
                                                Map<Integer, ParameterContext> param, SqlKind kind, RexNode left,
                                                RexNode right) {
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

        for (String colName : colNames) {
            if (colName.equalsIgnoreCase(columnInfo.getName())) {
                Object value = getValue(constant, columnInfo, param);
                if (value != null) {
                    return new ExtComparative(colName, comparisonOperator, value);
                }
            }
        }

        return null;
    }

    public static Comparative getComparativeIn(RexCall rexNode, RelDataType rowType, Set<String> colNames,
                                               Map<Integer, ParameterContext> param) {
        if (rexNode instanceof RexSubQuery) {
            return null;
        }

        List<RexNode> operands = rexNode.getOperands();
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        boolean columnInValue = true;
        RexCall row = null;
        if (left instanceof RexInputRef && right.getKind() == SqlKind.ROW) {
            // id in (1, 2)
            row = (RexCall) right;
        } else if (right.getKind() == SqlKind.ROW) {
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
            for (String colName : colNames) {
                if (null != value && null != columnInfo && colName.equalsIgnoreCase(columnInfo.getName())) {
                    if (rowDynamic) {
                        or.getList().add(new DynamicComparative(op, value, -1));
                    } else {
                        or.getList().add(new Comparative(op, value));
                    }
                } else {
                    continue;
                }
            }

        } // end of for

        return or;
//        }
//
//        return null;
    }

    /**
     * Support {@code InputRef OP Constant} or {@code Constant OP InputRef}
     */
    private static boolean isSupportedExpr(RexCall rexNode) {
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

    private static boolean isConstant(RexNode rexNode) {
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
        return false;
    }

    public static Comparative getComparativeAndOr(RexCall rexCall, RelDataType rowType, Set<String> colNames,
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
            Comparative subComp = getComparative(subFilter, rowType, colNames, param);
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
     * @param colNames @return
     */
    public static Comparative getComparative(RexNode rexNode, RelDataType rowType, Set<String> colNames,
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
                /**
                 * Calcite 默认会将 IN 全部转成
                 * OR，详见：{@link org.apache.calcite.sql2rel.SqlToRelConverter#convertInToOr}
                 */
                return getComparativeIn((RexCall) rexNode, rowType, colNames, param);
            case LIKE:
            case NOT:
                return null;
            case AND:
                return getComparativeAndOr((RexCall) rexNode, rowType, colNames, new ComparativeAND(), param);
            case OR:
                return getComparativeAndOr((RexCall) rexNode, rowType, colNames, new ComparativeOR(), param);
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                return getComparativeComparison((RexCall) rexNode, rowType, colNames, param);
            case BETWEEN:
                return getComparativeBetween((RexCall) rexNode, rowType, colNames, param);
            case IS_NOT_FALSE:
            case IS_NOT_TRUE:
            case IS_NOT_NULL:
            case IS_NULL:
            case IS_FALSE:
            case IS_TRUE:
                // 这些运算符不参与下推判断
                return null;
            case CAST:
                return getComparative(((RexCall) rexNode).getOperands().get(0), rowType, colNames, param);
            default:
                return null;
            } // end of switch
        }

        return comp;
    }

    private static Comparative getComparativeBetween(RexCall rexNode, RelDataType rowType, Set<String> colName,
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
    public static <T extends RexNode> Map<String, Comparative> getInsertComparative(ImmutableList<T> rowValues,
                                                                                    List<Pair<Integer, RelDataTypeField>> shardColumns,
                                                                                    Map<Integer, ParameterContext> param,
                                                                                    Map<Integer, Long> sequenceValues) {

        Map<String, Comparative> comparatives = new HashMap<>();
        for (Pair<Integer, RelDataTypeField> column : shardColumns) {
            int fieldIndex = column.getKey();
            RelDataTypeField columnInfo = column.getValue();

            Long seqVal = sequenceValues == null ? null : sequenceValues.get(fieldIndex);
            Object value;
            if (seqVal != null) {
                value = seqVal;
            } else {
                T rexNode = rowValues.get(fieldIndex);
                value = getValue(rexNode, columnInfo, param);
            }

            Comparative comparative = new Comparative(COMPARATIVE_MAP.get(SqlKind.EQUALS), value);
            comparatives.put(columnInfo.getName(), comparative);
        }
        return comparatives;
    }

    /**
     *
     */
    private static Object getValue(RexNode constant, RelDataTypeField type, Map<Integer, ParameterContext> param) {
        try {
            final DataType dataType = DataTypeUtil.calciteToDrdsType(type.getValue());

            if (constant instanceof RexLiteral) {
                RexLiteral value = (RexLiteral) constant;
                return dataType.convertJavaFrom(value.getValue3());
            }

            if (constant instanceof RexDynamicParam && MapUtils.isNotEmpty(param)) {
                RexDynamicParam rdm = (RexDynamicParam) constant;

                Object valueObj;
                // RexDynamicParam index start from 0, param index start from 1
                valueObj = param.get(rdm.getIndex() + 1).getValue();

                return dataType.convertJavaFrom(valueObj);
            } else if (constant instanceof RexDynamicParam) {
                return constant;
            }

            if (constant.getKind() == SqlKind.CAST) {
                RexNode operand0 = ((RexCall) constant).getOperands().get(0);
                return getValue(operand0, type, param);
            }
        } catch (Exception e) {
            logger.error("get value failed! ", e);
            throw e;
        }

        // scalar functions
        return null;
    }

}

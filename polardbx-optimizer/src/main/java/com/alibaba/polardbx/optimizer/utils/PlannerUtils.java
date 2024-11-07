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

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeBaseList;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.util.DynamicParamInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.IndexedDynamicParamInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.RuntimeFilterDynamicParamInfo;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepUtil;
import com.alibaba.polardbx.optimizer.rule.ExtPartitionOptimizerRule;
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sharding.result.RelShardInfo;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.GroupConcatCall;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author lingce.ldm 2017-11-16 10:20
 */
public class PlannerUtils {

    public final static int TABLE_NAME_PARAM_INDEX = -1;
    public final static int SCALAR_SUBQUERY_PARAM_INDEX = -2;
    public final static int APPLY_SUBQUERY_PARAM_INDEX = -3;
    private final static Logger logger = LoggerFactory.getLogger(PlannerUtils.class);

    public static RelMetadataQuery newMetadataQuery() {
        return RelMetadataQuery.instance(DrdsRelMetadataProvider.DEFAULT);
    }

    public static Map<String, List<List<String>>> convertTargetDB(List<List<TargetDB>> targetDBs) {
        return convertTargetDB(targetDBs, null, false, null);
    }

    public static Map<String, List<List<String>>> convertTargetDB(List<List<TargetDB>> targetDBs, String schemaName) {
        return convertTargetDB(targetDBs, schemaName, false, null);
    }

    public static Map<String, List<List<String>>> convertTargetDB(List<List<TargetDB>> targetDBs,
                                                                  String schemaName,
                                                                  boolean crossSingleTable) {
        return convertTargetDB(targetDBs, schemaName, crossSingleTable, null);
    }

    /**
     * <pre>
     * 转换 DataNodeChooser.shard 输出结果为构建 PhyTableOperation 对象需要的结构
     * 表示形式：
     * 前者：List[逻辑表 List[分库 List[分表]]]
     * 后者：Map[分库 List[按下标分组 List[一个物理 SQL 中的所有表]]]
     *
     * 这里需要处理广播表
     * </pre>
     */
    public static Map<String, List<List<String>>> convertTargetDB(List<List<TargetDB>> targetDBs,
                                                                  String schemaName,
                                                                  boolean crossSingleTable,
                                                                  ExecutionContext executionContext) {
        Map<String, List<List<String>>> result = new HashMap<>();
        List<TargetDB> broadcastTable = new ArrayList<>();
        List<List<TargetDB>> singleTable = new ArrayList<>();

        /**
         * Whether the first table(NOT a broadcast table)
         */
        boolean first = true;
        int maxDbCount = -1;

        /**
         * 遍历每个逻辑表
         */
        for (int i = 0; i < targetDBs.size(); i++) {
            List<TargetDB> t = targetDBs.get(i);
            /**
             * 暂存广播表,遇到第一个不是广播表时处理
             */
            if (isBroadcast(t, schemaName, executionContext)) {
                broadcastTable.add(t.get(0));
                continue;
            }

            if (isSingleTablePerDB(t) && crossSingleTable) {
                singleTable.add(t);
                continue;
            }

            if (maxDbCount != t.size()) {
                if (maxDbCount < 0) {
                    maxDbCount = t.size();
                } else {
                    /**
                     * group count not match, something is wrong.
                     */
                    return MapUtils.EMPTY_MAP;
                }
            }

            /**
             * 遍历逻辑表所属的每个 Group
             */
            for (TargetDB tdb : t) {
                String dbIndex = tdb.getDbIndex();
                String[] tableNames = tdb.getTableNames().toArray(new String[0]);
                List<List<String>> v = result.get(dbIndex);
                if (v == null) {
                    if (first) {
                        v = new ArrayList<>(tableNames.length);
                        initListWithEmptyList(v, tableNames.length);
                        result.put(dbIndex, v);
                    } else {
                        /**
                         * Do not have the same group, something is wrong.
                         */
                        return MapUtils.EMPTY_MAP;
                    }
                }

                if (i > 0 && tableNames.length != v.size()) {
                    return MapUtils.EMPTY_MAP;
                }

                /**
                 * 添加之前的广播表
                 */
                if (!broadcastTable.isEmpty()) {
                    for (int k = 0; k < tableNames.length; k++) {
                        List<String> names = v.get(k);
                        if (names == null) {
                            names = new ArrayList<>();
                        }
                        addBroadcastTable(broadcastTable, names);
                    }
                }

                /**
                 * 添加分库单表
                 */
                if (!singleTable.isEmpty()) {
                    for (int k = 0; k < tableNames.length; k++) {
                        List<String> names = v.get(k);
                        if (names == null) {
                            names = new ArrayList<>();
                        }
                        addSingleTable(singleTable, names);
                    }
                }

                /**
                 * 遍历 Group 中的每个物理表
                 */
                for (int j = 0; j < tableNames.length; j++) {
                    List<String> names = v.get(j);
                    if (names == null) {
                        names = new ArrayList<>();
                    }
                    names.add(tableNames[j]);
                }
            }
            first = false;
            broadcastTable.clear();
            singleTable.clear();
        }

        /**
         * 添加剩余的广播表
         */
        if (!broadcastTable.isEmpty()) {
            addRemainingBroadcastTable(broadcastTable, result);
        }

        if (!singleTable.isEmpty()) {
            addRemainingSingleTable(singleTable, result);
        }

        return result;
    }

    private static void addSingleTable(List<List<TargetDB>> singleTables, List<String> names) {
        for (List<TargetDB> singleTable : singleTables) {
            // for (TargetDB tdb : singleTable) {
            names.add(singleTable.get(0).getTableNames().iterator().next());
            // }
        }

    }

    private static void addRemainingSingleTable(List<List<TargetDB>> singleTables,
                                                Map<String, List<List<String>>> result) {
        if (result.size() == 0) {
            for (List<TargetDB> singleTable : singleTables) {
                for (TargetDB tdb : singleTable) {
                    String dbIndex = tdb.getDbIndex();
                    if (!result.containsKey(dbIndex)) {
                        result.put(dbIndex, new ArrayList<List<String>>());
                        result.get(dbIndex).add(new ArrayList<String>());
                    }

                    result.get(dbIndex).get(0).addAll(tdb.getTableNames());
                }
            }
        } else {
            for (List<List<String>> v : result.values()) {
                for (List<String> t : v) {
                    addSingleTable(singleTables, t);
                }
            }
        }
    }

    private static boolean isSingleTablePerDB(List<TargetDB> targetDBs) {
        if (targetDBs.size() == 0) {
            return false;
        }
        TargetDB db = targetDBs.get(0);
        if (db.getTableNames().size() != 1) {
            return false;
        }
        return true;
    }

    /**
     * 每个 broadcast 均在 target 中添加一个
     */
    private static void addBroadcastTable(List<TargetDB> broadcastTable, List<String> target) {
        for (TargetDB tdb : broadcastTable) {
            target.add(tdb.getTableNames().iterator().next());
        }
    }

    private static void addRemainingBroadcastTable(List<TargetDB> broadcastTable,
                                                   Map<String, List<List<String>>> result) {
        /**
         * 只有广播表
         */
        if (result.size() == 0) {
            for (TargetDB tdb : broadcastTable) {
                String dbIndex = tdb.getDbIndex();
                if (!result.containsKey(dbIndex)) {
                    result.put(dbIndex, new ArrayList<List<String>>());
                    result.get(dbIndex).add(new ArrayList<String>());
                }

                result.get(dbIndex).get(0).addAll(tdb.getTableNames());
            }
        } else {
            for (List<List<String>> v : result.values()) {
                for (List<String> t : v) {
                    addBroadcastTable(broadcastTable, t);
                }
            }
        }
    }

    private static void initListWithEmptyList(List list, int num) {
        for (int i = 0; i < num; i++) {
            list.add(new ArrayList<>());
        }
    }

    private static boolean isBroadcast(List<TargetDB> targetDBs, String schemaName) {
        return isBroadcast(targetDBs, schemaName, null);
    }

    private static boolean isBroadcast(List<TargetDB> targetDBs, String schemaName,
                                       ExecutionContext executionContext) {
        if (targetDBs.size() != 1) {
            return false;
        }

        TargetDB db = targetDBs.get(0);
        if (db.getTableNames().size() != 1) {
            return false;
        }

        String tableName = db.getTableNames().iterator().next();
        TddlRuleManager or;
        if (executionContext != null) {
            or = executionContext.getSchemaManager(schemaName).getTddlRuleManager();
        } else {
            or = OptimizerContext.getContext(schemaName).getRuleManager();
        }
        //use the physical name to get the tableRule, then use tableRule to see whether it's a broadcast table
        try {
            String logicalTableName = null;
            if (TStringUtil.isEmpty(schemaName)) {
                schemaName = DefaultSchema.getSchemaName();
            }
            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                String fullyQualifiedPhysicalTableName = (db.getDbIndex() + "." + tableName).toLowerCase();
                Set<String> logicalNameSet = or.getLogicalTableNames(fullyQualifiedPhysicalTableName, schemaName);
                if (CollectionUtils.isEmpty(logicalNameSet)) {
                    return false;
                }
                logicalTableName = logicalNameSet.iterator().next();
            } else {
                logicalTableName = db.getLogTblName();
            }
            return or.isBroadCast(logicalTableName);
        } catch (Exception e) {
            return or.isBroadCast(tableName);
        }
    }

    /**
     * 是否所有的表均为单表
     */
    public static boolean allTableSingle(List<String> tableName, String schemaName, TddlRuleManager tddlRuleManager) {

        // Check all table single for partition table
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            for (String table : tableName) {
                // Check if all tables are single table or broadcast table
                PartitionInfo partInfo = tddlRuleManager.getPartitionInfoManager().getPartitionInfo(table);
                if (partInfo != null) {
                    if (!partInfo.isSingleTable() && !partInfo.isBroadcastTable()) {
                        // For partitioned/gsi table, check if it has only one partitions
                        if (!partInfo.isSinglePartition()) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        for (String table : tableName) {
            TableRule rule = tddlRuleManager.getTableRule(table);
            if (!(isSingleTable(rule) || tddlRuleManager.isBroadCast(table))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断是否为单库单表
     */
    public static boolean isSingleTable(TableRule tableRule) {
        if (tableRule != null) {
            if (tableRule.getActualTopology().size() == 1) {
                for (Map.Entry<String, Set<String>> dbEntry : tableRule.getActualTopology().entrySet()) {
                    if (dbEntry.getValue().size() > 1) {
                        /**
                         * 分表
                         */
                        return false;
                    }
                }
                /**
                 * 单库
                 */
                return true;
            } else {
                /**
                 * 分库
                 */
                return false;
            }
        } else {
            /**
             * 没有规则
             */
            return true;
        }
    }

    public static String buildTableNameParamForXDriver(String t) {
        try {
            // Double the backtick.
            if (t.contains("`")) {
                t = t.replaceAll("`", "``");
            }
            final int firstDotIndex = t.indexOf(".");
            if (firstDotIndex > 0 && firstDotIndex < (t.length() - 1)) {
                final String schemaName = t.substring(0, firstDotIndex);
                final String tableName = t.substring(firstDotIndex + 1);
                // Check if it's a real schema since use may specify
                // a table name containing dot, such as `xxx.yyy`.
                OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
                if (optimizerContext != null) {
                    // This is a real schema to create the shadow table.
                    return new TableName("`" + schemaName + "`.`" + tableName + "`").getTableName();
                } else {
                    // There is no schema here, so we should build a
                    // table name containing dot.
                    return new TableName("`" + t + "`").getTableName();
                }
            } else {
                return new TableName("`" + t + "`").getTableName();
            }
        } catch (SQLException e) {
            logger.error("Build table name for X-Driver error.", e);
            throw new OptimizerException(e);
        }
    }

    public static ParameterContext buildParameterContextForTableName(String tableName, int index) {
        ParameterContext pc = new ParameterContext();
        pc.setParameterMethod(ParameterMethod.setTableName);
        pc.setArgs(new Object[] {index, buildTableNameParamForXDriver(tableName)});
        return pc;
    }

    public static ParameterContext buildParameterContextForScalar(int index) {
        ParameterContext pc = new ParameterContext();
        pc.setParameterMethod(ParameterMethod.setObject1);
        pc.setArgs(new Object[] {index, null});
        return pc;
    }

    public static ParameterContext changeParameterContextIndex(ParameterContext oldPc, int index) {
        ParameterContext pc = new ParameterContext();
        pc.setParameterMethod(oldPc.getParameterMethod());
        Object[] args = oldPc.getArgs();
        Object[] newArgs = Arrays.copyOf(args, args.length);
        newArgs[0] = index;
        pc.setArgs(newArgs);
        return pc;
    }

    public static ParameterContext changeParameterContextValue(ParameterContext oldPc, Object value) {
        ParameterContext pc = new ParameterContext();
        pc.setParameterMethod(oldPc.getParameterMethod());
        Object[] args = oldPc.getArgs();
        Object[] newArgs = Arrays.copyOf(args, args.length);
        newArgs[1] = value;
        pc.setArgs(newArgs);
        return pc;
    }

    /**
     * 获取 SqlNode 中 SqlDynamicParam 节点对应的 index 值
     */
    @Deprecated
    public static List<Integer> getDynamicParamIndex(SqlNode node) {
        return getDynamicParamInfoList(node)
            .stream()
            .peek(c -> GeneralUtil
                .check(c instanceof IndexedDynamicParamInfo, () -> "Unexpected dynamic info type: " + c))
            .map(IndexedDynamicParamInfo.class::cast)
            .map(IndexedDynamicParamInfo::getParamIndex)
            .collect(Collectors.toList());
    }

    /**
     * 获取 SqlNode 中 SqlDynamicParam 节点对应的 index 值
     */
    public static List<DynamicParamInfo> getDynamicParamInfoList(SqlNode node) {
        List<DynamicParamInfo> params = new ArrayList<>();
        getDynamicParamIndex(node, params);
        return params;
    }

    private static void getDynamicParamIndex(SqlNode node, List<DynamicParamInfo> paramInfoList) {
        if (node instanceof SqlNodeList) {
            SqlNodeList nodes = (SqlNodeList) node;
            List<SqlNode> nodeList = nodes.getList();
            getDynamicParamIndex(paramInfoList, nodeList);
            return;
        }
        if (node instanceof SqlDynamicParam) {
            SqlDynamicParam dynamicParam = (SqlDynamicParam) node;
            int index = dynamicParam.getIndex();
            IndexedDynamicParamInfo indexedDynamicParamInfo = new IndexedDynamicParamInfo(index);
            indexedDynamicParamInfo.setDynamicKey(dynamicParam.getDynamicKey());
            paramInfoList.add(indexedDynamicParamInfo);
            return;
        }

        if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            List<SqlNode> sqlNodeList = Lists.newArrayList();
            if (call instanceof SqlCase) {
                for (int m = 0; m < ((SqlCase) call).getWhenOperands().size(); m++) {
                    sqlNodeList.add(((SqlCase) call).getWhenOperands().get(m));
                    sqlNodeList.add(((SqlCase) call).getThenOperands().get(m));
                }
                sqlNodeList.add(((SqlCase) call).getElseOperand());
            } else if (call.getOperator() instanceof SqlRuntimeFilterFunction) {
                paramInfoList.addAll(RuntimeFilterDynamicParamInfo
                    .fromRuntimeFilterId(((SqlRuntimeFilterFunction) call.getOperator()).getId()));
                sqlNodeList.addAll(call.getOperandList());
            } else if (call instanceof GroupConcatCall) {
                sqlNodeList.addAll(call.getOperandList());
                sqlNodeList.addAll(((GroupConcatCall) call).getOrderOperands());
            } else if (call instanceof SqlSelect) {
                SqlSelect sqlSelect = (SqlSelect) call;
                sqlNodeList.addAll(sqlSelect.getParameterizableOperandList());
            } else if (call instanceof SqlDelete) {
                final SqlDelete delete = (SqlDelete) call;
                if (!delete.singleTable()) {
                    sqlNodeList.addAll(delete.getParameterizableOperandList());
                } else {
                    sqlNodeList.addAll(call.getOperandList());
                }
            } else {
                sqlNodeList.addAll(call.getOperandList());
            }
            getDynamicParamIndex(paramInfoList, sqlNodeList);
            return;
        }
    }

    public static boolean atSingleGroup(List<String> shardColumns, RelShardInfo relShardInfo,
                                        boolean allowFalseCondition) {
        if (!relShardInfo.isUsePartTable()) {
            return atSingleGroup(shardColumns, relShardInfo.getAllComps(), allowFalseCondition);
        } else {
            return PartitionPruneStepUtil
                .onlyContainEqualCondition(shardColumns, relShardInfo.getPartPruneStepInfo(), allowFalseCondition);
        }
    }

    private static void getDynamicParamIndex(List<DynamicParamInfo> paramInfoList, List<SqlNode> nodeList) {
        for (SqlNode n : nodeList) {
            getDynamicParamIndex(n, paramInfoList);
        }
    }

    /**
     * 根据filter 条件判断拆分键所在列是否仅有等值条件与之对应
     */
    private static boolean atSingleGroup(List<String> shardColumns, Map<String, Comparative> comparatives,
                                         boolean allowFalseCondition) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(shardColumns));
        if (comparatives.isEmpty()) {
            return false;
        }

        for (String col : shardColumns) {
            Comparative c = comparatives.get(col);
            if (!comparativeIsAEqual(c, allowFalseCondition)) {
                return false;
            }
        }

        return true;
    }

    public static int guessShardCount(List<String> shardColumns, RelShardInfo relShardInfo,
                                      final int totalUpperBound) {
        if (!relShardInfo.isUsePartTable()) {
            return guessShardCount(shardColumns, relShardInfo.getAllComps(), totalUpperBound);
        } else {
            return PartitionPruneStepUtil
                .guessShardCount(shardColumns, relShardInfo.getPartPruneStepInfo(), totalUpperBound);
        }
    }

    private static int guessShardCount(List<String> shardColumns, Map<String, Comparative> comparatives,
                                       final int totalUpperBound) {
        if (shardColumns == null || shardColumns.isEmpty()) {
            return totalUpperBound;
        }
        if (comparatives.isEmpty()) {
            return totalUpperBound;
        }

        final int perColumnUpperBound = totalUpperBound / shardColumns.size();

        int calUpperBound = 0;

        for (String col : shardColumns) {
            Comparative c = comparatives.get(col);
            calUpperBound += guessComparativeShardCount(c, perColumnUpperBound);
        }

        return calUpperBound;
    }

    private static int guessComparativeShardCount(Comparative c, int upperBound) {
        if (c == null) {
            return upperBound;
        }

        if (c instanceof ComparativeAND) {
            int andCount = upperBound;
            for (Comparative operand : ((ComparativeAND) c).getList()) {
                andCount = Math.min(andCount, guessComparativeShardCount(operand, upperBound));
            }
            return andCount;
        } else if (c instanceof ComparativeOR) {
            int orCount = 0;
            for (Comparative operand : ((ComparativeOR) c).getList()) {
                orCount += guessComparativeShardCount(operand, upperBound);
            }
            return Math.min(orCount, upperBound);
        }

        if (c.getValue() instanceof Comparative || c instanceof ComparativeBaseList) {
            return upperBound;
        }

        if (c.getComparison() == Comparative.Equivalent) {
            return 1;
        }

        return upperBound;
    }

    /**
     * 判断 Comparative 是一个简单的等值比较
     */
    public static boolean comparativeIsASimpleEqual(Comparative c) {
        if (c.getValue() instanceof Comparative) {
            return false;
        }

        return c.getComparison() == Comparative.Equivalent;
    }

    public static boolean comparativeIsAEqual(Comparative c, boolean allowFalseCondition) {
        if (c == null) {
            return false;
        }

        if (c instanceof ComparativeAND && allowFalseCondition) {
            for (Comparative operand : ((ComparativeAND) c).getList()) {
                if (!comparativeIsAEqual(operand, allowFalseCondition)) {
                    return false;
                }
            }
            return true;
        }

        if (c.getValue() instanceof Comparative || c instanceof ComparativeBaseList) {
            return false;
        }

        if (c.getComparison() == Comparative.Equivalent) {
            return true;
        }

        return false;
    }

    /**
     * 判断是否有聚合函数带 distinct
     */
    public static boolean haveAggWithDistinct(List<AggregateCall> aggCalls) {
        for (AggregateCall call : aggCalls) {
            if (call.isDistinct() || call.filterArg > -1) {
                return true;
            }
        }
        return false;
    }

    public static List<Integer> buildNewGroupSet(LogicalAggregate agg) {
        List<AggregateCall> aggCalls = agg.getAggCallList();
        List<Integer> groupSet = Lists.newArrayList(agg.getGroupSet().asList());
        /**
         * 如果聚合函数有 distinct,仅支持一个聚合函数,其他均报错
         */
        if (haveAggWithDistinct(aggCalls)) {
            int aggCallSize = aggCallSizeIgnoreInternal(aggCalls);
            if (aggCallSize > 1) {
                Set<Integer> toAdd = Sets.newHashSet();

                int index = -1;
                for (AggregateCall aggregateCall : aggCalls) {
                    if (!aggregateCall.isDistinct()
                        && !aggregateCall.getAggregation().getKind().equals(SqlStdOperatorTable.__FIRST_VALUE)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_MORE_AGG_WITH_DISTINCT);
                    }
                    toAdd.addAll(aggregateCall.getArgList());
                    index++;
                }
                if (toAdd.size() == 1) {
                    /**
                     * 如果最后一个 GroupSet 已经包含该字段,则无需添加
                     */
                    List<Integer> argList = aggCalls.get(index).getArgList();
                    if (groupSet.size() > 0 && argList.size() == 1 && Util.last(groupSet) == argList.get(0)) {
                        // Nothing to do.
                    } else {
                        groupSet.addAll(argList);
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_MORE_AGG_WITH_DISTINCT,
                        "Only support one aggregation function with DISTINCT.");
                }
            } else if (aggCallSize == 1) {
                /**
                 * 如果最后一个 GroupSet 已经包含该字段,则无需添加
                 */
                for (AggregateCall call : aggCalls) {
                    if (call.getAggregation().getKind().equals(SqlKind.__FIRST_VALUE)) {
                        continue;
                    }
                    List<Integer> argList = call.getArgList();
                    if (groupSet.size() > 0 && argList.size() == 1 && Util.last(groupSet) == argList.get(0)) {
                        // Nothing to do.
                    } else {
                        groupSet.addAll(argList);
                    }
                }
            }
        }

        return groupSet;
    }

    public static boolean shouldNotPushDistinctAgg(LogicalAggregate agg, List<String> shardColumns) {
        List<AggregateCall> aggCalls = agg.getAggCallList();
        /**
         * 如果聚合函数有 distinct,仅支持一个聚合函数,其他均不下推
         */
        if (haveAggWithDistinct(aggCalls)) {
            int aggCallSize = aggCallSizeIgnoreInternal(aggCalls);
            if (aggCallSize > 1) {
                return true;
            }
        }
        return false;
    }

    public static boolean canSplitDistinct(Set<Integer> shardIndex, Aggregate aggregate) {
        boolean enableSplitDistinct = true;
        for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
            AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
            if (aggregateCall.isDistinct()) {
                final List<Integer> argList = aggregateCall.getArgList();
                final ImmutableBitSet distinctArg = ImmutableBitSet.builder()
                    .addAll(argList)
                    .build();
                final List<Integer> columns = aggregate.getGroupSet().union(distinctArg).toList();
                enableSplitDistinct = enableSplitDistinct && shardIndex.stream().allMatch(columns::contains);
            }
        }
        return enableSplitDistinct;
    }

    public static boolean isAllowedAggWithDistinct(LogicalAggregate agg, List<String> shardColumns) {
        List<AggregateCall> aggCalls = agg.getAggCallList();
        /**
         * 如果聚合函数有 distinct,仅支持一个聚合函数,其他均报错
         */
        if (haveAggWithDistinct(aggCalls)) {
            int aggCallSize = aggCallSizeIgnoreInternal(aggCalls);
            if (aggCallSize > 1) {
                return false;
//                throw new TddlRuntimeException(ErrorCode.ERR_MORE_AGG_WITH_DISTINCT,
//                    "Only support one aggregation function with DISTINCT.");
            } else if (aggCallSize == 1) {
                /**
                 * Check if aggCalls' args list contains full of shard columns.
                 */
                for (AggregateCall call : aggCalls) {
                    if (call.getAggregation().getKind().equals(SqlKind.__FIRST_VALUE)) {
                        continue;
                    }
                    /**
                     * do not push down distinct if group_concat has order list
                     * (size > 0)
                     */
                    if (call instanceof GroupConcatAggregateCall
                        && ((GroupConcatAggregateCall) call).getOrderList() != null
                        && ((GroupConcatAggregateCall) call).getOrderList().size() != 0) {
                        return false;
                    }
                    List<Integer> argsList = call.getArgList();
                    List<String> argsStringList = Lists.newArrayList();
                    for (Integer i : argsList) {
                        String fieldName = agg.getInput().getRowType().getFieldNames().get(i);
                        argsStringList.add(fieldName);
                    }
                    if (argsStringList.containsAll(shardColumns) && shardColumns.containsAll(argsStringList)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public static int aggCallSizeIgnoreInternal(List<AggregateCall> calls) {
        int i = 0;
        for (AggregateCall call : calls) {
            if (call.getAggregation().getKind().equals(SqlKind.__FIRST_VALUE)) {
                i++;
            }
        }
        return calls.size() - i;
    }

    public static Set<String> getGroupIntersection(List<List<TargetDB>> logicalTableGroupPhyTable) {
        return getGroupIntersection(logicalTableGroupPhyTable, null);
    }

    /**
     * get group intersection for LogicalView contains join skip broadcast table
     * by default
     */
    public static Set<String> getGroupIntersection(List<List<TargetDB>> logicalTableGroupPhyTable, String schemaName) {
        final Set<String> result = new HashSet<>();

        if (null == logicalTableGroupPhyTable || logicalTableGroupPhyTable.size() <= 0) {
            return result;
        }

        /**
         * init by first logical table
         */
        boolean allBroadcast = true;
        for (List<TargetDB> logicalTable : logicalTableGroupPhyTable) {
            // skip broadcast table
            if (!isBroadcast(logicalTable, schemaName)) {
                allBroadcast &= false;
                for (TargetDB groupPhyTable : logicalTable) {
                    result.add(groupPhyTable.getDbIndex());
                }
            }
        }

        if (allBroadcast) {
            if (logicalTableGroupPhyTable.get(0).size() > 0) {
                result.add(logicalTableGroupPhyTable.get(0).get(0).getDbIndex());
            }
            return result;
        }

        /**
         * get intersection
         */
        Iterator<String> it = result.iterator();
        while (it.hasNext()) {
            final String currentGroup = it.next();
            /**
             * currentGroup must exists in every logical table
             */
            for (int index = 1; index < logicalTableGroupPhyTable.size(); index++) {
                final List<TargetDB> grouPhyTableList = logicalTableGroupPhyTable.get(index);

                // skip broadcast table
                if (isBroadcast(grouPhyTableList, schemaName)) {
                    continue;
                }

                boolean exists = false;
                for (TargetDB targetDB : grouPhyTableList) {
                    if (TStringUtil.equals(currentGroup, targetDB.getDbIndex())) {
                        exists = true;
                        break;
                    }
                }

                if (!exists) {
                    it.remove();
                    break;
                }
            } // end of for
        } // end of while

        return result;
    }

    /**
     * clear groups not in groupFilter
     */
    public static List<List<TargetDB>> filterGroup(List<List<TargetDB>> logicalTableGroupPhyTable,
                                                   Set<String> groupFilter, String schemaName) {
        if (null == groupFilter || groupFilter.size() <= 0 || null == logicalTableGroupPhyTable) {
            return logicalTableGroupPhyTable;
        }

        for (List<TargetDB> groupPhyTables : logicalTableGroupPhyTable) {
            if (isBroadcast(groupPhyTables, schemaName)) {
                continue;
            }

            Iterator<TargetDB> it = groupPhyTables.iterator();
            while (it.hasNext()) {
                final TargetDB group = it.next();
                if (!groupFilter.contains(group.getDbIndex())) {
                    it.remove();
                    continue;
                }
            }
        }

        return logicalTableGroupPhyTable;
    }

    /**
     * if broadcast, add all groups
     */
    public static List<List<TargetDB>> fillGroup(List<List<TargetDB>> logicalTableGroupPhyTable, List<Group> groups,
                                                 TableRule tableRule) {
        if (tableRule == null) {
            return logicalTableGroupPhyTable;
        }
        if (!isSingleTable(tableRule) || (!tableRule.isBroadcast())) {
            return logicalTableGroupPhyTable;
        }
        if (logicalTableGroupPhyTable == null || groups == null) {
            return logicalTableGroupPhyTable;
        }
        if (logicalTableGroupPhyTable.size() == 0) {
            return logicalTableGroupPhyTable;
        }
        if (groups.size() == 0) {
            return logicalTableGroupPhyTable;
        }
        if (groups.size() == logicalTableGroupPhyTable.size()) {
            return logicalTableGroupPhyTable;
        }
        final List<TargetDB> targetDBS = logicalTableGroupPhyTable.get(0);
        if (targetDBS.size() > 1) {
            return logicalTableGroupPhyTable;
        }
        final TargetDB targetDB = targetDBS.get(0);
        if (targetDB == null) {
            return logicalTableGroupPhyTable;
        }

        List<List<TargetDB>> logicalTableGroupPhyTableReturn = new ArrayList<>();
        final List<TargetDB> objects = Lists.newArrayList();
        for (Group group : groups) {
            TargetDB targetDBTemp = new TargetDB();
            targetDBTemp.setDbIndex(group.getName());
            targetDBTemp.setTableNames(targetDB.getTableNameMap());
            objects.add(targetDBTemp);
        }
        logicalTableGroupPhyTableReturn.add(objects);
        return logicalTableGroupPhyTableReturn;
    }

    public static RelDataType deriveJoinType(RelDataTypeFactory typeFactory, RelDataType leftType,
                                             RelDataType rightType) {
        List<RelDataType> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        addFields(leftType.getFieldList(), typeList, nameList);
        addFields(rightType.getFieldList(), typeList, nameList);

        return typeFactory.createStructType(typeList, nameList);
    }

    private static void addFields(List<RelDataTypeField> fieldList, List<RelDataType> typeList, List<String> nameList) {
        for (RelDataTypeField field : fieldList) {
            String name = field.getName();
            nameList.add(name);
            typeList.add(field.getType());
        }
    }

    /**
     * Build shard columns comparative for table by the filter condition
     */
    public static Map<String, Comparative> buildComparative(Map<String, Comparative> comparatives, RexNode rexNode,
                                                            RelDataType rowType, String tableName, String schemaName,
                                                            ExecutionContext executionContext) {
        TddlRuleManager or = executionContext.getSchemaManager(schemaName).getTddlRuleManager();
        OptimizerContext oc = OptimizerContext.getContext(schemaName);
        List<String> shardColumns = or.getSharedColumns(tableName);
        return buildColumnsComparative(comparatives, rexNode, rowType, shardColumns, oc.getPartitioner());
    }

    public static Map<String, Comparative> buildColumnsComparative(Map<String, Comparative> comparatives,
                                                                   RexNode rexNode,
                                                                   RelDataType rowType, List<String> columns,
                                                                   Partitioner partitioner) {
        for (String column : columns) {
            Comparative curComp = partitioner.getComparative(rexNode, rowType, column, null);
            if (curComp == null) {
                continue;
            }

            Comparative originComp = comparatives.get(column);
            if (originComp != null) {
                ComparativeBaseList newComp = new ComparativeAND(originComp);
                newComp.addComparative(curComp);
                comparatives.put(column, newComp);
            } else {
                comparatives.put(column, curComp);
            }
        }
        return comparatives;
    }

    /**
     * Build shard columns comparative for table by the filter condition build
     * once time only,before execute
     */
    public static Map<String, Comparative> buildComparativeWithAllColumn(RexNode rexNode, RelDataType rowType,
                                                                         String tableName, String schemaName) {
        Map<String, Comparative> comparatives = new HashMap<>();
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        ExtPartitionOptimizerRule orExt = new ExtPartitionOptimizerRule(or.getTddlRule());
        List<String> shardColumns = or.getSharedColumns(tableName);
        final HashSet<String> stringColumns = Sets.newHashSet(shardColumns);
        final Comparative comparative = orExt.getComparative(rexNode, rowType, stringColumns, null);
        if (comparative != null) {
            for (String column : shardColumns) {
                comparatives.put(column, comparative);
            }
        }
        return comparatives;
    }

    /**
     * Build shard columns comparative for table by the filter condition build
     * once time only,before execute
     */
    public static Map<String, Comparative> buildComparativeWithAllColumn(Map<String, Comparative> comparativeParams,
                                                                         RexNode rexNode, RelDataType rowType,
                                                                         String tableName, String schemaName,
                                                                         ExecutionContext executionContext) {
        Map<String, Comparative> comparatives = new HashMap<>();
        TddlRuleManager or = executionContext.getSchemaManager(schemaName).getTddlRuleManager();
        ExtPartitionOptimizerRule orExt = new ExtPartitionOptimizerRule(or.getTddlRule());
        List<String> shardColumns = or.getSharedColumns(tableName);
        final HashSet<String> stringColumns = Sets.newHashSet(shardColumns);
        final Comparative comparative = orExt.getComparative(rexNode, rowType, stringColumns, null);

        if (comparative != null) {
            for (String column : shardColumns) {
                Comparative originComp = comparativeParams.get(column);
                if (originComp != null) {
                    ComparativeBaseList newComp = new ComparativeAND(originComp);
                    newComp.addComparative(comparative);
                    comparatives.put(column, newComp);
                } else {
                    comparatives.put(column, comparative);
                }
            }
        }

        return comparatives;
    }

    /**
     * Build new param.
     */
    public static Map<Integer, ParameterContext> buildParam(List<String> tableNames,
                                                            Map<Integer, ParameterContext> param,
                                                            List<Integer> paramIndex) {
        Map<Integer, ParameterContext> newParam = new HashMap<>();
        /**
         * Add tableName
         */
        int index = 1;
        int tableIndex = -1;
        for (int i : paramIndex) {
            if (i == TABLE_NAME_PARAM_INDEX) {
                if (ConfigDataMode.isFastMock()) {
                    continue;
                }
                tableIndex += 1;
                newParam.put(index, buildParameterContextForTableName(tableNames.get(tableIndex), index));
            } else if (i == SCALAR_SUBQUERY_PARAM_INDEX) {
                // do nothing
            } else if (i == APPLY_SUBQUERY_PARAM_INDEX) {
                // do nothing
            } else {
                newParam.put(index, changeParameterContextIndex(param.get(i + 1), index));
            }
            index++;
        }
        return newParam;
    }

    public static Map<Integer, ParameterContext> buildBatchParam(List<String> tableNames,
                                                                 List<Map<Integer, ParameterContext>> batchParams,
                                                                 List<Integer> paramIndex) {
        Map<Integer, ParameterContext> newParam = new HashMap<>();
        /**
         * Add tableName
         */
        int index = 1;

        for (Map<Integer, ParameterContext> param : batchParams) {
            int tableIndex = -1;
            for (int i : paramIndex) {
                if (i == TABLE_NAME_PARAM_INDEX) {
                    if (ConfigDataMode.isFastMock()) {
                        continue;
                    }
                    tableIndex += 1;
                    newParam.put(index, buildParameterContextForTableName(tableNames.get(tableIndex), index));
                } else if (i == SCALAR_SUBQUERY_PARAM_INDEX) {
                    // do nothing
                } else if (i == APPLY_SUBQUERY_PARAM_INDEX) {
                    // do nothing
                } else {
                    newParam.put(index, changeParameterContextIndex(param.get(i + 1), index));
                }
                index++;
            }
        }
        return newParam;
    }

    public static Map<Integer, ParameterContext> buildParam(List<String> tableNames,
                                                            List<String> referencedTableNames) {
        Map<Integer, ParameterContext> newParam = new HashMap<>();

        int tableCount = tableNames.size();
        int refTableCount = referencedTableNames.size();

        for (int index = 0; index < tableCount; index++) {
            if (ConfigDataMode.isFastMock()) {
                continue;
            }
            newParam.put(index + 1, buildParameterContextForTableName(tableNames.get(index), index + 1));
        }

        for (int index = 0; index < refTableCount; index++) {
            if (ConfigDataMode.isFastMock()) {
                continue;
            }
            newParam.put(index + tableCount + 1,
                buildParameterContextForTableName(referencedTableNames.get(index), index + tableCount + 1));
        }

        return newParam;
    }

    public static Map<String, DataType> buildDataType(List<String> columns, TableMeta tm) {
        Map<String, DataType> dataTypes = new HashMap<>();
        for (String col : columns) {
            dataTypes.put(col, tm.getColumn(col).getDataType());
        }
        return dataTypes;
    }

    public static boolean tableRuleIsIdentical(TableRule ltr, TableRule rtr) {
        return shardingEquals(ltr, rtr, false);
    }

    /**
     * <pre>
     * 判断两个分表规则的拆分方式（或路由算法）是否完全相等(这里通常忽略拆分键的列名)
     *
     * 通常用于判断JOIN的左右两表是否使用相同的拆分方式，然后判断能否JOIN下推
     *
     * </pre>
     *
     * @param ltr 左表的规则
     * @param rtr 右表的规则
     * @return onlyCompareDbSharding 是否仅比较两个表的分库的拆分方式
     */
    public static boolean shardingEquals(TableRule ltr, TableRule rtr, boolean onlyCompareDbSharding) {

        if (ltr == null || rtr == null) {

            if (ltr == null && rtr == null) {
                // both are single tb of single db
                return true;
            } else if (ltr == null && rtr != null && !rtr.isBroadcast()) {
                if (rtr.getDbCount() == 1 && rtr.getTbCount() == 1) {
                    // right tb is a single tb
                    return true;
                }
            } else if (ltr != null && !ltr.isBroadcast() && rtr == null) {
                if (ltr.getDbCount() == 1 && ltr.getTbCount() == 1) {
                    // left tb is a single tb
                    return true;
                }
            }
            return false;
        }

        if (!StringUtils.equals(ltr.getDbNamePattern(), rtr.getDbNamePattern())) {
            return false;
        }

        // 检测是否有一侧为广播表
        if (ltr.isBroadcast() != rtr.isBroadcast()) {

            if (onlyCompareDbSharding) {

                if (ltr.isBroadcast() && rtr.getTbCount() == 1 && rtr.getDbCount() == 1) {
                    // one is broaccast tb and the other tb is single tb
                    return false;
                } else if (rtr.isBroadcast() && ltr.getTbCount() == 1 && rtr.getDbCount() == 1) {
                    // one is broaccast tb and the other tb is single tb
                    return false;
                }
            }

            return false;
        }

        // join group 不同
        if (!TStringUtil.equalsIgnoreCase(ltr.getJoinGroup(), rtr.getJoinGroup())) {
            return false;
        }

        // 分库总数不同
        if ((ltr.getDbCount() != rtr.getDbCount()) || ltr.getDbCount() == 0) {
            return false;
        }

        if (!onlyCompareDbSharding) {
            // 如果仅仅比较两表规则的分库的拆分算法，以下信息不用校验

            // 分表总数不同
            if ((ltr.getTbCount() != rtr.getTbCount()) || ltr.getTbCount() == 0) {
                return false;
            }

            // 比较拆分键的字段数目
            int shardColumnCount = ltr.getShardColumns().size();
            if (!(shardColumnCount == rtr.getShardColumns().size() && shardColumnCount > 0)) {
                return false;
            }
        }

        // 检测是否为简单规则, 如果是复杂的groovy规则，则对比其它内容
        boolean isLtrSimple = ltr.isSimple();
        boolean isRtrSimple = rtr.isSimple();
        if (isLtrSimple != isRtrSimple) {
            return false;
        }

        // 如果两者都不是简单规则
        if (isLtrSimple == false) {

            // 检测是否为 分库规则是否为新型的ShardFunctionMeta规则
            ShardFunctionMeta dbShardFuncMetaOfLtr = ltr.getDbShardFunctionMeta();
            ShardFunctionMeta dbShardFuncMetaOfRtr = rtr.getDbShardFunctionMeta();

            if (dbShardFuncMetaOfLtr == null && dbShardFuncMetaOfRtr != null) {
                return false;
            } else if (dbShardFuncMetaOfLtr != null && dbShardFuncMetaOfRtr == null) {
                return false;
            } else if (dbShardFuncMetaOfLtr != null && dbShardFuncMetaOfRtr != null) {
                if (!dbShardFuncMetaOfLtr.isShardingEquals(dbShardFuncMetaOfRtr)) {
                    return false;
                }
            } else {
                if (!StringUtils.equalsIgnoreCase(ltr.getDbGroovyShardMethodName(), rtr.getDbGroovyShardMethodName())) {
                    return false;
                }
            }

            if (!onlyCompareDbSharding) {

                // 检测是否为 分库规则是否为新型的ShardFunctionMeta规则
                ShardFunctionMeta tbShardFuncMetaOfLtr = ltr.getTbShardFunctionMeta();
                ShardFunctionMeta tbShardFuncMetaOfRtr = rtr.getTbShardFunctionMeta();

                if (tbShardFuncMetaOfLtr == null && tbShardFuncMetaOfRtr != null) {
                    return false;
                } else if (tbShardFuncMetaOfLtr != null && tbShardFuncMetaOfRtr == null) {
                    return false;
                } else if (tbShardFuncMetaOfLtr != null && tbShardFuncMetaOfRtr != null) {
                    if (!tbShardFuncMetaOfLtr.isShardingEquals(tbShardFuncMetaOfRtr)) {
                        return false;
                    }
                } else {
                    if (!StringUtils.equalsIgnoreCase(ltr.getTbGroovyShardMethodName(),
                        rtr.getTbGroovyShardMethodName())) {
                        return false;
                    }
                }
            }

        } else {

            // 如果是简单规则，检测它的拆分键的枚举类型
            if (ltr.getPartitionType() != rtr.getPartitionType()) {
                return false;
            }
        }

        final List<MappingRule> extPartitions1 = ltr.getExtPartitions();
        final List<MappingRule> extPartitions2 = rtr.getExtPartitions();
        // exist not empty list at least
        if (!(extPartitions1 == null && extPartitions2 == null)) {
            if (extPartitions1 == null || extPartitions2 == null) {
                return false;
            }
            if (!ListUtils.isEqualList(extPartitions1, extPartitions2)) {
                return false;
            }
        }

        return true;
    }

    public static boolean isSingle(Map<String, List<List<String>>> targetTable) {
        return tableCount(targetTable) <= 1;
    }

    public static int tableCount(Map<String, List<List<String>>> targetTable) {
        int result = 0;

        for (Entry<String, List<List<String>>> entry : targetTable.entrySet()) {
            result += entry.getValue().size();
        }

        return result;
    }

    public static SqlNode buildOrTree(List<SqlNode> subClauses) {
        return buildTree(subClauses, SqlStdOperatorTable.OR);
    }

    public static SqlNode buildAndTree(List<SqlNode> subClauses) {
        return buildTree(subClauses, SqlStdOperatorTable.AND);
    }

    public static SqlNode buildTree(List<SqlNode> subClauses, SqlOperator operator) {
        if (subClauses.size() == 0) {
            return null;
        }
        if (subClauses.size() == 1) {
            return subClauses.get(0);
        }
        if (subClauses.size() == 2) {
            return new SqlBasicCall(operator, subClauses.toArray(new SqlNode[2]), SqlParserPos.ZERO);
        }
        SqlNode subTree = buildTree(subClauses.subList(1, subClauses.size()), operator);
        return new SqlBasicCall(operator, new SqlNode[] {subClauses.get(0), subTree}, SqlParserPos.ZERO);
    }

    public static List<RexNode> mergeProject(Project topProject, Project bottomProject, RelOptRuleCall call) {
        final RelBuilder relBuilder = call.builder();
        return mergeProject(topProject, bottomProject, relBuilder);

    }

    public static List<RexNode> mergeProject(Project topProject, Project bottomProject, RelBuilder relBuilder) {
        // If one or both projects are permutations, short-circuit the complex logic
        // of building a RexProgram.
        final Permutation topPermutation = topProject.getPermutation();
        if (topPermutation != null) {
            if (topPermutation.isIdentity()) {
                // Let ProjectRemoveRule handle this.
                return null;
            }
            final Permutation bottomPermutation = bottomProject.getPermutation();
            if (bottomPermutation != null) {
                if (bottomPermutation.isIdentity()) {
                    // Let TableLookupRemoveRule handle this.
                    return null;
                }
                final Permutation product = topPermutation.product(bottomPermutation);

                // replace the two projects with a combined projection
                return relBuilder.fields(product);
            }
        }

        return RelOptUtil.pushPastProject(topProject.getProjects(), bottomProject);
    }

    /**
     * Walks over an expression, copying every node, and fully-qualifying every
     * identifier.
     */
    public static class DeepCopier extends SqlShuttle {

        /**
         * Copies a SqlNode.
         */
        public static SqlNode copy(SqlNode origin) {
            // noinspection deprecation
            return origin.accept(new DeepCopier());
        }

        @Override
        public SqlNode visit(SqlNodeList list) {
            SqlNodeList copy = new SqlNodeList(list.getParserPosition());
            for (SqlNode node : list) {
                copy.add(node.accept(this));
            }
            return copy;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler(call, true);
            call.getOperator().acceptCall(this, call, false, argHandler);
            return argHandler.result();
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            return SqlNode.clone(literal);
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            return SqlNode.clone(id);
        }

        @Override
        public SqlNode visit(SqlDataTypeSpec type) {
            return SqlNode.clone(type);
        }

        @Override
        public SqlNode visit(SqlDynamicParam param) {
            return SqlNode.clone(param);
        }

        @Override
        public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
            return SqlNode.clone(intervalQualifier);
        }
    }

}

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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * 优化器中使用Tddl Rule的一些工具方法，需要依赖{@linkplain TddlRule}自己先做好初始化
 *
 * @since 5.0.0
 */
public class TddlRuleManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(TddlRuleManager.class);

    protected static final String TABLE_PATTERN_FORMAT = "(.*)(\\_(\\d+))";       // 匹配_数字
    protected static final Pattern TABLE_PATTERN = Pattern.compile(TABLE_PATTERN_FORMAT);

    static final Map<SqlKind, Integer> COMPARATIVE_MAP = new HashMap<>(8);

    static {
        TddlRuleManager.COMPARATIVE_MAP.put(SqlKind.EQUALS, Comparative.Equivalent);
        TddlRuleManager.COMPARATIVE_MAP.put(SqlKind.NOT_EQUALS, Comparative.NotEquivalent);
        TddlRuleManager.COMPARATIVE_MAP.put(SqlKind.GREATER_THAN, Comparative.GreaterThan);
        TddlRuleManager.COMPARATIVE_MAP.put(SqlKind.GREATER_THAN_OR_EQUAL, Comparative.GreaterThanOrEqual);
        TddlRuleManager.COMPARATIVE_MAP.put(SqlKind.LESS_THAN, Comparative.LessThan);
        TddlRuleManager.COMPARATIVE_MAP.put(SqlKind.LESS_THAN_OR_EQUAL, Comparative.LessThanOrEqual);
    }

    private final TddlRule tddlRule;
    private List<String> groupNames = null;
    private final String schemaName;
    private PartitionInfoManager partitionInfoManager;
    private TableGroupInfoManager tableGroupInfoManager;

    public TddlRuleManager(TddlRule tddlRule, PartitionInfoManager partitionInfoManager,
                           TableGroupInfoManager tableGroupInfoManager, String schemaName) {
        this.tddlRule = tddlRule;
        this.schemaName = schemaName;
        this.partitionInfoManager = partitionInfoManager;
        this.tableGroupInfoManager = tableGroupInfoManager;
    }

    public Collection<TableRule> getTableRules() {
        return tddlRule.getTables();
    }

    @Override
    protected void doInit() {
        if (tddlRule != null && !tddlRule.isInited()) {
            tddlRule.init();
        }

        if (partitionInfoManager != null && !partitionInfoManager.isInited()) {
            partitionInfoManager.init();
        }

        if (tableGroupInfoManager != null && !tableGroupInfoManager.isInited()) {
            tableGroupInfoManager.init();
        }
    }

    /**
     * 为了可以让CostBaedOptimizer可以订阅tddlconfig的改变所以暴露
     */
    public TddlRule getTddlRule() {
        return tddlRule;
    }

    public String getDefaultDbIndex() {
        if (!DbInfoManager.getInstance().isNewPartitionDb(this.schemaName)) {
            return this.tddlRule.getDefaultDbIndex();
        } else {
            return this.partitionInfoManager.getDefaultDbIndex();
        }
    }

    @Override
    protected void doDestroy() {

        if (tddlRule != null && tddlRule.isInited()) {
            tddlRule.destroy();
        }

        if (partitionInfoManager != null && partitionInfoManager.isInited()) {
            partitionInfoManager.destroy();
        }

        if (tableGroupInfoManager != null && tableGroupInfoManager.isInited()) {
            tableGroupInfoManager.destroy();
        }
    }

    /**
     * 根据逻辑表返回一个随机的物理目标库TargetDB
     */
    public TargetDB shardAny(String logicTable) {

        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            PhysicalPartitionInfo prunedPartitionInfo = partitionInfoManager.getFirstPhysicalPartition(logicTable);
            TargetDB target = new TargetDB();
            target.setDbIndex(prunedPartitionInfo.getGroupKey());
            target.addOneTable(prunedPartitionInfo.getPhyTable());
            return target;
        }

        TableRule tableRule = getTableRule(logicTable);
        if (tableRule == null) {
            System.out.println(logicTable + " rule==null");
            // 设置为同名，同名不做转化
            TargetDB target = new TargetDB();
            target.setDbIndex(getDefaultDbIndex(logicTable));
            target.addOneTable(logicTable);
            return target;
        } else {
            Map<String, Set<String>> topologys = tableRule.getStaticTopology();
            if (topologys == null || topologys.size() == 0) {
                topologys = tableRule.getActualTopology();
            }

            for (String group : topologys.keySet()) {
                Set<String> tableNames = topologys.get(group);
                if (tableNames == null || tableNames.isEmpty()) {
                    continue;
                }

                TargetDB target = new TargetDB();
                target.setDbIndex(group);
                target.addOneTable(tableNames.iterator().next());
//                if (ConfigDataMode.isFastMock()) {
//                    for (String tableName : target.getTableNames()) {
//                        MockDataManager.phyTableToLogicalTableName.put(tableName, logicTable);
//                    }
//                }
                return target;
            }
        }
        throw new IllegalArgumentException("can't find any target db. table is " + logicTable + ". ");
    }

    /**
     * 判断一下逻辑表是否是一张物理单库单表
     */
    public boolean isTableInSingleDb(String logicTable) {

        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            return partitionInfoManager.isSingleTable(logicTable);
        }

        TableRule tableRule = getTableRule(logicTable);
        if (tableRule != null && tableRule.isBroadcast()) {
            return false;
        }
        if (tableRule == null
            || (GeneralUtil.isEmpty(tableRule.getDbShardRules()) && GeneralUtil.isEmpty(tableRule.getTbShardRules()))) {
            // 判断是否是单库单表
            return true;
        }

        return false;
    }

    public String getDefaultDbIndex(String logicalTable) {

        String dbName = this.schemaName;
        if (DbInfoManager.getInstance().isNewPartitionDb(dbName)) {
            if (logicalTable == null || partitionInfoManager.isBroadcastTable(logicalTable)) {
                return partitionInfoManager.getDefaultDbIndex();
            } else {
                PhysicalPartitionInfo prunedPartitionInfo =
                    partitionInfoManager.getFirstPhysicalPartition(logicalTable);
                if (prunedPartitionInfo != null) {
                    return prunedPartitionInfo.getGroupKey();
                }
            }
        }
        String defaultDb = tddlRule.getDefaultDbIndex(logicalTable);
        if (defaultDb == null) {
            if (logicalTable == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_DEFAULT_DB_INDEX_IS_NULL);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_NO_RULE, logicalTable);
            }
        }
        return defaultDb;
    }

    public boolean isBroadCast(String logicTable) {
        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            return partitionInfoManager.isBroadcastTable(logicTable);
        }

        TableRule table = getTableRule(logicTable);
        return table != null ? table.isBroadcast() : false;// 没找到表规则，默认为单库，所以不是广播表
    }

    public List<String> getActualSharedColumns(String logicTable) {
        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            if (partitionInfoManager.isPartitionedTable(logicTable)) {
                PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(logicTable);
                return PartitionInfoUtil.getAllLevelActualPartColumnsAsNoDuplicatedList(partInfo);
            } else {
                return new ArrayList<>();
            }
        } else {
            return getSharedColumns(logicTable);
        }
    }

    public List<String> getSharedColumns(String logicTable) {

        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            if (partitionInfoManager.isPartitionedTable(logicTable)) {
                PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(logicTable);
                return partInfo.getPartitionColumnsNotReorder();
            } else {
                return new ArrayList<>();
            }
        }

        TableRule tableRule = getTableRule(logicTable);

        List<String> shardColumns;
        if (!(TddlRuleManager.isSingleTable(tableRule) || tableRule.isBroadcast())) {
            shardColumns = tableRule.getShardColumns();
        } else {
            shardColumns = new ArrayList<>();
        }
        return shardColumns;
        //return table != null ? table.getShardColumns() : new ArrayList<String>();// 没找到表规则，默认为单库
    }

    public List<String> getSharedColumnsForGsi(String logicTable) {

        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(logicTable);
            return partInfo.getPartitionColumns();
        }

        TableRule tableRule = getTableRule(logicTable);

        List<String> shardColumns;
        if (!(TddlRuleManager.isSingleTable(tableRule) || tableRule.isBroadcast())) {
            shardColumns = tableRule.getShardColumns();
        } else {
            shardColumns = new ArrayList<>();
        }
        return shardColumns;
    }

    public TableRule getTableRule(String logicTable) {
        return tddlRule.getTable(logicTable);
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

    /**
     * 将defaultDb上的表和规则中的表做一次合并
     */
    public Set<String> mergeTableRule(List<String> defaultDbTables) {
        Set<String> result = new HashSet<String>();
        Collection<TableRule> tableRules = tddlRule.getTables();

        Map<String, String> dbIndexMap = tddlRule.getDbIndexMap();

        // // 添加下分库分表数据
        for (TableRule tableRule : tableRules) {
            String table = tableRule.getVirtualTbName();
            // 针对二级索引的表名，不加入到tables中
            if (!StringUtils.contains(table, "._")) {
                result.add(table);
            }
        }

        for (Map.Entry<String, String> entry : dbIndexMap.entrySet()) {
            // 针对二级索引的表名，不加入到tables中
            if (!StringUtils.contains(entry.getKey(), "._")) {
                result.add(entry.getKey());
            }
        }

        return mergeTableRule(result, tableRules, dbIndexMap, defaultDbTables, false);
    }

    public Set<String> mergeTableRule(
        Set<String> result, Collection<TableRule> tableRules, Map<String, String> dbIndexMap,
        List<String> defaultDbTables,
        boolean strict) {
        // If there is no tables from default physical database, then we need to do nothing.
        if (defaultDbTables == null || defaultDbTables.isEmpty()) {
            return result;
        }

        Map<TableRule, Map<String, Set<String>>> tableRuleTopology = new HashMap<>();
        if (defaultDbTables.size() > 0) {
            for (TableRule tableRule : tableRules) {
                Map<String, Set<String>> caseInsensitiveTopology = new HashMap<>();
                for (String key : tableRule.getActualTopology().keySet()) {
                    Set<String> tables = tableRule.getActualTopology().get(key);
                    Set<String> s = new TreeSet<String>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                    s.addAll(tables);
                    caseInsensitiveTopology.put(key, s);
                }
                tableRuleTopology.put(tableRule, caseInsensitiveTopology);
            }
        }

        // 过滤掉分库分表
        for (String table : defaultDbTables) {
            boolean found = false;
            for (TableRule tableRule : tableRules) {
                if (isActualTable(table, tableRuleTopology.get(tableRule))) {
                    found = true;
                    break;
                }
            }

            if (dbIndexMap.containsKey(table)) {
                found = true;
            }

            if (!found) {
                if (strict) {
                    //严格过滤掉未配置路由规则的分库分表
                    if (TABLE_PATTERN.matcher(table).matches()) {
                        continue;
                    }
                }
                result.add(table);
            }
        }
        return result;
    }

    private boolean isActualTable(String actualTable, Map<String, Set<String>> topology) {
        if (actualTable == null) {
            return false;
        }

        for (Set<String> tables : topology.values()) {
            if (tables.contains(actualTable)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 判断一下规则中是否只有一个db库
     */
    public boolean isSingleDbIndex() {

        if (ConfigDataMode.isFastMock()) {
            return false;
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(this.schemaName)) {
            return false;
        }

        Collection<TableRule> tableRules = tddlRule.getTables();
        Map<String, String> dbIndexMap = tddlRule.getDbIndexMap();
        // 没有任何规则
        if (tableRules.isEmpty() && dbIndexMap.isEmpty()) {
            return true;
        }

        String defaultDbIndex = tddlRule.getDefaultDbIndex();
        if (!tableRules.isEmpty()) {
            for (TableRule table : tableRules) {
                // 逻辑表名应该和物理表名相等
                if (table.getTbNamePattern() != null) {
                    if (!TStringUtil.equalsIgnoreCase(table.getVirtualTbName(), table.getTbNamePattern())) {
                        return false;
                    }
                }

                // 每个表的库名都应该和defaultDbIndex相等
                if (table.getDbNamePattern() != null) {
                    if (!TStringUtil.equals(table.getDbNamePattern(), defaultDbIndex)) {
                        return false;
                    }
                }

                if (!(GeneralUtil.isEmpty(table.getDbShardRules()) && GeneralUtil.isEmpty(table.getTbShardRules()))) {
                    return false;
                }

                // 如果有配置广播表，则也不认为他是单库单表的
                if (table.isBroadcast()) {
                    return false;
                }
            }
        }

        if (!dbIndexMap.isEmpty()) {
            for (String dbIndex : dbIndexMap.values()) {
                if (defaultDbIndex == null) {
                    defaultDbIndex = dbIndex;
                } else if (!defaultDbIndex.equals(dbIndex)) {
                    // 出现不同的库
                    return false;
                }
            }
        }
        Group defaultGroup = OptimizerContext.getContext(schemaName).getMatrix().getGroup(defaultDbIndex);
        if (defaultGroup != null && GroupType.MYSQL_JDBC.equals(defaultGroup.getType())) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 是否存在分库或分表规则
     */
    public boolean isShard(String logicTable) {
        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            return partitionInfoManager.isPartitionedTable(logicTable);
        }

        TableRule tableRule = getTableRule(logicTable);
        if (tableRule != null
            && (GeneralUtil.isNotEmpty(tableRule.getTbShardRules())
            || GeneralUtil.isNotEmpty(tableRule.getDbShardRules()))) {
            // 分库或分表规则存在且不为空
            return true;
        } else {
            return false;
        }
    }

    public boolean needCheckIfExistsGsi(String logicTable) {
        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            return true;
        } else {
            TableRule tableRule = getTableRule(logicTable);
            if (tableRule == null) {
                return false;
            }
            return true;
        }
    }

    public boolean isTddlShardedTable(String logicTable) {
        if (partitionInfoManager.isPartitionedTable(logicTable)) {
            return false;
        }

        TableRule tableRule = getTableRule(logicTable);
        if (tableRule != null
            && (GeneralUtil.isNotEmpty(tableRule.getTbShardRules())
            || GeneralUtil.isNotEmpty(tableRule.getDbShardRules()))) {
            // 分库或分表规则存在且不为空
            return true;
        } else {
            return false;
        }
    }

    public boolean checkTableExists(String logicTable) {
        /**
         * Check if logicTable a partitioned table
         */
        if (partitionInfoManager.isNewPartDbTable(logicTable)) {
            return true;
        }

        /**
         * logicTable is NOT a new partitioned table, so check its tableRule
         */
        TableRule tableRule = getTableRule(logicTable);
        return tableRule != null;
    }

    public boolean isShardOrBroadCast(String logicTable) {
        if (isShard(logicTable)) {
            return true;
        }
        if (isBroadCast(logicTable)) {
            return true;
        }
        return false;
    }

    /**
     * 用来给CreateTable DDL做新Rule上推的参考用的
     */
    public Map<String, String> getRuleStrs() {
        if (tddlRule != null) {
            return tddlRule.getCurrentRuleStrMap();
        }
        return null;
    }

    public Set<String> getLogicalTableNames(String fullyQualifiedPhysicalTableName, String schemaName) {
        if (groupNames == null) {
            List<Group> groups = OptimizerContext.getContext(schemaName).getMatrix().getGroups();
            List<String> newGroupNames = new ArrayList<>(groups.size());
            for (Group group : groups) {
                if (group.getType() == GroupType.MYSQL_JDBC
                    && !TStringUtil.equalsIgnoreCase(group.getName(), "DUAL_GROUP")) {
                    newGroupNames.add(group.getName());
                }
            }
            groupNames = newGroupNames;
        }
        return tddlRule.getLogicalTableNames(fullyQualifiedPhysicalTableName, groupNames);
    }

    public boolean containExtPartitions(String logicalTable) {

        if (partitionInfoManager.isNewPartDbTable(logicalTable)) {
            return false;
        }

        TableRule tr = getTableRule(logicalTable);
        if (null != tr && tr.getExtPartitions() != null && tr.getExtPartitions().size() > 0) {
            return true;
        }
        return false;
    }

    public PartitionInfoManager getPartitionInfoManager() {
        return partitionInfoManager;
    }

    public TableGroupInfoManager getTableGroupInfoManager() {
        return tableGroupInfoManager;
    }

    public List<TargetDB> shard(String logicTable, boolean isWrite, boolean forceAllowFullTableScan,
                                Map<String, Comparative> comparatives, Map<Integer, ParameterContext> param,
                                Map<String, Object> calcParams, ExecutionContext ec) {
        Partitioner partitioner = OptimizerContext.getContext(this.schemaName).getPartitioner();
        return partitioner.shard(logicTable, isWrite, forceAllowFullTableScan, comparatives, param, calcParams, ec);
    }

    public String getSchemaName() {
        return schemaName;
    }
}

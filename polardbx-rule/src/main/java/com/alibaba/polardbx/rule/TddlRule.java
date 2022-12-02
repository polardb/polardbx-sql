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

package com.alibaba.polardbx.rule;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeMapChoicer;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.rule.exception.RouteCompareDiffException;
import com.alibaba.polardbx.rule.exception.TddlRuleException;
import com.alibaba.polardbx.rule.gms.TddlRuleGmsConfig;
import com.alibaba.polardbx.rule.model.Field;
import com.alibaba.polardbx.rule.model.MatcherResult;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.ComparativeStringAnalyser;
import com.alibaba.polardbx.rule.utils.MatchResultCompare;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.common.eagleeye.EagleeyeHelper.TEST_TABLE_PREFIX;
import static com.alibaba.polardbx.rule.VirtualTableRoot.testTableRuleMap;

/**
 * 类名取名兼容老的rule代码<br/>
 * 结合tddl的动态规则管理体系，获取对应{@linkplain VirtualTableRule}
 * 规则定义，再根据sql中condition或者是setParam()提交的参数计算出路由规则 {@linkplain MatcherResult}
 * <p>
 * <pre>
 * condition简单语法： KEY CMP VALUE [:TYPE]
 * 1. KEY： 类似字段名字，用户随意定义
 * 2. CMP： 链接符，比如< = > 等，具体可查看{@linkplain Comparative}
 * 3. VALUE: 对应的值，比如1
 * 4. TYPE: 描述VALUE的类型，可选型，如果不填默认为Long类型。支持: int/long/string/date，可以使用首字母做为缩写，比如i/l/s/d。
 *
 * 几个例子：
 * 1. id = 1
 * 2. id = 1 : long
 * 3. id > 1 and id < 1 : long
 * 4. gmt_create = 2011-11-11 : date
 * 5. id in (1,2,3,4) : long
 * </pre>
 *
 * @author jianghang 2013-11-5 下午8:11:43
 * @since 5.0.0
 */
public class TddlRule extends TddlRuleGmsConfig implements TddlTableRule {

    private VirtualTableRuleMatcher matcher = new VirtualTableRuleMatcher();

    // These are shadow tables (__test_xxx) being created for EagleEye.
    private Map<String, String> physicalTableNamePrefixesForShadowTables = new ConcurrentHashMap<>();

    @Override
    public void doInit() {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TddlRule start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + this.appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + this.unitName);

        super.doInit();
    }

    @Override
    public MatcherResult route(String vtab, String condition) {
        return route(vtab, condition, super.getCurrentRule());
    }

    @Override
    public MatcherResult route(String vtab, String condition, String version) {
        return route(vtab, condition, super.getVersionRule(version));
    }

    @Override
    public MatcherResult route(String vtab, String condition, VirtualTableRoot specifyVtr) {
        return route(vtab, generateComparativeMapChoicer(condition), Lists.newArrayList(), specifyVtr);
    }

    @Override
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args) {
        return route(vtab, choicer, args, super.getCurrentRule());
    }

    @Override
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args, String version) {
        return route(vtab, choicer, args, super.getVersionRule(version));
    }

    @Override
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                               VirtualTableRoot specifyVtr) {
        return route(vtab, choicer, args, specifyVtr, false, null);
    }

    @Override
    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args) throws RouteCompareDiffException {
        return routeMverAndCompare(isSelect, vtab, choicer, args, false, null);
    }

    @Override
    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args, boolean forceAllowFullTableScan,
                                             List<TableRule> ruleList) throws RouteCompareDiffException {
        return routeMverAndCompare(isSelect, vtab, choicer, args, forceAllowFullTableScan, ruleList, null);
    }

    @Override
    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args, boolean forceAllowFullTableScan,
                                             List<TableRule> ruleList, Map<String, Object> calcParams)
        throws RouteCompareDiffException {
        if (testTableRuleMap.get(vtab) != null) {
            return matcher.match(vtab,
                choicer,
                args,
                testTableRuleMap.get(vtab),
                true,
                forceAllowFullTableScan,
                calcParams);
        }
        if (super.getAllVersions().size() == 0) {
            if (allowEmptyRule) {
                return defaultRoute(vtab, null);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                    "routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!");
            }
        }

        // 如果只有单套规则,直接返回这套规则的路由结果
        if (super.getAllVersions().size() == 1) {
            return route(vtab, choicer, args, super.getCurrentRule(), forceAllowFullTableScan, ruleList, calcParams);
        }

        // 如果不止一套规则,那么计算两套规则,默认都返回新规则
        List<String> versions = super.getAllVersions();
        if (versions.size() != 2) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                "not support more than 2 copy rule compare,versions is:" + versions);
        }

        // 第一个排位的为旧规则
        MatcherResult oldResult = route(vtab,
            choicer,
            args,
            super.getCurrentRule(),
            forceAllowFullTableScan,
            ruleList,
            calcParams);
        if (isSelect) {
            return oldResult;
        } else {
            // 第二个排位的为新规则
            MatcherResult newResult = route(vtab,
                choicer,
                args,
                super.getVersionRule(super.getAllVersions().get(1)),
                forceAllowFullTableScan,
                ruleList,
                calcParams);
            boolean compareResult = MatchResultCompare.matchResultCompare(newResult, oldResult);
            if (compareResult) {
                return oldResult;
            } else {
                throw new RouteCompareDiffException();
            }
        }
    }

    /**
     * 返回对应表的defaultDbIndex
     */
    public String getDefaultDbIndex(String vtab) {
        return getDefaultDbIndex(vtab, super.getCurrentRule());
    }

    /**
     * 返回整个库的defaultDbIndex
     */
    @Override
    public String getDefaultDbIndex() {
        return getDefaultDbIndex(null, super.getCurrentRule());
    }

    private MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                                VirtualTableRoot specifyVtr, boolean forceAllowFullTableScan,
                                List<TableRule> ruleList) {
        return route(vtab, choicer, args, specifyVtr, forceAllowFullTableScan, ruleList, null);
    }

    private MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                                VirtualTableRoot specifyVtr, boolean forceAllowFullTableScan, List<TableRule> ruleList,
                                Map<String, Object> calcParams) {
        if (ruleList == null) {
            if (specifyVtr != null) {
                TableRule rule = specifyVtr.getVirtualTable(vtab);

                if (rule != null) {
                    return matcher.match(vtab, choicer, args, rule, true, forceAllowFullTableScan, calcParams);
                }
            }

            // 不存在规则，返回默认的
            return defaultRoute(vtab, specifyVtr);
        }

        /**
         * 给定了rule，不从virtualRoot中拿 TODO: 目前只处理一个TableRule bean的情况
         */
        return matcher.match(vtab, choicer, args, ruleList.get(0), true, forceAllowFullTableScan, calcParams);
    }

    // ================ helper method ================

    /**
     * 没有分库分表的逻辑表，返回指定库表
     */
    private MatcherResult defaultRoute(String vtab, VirtualTableRoot vtrCurrent) {
        TargetDB targetDb = new TargetDB();
        // 设置默认的链接库，比如就是groupKey
        targetDb.setDbIndex(this.getDefaultDbIndex(vtab, vtrCurrent));
        // 设置表名，同名不做转化
        Map<String, Field> tableNames = new HashMap<String, Field>(1);
        tableNames.put(vtab, null);
        targetDb.setTableNames(tableNames);

        return new MatcherResult(Arrays.asList(targetDb),
            new HashMap<String, Comparative>(),
            new HashMap<String, Comparative>());
    }

    /**
     * 没有分库分表的逻辑表，先从dbIndex中获取映射的库，没有则返回默认的库
     */
    private String getDefaultDbIndex(String vtab, VirtualTableRoot vtrCurrent) {
        if (vtrCurrent == null) {
            return super.defaultDbIndex;
        }

        if (vtab != null) {
            Map<String, String> dbIndexMap = vtrCurrent.getDbIndexMap();
            if (dbIndexMap != null && dbIndexMap.get(vtab) != null) {
                return dbIndexMap.get(vtab);
            }
        }

        String index = vtrCurrent.getDefaultDbIndex();

        if (index == null) {
            index = super.defaultDbIndex;
        }

        return index;
    }

    protected ComparativeMapChoicer generateComparativeMapChoicer(String condition) {
        Map<String, Comparative> comparativeMap = ComparativeStringAnalyser.decodeComparativeString2Map(condition);
        return new SimpleComparativeMapChoicer(comparativeMap);
    }

    public static class SimpleComparativeMapChoicer implements ComparativeMapChoicer {

        private Map<String, Comparative> comparativeMap = TreeMaps.caseInsensitiveMap();

        public SimpleComparativeMapChoicer() {

        }

        public SimpleComparativeMapChoicer(Map<String, Comparative> comparativeMap) {
            this.comparativeMap = comparativeMap;
        }

        @Override
        public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
            return this.comparativeMap;
        }

        @Override
        public Comparative getColumnComparative(List<Object> arguments, String colName) {
            return this.comparativeMap.get(colName);
        }
    }

    public TableRule getTable(String tableName) {
        if (testTableRuleMap.get(tableName) != null) {
            return testTableRuleMap.get(tableName);
        }
        VirtualTableRoot vtr = super.getCurrentRule();
        TableRule rule = null;
        if (vtr != null) {
            rule = vtr.getVirtualTable(tableName);
        }

        return rule;
    }

    /**
     * 获取所有的规则表
     */
    public Collection<TableRule> getTables() {
        List<TableRule> result = new ArrayList<TableRule>();
        VirtualTableRoot vrt = super.getCurrentRule();
        if (vrt == null) {
            return result;
        }

        if (ConfigDataMode.isFastMock()) {
            return vrt.getTableRules().values();
        }
        synchronized (vtrs) {
            Map<String, TableRule> tables = vrt.getTableRules();
            result.addAll(tables.values());
        }

        return result;
    }

    public Map<String, String> getDbIndexMap() {
        Map<String, String> result = TreeMaps.caseInsensitiveMap();
        VirtualTableRoot vrt = super.getCurrentRule();
        if (vrt == null) {
            return result;
        }
        Map<String, String> dbIndexMap = vrt.getDbIndexMap();
        if (dbIndexMap != null) {
            result.putAll(dbIndexMap);
        }
        return result;
    }

    public void prepareForShadowTable(String shadowTableName) {
        // The prefix of a shadow table must start with this.
        if (TStringUtil.isNotEmpty(shadowTableName)
            && TStringUtil.startsWithIgnoreCase(shadowTableName, TEST_TABLE_PREFIX)) {
            // Check if the corresponding formal table exists.
            String formalTableName = TStringUtil.substring(shadowTableName, TEST_TABLE_PREFIX.length());
            if (TStringUtil.isNotEmpty(formalTableName) && !SystemTables.contains(formalTableName)) {
                // The formal table exists and isn't a system table.
                TableRule tableRule = this.getTable(formalTableName);
                if (tableRule != null) {
                    String tableNamePrefixFromFormalTable = tableRule.extractTableNamePrefix();
                    if (TStringUtil.isNotEmpty(tableNamePrefixFromFormalTable)) {
                        physicalTableNamePrefixesForShadowTables.put(shadowTableName, tableNamePrefixFromFormalTable);
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "The formal table '" + formalTableName
                            + "' doesn't exist. A shadow table must be associated with an existing formal table.");
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "The system table cannot be associated with a shadow table");
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Shadow table name for EagleEye must start with the prefix '" + TEST_TABLE_PREFIX + "'");
        }
    }

    public String getTableNamePrefixForShadowTable(String shadowTableName) {
        String tableNamePrefix = null;
        if (physicalTableNamePrefixesForShadowTables.keySet().contains(shadowTableName)) {
            tableNamePrefix = physicalTableNamePrefixesForShadowTables.get(shadowTableName);
        }
        return tableNamePrefix;
    }

    public void cleanupForShadowTable(String shadowTableName) {
        physicalTableNamePrefixesForShadowTables.remove(shadowTableName);
    }

    /**
     * Get the logical table names that the physical table name belongs to. Note
     * that a physical table name may be associated with multiple logical tables
     * even if only one logical table is consistent.
     */
    public Set<String> getLogicalTableNames(String fullyQualifiedPhysicalTableName, List<String> groupNames) {
        Map<String, Set<String>> tableNameMappings = getTableNameMappings(groupNames);
        return tableNameMappings.get(fullyQualifiedPhysicalTableName);
    }

    private Map<String, Set<String>> getTableNameMappings(List<String> groupNames) {
        if (versionIndex == null || versionIndex.isEmpty()) {
            if (testTableRuleMap.size() > 0) {
                Map<String, Set<String>> tableNameMappings = Maps.newConcurrentMap();
                initTableNameMappings(testTableRuleMap.values(), tableNameMappings, groupNames);
                return tableNameMappings;
            } else {
                throw new TddlRuleException("Unexpected: No rule version found for table name lookup");
            }
        }

        Map<String, Set<String>> tableNameMappings = getVersionedTableNames().get(versionIndex.get(0));

        if (tableNameMappings == null || tableNameMappings.isEmpty()) {
            synchronized (vtrs) {
                tableNameMappings = getVersionedTableNames().get(versionIndex.get(0));
                if (tableNameMappings == null || tableNameMappings.isEmpty()) {
                    tableNameMappings = Maps.newConcurrentMap();
                    Collection<TableRule> tableRules = getTables();
                    initTableNameMappings(tableRules, tableNameMappings, groupNames);
                    getVersionedTableNames().put(versionIndex.get(0), tableNameMappings);
                }
            }
        }

        return tableNameMappings;
    }

    private void initTableNameMappings(Collection<TableRule> tableRules,
                                       Map<String, Set<String>> tableNameMappings,
                                       List<String> groupNames) {
        for (TableRule tableRule : tableRules) {
            String logicalTableName = tableRule.getVirtualTbName();
            if ((tableRule.getDbPartitionKeys() == null || tableRule.getDbPartitionKeys().isEmpty())
                && (tableRule.getTbPartitionKeys() == null || tableRule.getTbPartitionKeys().isEmpty())) {
                // The group and physical table names are incomplete in the
                // topology for single or broadcast table, so we have to build
                // them manually.
                for (String groupName : groupNames) {
                    // The physical table has the same name as the logical
                    // table.
                    addGroupAndPhysicalTableName(groupName + "." + tableRule.getTbNamePattern(),
                        logicalTableName,
                        tableNameMappings);
                }
            } else {
                // We can get all group and physical table names from the
                // topology for sharding table.
                Map<String, Set<String>> topology = tableRule.getActualTopology();
                for (Entry<String, Set<String>> groupAndPhyTableNames : topology.entrySet()) {
                    String groupName = groupAndPhyTableNames.getKey();
                    for (String physicalTableName : groupAndPhyTableNames.getValue()) {
                        addGroupAndPhysicalTableName(groupName + "." + physicalTableName,
                            logicalTableName,
                            tableNameMappings);
                    }
                }
            }
        }
    }

    private void addGroupAndPhysicalTableName(String fullyQualifiedPhysicalTableName, String logicalTableName,
                                              Map<String, Set<String>> tableNameMappings) {
        fullyQualifiedPhysicalTableName = TStringUtil.remove(fullyQualifiedPhysicalTableName, '`').toLowerCase();
        Set<String> logicalTableNames = tableNameMappings.get(fullyQualifiedPhysicalTableName);
        if (logicalTableNames == null) {
            logicalTableNames = new HashSet<>();
            tableNameMappings.put(fullyQualifiedPhysicalTableName.toLowerCase(), logicalTableNames);
        }
        logicalTableNames.add(logicalTableName);
    }

    public TddlRule copy() {
        TddlRule newRule = new TddlRule();
        newRule.versionIndex.putAll(versionIndex);

        if (versionIndex.get(0) != null) {
            VirtualTableRoot newVirtualTableRoot = vtrs.get(versionIndex.get(0)).copy();
            newRule.vtrs.put(versionIndex.get(0), newVirtualTableRoot);
        }

        return newRule;
    }
}

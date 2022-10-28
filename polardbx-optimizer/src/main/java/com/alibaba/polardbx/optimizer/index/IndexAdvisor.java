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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class IndexAdvisor {

    protected final Logger logger = LoggerFactory.getLogger(IndexAdvisor.class);

    private ExecutionPlan executionPlan;

    private ExecutionContext executionContext;

    private PlannerContext plannerContext;

    private RelMetadataQuery mq;

    private Map<String, Map<String, Configuration>> configurationMap;

    private CoverableColumnSet coverableColumnSet;

    private PartitionRuleSet partitionRuleSet;

    private Map<String, Collection<HumanReadableRule>> partitionPolicyMap;

    private AdviseType adviseType;

    public enum AdviseType {
        LOCAL_INDEX,
        GLOBAL_INDEX,
        GLOBAL_COVERING_INDEX,
        BROADCAST
    }

    public IndexAdvisor(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        this.executionPlan = executionPlan;
        this.executionContext = executionContext;
        this.plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());
        this.mq = executionPlan.getPlan().getCluster().getMetadataQuery();
        this.configurationMap = new HashMap<>();
    }

    public AdviceResult advise(AdviseType adviseType) {
        this.adviseType = adviseType;

        SqlNode ast = executionPlan.getAst();
        if (ast == null) {
            return null;
        }
        if (!(ast.getKind().belongsTo(SqlKind.QUERY) || ast.getKind().belongsTo(SqlKind.DML))) {
            return null;
        }

        // init
        executionPlan.getPlan().getCluster().invalidateMetadataQuery();
        this.mq = executionPlan.getPlan().getCluster().getMetadataQuery();
        this.configurationMap = new HashMap<>();

        RelNode unOptimizedPlan = executionContext.getUnOptimizedPlan();
        RelNode logicalPlan = Planner.getInstance().optimizeBySqlWriter(unOptimizedPlan, plannerContext);
        RelNode physicalPlan = Planner.getInstance().optimizeByPlanEnumerator(logicalPlan, plannerContext);
        RelOptCost originalCost = mq.getCumulativeCost(physicalPlan);

        AdviceResult adviceResult = new AdviceResult(originalCost, executionPlan.getPlan());

        //prepare broadcast tables
        if (adviseType == AdviseType.BROADCAST) {
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("origin rowcount ").append(mq.getRowCount(physicalPlan)).append("\n");
                sb.append(RelUtils
                        .toString(physicalPlan, executionContext.getParams().getCurrentParameter(),
                            RexUtils.getEvalFunc(executionContext), executionContext))
                    .append("original cost").append(originalCost.toString());
                logger.debug(sb.toString());
            }
            BroadcastContext initContext = initBroadcast(unOptimizedPlan, originalCost);
            if (initContext != null) {
                for (Map.Entry<String, Set<String>> entry : initContext.broadcastTables.entrySet()) {
                    if (entry.getValue().size() > 0) {
                        executionContext.getSchemaManager(entry.getKey())
                            .getTddlRuleManager().getTddlRule().getVersionedTableNames().clear();
                    }
                }

                RelNode newLogicalPlan = Planner.getInstance().optimizeBySqlWriter(unOptimizedPlan, plannerContext);
                RelNode finalPlan = Planner.getInstance().optimizeByPlanEnumerator(newLogicalPlan, plannerContext);
                RelOptCost finalCost = mq.getCumulativeCost(finalPlan);
                Configuration finalConfiguration =
                    new Configuration(new HashSet<>(), finalCost, initContext.broadcastTables);

                if (lessThan(finalCost, originalCost)) {
                    adviceResult.setConfiguration(finalConfiguration);
                    adviceResult.setAfterPlan(finalPlan, "\n" + RelUtils
                        .toString(finalPlan, executionContext.getParams().getCurrentParameter(),
                            RexUtils.getEvalFunc(executionContext), executionContext));
                    endWhatIf(initContext);
                    adviceResult.setInfo(adviseType.name());
                    return adviceResult;
                }
                endWhatIf(initContext);
            }
        } else {
            // find indexable column
            IndexableColumnSet indexableColumnSet = analyzeIndexableColumn(logicalPlan);

            // find coverable column
            coverableColumnSet = analyzeCoverableColumn(logicalPlan);

            // find partition rule
            partitionRuleSet = analyzePartitionRule(indexableColumnSet);

            partitionPolicyMap = analyzePartitionPolicyMap(partitionRuleSet);

            // find candidate index
            Map<String, Map<String, Set<CandidateIndex>>> candidateMap = buildCandidateIndex(indexableColumnSet);

            // enumerate configuration
            Set<CandidateIndex> finalCandidateSet = enumerateConfiguration(candidateMap, logicalPlan, originalCost);
            WhatIfContext whatIfContext = beginWhatIf(logicalPlan, finalCandidateSet);
            RelNode finalPlan = Planner.getInstance().optimizeByPlanEnumerator(logicalPlan, plannerContext);
            RelOptCost finalCost = mq.getCumulativeCost(finalPlan);
            endWhatIf(whatIfContext);

            if (lessThan(finalCost, originalCost)) {
                Configuration finalConfiguration = new Configuration(finalCandidateSet, finalCost);
                adviceResult.setConfiguration(finalConfiguration);
                WhatIfContext ctx = beginWhatIf(finalPlan, finalCandidateSet);
                adviceResult.setAfterPlan(finalPlan, "\n" + RelUtils
                    .toString(finalPlan, executionContext.getParams().getCurrentParameter(),
                        RexUtils.getEvalFunc(executionContext), executionContext));
                endWhatIf(ctx);
                adviceResult.setInfo(adviseType.name());
                return adviceResult;
            }
        }

        Set<Pair<String, String>> tableSet = PlanManagerUtil.getTableSetFromAst(executionPlan.getAst());
        String defaultSchemaName = executionContext.getSchemaName();
        StringBuilder infoBuidler = new StringBuilder();
        infoBuidler.append("Advisor can't not find any useful index. You can try to analyze tables to refresh ");
        infoBuidler.append("statistics during the low business period by ");
        infoBuidler.append("`ANALYZE TABLE ");
        infoBuidler.append(tableSet.stream().map(pair -> {
            String schemaName = pair.getKey() == null ? defaultSchemaName : pair.getKey();
            String tableName = pair.getValue();
            return "`" + schemaName + "`.`" + tableName + "`";
        }).collect(Collectors.joining(",")));
        infoBuidler.append("`");
        adviceResult.setInfo(infoBuidler.toString());
        return adviceResult;
    }

    private IndexableColumnSet analyzeIndexableColumn(RelNode logicalPlan) {
        IndexableColumnRelFinder indexableColumnRelFinder = new IndexableColumnRelFinder(mq);
        indexableColumnRelFinder.go(logicalPlan);
        IndexableColumnSet indexableColumnSet = indexableColumnRelFinder.getIndexableColumnSet();
        return indexableColumnSet;
    }

    private CoverableColumnSet analyzeCoverableColumn(RelNode logicalPlan) {
        CoverableColumnRelFinder coverableColumnRelFinder = new CoverableColumnRelFinder(mq);
        coverableColumnRelFinder.go(logicalPlan);
        CoverableColumnSet coverableColumnSet = coverableColumnRelFinder.getCoverableColumnSet();
        return coverableColumnSet;
    }

    private PartitionRuleSet analyzePartitionRule(IndexableColumnSet indexableColumnSet) {
        PartitionRuleSet partitionRuleSet = new PartitionRuleSet();
        for (String schemaName : indexableColumnSet.m.keySet()) {
            for (String tableName : indexableColumnSet.m.get(schemaName).keySet()) {

                PartitionInfoManager partitionInfo =
                    executionContext.getSchemaManager(schemaName).getTddlRuleManager().getPartitionInfoManager();
                if (partitionInfo.isNewPartDbTable(tableName)) {
                    HumanReadableRule humanReadableRule =
                        HumanReadableRule.getHumanReadableRule(partitionInfo.getPartitionInfo(tableName));
                    partitionRuleSet.addPartitionRule(schemaName, tableName, humanReadableRule);
                } else {
                    TableRule tableRule =
                        executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTableRule(tableName);
                    HumanReadableRule humanReadableRule = HumanReadableRule.getHumanReadableRule(tableRule);
                    partitionRuleSet.addPartitionRule(schemaName, tableName, humanReadableRule);
                    // gsi
                    List<String> gsiList = GlobalIndexMeta
                        .getPublishedIndexNames(tableName, schemaName, executionContext);
                    for (String gsi : gsiList) {
                        tableRule =
                            executionContext.getSchemaManager(schemaName).getTddlRuleManager().getTableRule(gsi);
                        humanReadableRule = HumanReadableRule.getHumanReadableRule(tableRule);
                        partitionRuleSet.addPartitionRule(schemaName, tableName, humanReadableRule);
                    }
                }
            }
        }
        return partitionRuleSet;
    }

    private Map<String, Collection<HumanReadableRule>> analyzePartitionPolicyMap(PartitionRuleSet partitionRuleSet) {
        Map<String, Collection<HumanReadableRule>> result = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (String schemaName : partitionRuleSet.m.keySet()) {
            result.put(schemaName.toLowerCase(), partitionRuleSet.getPartitionPolicys(schemaName));
        }
        return result;
    }

    private Map<String, Map<String, Set<CandidateIndex>>> buildCandidateIndex(IndexableColumnSet indexableColumnSet) {
        int MAX_MULTI_COLUMN_INDEX_SIZE = 2;
        // First, consider every single column index
        Set<CandidateIndex> singleColumnCandidateIndexSet = getSingleColumnCandidateIndexSet(indexableColumnSet);
        Set<CandidateIndex> candidateIndexLevelOne = new HashSet<>();
        List<Set<CandidateIndex>> candidateIndexLevel = new ArrayList<>();

        for (CandidateIndex singleColumnCandidateIndex : singleColumnCandidateIndexSet) {
            selectCandidateIndex(singleColumnCandidateIndex, candidateIndexLevelOne);
        }

        // Second, consider multi column index
        candidateIndexLevel.add(candidateIndexLevelOne);
        int currentLevel = 2;
        while (currentLevel <= MAX_MULTI_COLUMN_INDEX_SIZE) {
            Set<CandidateIndex> currentLevelCandidateIndexSet = new HashSet<>();
            int lastLevelidx = currentLevel - 2;
            Set<CandidateIndex> lastLevelCandidateIndexSet = candidateIndexLevel.get(lastLevelidx);
            // currentLevel = lastLevel + levelOne
            for (CandidateIndex lastLevelCandidateIndex : lastLevelCandidateIndexSet) {
                for (CandidateIndex levelOneCandidateIndex : candidateIndexLevelOne) {
                    Set<CandidateIndex> levelUpIndexSet =
                        levelUp(lastLevelCandidateIndex, levelOneCandidateIndex);
                    if (levelUpIndexSet == null) {
                        continue;
                    }
                    for (CandidateIndex currentLevelCandidateIndex : levelUpIndexSet) {
                        selectCandidateIndex(currentLevelCandidateIndex, currentLevelCandidateIndexSet);
                    }
                }
            }
            candidateIndexLevel.add(currentLevelCandidateIndexSet);
            currentLevel++;
        }

        // schema -> table -> candidateIndex
        Map<String, Map<String, Set<CandidateIndex>>> candidateMap = new HashMap<>();

        for (Set<CandidateIndex> candidateIndexSet : candidateIndexLevel) {
            for (CandidateIndex candidateIndex : candidateIndexSet) {
                String schemaName = candidateIndex.getSchemaName().toLowerCase();
                String tableName = candidateIndex.getTableName().toLowerCase();
                Map<String, Set<CandidateIndex>> tableMap = candidateMap.get(schemaName);
                if (tableMap == null) {
                    tableMap = new HashMap<>();
                    candidateMap.put(schemaName, tableMap);
                }
                Set<CandidateIndex> notExistsCandidateSet = tableMap.get(tableName);
                if (notExistsCandidateSet == null) {
                    notExistsCandidateSet = new HashSet<>();
                    tableMap.put(tableName, notExistsCandidateSet);
                }
                if (!candidateIndex.alreadyExist()) {
                    notExistsCandidateSet.add(candidateIndex);
                }
            }
        }

        return candidateMap;
    }

    private Set<CandidateIndex> enumerateConfiguration(Map<String, Map<String, Set<CandidateIndex>>> candidateMap,
                                                       RelNode logicalPlan, RelOptCost originalCost) {
        // configuration enumeration - greedy search
        // schema.table.configuration independent property
        // we enumerate every schema.table best configuration and then add them together
        // best configuration with max-cost-improve and min-candidateIndex-num
        int MAX_INDEX_PER_CONFIGURATION = 2;
        if (adviseType == AdviseType.GLOBAL_COVERING_INDEX || adviseType == AdviseType.GLOBAL_INDEX) {
            MAX_INDEX_PER_CONFIGURATION = 1;
        }

        for (int combination = 1; combination <= MAX_INDEX_PER_CONFIGURATION; combination++) {
            for (Map<String, Set<CandidateIndex>> tableMap : candidateMap.values()) {
                for (Set<CandidateIndex> notExistsCandidateSet : tableMap.values()) {
                    int combinationSize = Math.min(notExistsCandidateSet.size(), combination);
                    Set<Set<CandidateIndex>> combinationSet = Sets.combinations(notExistsCandidateSet, combinationSize);
                    for (Set<CandidateIndex> candidateIndexSet : combinationSet) {
                        tryConfiguration(logicalPlan, originalCost, candidateIndexSet);
                    }
                }
            }
        }

        // In GSI case, we suppose only one Table GSI is the most significant index.
        // (To avoid table partition policy change each other for join, which is high cost for maintaining GSI)
        // so finalCandidateSet should only contain one table Configuration for GSI case.
        Set<CandidateIndex> finalCandidateSet = new HashSet<>();
        if (adviseType == AdviseType.GLOBAL_COVERING_INDEX || adviseType == AdviseType.GLOBAL_INDEX) {
            Configuration bestTableConfiguration = null;
            for (Map<String, Configuration> tableConfiguration : configurationMap.values()) {
                for (Configuration configuration : tableConfiguration.values()) {
                    if (bestTableConfiguration == null) {
                        bestTableConfiguration = configuration;
                    } else if (configuration.getAfterCost().isLt(bestTableConfiguration.getAfterCost())) {
                        bestTableConfiguration = configuration;
                    }
                }
            }
            if (bestTableConfiguration != null) {
                finalCandidateSet.addAll(bestTableConfiguration.getCandidateIndexSet());
            }
        } else {
            for (Map<String, Configuration> tableConfiguration : configurationMap.values()) {
                for (Configuration configuration : tableConfiguration.values()) {
                    finalCandidateSet.addAll(configuration.getCandidateIndexSet());
                }
            }
        }

        return finalCandidateSet;
    }

    /**
     * all candidateIndex in candidateIndexSet must be with the same schema.table
     */
    private boolean tryConfiguration(RelNode logicalPlan, RelOptCost originalCost,
                                     Set<CandidateIndex> candidateIndexSet) {

        if (isConfigurationRedundant(candidateIndexSet)) {
            return false;
        }

        WhatIfContext whatIfContext = beginWhatIf(logicalPlan, candidateIndexSet);
        RelNode physicalPlan = Planner.getInstance().optimizeByPlanEnumerator(logicalPlan, plannerContext);
        RelOptCost newCost = mq.getCumulativeCost(physicalPlan);
        endWhatIf(whatIfContext);
        // make the plan cost lower!
        if (lessThan(newCost, originalCost)) {
            Configuration configuration = new Configuration(candidateIndexSet, newCost);
            String schemaName = candidateIndexSet.iterator().next().getSchemaName().toLowerCase();
            String tableName = candidateIndexSet.iterator().next().getTableName().toLowerCase();
            Map<String, Configuration> tableConfigurationMap = configurationMap.get(schemaName);
            if (tableConfigurationMap == null) {
                tableConfigurationMap = new HashMap<>();
                configurationMap.put(schemaName, tableConfigurationMap);
            }
            Configuration oldConfiguration = tableConfigurationMap.get(tableName);
            if (oldConfiguration == null) {
                tableConfigurationMap.put(tableName, configuration);
            } else if (configuration.getAfterCost().isLt(oldConfiguration.getAfterCost().multiplyBy(1.01))
                && oldConfiguration.getAfterCost().isLt(configuration.getAfterCost().multiplyBy(1.01))) {
                // 1% range, we choose small candidateIndexSet size and small index length
                if (configuration.getCandidateIndexSet().size() < oldConfiguration.getCandidateIndexSet().size()) {
                    tableConfigurationMap.put(tableName, configuration);
                } else if (configuration.getCandidateIndexSet().size()
                    == oldConfiguration.getCandidateIndexSet().size()
                    && configuration.totalIndexLength() < oldConfiguration.totalIndexLength()) {
                    tableConfigurationMap.put(tableName, configuration);
                }
            } else if (configuration.getAfterCost().isLt(oldConfiguration.getAfterCost().multiplyBy(0.99))) {
                tableConfigurationMap.put(tableName, configuration);
            }
            return true;
        }
        return false;
    }

    class WhatIfContext {

        public Map<String, SchemaManager> oldSchemaManagers;
        public List<Pair<String, TableScan>> tableScans;
        public Map<TableScan, TableMeta> oldMap;

        public WhatIfContext(
            Map<String, SchemaManager> oldSchemaManagers,
            List<Pair<String, TableScan>> tableScans,
            Map<TableScan, TableMeta> oldMap) {
            this.oldSchemaManagers = oldSchemaManagers;
            this.tableScans = tableScans;
            this.oldMap = oldMap;
        }
    }

    class BroadcastContext extends WhatIfContext {
        private Map<String, Set<String>> broadcastTables;

        public BroadcastContext(
            Map<String, SchemaManager> oldSchemaManagers,
            List<Pair<String, TableScan>> tableScans,
            Map<TableScan, TableMeta> oldMap,
            Map<String, Set<String>> broadcastTables) {
            super(oldSchemaManagers, tableScans, oldMap);
            this.broadcastTables = broadcastTables;
        }

    }

    class BroadcastInfo {
        TableRule rule;
        TableRule oldRule;
        PartitionInfoManager.PartInfoCtx infoCtx;

        BroadcastInfo(TableRule rule, TableRule oldRule) {
            this.rule = rule;
            this.oldRule = oldRule;
            infoCtx = null;
        }

        BroadcastInfo(PartitionInfoManager.PartInfoCtx infoCtx) {
            this.rule = null;
            this.oldRule = null;
            this.infoCtx = infoCtx;
        }

        void rollback(SchemaManager schemaManager, SchemaManager oldSchemaManager,
                      String tableName, TableMeta tableMeta) {
            if (infoCtx != null) {
                PartitionInfoManager.PartInfoCtx ctx = oldSchemaManager.
                    getTddlRuleManager().getPartitionInfoManager().getPartInfoCtx(tableName);
                schemaManager.getTddlRuleManager().getPartitionInfoManager().putPartInfoCtx(
                    tableName, ctx);
                tableMeta.setPartitionInfo(ctx.getPartInfo());
            } else {
                // rollback old table
                if (schemaManager.getTddlRuleManager().isBroadCast(tableName)) {
                    ((WhatIfTddlRuleManager) (schemaManager.getTddlRuleManager())).addTableRule(tableName,
                        oldRule);
                }
            }
        }

        void prepare(SchemaManager schemaManager, String tableName, TableMeta tableMeta) {
            if (infoCtx != null) {
                PartitionInfoManager partitionInfoManager = schemaManager.getTddlRuleManager()
                    .getPartitionInfoManager();
                partitionInfoManager.putPartInfoCtx(tableName, infoCtx);
                tableMeta.setPartitionInfo(infoCtx.getPartInfo());
            } else {
                ((WhatIfTddlRuleManager) (schemaManager.getTddlRuleManager())).addTableRule(tableName, rule);
            }
        }
    }

    /**
     * find all candidate broadcast tables which are small enough
     *
     * @param schemaManager read schemaManager
     * @param tables tables to be test
     * @return tables small enough
     */
    public Set<String> candidateBroadCastTable(SchemaManager schemaManager, Set<String> tables) {
        Set<String> candidateBroadCastTables = new TreeSet<>();
        for (String table : tables) {
            if (schemaManager.getTable(table).getRowCount() < executionContext.getParamManager().getInt(
                ConnectionParams.INDEX_ADVISOR_BROADCAST_THRESHOLD)) {
                candidateBroadCastTables.add(table);
            }
        }
        return candidateBroadCastTables;
    }

    public BroadcastInfo prepareInfo(String schemaName, String tableName,
                                     SchemaManager schemaManager,
                                     SchemaManager oldSchemaManager,
                                     TableMeta whatIfTableMeta,
                                     TableMeta oldTableMeta) {
        BroadcastInfo broadcastInfo = null;
        if (oldTableMeta.getRowCount() < executionContext.getParamManager().getInt(
            ConnectionParams.INDEX_ADVISOR_BROADCAST_THRESHOLD)) {
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {

                PartitionInfo partitionInfo = PartitionInfoBuilder
                    .buildPartitionInfoByPartDefAst(schemaName, tableName, null, null,
                        null, null,
                        new ArrayList<>(whatIfTableMeta.getPrimaryKey()),
                        whatIfTableMeta.getAllColumns(),
                        PartitionTableType.BROADCAST_TABLE,
                        executionContext);

                partitionInfo.setStatus(TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);

                PartitionInfoManager partitionInfoManager = schemaManager.getTddlRuleManager()
                    .getPartitionInfoManager();
                PartitionInfoManager.PartInfoCtx partInfoCtx =
                    new PartitionInfoManager.PartInfoCtx(partitionInfoManager,
                        tableName,
                        partitionInfo.getTableGroupId(),
                        partitionInfo);
                broadcastInfo = new BroadcastInfo(partInfoCtx);
            } else {

                // make sure old table was not broadcast
                if (!schemaManager.getTddlRuleManager().isBroadCast(tableName)) {
                    TableRule tableRule =
                        TableRuleBuilder.buildBroadcastTableRule(tableName, whatIfTableMeta);
                    broadcastInfo = new BroadcastInfo(tableRule,
                        oldSchemaManager.getTddlRuleManager().getTableRule(tableName));
                }
            }
        }
        return broadcastInfo;
    }

    private BroadcastContext initBroadcast(RelNode unOptimizedPlan, RelOptCost originalCost) {
        TableScanFinder tableScanFinder = new TableScanFinder();
        unOptimizedPlan.accept(tableScanFinder);
        Map<String, Map<String, List<TableScan>>> tableScanClass = tableScanFinder.getMappedResult(
            executionContext.getSchemaName());

        Map<String, Set<String>> candidateBroadCastTables = new TreeMap<>();
        Set<String> schemaNames = tableScanClass.keySet();

        Map<String, SchemaManager> oldSchemaManagers = executionContext.getSchemaManagers();
        Map<String, SchemaManager> whatIfSchemaManagers = new ConcurrentHashMap<>(oldSchemaManagers);
        executionContext.setSchemaManagers(whatIfSchemaManagers);
        for (String schemaName : schemaNames) {
            SchemaManager oldSchemaManager = oldSchemaManagers.get(schemaName);
            // build candidate table meta
            Set<String> tables = candidateBroadCastTable(oldSchemaManager, tableScanClass.get(schemaName).keySet());

            candidateBroadCastTables.put(schemaName, tables);

            WhatIfSchemaManager whatIfSchemaManager =
                new WhatIfSchemaManager(oldSchemaManager, new HashSet<>(), tables, executionContext);
            whatIfSchemaManager.init();
            executionContext.getSchemaManagers().put(schemaName, whatIfSchemaManager);
        }


        Map<String, Map<String, BroadcastInfo>> broadcastTableInfo = new HashMap<>(schemaNames.size());
        for (String schemaName : schemaNames) {
            broadcastTableInfo.put(schemaName, new HashMap<>());
        }

        boolean anyBroadcast = false;
        TableScanCounter tsc = new TableScanCounter();
        unOptimizedPlan.accept(tsc);
        int oldTableScanCount = tsc.getCount();
        // try all schemas
        for (Map.Entry<String, Map<String, List<TableScan>>> pair : tableScanClass.entrySet()) {
            String schemaName = pair.getKey();

            //try all tables
            for (Map.Entry<String, List<TableScan>> entry : pair.getValue().entrySet()) {
                String tableName = entry.getKey();
                if (!candidateBroadCastTables.get(schemaName).contains(tableName)) {
                    continue;
                }
                List<TableScan> tableScans = entry.getValue();
                BroadcastInfo broadcastInfo = null;

                //substitute all tableScan of the same table
                for (TableScan tableScan : tableScans) {
                    RelOptTable relOptTable = tableScan.getTable();
                    TableMeta oldTableMeta = CBOUtil.getTableMeta(relOptTable);
                    SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
                    TableMeta whatIfTableMeta = schemaManager.getTable(oldTableMeta.getTableName());
                    //don't broadcast gsi
                    if (whatIfTableMeta.isGsi()) {
                        continue;
                    }
                    // make sure the table is small enough to be broadcast
                    if (broadcastInfo == null) {
                        broadcastInfo =
                            prepareInfo(schemaName, tableName, schemaManager,
                                oldSchemaManagers.get(schemaName), whatIfTableMeta, oldTableMeta);
                    }
                    if (broadcastInfo != null) {
                        ((RelOptTableImpl) relOptTable).setImplTable(whatIfTableMeta);
                        broadcastInfo.prepare(schemaManager, tableName, whatIfTableMeta);
                    }
                }

                //check the result of making the table broadcast
                if (broadcastInfo != null) {
                    //clear old meta
                    whatIfSchemaManagers.get(schemaName).getTddlRuleManager().
                        getTddlRule().getVersionedTableNames().clear();

                    RelNode logicalPlan =
                        Planner.getInstance().optimizeBySqlWriter(unOptimizedPlan, plannerContext);
                    RelNode physicalPlan =
                        Planner.getInstance().optimizeByPlanEnumerator(logicalPlan, plannerContext);
                    RelOptCost newCost = mq.getCumulativeCost(physicalPlan);
                    if (logger.isDebugEnabled()) {
                        StringBuilder sb = new StringBuilder("\n");
                        sb.append("whatif rowcount ").append(mq.getRowCount(physicalPlan))
                            .append("\n");
                        sb.append(tableName).append("\n").
                            append(RelUtils.toString(physicalPlan, executionContext.getParams().getCurrentParameter(),
                                RexUtils.getEvalFunc(executionContext), executionContext))
                            .append(originalCost.toString()).append("\n").append(newCost.toString());
                    }

                    // smaller cost
                    if (lessThan(newCost, originalCost)) {
                        tsc = new TableScanCounter();
                        physicalPlan.accept(tsc);
                        // more tables are pushed down
                        if (tsc.getCount() < oldTableScanCount) {
                            anyBroadcast = true;
                            //record the useful table
                            if (!broadcastTableInfo.containsKey(schemaName)) {
                                broadcastTableInfo.put(schemaName, new HashMap<>());
                            }
                            broadcastTableInfo.get(schemaName).put(tableName, broadcastInfo);
                        }
                    }

                    // rollback
                    for (TableScan tableScan : tableScans) {
                        RelOptTable relOptTable = tableScan.getTable();
                        TableMeta oldTableMeta = CBOUtil.getTableMeta(relOptTable);
                        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
                        TableMeta whatIfTableMeta = schemaManager.getTable(tableName);
                        if (whatIfTableMeta.isGsi()) {
                            continue;
                        }

                        ((RelOptTableImpl) relOptTable).setImplTable(oldTableMeta);
                        broadcastInfo.rollback(schemaManager, oldSchemaManagers.get(schemaName),
                            tableName, whatIfTableMeta);
                    }
                }

            }
        }

        //prepare executionContext with new broadcast
        Map<TableScan, TableMeta> oldMap = new HashMap<>();
        for (Pair<String, TableScan> pair : tableScanFinder.getResult()) {
            String schemaName = pair.getKey();
            TableScan tableScan = pair.getValue();
            RelOptTable relOptTable = tableScan.getTable();
            if ((relOptTable instanceof RelOptTableImpl)) {
                TableMeta oldTableMeta = CBOUtil.getTableMeta(relOptTable);
                oldMap.put(tableScan, oldTableMeta);
                TableMeta whatIfTableMeta =
                    whatIfSchemaManagers.get(schemaName).getTable(oldTableMeta.getTableName());
                ((RelOptTableImpl) relOptTable).setImplTable(whatIfTableMeta);

                String tableName = whatIfTableMeta.getTableName();
                if (broadcastTableInfo.get(schemaName).containsKey(tableName)) {
                    broadcastTableInfo.get(schemaName).get(tableName).prepare(
                        executionContext.getSchemaManager(schemaName), tableName, whatIfTableMeta);
                }
            }
        }
        if (!anyBroadcast) {
            return null;
        }

        Map<String, Set<String>> broadcastTables = new HashMap<>();
        for (Map.Entry<String, Map<String, BroadcastInfo>> entry : broadcastTableInfo.entrySet()) {
            broadcastTables.put(entry.getKey(), entry.getValue().keySet());
        }
        return new BroadcastContext(oldSchemaManagers, tableScanFinder.getResult(), oldMap, broadcastTables);
    }

    private WhatIfContext beginWhatIf(RelNode logicalPlan, Set<CandidateIndex> candidateIndexSet) {
        Set<String> schemaNames = candidateIndexSet.stream()
            .map(x -> x.getSchemaName().toLowerCase())
            .collect(Collectors.toSet());

        Map<String, SchemaManager> oldSchemaManagers = executionContext.getSchemaManagers();
        Map<String, SchemaManager> whatIfSchemaManagers = new ConcurrentHashMap<>(oldSchemaManagers);
        executionContext.setSchemaManagers(whatIfSchemaManagers);
        for (String schemaName : schemaNames) {
            SchemaManager oldSchemaManager = executionContext.getSchemaManager(schemaName);
            SchemaManager whatIfSchemaManager =
                new WhatIfSchemaManager(oldSchemaManager, candidateIndexSet, executionContext);
            whatIfSchemaManager.init();
            executionContext.getSchemaManagers().put(schemaName, whatIfSchemaManager);
        }

        Map<TableScan, TableMeta> oldMap = new HashMap<>();
        TableScanFinder tableScanFinder = new TableScanFinder();
        logicalPlan.accept(tableScanFinder);
        List<Pair<String, TableScan>> tableScans = tableScanFinder.getResult();
        for (Pair<String, TableScan> pair : tableScans) {
            String schemaName = pair.getKey();
            TableScan tableScan = pair.getValue();
            RelOptTable relOptTable = tableScan.getTable();
            if ((relOptTable instanceof RelOptTableImpl)) {
                TableMeta oldTableMeta = CBOUtil.getTableMeta(relOptTable);
                oldMap.put(tableScan, oldTableMeta);
                TableMeta whatIfTableMeta =
                    executionContext.getSchemaManager(schemaName).getTable(oldTableMeta.getTableName());
                ((RelOptTableImpl) relOptTable).setImplTable(whatIfTableMeta);
            }
        }
        return new WhatIfContext(oldSchemaManagers, tableScans, oldMap);
    }

    private void endWhatIf(WhatIfContext whatIfContext) {
        // replace back
        executionContext.setSchemaManagers(whatIfContext.oldSchemaManagers);
        for (Pair<String, TableScan> pair : whatIfContext.tableScans) {
            TableScan tableScan = pair.getValue();
            RelOptTable relOptTable = tableScan.getTable();
            if ((relOptTable instanceof RelOptTableImpl)) {
                ((RelOptTableImpl) relOptTable).setImplTable(whatIfContext.oldMap.get(tableScan));
            }
        }
    }

    private boolean isConfigurationRedundant(Set<CandidateIndex> candidateIndexSet) {
        // some index a in candidateIndexSet is cover by some index b in candidateIndexSet
        for (CandidateIndex indexA : candidateIndexSet) {
            for (CandidateIndex indexB : candidateIndexSet) {
                if (indexA == indexB) {
                    continue;
                }
                if (indexA.getSchemaName().equalsIgnoreCase(indexB.getSchemaName())
                    && indexA.getTableName().equalsIgnoreCase(indexB.getTableName())
                    && indexA.getColumnNames().size() < indexB.getColumnNames().size()) {
                    boolean redundant = true;
                    for (int i = 0; i < indexA.getColumnNames().size(); i++) {
                        if (!indexA.getColumnNames().get(i).equalsIgnoreCase(indexB.getColumnNames().get(i))) {
                            redundant = false;
                        }
                    }
                    if (redundant == true) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void selectCandidateIndex(CandidateIndex currentLevelCandidateIndex,
                                      Set<CandidateIndex> currentLevelCandidateIndexSet) {

        HumanReadableRule rule = partitionRuleSet.getRule(
            currentLevelCandidateIndex.getSchemaName(), currentLevelCandidateIndex.getTableName());

        // Do not consider GSI for single or broadcast table
        if ((rule.isSingle() || rule.isBroadcast()) && currentLevelCandidateIndex.isGsi()) {
            return;
        }

        // consider cardinality
        if (currentLevelCandidateIndex.isHighCardinality()
            && (currentLevelCandidateIndex.isNotCoverPrimaryUniqueKey() || currentLevelCandidateIndex.isGsi())
            && !currentLevelCandidateIndex.getTableMeta().isGsi()
            && !currentLevelCandidateIndex.notSupportPartitionGsi()) {
            currentLevelCandidateIndexSet.add(currentLevelCandidateIndex);
        }
    }

    private Set<CandidateIndex> levelUp(CandidateIndex a, CandidateIndex b) {
        if (!a.getSchemaName().equalsIgnoreCase(b.getSchemaName())) {
            return null;
        }
        if (!a.getTableName().equalsIgnoreCase(b.getTableName())) {
            return null;
        }

        Set<String> columnSet = new HashSet<>();
        columnSet.addAll(
            a.getColumnNames().stream().map(columnName -> columnName.toLowerCase()).collect(Collectors.toList()));
        columnSet.addAll(
            b.getColumnNames().stream().map(columnName -> columnName.toLowerCase()).collect(Collectors.toList()));

        if (columnSet.size() != a.getColumnNames().size() + b.getColumnNames().size()) {
            return null;
        }

        List<String> newColumnNames = new ArrayList<>();
        newColumnNames.addAll(a.getColumnNames());
        newColumnNames.addAll(b.getColumnNames());

        String schemaName = a.getSchemaName();
        String tableName = a.getTableName();

        Set<CandidateIndex> candidateIndexSet = new HashSet<>();
        CandidateIndex candidateIndex;

        Collection<HumanReadableRule> rules = partitionPolicyMap.get(schemaName);

        switch (adviseType) {
        case LOCAL_INDEX:
            candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, false, null);
            candidateIndexSet.add(candidateIndex);
            break;
        case GLOBAL_INDEX:
            if (a.hasChangePartitionPolicy() || rules.isEmpty()) {
                candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true, null);
                candidateIndex.changePartitionPolicy(a.getHumanReadableRule());
                candidateIndexSet.add(candidateIndex);
            } else {
                boolean successOnce = false;
                for (HumanReadableRule rule : rules) {
                    candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true, null);
                    boolean success = candidateIndex.changePartitionPolicy(rule);
                    if (success) {
                        candidateIndexSet.add(candidateIndex);
                    }
                    successOnce = successOnce || success;
                }
                if (!successOnce) {
                    candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true, null);
                    candidateIndexSet.add(candidateIndex);
                }
            }
            break;
        case GLOBAL_COVERING_INDEX:
            if (a.hasChangePartitionPolicy() || rules.isEmpty()) {
                candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true,
                    coverableColumnSet.getCoverableColumns(schemaName, tableName));
                candidateIndex.changePartitionPolicy(a.getHumanReadableRule());
                candidateIndexSet.add(candidateIndex);
            } else {
                boolean successOnce = false;
                for (HumanReadableRule rule : rules) {
                    candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true,
                        coverableColumnSet.getCoverableColumns(schemaName, tableName));
                    boolean success = candidateIndex.changePartitionPolicy(rule);
                    if (success) {
                        candidateIndexSet.add(candidateIndex);
                    }
                    successOnce = successOnce || success;
                }
                if (!successOnce) {
                    candidateIndex = new CandidateIndex(schemaName, tableName, newColumnNames, true,
                        coverableColumnSet.getCoverableColumns(schemaName, tableName));
                    candidateIndexSet.add(candidateIndex);
                }
            }
            break;
        default:
            throw new AssertionError("unKnown type");
        }

        return candidateIndexSet;
    }

    public Set<CandidateIndex> getSingleColumnCandidateIndexSet(IndexableColumnSet indexableColumnSet) {
        Set<CandidateIndex> candidateIndexSet = new HashSet<>();
        for (String schemaName : indexableColumnSet.m.keySet()) {
            for (String tableName : indexableColumnSet.m.get(schemaName).keySet()) {
                for (String columnName : indexableColumnSet.m.get(schemaName).get(tableName)) {
                    List<String> index = new ArrayList<>();
                    index.add(columnName);

                    Collection<HumanReadableRule> rules = partitionPolicyMap.get(schemaName);

                    CandidateIndex candidateIndex;

                    switch (adviseType) {
                    case LOCAL_INDEX:
                        candidateIndex = new CandidateIndex(schemaName, tableName, index, false, null);
                        candidateIndexSet.add(candidateIndex);
                        break;
                    case GLOBAL_INDEX: {
                        if (rules.isEmpty()) {
                            candidateIndex = new CandidateIndex(schemaName, tableName, index, true, null);
                            candidateIndexSet.add(candidateIndex);
                        } else {
                            boolean successOnce = false;
                            for (HumanReadableRule rule : rules) {
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, true, null);
                                boolean success = candidateIndex.changePartitionPolicy(rule);
                                if (success) {
                                    candidateIndexSet.add(candidateIndex);
                                }
                                successOnce = successOnce || success;
                            }
                            if (!successOnce) {
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, true, null);
                                candidateIndexSet.add(candidateIndex);
                            }
                        }
                        break;
                    }
                    case GLOBAL_COVERING_INDEX: {
                        if (rules.isEmpty()) {
                            candidateIndex = new CandidateIndex(schemaName, tableName, index, true,
                                coverableColumnSet.getCoverableColumns(schemaName, tableName));
                            candidateIndexSet.add(candidateIndex);
                        } else {
                            boolean successOnce = false;
                            for (HumanReadableRule rule : rules) {
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, true,
                                    coverableColumnSet.getCoverableColumns(schemaName, tableName));
                                boolean success = candidateIndex.changePartitionPolicy(rule);
                                if (success) {
                                    candidateIndexSet.add(candidateIndex);
                                }
                                successOnce = successOnce || success;
                            }
                            if (!successOnce) {
                                candidateIndex = new CandidateIndex(schemaName, tableName, index, true,
                                    coverableColumnSet.getCoverableColumns(schemaName, tableName));
                                candidateIndexSet.add(candidateIndex);
                            }
                        }
                        break;
                    }
                    default:
                        throw new AssertionError("unKnown type");
                    }
                }
            }
        }
        return candidateIndexSet;
    }

    private boolean lessThan(RelOptCost cost1, RelOptCost cost2) {
        return cost1.isLt(cost2) && (cost1.getIo() < 0.95 * cost2.getIo() || cost1.getNet() < 0.95 * cost2.getNet());
    }
}

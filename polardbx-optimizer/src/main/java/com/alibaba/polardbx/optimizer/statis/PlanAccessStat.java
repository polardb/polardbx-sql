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

package com.alibaba.polardbx.optimizer.statis;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PlanAccessStat {

    private static final String ANY_OTHER_TABLE_NAME = "any";

    public static String buildFullTblNameLowercase(String db, String tb) {
        StringBuilder sb = new StringBuilder();
        sb.append(db).append(".").append(tb);
        return sb.toString().toLowerCase();
    }

    public static String buildRelationKey(String firstTb, String secondTb) {
        if (secondTb == null) {
            secondTb = ANY_OTHER_TABLE_NAME;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(firstTb).append("--").append(secondTb);
        return sb.toString();
    }

    public static String fetchTableType(String db, String tb) {
        StringBuilder resultSb = new StringBuilder("");
        if (SystemDbHelper.isDBBuildIn(db)) {
            resultSb.append("system");
            return resultSb.toString();
        }

        TableMeta tbMeta = OptimizerContext.getContext(db).getLatestSchemaManager().getTable(tb);
        if (tbMeta == null) {
            return resultSb.toString();
        }

        PartitionInfo partInfo = tbMeta.getPartitionInfo();
        if (partInfo != null) {
            resultSb.append("auto/");
            if (partInfo.isGsiSingleOrSingleTable()) {
                resultSb.append("single");
            } else if (partInfo.isGsiBroadcastOrBroadcast()) {
                resultSb.append("broadcast");
            } else {
                resultSb.append("sharding");
            }
            return resultSb.toString();
        }

        TddlRuleManager tm = OptimizerContext.getContext(db).getLatestSchemaManager().getTddlRuleManager();
        TableRule tableRule = tm.getTableRule(tb);
        if (tableRule != null) {
            resultSb.append("drds/");
            if (tm.isBroadCast(tb)) {
                resultSb.append("broadcast");
            } else if (tm.isShard(tb)) {
                resultSb.append("sharding");
            } else {
                resultSb.append("single");
            }
        } else {
            resultSb.append("drds/single");
        }

        return resultSb.toString();
    }

    public static class TableJoinStatInfo {
        public String relationKey;
        public String tblName;
        public String otherTblName;
        public Long accessCount = 0L;
        public Set<String> templateIdSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        public String templateIdSetStr;
    }

    public static class PlanJoinClosureStatInfo {
        public String closureKey;
        public Set<String> joinClosureTableSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        public String joinTableSetStr;
        public Long accessCount = 0L;
        public Long templateCount = 0L;
        public Set<String> templateIdSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        public String templateIdSetStr;

    }

    /**
     * Key： join_relation_key
     * Val:  stat_info
     */
    public static Map<String, TableJoinStatInfo> statTableJoinAccessStat() {

        Map<String, TableJoinStatInfo> tblJoinAccessStatMap = new HashMap<>();

        ConcurrentMap<PlanCache.CacheKey, ExecutionPlan> planCacheInfo = PlanCache.getInstance().getCache().asMap();
        for (Map.Entry<PlanCache.CacheKey, ExecutionPlan> planCacheItem : planCacheInfo.entrySet()) {
            ExecutionPlan plan = planCacheItem.getValue();
            Set<Pair<String, String>> tbSet = plan.getTableSet();
            AtomicLong hitCount = plan.getHitCount();
            String templateId = plan.getCacheKey().getTemplateId();

            String currDb = planCacheItem.getKey().getSchema();
            Iterator<Pair<String, String>> tbSetItor = tbSet.iterator();
            TreeSet<String> allFullTbNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            while (tbSetItor.hasNext()) {
                Pair<String, String> dbAndTb = tbSetItor.next();
                String dbName = dbAndTb.getKey();
                if (dbName == null) {
                    dbName = currDb;
                }
                String tbName = dbAndTb.getValue();
                String fullTbName = buildFullTblNameLowercase(dbName, tbName);
                allFullTbNameSet.add(fullTbName);
            }
            Iterator<String> firstTbItor = allFullTbNameSet.iterator();
            if (!firstTbItor.hasNext()) {
                continue;
            }
            String firstTb = firstTbItor.next();
            Iterator<String> otherTbItor = allFullTbNameSet.iterator();
            while (otherTbItor.hasNext()) {
                String otherTb = otherTbItor.next();
                if (otherTb.equals(firstTb)) {
                    continue;
                }

                String relKey = buildRelationKey(firstTb, otherTb);
                TableJoinStatInfo statInfo = tblJoinAccessStatMap.get(relKey);
                if (statInfo == null) {
                    statInfo = new TableJoinStatInfo();
                    statInfo.relationKey = relKey;
                    statInfo.tblName = firstTb;
                    statInfo.otherTblName = otherTb;
                    statInfo.accessCount = hitCount.get();
                    statInfo.templateIdSet.add(templateId);
                    tblJoinAccessStatMap.put(relKey, statInfo);
                } else {
                    statInfo.accessCount += hitCount.get();
                    if (!statInfo.templateIdSet.contains(templateId)) {
                        statInfo.templateIdSet.add(templateId);
                    }
                }
            }
        }

        for (Map.Entry<String, PlanAccessStat.TableJoinStatInfo> statItem : tblJoinAccessStatMap.entrySet()) {
            PlanAccessStat.TableJoinStatInfo statInfo = statItem.getValue();
            statInfo.templateIdSetStr = String.join(",", statInfo.templateIdSet);
        }
        return tblJoinAccessStatMap;
    }

    /**
     * Key: full_table_name
     * Val: stat info of one table
     */
    public static Map<String, PlanAccessStat.TableJoinStatInfo> statTableAccess() {
        Map<String, PlanAccessStat.TableJoinStatInfo> tblAccessStatMap = new HashMap<>();
        ConcurrentMap<PlanCache.CacheKey, ExecutionPlan> planCacheInfo = PlanCache.getInstance().getCache().asMap();
        for (Map.Entry<PlanCache.CacheKey, ExecutionPlan> planCacheItem : planCacheInfo.entrySet()) {
            ExecutionPlan plan = planCacheItem.getValue();
            long hintCnt = plan.getHitCount().get();
            String templateId = plan.getCacheKey().getTemplateId();
            Set<Pair<String, String>> tbSet = planCacheItem.getValue().getTableSet();
            String currDb = planCacheItem.getKey().getSchema();
            Iterator<Pair<String, String>> tbSetItor = tbSet.iterator();
            while (tbSetItor.hasNext()) {
                Pair<String, String> dbAndTb = tbSetItor.next();
                String dbName = dbAndTb.getKey();
                if (dbName == null) {
                    dbName = currDb;
                }
                String tbName = dbAndTb.getValue();
                String fullTbName = buildFullTblNameLowercase(dbName, tbName);
                PlanAccessStat.TableJoinStatInfo statInfoOfTb = tblAccessStatMap.get(fullTbName);
                if (statInfoOfTb == null) {
                    statInfoOfTb = new TableJoinStatInfo();
                    String relKey = PlanAccessStat.buildRelationKey(fullTbName, null);
                    statInfoOfTb.relationKey = relKey;
                    statInfoOfTb.tblName = fullTbName;
                    statInfoOfTb.otherTblName = "";
                    statInfoOfTb.accessCount = 1L;
                    statInfoOfTb.templateIdSet.add(templateId);
                    tblAccessStatMap.put(fullTbName, statInfoOfTb);
                } else {
                    statInfoOfTb.accessCount += hintCnt;
                    if (!statInfoOfTb.templateIdSet.contains(templateId)) {
                        statInfoOfTb.templateIdSet.add(templateId);
                    }
                }
            }
        }

        for (Map.Entry<String, PlanAccessStat.TableJoinStatInfo> statItem : tblAccessStatMap.entrySet()) {
            PlanAccessStat.TableJoinStatInfo statInfo = statItem.getValue();
            statInfo.templateIdSetStr = String.join(",", statInfo.templateIdSet);
        }
        return tblAccessStatMap;
    }

    public static List<PlanJoinClosureStatInfo> collectTableJoinClosureStat(
        List<List<Map<String, Object>>> joinClosureListOfCn) {

        /**
         * The sync result of ShowTableJoinClosureSyncAction of each cn
         */
//        result.addColumn("JOIN_CLOSURE_KEY", DataTypes.StringType);
//        result.addColumn("JOIN_CLOSURE_TABLE_SET", DataTypes.StringType);
//        result.addColumn("JOIN_CLOSURE_SIZE", DataTypes.LongType);
//        result.addColumn("ACCESS_COUNT", DataTypes.LongType);
//        result.addColumn("TEMPLATE_ID_SET", DataTypes.LongType);

        /**
         * Build Join closure by join graph
         */
        Map<String, Set<String>> globalJoinGraph = new HashMap<>();
        List<List<PlanJoinClosureStatInfo>> closureStatsOfAllCn = new ArrayList<>();
        for (int i = 0; i < joinClosureListOfCn.size(); i++) {
            List<Map<String, Object>> closureRecInfosOfOneCn = joinClosureListOfCn.get(i);
            if (closureRecInfosOfOneCn == null || closureRecInfosOfOneCn.isEmpty()) {
                /**
                 * Maybe some cn exec sync action failed or some cn is not init
                 */
                continue;
            }
            List<PlanJoinClosureStatInfo> closureStatsOfOneCn = new ArrayList<>();
            for (int j = 0; j < closureRecInfosOfOneCn.size(); j++) {
                Map<String, Object> closeRecInfo = closureRecInfosOfOneCn.get(j);
                if (closeRecInfo == null) {
                    continue;
                }

                String closureKey = (String) closeRecInfo.get("JOIN_CLOSURE_KEY");
                Long accessCount = (Long) closeRecInfo.get("ACCESS_COUNT");

                String tbSetStr = (String) closeRecInfo.get("JOIN_CLOSURE_TABLE_SET");
                String[] tbSetArr = tbSetStr.split(",");
                List<String> tbSetList = new ArrayList<>();
                for (int k = 0; k < tbSetArr.length; k++) {
                    tbSetList.add(tbSetArr[k]);
                }
                Set<String> tbSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                tbSet.addAll(tbSetList);

                String templateIdSetStr = (String) closeRecInfo.get("TEMPLATE_ID_SET");
                String[] templateIdSetArr = templateIdSetStr.split(",");
                Set<String> templateIdSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                for (int k = 0; k < templateIdSetArr.length; k++) {
                    templateIdSet.add(templateIdSetArr[k]);
                }

                PlanJoinClosureStatInfo statInfo = new PlanJoinClosureStatInfo();
                statInfo.closureKey = closureKey;
                statInfo.accessCount = accessCount;

                statInfo.templateIdSetStr = templateIdSetStr;
                statInfo.templateIdSet = templateIdSet;

                statInfo.joinTableSetStr = tbSetStr;
                statInfo.joinClosureTableSet = tbSet;
                closureStatsOfOneCn.add(statInfo);

                PlanAccessStat.addJoinTablesToJoinGraph(globalJoinGraph, tbSetList);
            }
            closureStatsOfAllCn.add(closureStatsOfOneCn);
        }
        List<Set<String>> globalJoinClosureList = PlanAccessStat.buildJoinClosure(globalJoinGraph);

        List<PlanJoinClosureStatInfo> globalJoinClosureStatInfoList = new ArrayList<>();
        for (int i = 0; i < globalJoinClosureList.size(); i++) {
            Set<String> globalJoinClosureTbSet = globalJoinClosureList.get(i);
            String tbSetStr = String.join(",", globalJoinClosureTbSet);
            String closureKey = Integer.toHexString(tbSetStr.hashCode());
            PlanJoinClosureStatInfo statInfo = new PlanJoinClosureStatInfo();
            statInfo.closureKey = closureKey;
            statInfo.joinClosureTableSet = globalJoinClosureTbSet;
            statInfo.joinTableSetStr = tbSetStr;
            globalJoinClosureStatInfoList.add(statInfo);
        }

        for (int i = 0; i < closureStatsOfAllCn.size(); i++) {
            List<PlanJoinClosureStatInfo> statInfosOfOneCn = closureStatsOfAllCn.get(i);
            for (int j = 0; j < statInfosOfOneCn.size(); j++) {
                PlanJoinClosureStatInfo statInfo = statInfosOfOneCn.get(j);
                String firstTb = statInfo.joinClosureTableSet.iterator().next();
                for (int k = 0; k < globalJoinClosureList.size(); k++) {
                    PlanJoinClosureStatInfo globalStatInfo = globalJoinClosureStatInfoList.get(k);
                    if (globalStatInfo.joinClosureTableSet.contains(firstTb)) {
                        globalStatInfo.accessCount += statInfo.accessCount;
                        for (String templateId : statInfo.templateIdSet) {
                            if (!globalStatInfo.templateIdSet.contains(templateId)) {
                                globalStatInfo.templateIdSet.add(templateId);
                            }
                        }
                    }
                }
            }
        }
        for (int i = 0; i < globalJoinClosureStatInfoList.size(); i++) {
            PlanJoinClosureStatInfo globalStatInfo = globalJoinClosureStatInfoList.get(i);
            globalStatInfo.templateCount = Long.valueOf(globalStatInfo.templateIdSet.size());
        }

        return globalJoinClosureStatInfoList;
    }

    public static Map<String, PlanJoinClosureStatInfo> tableJoinClosureStat() {

        /**
         * Build join graph
         *
         * key: node(fullTbName)
         * val: neighborSet(join-relations)
         */
        Map<String, Set<String>> joinGraph = new HashMap<>();
        ConcurrentMap<PlanCache.CacheKey, ExecutionPlan> planCacheInfo = PlanCache.getInstance().getCache().asMap();
        for (Map.Entry<PlanCache.CacheKey, ExecutionPlan> planCacheItem : planCacheInfo.entrySet()) {
            Set<Pair<String, String>> tbSet = planCacheItem.getValue().getTableSet();
            String currDb = planCacheItem.getKey().getSchema();
            Iterator<Pair<String, String>> tbSetItor = tbSet.iterator();
            List<String> joinTblList = new ArrayList<>();
            while (tbSetItor.hasNext()) {
                Pair<String, String> dbAndTb = tbSetItor.next();
                String dbName = dbAndTb.getKey();
                if (dbName == null) {
                    dbName = currDb;
                }
                String tbName = dbAndTb.getValue();
                String fullTbName = buildFullTblNameLowercase(dbName, tbName);
                joinTblList.add(fullTbName);
            }
            PlanAccessStat.addJoinTablesToJoinGraph(joinGraph, joinTblList);
        }

        /**
         * Build Join closure by join graph
         */
        List<Set<String>> joinClosureList = PlanAccessStat.buildJoinClosure(joinGraph);
        Map<String, PlanJoinClosureStatInfo> joinClosureStatInfoMap = new HashMap<>();
        for (int i = 0; i < joinClosureList.size(); i++) {
            PlanJoinClosureStatInfo statInfo = new PlanJoinClosureStatInfo();
            Set<String> joinClosureTblSet = joinClosureList.get(i);

            statInfo.joinClosureTableSet = joinClosureTblSet;
            statInfo.joinTableSetStr = String.join(",", joinClosureTblSet);
            String closureKey = Integer.toHexString(statInfo.joinTableSetStr.hashCode());
            statInfo.closureKey = closureKey;

            joinClosureStatInfoMap.put(closureKey, statInfo);
        }

        for (Map.Entry<PlanCache.CacheKey, ExecutionPlan> planCacheItem : planCacheInfo.entrySet()) {
            ExecutionPlan plan = planCacheItem.getValue();
            Set<Pair<String, String>> tbSet = planCacheItem.getValue().getTableSet();
            String currDb = planCacheItem.getKey().getSchema();
            Iterator<Pair<String, String>> tbSetItor = tbSet.iterator();
            if (!tbSetItor.hasNext()) {
                continue;
            }
            Pair<String, String> dbAndTb = tbSetItor.next();
            String dbName = dbAndTb.getKey();
            if (dbName == null) {
                dbName = currDb;
            }
            String tbName = dbAndTb.getValue();
            String firstFullTbName = buildFullTblNameLowercase(dbName, tbName);
            for (Map.Entry<String, PlanJoinClosureStatInfo> closureStatItem : joinClosureStatInfoMap.entrySet()) {
                PlanJoinClosureStatInfo statInfo = closureStatItem.getValue();
                if (statInfo.joinClosureTableSet.contains(firstFullTbName)) {
                    statInfo.accessCount += plan.getHitCount().get();
                    String templatedId = plan.getCacheKey().getTemplateId();
                    if (!statInfo.templateIdSet.contains(templatedId)) {
                        statInfo.templateIdSet.add(templatedId);
                    }
                    break;
                }
            }

            for (Map.Entry<String, PlanJoinClosureStatInfo> closureStatItem : joinClosureStatInfoMap.entrySet()) {
                PlanJoinClosureStatInfo statInfo = closureStatItem.getValue();
                statInfo.templateIdSetStr = String.join(",", statInfo.templateIdSet);
            }
        }
        return joinClosureStatInfoMap;
    }

    // DFS 遍历
    private static void joinGraphDfs(Map<String, Set<String>> graph, String node, Set<String> visited,
                                     Set<String> component) {
        visited.add(node);
        component.add(node);
        for (String neighbor : graph.get(node)) {
            if (!visited.contains(neighbor)) {
                joinGraphDfs(graph, neighbor, visited, component);
            }
        }
    }

    /**
     * 求JOIN的连通分量, 即JOIN闭包
     */
    public static List<Set<String>> buildJoinClosure(Map<String, Set<String>> graph) {
        Set<String> visited = new HashSet<>();
        List<Set<String>> connected = new ArrayList<>();
        for (String node : graph.keySet()) {
            if (!visited.contains(node)) {
                Set<String> component = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                joinGraphDfs(graph, node, visited, component);
                visited.addAll(component);
                connected.add(component);
            }
        }
        return connected;
    }

    /**
     * Build join graph
     */
    public static void addJoinTablesToJoinGraph(Map<String, Set<String>> joinGraph /* Output */,
                                                List<String> joinTables /*Input*/) {

        for (int i = 0; i < joinTables.size(); i++) {
            String fullTbName = joinTables.get(i);
            Set<String> neighborTblSet = joinGraph.get(fullTbName);
            if (neighborTblSet == null) {
                neighborTblSet = new HashSet<>();
                joinGraph.put(fullTbName, neighborTblSet);
            }
        }
        if (joinTables.size() > 0) {
            String firstTbl = joinTables.get(0);
            Set<String> neighborTblSet = joinGraph.get(firstTbl);
            for (int i = 1; i < joinTables.size(); i++) {
                String otherTbl = joinTables.get(i);
                if (!neighborTblSet.contains(otherTbl)) {
                    neighborTblSet.add(otherTbl);
                }
                Set<String> neighborTblSetOfOtherTbl = joinGraph.get(otherTbl);
                if (!neighborTblSetOfOtherTbl.contains(firstTbl)) {
                    neighborTblSetOfOtherTbl.add(firstTbl);
                }
            }
        }
    }

}

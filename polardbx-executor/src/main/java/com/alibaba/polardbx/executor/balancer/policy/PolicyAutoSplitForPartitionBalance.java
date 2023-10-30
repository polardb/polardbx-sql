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

package com.alibaba.polardbx.executor.balancer.policy;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.action.ActionInitPartitionDb;
import com.alibaba.polardbx.executor.balancer.action.ActionLockResource;
import com.alibaba.polardbx.executor.balancer.action.ActionSplitPartition;
import com.alibaba.polardbx.executor.balancer.action.ActionSplitTablePartition;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.balancer.action.ActionWriteDataDistLog;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.serial.DataDistInfo;
import com.alibaba.polardbx.executor.balancer.solver.MixedModel;
import com.alibaba.polardbx.executor.balancer.solver.PartitionSplitInfo;
import com.alibaba.polardbx.executor.balancer.solver.Solution;
import com.alibaba.polardbx.executor.balancer.splitpartition.SplitPoint;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.rebalance.RebalanceTarget;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.balancer.policy.PolicyUtils.getGroupDetails;

public class PolicyAutoSplitForPartitionBalance implements BalancePolicy {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyPartitionBalance.class);

    @Override
    public String name() {
        return SqlRebalance.POLICY_AUTO_SPLIT_FOR_PARTITION_BALANCE;
    }

    private String groupByGroup(PartitionGroupStat p) {
        return p.getFirstPartition().getLocation().getGroupKey();
    }

    /**
     *
     */

    @Override
    public List<BalanceAction> applyToMultiDb(ExecutionContext ec,
                                              Map<String, BalanceStats> stats,
                                              BalanceOptions options,
                                              List<String> schemaNameList) {
        List<BalanceAction> result = new ArrayList<>();

        // Initialize new storage instance if needed
        List<DbInfoRecord> dbRecords = DbTopologyManager.getNewPartDbInfoFromMetaDb();
        boolean refreshTopology = false;
        if (CollectionUtils.isNotEmpty(dbRecords)) {
            ActionInitPartitionDb actionInit = new ActionInitPartitionDb(ec.getSchemaName());
            result.add(actionInit);
            refreshTopology = true;
        }

        // Balance each database
        for (String schema : schemaNameList) {
            for (BalanceAction action : applyToDb(ec, stats.get(schema), options, schema)) {
                if (!action.getName().equals(ActionInitPartitionDb.getActionName())) {
                    result.add(action);
                } else if (!refreshTopology) {
                    result.add(action);
                    refreshTopology = true;
                }
            }
        }

        return result;
    }

    @Override
    public List<BalanceAction> applyToTable(ExecutionContext ec,
                                            BalanceOptions options,
                                            BalanceStats stats,
                                            String schemaName,
                                            String tableName) {

        DdlHelper.getServerConfigManager().executeBackgroundSql("refresh topology", schemaName, null);

        List<BalanceAction> actions = new ArrayList<>();

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.NON_HOT_SPLIT;
        if (!options.solveLevel.equals("DEFAULT") && options.solveLevel.equalsIgnoreCase("hot_split")) {
            solveLevel = MixedModel.SolveLevel.HOT_SPLIT;
        }

        String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        ActionLockResource lock = new ActionLockResource(schemaName, Sets.newHashSet(name));
        actions.add(lock);

        actions.add(new ActionInitPartitionDb(schemaName));

        // structurize pg list into pg map

        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();
        if (pgList.isEmpty()) {
            return actions;
        }
        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);

        List<PartitionGroupStat> toRebalancePgList = pgList.stream()
            .filter(pg -> LocalityInfoUtils.withoutRestrictedAllowedGroup(schemaName, pg.pg.getTg_id(),
                pg.pg.getPartition_name()))
            .collect(Collectors.toList());

        if (toRebalancePgList.isEmpty() || !supportAutoSplit(
            toRebalancePgList.get(0).getLargestSizePartition().get().getPartitionStrategy())) {
            return actions;
        }

        int N = toRebalancePgList.size();
        int M = groupDetail.size();
        int[] originalPlace = new int[N];
        double[] partitionSize = new double[N];
        Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        List<String> groupNames = groupDetail.keySet().stream().collect(Collectors.toList());
        for (int i = 0; i < M; i++) {
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
        }
        for (int i = 0; i < N; i++) {
            PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
            toRebalancePgMap.put(i, partitionGroupStat);
            String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
            originalPlace[i] = groupDetailReverseMap.get(groupKey);
            partitionSize[i] = partitionGroupStat.getDataRows();
        }
        PartitionSplitInfo partitionSplitInfo = new PartitionSplitInfo(toRebalancePgMap, solveLevel);
        List<ActionSplitTablePartition> splitActions = new ArrayList<>();
        Solution solution = MixedModel.solveSplitPartition(M, N, originalPlace, partitionSize, partitionSplitInfo);
        if (solution.withValidSolve) {
            //How to generate split action.
            if (solution.withSplitPartition) {
                Map<Integer, Pair<List<SearchDatumInfo>, List<Double>>> splitPointsMap =
                    partitionSplitInfo.splitPointsMap;
                for (Integer splitIndex : splitPointsMap.keySet()) {
                    PartitionStat partitionStat =
                        toRebalancePgList.get(splitIndex).getLargestSizePartition().orElse(null);
                    if (partitionStat == null) {
                        continue;
                    }
                    List<SearchDatumInfo> splitPoints = splitPointsMap.get(splitIndex).getKey();
                    String partitionName = partitionStat.getPartitionName();
                    String newPartNamePattern = "%s_%d";
                    List<SplitPoint> fullSplitPoints = new ArrayList<>();
                    for (int j = 0; j < splitPoints.size(); j++) {
                        SplitPoint fullSplitPoint =
                            new SplitPoint(splitPoints.get(j), String.format(newPartNamePattern, partitionName, j + 1),
                                String.format(newPartNamePattern, partitionName, j + 2));
                        fullSplitPoints.add(fullSplitPoint);
                    }
                    ActionSplitTablePartition actionSplitTablePartition = new ActionSplitTablePartition(
                        partitionStat.getSchema(), partitionStat, fullSplitPoints, stats
                    );
                    splitActions.add(actionSplitTablePartition);
                }

                if (!splitActions.isEmpty()) {
                    LOG.info("DataBalance move partition for data balance: " + splitActions);
                }
            }
        } else {
            List<Integer> errorPartitions = partitionSplitInfo.errorPartitions;
            List<String> errorPartitionNames =
                errorPartitions.stream().map(o -> toRebalancePgList.get(o).pg.getPartition_name()).collect(
                    Collectors.toList());
            String errorInfo = String.format("Please check partitions of table [%s]: ", tableName) + StringUtils.join(
                errorPartitionNames, " ,") + " auto split failed!";
            throw new TddlNestableRuntimeException(errorInfo);
        }
        actions.addAll(splitActions);
        return actions;
    }

    @Override
    public List<BalanceAction> applyToTableGroup(ExecutionContext ec,
                                                 BalanceOptions options,
                                                 BalanceStats stats,
                                                 String schemaName,
                                                 String tableGroupName) {

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.NON_HOT_SPLIT;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.HOT_SPLIT;
        }
        DdlHelper.getServerConfigManager().executeBackgroundSql("refresh topology", schemaName, null);

        List<BalanceAction> actions = new ArrayList<>();

        String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        ActionLockResource lock = new ActionLockResource(schemaName, Sets.newHashSet(name));
        actions.add(lock);

        actions.add(new ActionInitPartitionDb(schemaName));

        // structurize pg list into pg map

        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();
        if (pgList.isEmpty()) {
            return actions;
        }
        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);

        List<PartitionGroupStat> toRebalancePgList = pgList.stream()
            .filter(pg -> LocalityInfoUtils.withoutRestrictedAllowedGroup(schemaName, pg.pg.getTg_id(),
                pg.pg.getPartition_name()))
            .collect(Collectors.toList());

        if (toRebalancePgList.isEmpty() || !supportAutoSplit(
            toRebalancePgList.get(0).getLargestSizePartition().get().getPartitionStrategy())) {
            return actions;
        }

        int N = toRebalancePgList.size();
        int M = groupDetail.size();
        int[] originalPlace = new int[N];
        double[] partitionSize = new double[N];
        Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        List<String> groupNames = groupDetail.keySet().stream().collect(Collectors.toList());
        for (int i = 0; i < M; i++) {
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
        }
        for (int i = 0; i < N; i++) {
            PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
            toRebalancePgMap.put(i, partitionGroupStat);
            String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
            originalPlace[i] = groupDetailReverseMap.get(groupKey);
            partitionSize[i] = partitionGroupStat.getDataRows();
        }
        PartitionSplitInfo partitionSplitInfo = new PartitionSplitInfo(toRebalancePgMap, solveLevel);
        List<ActionSplitPartition> splitActions = new ArrayList<>();
        Solution solution = MixedModel.solveSplitPartition(M, N, originalPlace, partitionSize, partitionSplitInfo);
        if (solution.withValidSolve) {
            //How to generate split action.
            if (solution.withSplitPartition) {
                Map<Integer, Pair<List<SearchDatumInfo>, List<Double>>> splitPointsMap =
                    partitionSplitInfo.splitPointsMap;
                for (Integer splitIndex : splitPointsMap.keySet()) {
                    PartitionStat partitionStat =
                        toRebalancePgList.get(splitIndex).getLargestSizePartition().orElse(null);
                    if (partitionStat == null) {
                        continue;
                    }
                    List<SearchDatumInfo> splitPoints = splitPointsMap.get(splitIndex).getKey();
                    String partitionName = partitionStat.getPartitionName();
                    String newPartNamePattern = "%s_%d";
                    List<SplitPoint> fullSplitPoints = new ArrayList<>();
                    for (int j = 0; j < splitPoints.size(); j++) {
                        SplitPoint fullSplitPoint =
                            new SplitPoint(splitPoints.get(j), String.format(newPartNamePattern, partitionName, j + 1),
                                String.format(newPartNamePattern, partitionName, j + 2));
                        fullSplitPoints.add(fullSplitPoint);
                    }
                    ActionSplitPartition actionSplitPartition = new ActionSplitPartition(
                        partitionStat.getSchema(), partitionStat, fullSplitPoints, stats
                    );
                    splitActions.add(actionSplitPartition);
                }

                if (!splitActions.isEmpty()) {
                    LOG.info("DataBalance move partition for data balance: " + splitActions);
                }
            }
        } else {
            List<Integer> errorPartitions = partitionSplitInfo.errorPartitions;
            List<String> errorPartitionNames =
                errorPartitions.stream().map(o -> toRebalancePgList.get(o).pg.getPartition_name()).collect(
                    Collectors.toList());
            String errorInfo =
                String.format("Please check partitions of tablegroup [%s]: ", tableGroupName) + StringUtils.join(
                    errorPartitionNames, " ,") + " auto split failed!";
            throw new TddlNestableRuntimeException(errorInfo);
        }
        actions.addAll(splitActions);
        return actions;
    }

    @Override
    public List<BalanceAction> applyToDb(ExecutionContext ec,
                                         BalanceStats stats,
                                         BalanceOptions options,
                                         String schema) {

        DdlHelper.getServerConfigManager().executeBackgroundSql("refresh topology", schema, null);

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.NON_HOT_SPLIT;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.HOT_SPLIT;
        }

        List<BalanceAction> actions = new ArrayList<>();

        String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schema);
        ActionLockResource lock = new ActionLockResource(schema, Sets.newHashSet(name));
        actions.add(lock);

        actions.add(new ActionInitPartitionDb(schema));

        // structurize pg list into pg map

        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();
        if (pgList.isEmpty()) {
            return actions;
        }
        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schema);

        // map dn to i, map partition group to j.
        // X[i][j] indicate that whether partition group j exists in dn i.
        // w[j] indicate the size of partition group j.
        // we would solve the problem by linear planning.
        // if number of partition group was more than 2048, number of dn is more than 32, we would preprocess this problem to reduce dimension.

        // firstly, filter partition group with locality or partition group in oss.
        // secondly, preprocessing problem and generate preprocessing map.
        // thridly, generate truely map of partitonGroupStat to
        // forthly, call solver and get the solution.
        // fively, map back from preprocessing map.
        // finally, map into actions.
        // what's important? hot value schedule.
        // auto split.

        List<PartitionGroupStat> toRebalanceAllPgList = pgList.stream()
            .filter(pg -> LocalityInfoUtils.withoutRestrictedAllowedGroup(schema, pg.pg.getTg_id(),
                pg.pg.getPartition_name()))
            .collect(Collectors.toList());

        if (toRebalanceAllPgList.size() < 0) {
            return actions;
        }

        Map<String, List<PartitionGroupStat>> toRebalancePgListGroupByTg =
            GeneralUtil.emptyIfNull(toRebalanceAllPgList).stream()
                .collect(Collectors.groupingBy(o -> o.getTgName(),
                    Collectors.mapping(o -> o, Collectors.toList())));

        int M = groupDetail.size();

        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<Integer, String> storageInstMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        List<String> groupNames = groupDetail.keySet().stream().collect(Collectors.toList());
        List<String> storageInsts = groupDetail.keySet().stream().map(o -> groupDetail.get(o).storageInstId).collect(
            Collectors.toList());
        for (int i = 0; i < M; i++) {
            storageInstMap.put(i, storageInsts.get(i));
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
        }

        List<String> tableGroupNames = new ArrayList<>(toRebalancePgListGroupByTg.keySet());
        DataDistInfo dataDistInfo = DataDistInfo.fromSchemaAndInstMap(schema, storageInstMap, groupDetailMap);
        for (int k = 0; k < tableGroupNames.size(); k++) {
            String tgName = tableGroupNames.get(k);
            List<PartitionGroupStat> toRebalancePgList = toRebalancePgListGroupByTg.get(tgName);

            if (toRebalancePgList.isEmpty() || !supportAutoSplit(
                toRebalancePgList.get(0).getLargestSizePartition().get().getPartitionStrategy())) {
                continue;
            }
            int N = toRebalancePgList.size();

            int[] originalPlace = new int[N];
            double[] partitionSize = new double[N];
            Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();

            for (int i = 0; i < N; i++) {
                PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
                toRebalancePgMap.put(i, partitionGroupStat);
                String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
                originalPlace[i] = groupDetailReverseMap.get(groupKey);
                partitionSize[i] = partitionGroupStat.getDataRows();
            }
            PartitionSplitInfo partitionSplitInfo = new PartitionSplitInfo(toRebalancePgMap, solveLevel);
            List<ActionSplitPartition> splitActions = new ArrayList<>();
            Solution solution = MixedModel.solveSplitPartition(M, N, originalPlace, partitionSize, partitionSplitInfo);
            Map<String, Pair<List<SplitPoint>, List<Double>>> allSplitPoints = new HashMap<>();
            if (solution.withValidSolve) {
                //How to generate split action.
                if (solution.withSplitPartition) {
                    Map<Integer, Pair<List<SearchDatumInfo>, List<Double>>> splitPointsMap =
                        partitionSplitInfo.splitPointsMap;
                    for (Integer splitIndex : splitPointsMap.keySet()) {
                        PartitionStat partitionStat =
                            toRebalancePgList.get(splitIndex).getLargestSizePartition().orElse(null);
                        if (partitionStat == null) {
                            continue;
                        }
                        List<SearchDatumInfo> splitPoints = splitPointsMap.get(splitIndex).getKey();
                        String partitionName = partitionStat.getPartitionName();
                        String newPartNamePattern = "%s_%d";
                        List<SplitPoint> fullSplitPoints = new ArrayList<>();
                        for (int j = 0; j < splitPoints.size(); j++) {
                            SplitPoint fullSplitPoint = new SplitPoint(splitPoints.get(j),
                                String.format(newPartNamePattern, partitionName, j + 1),
                                String.format(newPartNamePattern, partitionName, j + 2));
                            fullSplitPoints.add(fullSplitPoint);
                        }
                        allSplitPoints.put(partitionName,
                            Pair.of(fullSplitPoints, splitPointsMap.get(splitIndex).getValue()));
                        ActionSplitPartition actionSplitPartition = new ActionSplitPartition(
                            partitionStat.getSchema(), partitionStat, fullSplitPoints, stats
                        );
                        splitActions.add(actionSplitPartition);
                    }

                    if (!splitActions.isEmpty()) {
                        LOG.info("DataBalance move partition for data balance: " + splitActions);
                    }
                }
            } else {
                List<Integer> errorPartitions = partitionSplitInfo.errorPartitions;
                List<String> errorPartitionNames =
                    errorPartitions.stream().map(o -> toRebalancePgList.get(o).pg.getPartition_name())
                        .collect(
                            Collectors.toList());
                String errorInfo =
                    String.format("Please check partitions of tablegroup [%s]: ", tgName) + StringUtils.join(
                        errorPartitionNames, " ,") + " auto split failed!";
                LOG.error(errorInfo);
            }
            dataDistInfo.appendTgDataDist(tgName, toRebalancePgList, originalPlace, allSplitPoints);
            actions.addAll(splitActions);
        }
        String distLogInfo =
            String.format("[schema %s] estimated data distribution:\n %s", schema, JSON.toJSONString(dataDistInfo));
        EventLogger.log(EventType.REBALANCE_INFO, distLogInfo);
        ActionWriteDataDistLog actionWriteDataDistLog = new ActionWriteDataDistLog(schema, dataDistInfo);
        actions.add(actionWriteDataDistLog);

        return actions;
    }

    public static boolean supportAutoSplit(PartitionStrategy strategy) {
        return strategy.isRange() || strategy.isHashed();
    }
}

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
//

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.action.*;
import com.alibaba.polardbx.executor.balancer.serial.DataDistInfo;
import com.alibaba.polardbx.executor.balancer.solver.Solution;
import com.alibaba.polardbx.executor.balancer.solver.MixedModel;
import com.alibaba.polardbx.executor.balancer.solver.SolverExample;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseReleaseXLockTask;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.rebalance.RebalanceTarget;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.balancer.policy.PolicyUtils.getGroupDetails;

/**
 * Move partitions between storage node if un-balanced.
 *
 * @author jinkun.taojinkun
 * @since 2022/08
 */
public class PolicyPartitionBalance implements BalancePolicy {

    static int MAX_TABLEGROUP_SOLVED_BY_LP = 25;

    @Override
    public String name() {
        return SqlRebalance.POLICY_PARTITION_BALANCE;
    }

    /**
     *
     */

    @Override
    public List<BalanceAction> applyToShardingDb(ExecutionContext ec, BalanceOptions options, BalanceStats stats,
                                                 String schema) {
        List<ActionMoveGroup> actions = new ArrayList<>();

        // only apply to partitioning database
        if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return Lists.newArrayList();
        }

        List<GroupStats.GroupsOfStorage> groupList = stats.getGroups();
        if (CollectionUtils.isEmpty(groupList)) {
            return Collections.emptyList();
        }
        List<PolicyDataBalance.BucketOfGroups> buckets =
            groupList.stream().map(PolicyDataBalance.BucketOfGroups::new).collect(Collectors.toList());

        long totalStorageCount = buckets.size();
        List<String> emptyStorage = groupList.stream()
            .filter(x -> x.groups.isEmpty())
            .filter(x -> isStorageReady(x.storageInst))
            .map(x -> x.storageInst)
            .collect(Collectors.toList());
        if ((long) emptyStorage.size() == totalStorageCount) {
            return Lists.newArrayList();
        }
        long totalGroups = groupList.stream().mapToInt(x -> x.groups.size()).sum();
        double newAverage = totalGroups * 1.0 / totalStorageCount;

        // sort by group count descending, move from left to right
        Collections.sort(buckets);
        int left = 0, right = buckets.size() - 1;
        while (left < right) {
            PolicyDataBalance.BucketOfGroups lb = buckets.get(left);
            PolicyDataBalance.BucketOfGroups rb = buckets.get(right);

            if (lb.currentGroupCount() <= newAverage) {
                left++;
                continue;
            }
            if (rb.currentGroupCount() >= newAverage) {
                right--;
                continue;
            }
            if (lb.currentGroupCount() - 1 < rb.currentGroupCount() + 1) {
                left++;
                continue;
            }
            String g = lb.moveOut();
            if (g == null) {
                left++;
            }
            rb.moveIn(g);

            ActionMoveGroup moveGroup = new ActionMoveGroup(schema, Arrays.asList(g),
                rb.originGroups.storageInst, options.debug, stats);
            actions.add(moveGroup);
        }
        // shuffle actions to avoid make any storage node overload
        Collections.shuffle(actions);

        if (CollectionUtils.isEmpty(actions)) {
            return Lists.newArrayList();
        }
//
        final String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schema);
        final String schemaXLock = schema;
        ActionLockResource lock =
            new ActionLockResource(schema, com.google.common.collect.Sets.newHashSet(name, schemaXLock));

        MoveDatabaseReleaseXLockTask
            moveDatabaseReleaseXLockTask = new MoveDatabaseReleaseXLockTask(schema, schemaXLock);
        ActionTaskAdapter moveDatabaseXLockTaskAction = new ActionTaskAdapter(schema, moveDatabaseReleaseXLockTask);

        return Arrays.asList(lock, new ActionMoveGroups(schema, actions), moveDatabaseXLockTaskAction);
    }

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

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }

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

        List<PartitionGroupStat> toRebalancePgList = pgList.stream()
            .filter(pg -> LocalityInfoUtils.withoutRestrictedAllowedGroup(schemaName, pg.pg.getTg_id(),
                pg.pg.getPartition_name()))
            .collect(Collectors.toList());

        int N = toRebalancePgList.size();
        int M = groupDetail.size();
        int[] originalPlace = new int[N];
        double[] partitionSize = new double[N];
        Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        List<String> groupNames = new ArrayList<>(groupDetail.keySet());
        for (int i = 0; i < M; i++) {
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
        }
        for (int i = 0; i < N; i++) {
            PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
            toRebalancePgMap.put(i, partitionGroupStat);
            String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
            originalPlace[i] = groupDetailReverseMap.get(groupKey);
            //filter with tableName
            partitionSize[i] = partitionGroupStat.partitions.stream().filter(
                    o -> o.getPartitionRecord().getTableName().equals(tableName)).
                collect(Collectors.toList()).get(0).getPartitionRows();
        }
        double originalMu = MixedModel.caculateBalanceFactor(M, N, originalPlace, partitionSize).getValue();
        Date startTime = new Date();
        String logInfo = String.format(
            "[schema %s, table %s] start to solve move partition problem: M=%d, N=%d, originalPlace=%s, partitionSize=%s",
            schemaName, tableName, M, N, Arrays.toString(originalPlace), Arrays.toString(partitionSize));
        EventLogger.log(EventType.REBALANCE_INFO, logInfo);
        Solution solution = MixedModel.solveMovePartition(M, N, originalPlace, partitionSize, solveLevel);
        if (solution.withValidSolve) {
            Date endTime = new Date();
            Long costMillis = endTime.getTime() - startTime.getTime();
            logInfo =
                String.format(
                    "[schema %s, table %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
                    schemaName, tableName, costMillis, solution.strategy, originalMu, solution.mu,
                    Arrays.toString(solution.targetPlace));
            EventLogger.log(EventType.REBALANCE_INFO, logInfo);
            int[] targetPlace = solution.targetPlace;
//            double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//            double targetFactor = caculateBalanceFactor(M, N, targetPlace, partitionSize);
//            if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                return actions;
//            }
            List<Pair<PartitionStat, String>> moves = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                if (targetPlace[i] != originalPlace[i]) {
                    moves.add(Pair.of(toRebalancePgMap.get(i).getFirstPartition(), groupDetailMap.get(targetPlace[i])));
                }

            }

            GeneralUtil.emptyIfNull(moves).stream()
                .collect(Collectors.groupingBy(Pair::getValue, Collectors.mapping(Pair::getKey, Collectors.toList())))
                .forEach((toGroup, partitions) -> {
                    for (ActionMoveTablePartition act : ActionMoveTablePartition.createMoveToGroups(schemaName,
                        partitions,
                        toGroup, stats)) {
                        if (actions.size() >= options.maxActions) {
                            break;
                        }
                        actions.add(act);
                    }
                });

            if (!actions.isEmpty()) {
                EventLogger.log(EventType.REBALANCE_INFO, "DataBalance move partition for data balance: " + actions);
            }

        }
        return actions;
    }

    @Override
    public List<BalanceAction> applyToTableGroup(ExecutionContext ec,
                                                 BalanceOptions options,
                                                 BalanceStats stats,
                                                 String schemaName,
                                                 String tableGroupName) {

        DdlHelper.getServerConfigManager().executeBackgroundSql("refresh topology", schemaName, null);

        List<BalanceAction> actions = new ArrayList<>();

        String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        ActionLockResource lock = new ActionLockResource(schemaName, Sets.newHashSet(name));
        actions.add(lock);

        actions.add(new ActionInitPartitionDb(schemaName));

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }
        // structurize pg list into pg map

        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();
        if (pgList.isEmpty()) {
            return actions;
        }
        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);

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

        List<PartitionGroupStat> toRebalancePgList = pgList.stream()
            .filter(pg -> LocalityInfoUtils.withoutRestrictedAllowedGroup(schemaName, pg.pg.getTg_id(),
                pg.pg.getPartition_name()))
            .collect(Collectors.toList());

        int N = toRebalancePgList.size();
        int M = groupDetail.size();
        int[] originalPlace = new int[N];
        double[] partitionSize = new double[N];
        Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        List<String> groupNames = new ArrayList<>(groupDetail.keySet());
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
        Date startTime = new Date();
        String logInfo = String.format(
            "[schema %s, tablegroup %s] start to solve move partition problem: M=%d, N=%d, originalPlace=%s, partitionSize=%s",
            schemaName, tableGroupName, M, N, Arrays.toString(originalPlace), Arrays.toString(partitionSize));
        EventLogger.log(EventType.REBALANCE_INFO, logInfo);
        double originalMu = MixedModel.caculateBalanceFactor(M, N, originalPlace, partitionSize).getValue();
        Solution solution = MixedModel.solveMovePartition(M, N, originalPlace, partitionSize, solveLevel);
        if (solution.withValidSolve) {
            Date endTime = new Date();
            Long costMillis = endTime.getTime() - startTime.getTime();
            logInfo =
                String.format(
                    "[schema %s, tablegroup %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
                    schemaName, tableGroupName, costMillis, solution.strategy, originalMu, solution.mu,
                    Arrays.toString(solution.targetPlace));
            EventLogger.log(EventType.REBALANCE_INFO, logInfo);
            int[] targetPlace = solution.targetPlace;
//            double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//            double targetFactor = caculateBalanceFactor(M, N, targetPlace, partitionSize);
//            if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                return actions;
//            }
            List<Pair<PartitionStat, String>> moves = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                if (targetPlace[i] != originalPlace[i]) {
                    moves.add(Pair.of(toRebalancePgMap.get(i).getFirstPartition(), groupDetailMap.get(targetPlace[i])));
                }

            }

            for (int i = 0; i < moves.size(); ) {
                Long sumMoveSize = 0L;
                int j = i;
                int nextI;
                for (; j < moves.size() && sumMoveSize <= options.maxTaskUnitSize * 1024 * 1024; j++) {
                    sumMoveSize += moves.get(j).getKey().getPartitionDiskSize();
                }
                nextI = j;
                GeneralUtil.emptyIfNull(moves.subList(i, nextI)).stream().collect(
                    Collectors.groupingBy(Pair::getValue,
                        Collectors.mapping(Pair::getKey, Collectors.toList()))
                ).forEach((toGroup, partitions) -> {
                    for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schemaName, partitions,
                        toGroup, stats)) {
//                        if (actions.size() >= options.maxActions) {
//                            break;
//                        }
                        actions.add(act);
                    }
                });
                i = nextI;
            }

            if (!actions.isEmpty()) {
                EventLogger.log(EventType.REBALANCE_INFO, "DataBalance move partition for data balance: " + actions);
            }

        }
        return actions;
    }

    public List<BalanceAction> generateMoveSingleTableAction(String schemaName,
                                                             Map<String, List<PartitionStat>> singleTableMap,
                                                             Map<String, GroupDetailInfoRecord> groupDetail,
                                                             BalanceStats stats, BalanceOptions options,
                                                             ExecutionContext ec) {

        List<BalanceAction> actions = new ArrayList<>();

        boolean allowMoveBalancedSingleTable =
            ec.getParamManager().getBoolean(ConnectionParams.ALLOW_MOVING_BALANCED_SINGLE_TABLE);
        if (!allowMoveBalancedSingleTable) {
            return actions;
        }

        List<PartitionStat> toRebalancePartitionList =
            singleTableMap.values().stream().flatMap(o -> o.stream()).collect(Collectors.toList());
        int N = toRebalancePartitionList.size();
        if (N <= 0) {
            return actions;
        }
        int M = groupDetail.size();
        int[] originalPlace = new int[N];
        int[] targetPlace = new int[N];
        double[] partitionSize = new double[N];

        Map<Integer, PartitionStat> toRebalancePartitionMap = new HashMap<>();
        List<String> groupNames = new ArrayList<>(groupDetail.keySet());
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap<>();
        for (int i = 0; i < M; i++) {
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
        }

        for (int i = 0; i < N; i++) {
            PartitionStat partitionStat = toRebalancePartitionList.get(i);
            toRebalancePartitionMap.put(i, partitionStat);
            String groupKey = partitionStat.getLocation().getGroupKey();
            originalPlace[i] = groupDetailReverseMap.get(groupKey);
            partitionSize[i] = partitionStat.getDataRows();
            targetPlace[i] = originalPlace[i];
        }

        double originalMu = MixedModel.caculateBalanceFactor(M, N, originalPlace, partitionSize).getValue();
        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }
        Solution solution = null;
        Date startTime = new Date();
        if (options.shuffleDataDistribution != 0) {
            solution = MixedModel.solveShufflePartition(M, N, originalPlace, partitionSize);
        } else {
            solution = MixedModel.solveMovePartition(M, N, originalPlace, partitionSize, solveLevel);
        }
        if (solution.withValidSolve) {
            Date endTime = new Date();
            Long costMillis = endTime.getTime() - startTime.getTime();
            String logInfo = String.format(
                "[schema %s, tablegroup %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
                schemaName, "single_tablegroup", costMillis, solution.strategy, originalMu, solution.mu,
                Arrays.toString(solution.targetPlace));
            EventLogger.log(EventType.REBALANCE_INFO, logInfo);
            targetPlace = solution.targetPlace;
//            double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//            double targetFactor = caculateBalanceFactor(M, N, targetPlace, partitionSize);
//            if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                return actions;
//            }
            List<Pair<PartitionStat, String>> moves = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                if (targetPlace[i] != originalPlace[i]) {
                    moves.add(Pair.of(toRebalancePartitionList.get(i), groupDetailMap.get(targetPlace[i])));
                }
            }
            moves.sort(Comparator.comparingLong(o -> -o.getKey().getPartitionDiskSize()));

            for (Pair<PartitionStat, String> move : moves) {
                String toGroup = move.getValue();
                List<PartitionStat> partitions = new ArrayList<>();
                partitions.add(move.getKey());
                ActionMoveTablePartition act =
                    ActionMoveTablePartition.createMoveToGroups(schemaName, partitions, toGroup, stats).get(0);
                actions.add(act);
            }

        }
        return actions;
    }

    @Override
    public List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {

        DdlHelper.getServerConfigManager().executeBackgroundSql("refresh topology", schemaName, null);

        List<BalanceAction> actions = new ArrayList<>();

        String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        ActionLockResource lock = new ActionLockResource(schemaName, Sets.newHashSet(name));
        actions.add(lock);

        actions.add(new ActionInitPartitionDb(schemaName));

        // structurize pg list into pg map
        if (options.benchmarkCPU != 0) {
            solveBenchmarkExample(options.benchmarkCPU);
            return actions;
        }

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }

        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();
        Map<String, TableGroupConfig> tableGroupConfigMap = TableGroupUtils.getAllTableGroupInfoByDb(schemaName)
            .stream().collect(Collectors.toMap(o -> o.getTableGroupRecord().tg_name, o -> o));

        Set<String> singleAndBroadcastTgName = tableGroupConfigMap.keySet().stream()
            .filter(
                o -> tableGroupConfigMap.get(o).getTableGroupRecord().isBroadCastTableGroup()
                    || tableGroupConfigMap.get(o).getTableGroupRecord().isSingleTableGroup())
            .collect(Collectors.toSet());
        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);

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
//        stats.getTableGroupStats().stream().map(o->o.getTableGroupConfig().)
        Date filterStatsStartTime = new Date();
        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        Set<String> toRebalanceSequentialTg = tableGroupConfigMap.keySet().stream()
            .filter(o -> tableGroupConfigMap.get(o).getLocalityDesc().getHashRangeSequentialPlacement())
            .collect(Collectors.toSet());
        Map<String, List<PartitionGroupStat>> toRebalancePgListGroupByTg = GeneralUtil.emptyIfNull(pgList).stream()
            .collect(Collectors.groupingBy(o -> o.getTgName(), Collectors.mapping(o -> o, Collectors.toList())))
            .entrySet().stream().filter(o -> LocalityInfoUtils.withoutRestrictedLocality(groupDetailInfoExRecordList,
                tableGroupConfigMap.get(o.getKey()))).
//                filter(o->!toRebalanceSequentialTg.contains(o)).
    collect(Collectors.toMap(o -> o.getKey(), o -> o.getValue()));
        int toRebalancePgSize =
            toRebalancePgListGroupByTg.keySet().stream().mapToInt(o -> toRebalancePgListGroupByTg.get(o).size()).sum();

        Map<String, List<PartitionStat>> toRebalanceSingleTableListGroupByTg = GeneralUtil.emptyIfNull(pgList).stream()
            .collect(Collectors.groupingBy(o -> o.getTgName(), Collectors.mapping(o -> o, Collectors.toList())))
            .entrySet().stream()
            .filter(o -> LocalityInfoUtils.withSingleTableBalanceLocality(tableGroupConfigMap.get(o.getKey()))).collect(
                Collectors.toMap(o -> o.getKey(),
                    o -> o.getValue().stream().flatMap(p -> p.partitions.stream()).collect(Collectors.toList())));

        Date filterStatsMiddleTime = new Date();
        Long filterStatsCostMillis = filterStatsMiddleTime.getTime() - filterStatsStartTime.getTime();
        String filterStatslogInfo =
            String.format("[schema %s] filter toRebalancePgList middle in %d ms: totalPgList = %d, filterPgList = %d",
                schemaName, filterStatsCostMillis, pgList.size(), toRebalancePgSize);
        EventLogger.log(EventType.REBALANCE_INFO, filterStatslogInfo);

        if (toRebalancePgSize <= 0) {
            return actions;
        }

        int M = groupDetail.size();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<Integer, String> storageInstMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        List<String> groupNames = new ArrayList<>(groupDetail.keySet());
        List<String> storageInsts =
            groupNames.stream().map(o -> groupDetail.get(o).storageInstId).collect(Collectors.toList());
        //inst -> storagePool
        Map<String, String> storagePoolMap = StoragePoolManager.getInstance().storagePoolMap;
        //group -> storagePool
        Map<String, String> spMap = groupDetail.entrySet()
            .stream().map(entry ->
                new AbstractMap.SimpleEntry<>(entry.getKey(),
                    storagePoolMap.getOrDefault(entry.getValue().storageInstId,
                        StoragePoolManager.DEFAULT_STORAGE_POOL_NAME)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for (int i = 0; i < M; i++) {
            // index -> inst
            storageInstMap.put(i, storageInsts.get(i));
            // index -> group
            groupDetailMap.put(i, groupNames.get(i));
            // group -> index
            groupDetailReverseMap.put(groupNames.get(i), i);
        }
        Map<String, List<Integer>> spIndexMap = spMap.keySet().stream().collect(
            Collectors.groupingBy(o -> spMap.get(o),
                Collectors.mapping(o -> groupDetailReverseMap.get(o), Collectors.toList())));

        Map<String, Long> tgDataSize = new HashMap<>();
        for (String tgName : toRebalancePgListGroupByTg.keySet()) {
            Long tgSize =
                toRebalancePgListGroupByTg.get(tgName).stream().map(PartitionGroupStat::getTotalDiskSize).reduce(0L,
                    Long::sum);
            tgDataSize.put(tgName, tgSize);
        }
        List<String> tableGroupNames = toRebalancePgListGroupByTg.keySet().stream()
            .filter(o -> !singleAndBroadcastTgName.contains(o))
            .collect(Collectors.toList());

        Date filterStatsEndTime = new Date();
        filterStatsCostMillis = filterStatsEndTime.getTime() - filterStatsStartTime.getTime();
        filterStatslogInfo =
            String.format("[schema %s] filter toRebalancePgList final in %d ms: totalPgList = %d, filterPgList = %d",
                schemaName, filterStatsCostMillis, pgList.size(), toRebalancePgSize);
        tableGroupNames.sort(Comparator.comparingLong(key -> tgDataSize.get(key)).reversed());
        EventLogger.log(EventType.REBALANCE_INFO, filterStatslogInfo);

        DataDistInfo dataDistInfo = DataDistInfo.fromSchemaAndInstMap(schemaName, storageInstMap, groupDetailMap);
        List<MoveInfo> moves = new ArrayList<>();
        for (int k = 0; k < tableGroupNames.size(); k++) {

            String tgName = tableGroupNames.get(k);
            List<PartitionGroupStat> toRebalancePgList;
            List<PartitionGroupStat> toRebalancePgListTg = toRebalancePgListGroupByTg.get(tgName);
            if (toRebalanceSequentialTg.contains(tgName)) {
                List<String> partNames =
                    toRebalancePgListTg.get(0).getFirstPartition().getPartitionInfo().getPartitionBy()
                        .getOrderedPartitionSpecs()
                        .stream().map(o -> o.getName()).collect(Collectors.toList());
                Map<String, PartitionGroupStat> partToPartGroupStatMap = toRebalancePgListTg.stream()
                    .collect(Collectors.toMap(o -> o.getFirstPartition().getPartitionName(), o -> o));
                toRebalancePgList = partNames.stream().map(partToPartGroupStatMap::get).collect(Collectors.toList());
            }
            // classify the objects.
            Map<String, String> pgSpMap = LocalityInfoUtils.getAllowedStoragePoolOfPartitionGroup(schemaName, tgName);
            Map<String, List<PartitionGroupStat>> toRebalancePgListGroupBySp =
                GeneralUtil.emptyIfNull(toRebalancePgListTg).stream()
                    .collect(Collectors.groupingBy(o -> pgSpMap.get(o.getFirstPartition().getPartitionName()),
                        Collectors.mapping(o -> o, Collectors.toList())));
            for (String sp : toRebalancePgListGroupBySp.keySet()) {
                toRebalancePgList = toRebalancePgListGroupBySp.get(sp);
                int m = spIndexMap.get(sp).size();

                int N = toRebalancePgList.size();

                int[] originalPlace = new int[N];
                int[] targetPlace = new int[N];
                double[] partitionSize = new double[N];
                Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();

                for (int i = 0; i < N; i++) {
                    PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
                    toRebalancePgMap.put(i, partitionGroupStat);
                    String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
                    originalPlace[i] = groupDetailReverseMap.get(groupKey);
                    partitionSize[i] = partitionGroupStat.getDataRows();
                    targetPlace[i] = originalPlace[i];
                }
                Date startTime = new Date();
                String logInfo = String.format(
                    "[schema %s, tablegroup %s] start to solve move partition problem: M=%d, N=%d, originalPlace=%s, partitionSize=%s",
                    schemaName, tgName, m, N, Arrays.toString(originalPlace), Arrays.toString(partitionSize));
                EventLogger.log(EventType.REBALANCE_INFO, logInfo);

                Solution solution = null;
                int[] spIndexes = spIndexMap.get(sp).stream().mapToInt(Integer::intValue).toArray();
                double originalMu =
                    MixedModel.caculateBalanceFactor(m, N, originalPlace, spIndexes, partitionSize).getValue();
                if (options.shuffleDataDistribution != 0) {
                    solution = MixedModel.solveShufflePartition(m, N, originalPlace, partitionSize);
                } else if (toRebalanceSequentialTg.contains(tableGroupNames.get(k))) {
                    solution = MixedModel.solveMoveSequentialPartition(M, N, originalPlace, partitionSize);
                } else if (k < MAX_TABLEGROUP_SOLVED_BY_LP) {
                    solution = MixedModel.solveMovePartition(m, N, originalPlace, partitionSize, spIndexes, solveLevel);
                } else {
                    solution = MixedModel.solveMovePartitionByGreedy(m, N, originalPlace, spIndexes, partitionSize);
                }
                if (solution.withValidSolve) {
                    Date endTime = new Date();
                    Long costMillis = endTime.getTime() - startTime.getTime();
                    logInfo =
                        String.format(
                            "[schema %s, tablegroup %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
                            schemaName, tgName, costMillis, solution.strategy, originalMu, solution.mu,
                            Arrays.toString(solution.targetPlace));
                    EventLogger.log(EventType.REBALANCE_INFO, logInfo);
//                double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//                double targetFactor = caculateBalanceFactor(M, N, solution.targetPlace, partitionSize);
//                if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                    targetPlace = solution.targetPlace;
//                }
                    targetPlace = solution.targetPlace;

                    for (int i = 0; i < N; i++) {
                        if (targetPlace[i] != originalPlace[i]) {
                            moves.add(new MoveInfo(toRebalancePgMap.get(i).getFirstPartition(),
                                toRebalancePgMap.get(i).getTgName(),
                                groupDetailMap.get(targetPlace[i]),
                                toRebalancePgMap.get(i).getDataRows(),
                                toRebalancePgMap.get(i).getTotalDiskSize()
                            ));
                        }

                    }
                }
                dataDistInfo.appendTgDataDist(tgName, toRebalancePgList, originalPlace, targetPlace);
            }
        }

        moves.sort(Comparator.comparingLong(o -> -o.dataSize));
//        int toIndex = Math.min(options.maxActions * 8, moves.size());
//        moves = moves.subList(0, toIndex);
//        Map<String, List<MoveInfo>> movesGroupByTg = moves.stream().collect(
//            Collectors.groupingBy(o -> o.tgName, Collectors.mapping(o -> o, Collectors.toList()))
//        );
        for (int i = 0; i < moves.size(); ) {
            Long sumMoveSize = 0L;
            int j = i;
            int nextI;
            for (; j < moves.size() && sumMoveSize <= options.maxTaskUnitSize * 1024 * 1024; j++) {
                sumMoveSize += moves.get(j).dataSize;
            }
            nextI = j;
            Map<String, List<ActionMovePartition>> movePartitionActions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
//            Long finalSumMoveRows = sumMoveSize;
            GeneralUtil.emptyIfNull(moves.subList(i, nextI)).stream().collect(
                Collectors.groupingBy(o -> o.targetDn,
                    Collectors.mapping(o -> o.partitionStat, Collectors.toList()))
            ).forEach((toGroup, partitions) -> {
                for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schemaName, partitions,
                    toGroup, stats)) {
//                    if (actions.size() >= options.maxActions) {
//                        break;
//                    }
                    movePartitionActions.computeIfAbsent(act.getTableGroupName(), o -> new ArrayList<>()).add(act);
                }
            });
            if (!movePartitionActions.isEmpty()) {
                actions.add(new ActionMovePartitions(schemaName, movePartitionActions));
            }
            i = nextI;
        }

//        for (String tgName : movesGroupByTg.keySet()) {
//            List<MoveInfo> movesOfTg = movesGroupByTg.get(tgName);
//            movesOfTg.sort(Comparator.comparingLong(o -> -o.dataSize));
//            for (int i = 0; i < movesOfTg.size(); ) {
//                Long sumMoveRows = 0L;
//                int j = i;
//                int nextI;
//                for (; j < movesOfTg.size() && sumMoveRows <= options.maxTaskUnitRows; j++) {
//                    sumMoveRows += movesOfTg.get(j).tableRows;
//                }
//                nextI = j;
//                GeneralUtil.emptyIfNull(movesOfTg.subList(i, nextI)).stream().collect(
//                    Collectors.groupingBy(o -> o.targetDn,
//                        Collectors.mapping(o -> o.partitionStat, Collectors.toList()))
//                ).forEach((toGroup, partitions) -> {
//                    for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schema, partitions,
//                        toGroup, stats)) {
//                        if (actions.size() >= options.maxActions) {
//                            break;
//                        }
//                        actions.add(act);
//                    }
//                });
//                i = nextI;
//            }
//        }
        String distLogInfo =
            String.format("[schema %s] estimated data distribution: %s", schemaName, JSON.toJSONString(dataDistInfo));
        EventLogger.log(EventType.REBALANCE_INFO, distLogInfo);
        actions.addAll(
            generateMoveSingleTableAction(schemaName, toRebalanceSingleTableListGroupByTg, groupDetail, stats,
                options, ec));
        ActionWriteDataDistLog actionWriteDataDistLog = new ActionWriteDataDistLog(schemaName, dataDistInfo);
        actions.add(actionWriteDataDistLog);
        return actions;
    }

    public void solveBenchmarkExample(int times) {
        SolverExample example = SolverExample.generate102416SolverExample();
        for (int i = 0; i < times; i++) {
            MixedModel.solveMovePartition(example.M, example.N, example.originalPlace, example.partitionSize);
        }
    }

    public class MoveInfo {
        PartitionStat partitionStat;
        String targetDn;

        String tgName;
        Long tableRows;
        Long dataSize;

        public MoveInfo(PartitionStat partitionStat, String tgName, String targetDn, Long tableRows, Long dataSize) {
            this.partitionStat = partitionStat;
            this.tgName = tgName;
            this.targetDn = targetDn;
            this.tableRows = tableRows;
            this.dataSize = dataSize;
        }
    }

    protected boolean isStorageReady(String storageInst) {
        Map<String, StorageInstHaContext> storageStatusMap = StorageHaManager.getInstance().getStorageHaCtxCache();

        return Optional.ofNullable(storageStatusMap.get(storageInst))
            .map(StorageInstHaContext::isAllReplicaReady)
            .orElse(false);
    }

}

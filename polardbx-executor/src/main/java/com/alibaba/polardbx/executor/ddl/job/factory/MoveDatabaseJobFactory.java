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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.Balancer;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.ddl.job.builder.MoveDatabaseBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DdlBackfillCostRecordTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ImportTableSpaceDdlNormalTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InitNewStorageInstTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SyncLsnTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyLogTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyExecutorInitTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyFinishTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabaseItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabasePreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CHANGE_SET_APPLY_OPTIMIZATION;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveDatabaseJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final MoveDatabasePreparedData preparedData;
    protected final Map<String, MoveDatabaseItemPreparedData> tablesPrepareData;
    protected final Map<String, List<PhyDdlTableOperation>> logicalTablesPhysicalPlansMap;
    protected final Map<String, Map<String, List<List<String>>>> tablesTopologyMap;
    protected final Map<String, Map<String, Set<String>>> targetTablesTopology;
    protected final Map<String, Map<String, Set<String>>> sourceTablesTopology;
    protected final Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap;
    protected final ExecutionContext executionContext;
    protected final ComplexTaskMetaManager.ComplexTaskType taskType;
    final Map<String, String> sourceAndTarDnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    final Map<String, Pair<String, String>> storageInstAndUserInfos = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public MoveDatabaseJobFactory(DDL ddl, MoveDatabasePreparedData preparedData,
                                  Map<String, MoveDatabaseItemPreparedData> tablesPrepareData,
                                  Map<String, List<PhyDdlTableOperation>> logicalTablesPhysicalPlansMap,
                                  Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap,
                                  Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                  Map<String, Map<String, Set<String>>> targetTablesTopology,
                                  Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                  ComplexTaskMetaManager.ComplexTaskType taskType,
                                  ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.ddl = ddl;
        this.tablesPrepareData = tablesPrepareData;
        this.logicalTablesPhysicalPlansMap = logicalTablesPhysicalPlansMap;
        this.discardTableSpacePhysicalPlansMap = discardTableSpacePhysicalPlansMap;
        this.tablesTopologyMap = tablesTopologyMap;
        this.targetTablesTopology = targetTablesTopology;
        this.sourceTablesTopology = sourceTablesTopology;
        this.executionContext = executionContext;
        this.taskType = taskType;
    }

    @Override
    protected void validate() {
        String schemaName = preparedData.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                "it's not allow to execute move database command for partitioning databases");
        }
    }

    public void constructSubTasks(ExecutableDdlJob executableDdlJob,
                                  DdlTask tailTask,
                                  List<DdlTask> bringUpMoveDatabase) {
        ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask =
            new ChangeSetApplyExecutorInitTask(preparedData.getSchemaName(),
                ScaleOutUtils.getScaleoutTaskParallelism(executionContext));
        ChangeSetApplyFinishTask changeSetApplyFinishTask = new ChangeSetApplyFinishTask(preparedData.getSchemaName(),
            String.format("schema %s group %s start double write ", preparedData.getSchemaName(),
                preparedData.getSourceTargetGroupMap()));
        SyncLsnTask syncLsnTask = null;
        boolean syncLsnTaskAdded = false;
        final boolean useChangeSet = ChangeSetUtils.isChangeSetProcedure(executionContext);
        Map<String, String> tarGroupAndStorageIds = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if (useChangeSet) {
            for (Map.Entry<String, String> srcTarGroup : preparedData.getSourceTargetGroupMap().entrySet()) {
                String targetStorageId =
                    preparedData.getGroupAndStorageInstId().get(srcTarGroup.getKey()).getValue();
                tarGroupAndStorageIds.put(srcTarGroup.getValue(), targetStorageId);
            }
        }
        int parallelism = ScaleOutUtils.getScaleoutTaskParallelism(executionContext);
        Queue<DdlTask> leavePipeLineQueue = new LinkedList<>();

        for (Map.Entry<String, Map<String, List<List<String>>>> entry : tablesTopologyMap.entrySet()) {
            String schemaName = tablesPrepareData.get(entry.getKey()).getSchemaName();
            String logicalTableName = tablesPrepareData.get(entry.getKey()).getTableName();
            TableMeta tm = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);

            MoveDatabaseSubTaskJobFactory subTaskJobFactory;
            if (useChangeSet && ChangeSetUtils.supportUseChangeSet(taskType, tm)) {
                subTaskJobFactory = new MoveDatabaseChangeSetJobFactory(ddl, tablesPrepareData.get(entry.getKey()),
                    logicalTablesPhysicalPlansMap.get(entry.getKey()),
                    discardTableSpacePhysicalPlansMap.get(entry.getKey()),
                    sourceAndTarDnMap,
                    storageInstAndUserInfos,
                    tablesTopologyMap.get(entry.getKey()),
                    targetTablesTopology.get(entry.getKey()),
                    sourceTablesTopology.get(entry.getKey()),
                    changeSetApplyExecutorInitTask,
                    changeSetApplyFinishTask,
                    tarGroupAndStorageIds,
                    preparedData.isUsePhysicalBackfill(),
                    executionContext);
            } else {
                subTaskJobFactory = new MoveDatabaseSubTaskJobFactory(ddl, tablesPrepareData.get(entry.getKey()),
                    logicalTablesPhysicalPlansMap.get(entry.getKey()), tablesTopologyMap.get(entry.getKey()),
                    targetTablesTopology.get(entry.getKey()), sourceTablesTopology.get(entry.getKey()),
                    executionContext);
            }
            ExecutableDdlJob subTask = subTaskJobFactory.create();
            executableDdlJob.combineTasks(subTask);
            executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());
            executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
            executableDdlJob.addTaskRelationship(subTask.getTail(), bringUpMoveDatabase.get(0));

            if (preparedData.isUsePhysicalBackfill()) {
                if (!syncLsnTaskAdded) {
                    Map<String, Set<String>> sourceTableTopology = sourceTablesTopology.get(entry.getKey());
                    Map<String, String> targetGroupAndStorageIdMap = new HashMap<>();
                    Map<String, String> sourceGroupAndStorageIdMap = new HashMap<>();
                    for (String groupName : sourceTableTopology.keySet()) {
                        sourceGroupAndStorageIdMap.put(groupName,
                            DbTopologyManager.getStorageInstIdByGroupName(schemaName, groupName));
                        targetGroupAndStorageIdMap.put(preparedData.getSourceTargetGroupMap().get(groupName),
                            preparedData.getGroupAndStorageInstId().get(groupName).getValue());
                    }
                    syncLsnTask =
                        new SyncLsnTask(schemaName, sourceGroupAndStorageIdMap, targetGroupAndStorageIdMap);
                    executableDdlJob.addTask(syncLsnTask);
                    syncLsnTaskAdded = true;
                }
                for (List<DdlTask> pipeLine : subTaskJobFactory.getPhysicalyTaskPipeLine()) {
                    DdlTask parentLeaveNode;
                    if (leavePipeLineQueue.size() < parallelism) {
                        parentLeaveNode = syncLsnTask;
                    } else {
                        parentLeaveNode = leavePipeLineQueue.poll();
                    }
                    executableDdlJob.removeTaskRelationship(subTaskJobFactory.getBackfillTaskEdgeNodes().get(0),
                        subTaskJobFactory.getBackfillTaskEdgeNodes().get(1));
                    executableDdlJob.addTaskRelationship(subTaskJobFactory.getBackfillTaskEdgeNodes().get(0),
                        syncLsnTask);
                    executableDdlJob.addTaskRelationship(parentLeaveNode,
                        pipeLine.get(0));
                    executableDdlJob.addTaskRelationship(pipeLine.get(0),
                        pipeLine.get(1));
                    PhysicalBackfillTask physicalBackfillTask = (PhysicalBackfillTask) pipeLine.get(1);
                    Map<String, List<List<String>>> targetTables = new HashMap<>();
                    String tarGroupKey = physicalBackfillTask.getSourceTargetGroup().getValue();
                    String phyTableName = physicalBackfillTask.getPhysicalTableName();

                    targetTables.computeIfAbsent(tarGroupKey, k -> new ArrayList<>())
                        .add(Collections.singletonList(phyTableName));

                    ImportTableSpaceDdlNormalTask importTableSpaceDdlNormalTask = new ImportTableSpaceDdlNormalTask(
                        preparedData.getSchemaName(), entry.getKey(),
                        targetTables);

                    for (int i = 2; i < pipeLine.size(); i++) {
                        executableDdlJob.addTaskRelationship(pipeLine.get(1),
                            pipeLine.get(i));
                        executableDdlJob.addTaskRelationship(pipeLine.get(i),
                            importTableSpaceDdlNormalTask);
                    }
                    executableDdlJob.addTaskRelationship(importTableSpaceDdlNormalTask,
                        subTaskJobFactory.getBackfillTaskEdgeNodes().get(1));
                    leavePipeLineQueue.add(importTableSpaceDdlNormalTask);
                }
            }
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = preparedData.getSchemaName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        Map<String, List<Pair<String, String>>> instGroupDbInfos = new HashMap<>();
        final boolean shareStorageMode =
            executionContext.getParamManager().getBoolean(ConnectionParams.SHARE_STORAGE_MODE);

        Map<String, Long> tableVersion = getPrimaryTableVersions();
        BaseValidateTask moveDataBaseValidateTask = new MoveDatabaseValidateTask(schemaName, schemaName, tableVersion);

        for (Map.Entry<String, List<String>> entry : preparedData.getStorageGroups().entrySet()) {
            for (String sourceGroup : entry.getValue()) {
                if (!shareStorageMode) {
                    instGroupDbInfos.computeIfAbsent(entry.getKey(), o -> new ArrayList<>())
                        .add(Pair.of(GroupInfoUtil.buildScaloutGroupName(sourceGroup),
                            GroupInfoUtil.buildPhysicalDbNameFromGroupName(sourceGroup)));
                } else {
                    String targetPhyDb = GroupInfoUtil.buildScaleOutPhyDbName(schemaName, sourceGroup);
                    instGroupDbInfos.computeIfAbsent(entry.getKey(), o -> new ArrayList<>())
                        .add(Pair.of(GroupInfoUtil.buildScaloutGroupName(sourceGroup), targetPhyDb));
                }
            }
        }
        DdlTask initNewStorageInstTask = new InitNewStorageInstTask(schemaName, instGroupDbInfos);

        final List<String> allSourceGroup = new ArrayList();
        preparedData.getStorageGroups().entrySet().stream().forEach(o -> allSourceGroup.addAll(o.getValue()));
        DdlTask addMetaTask =
            new MoveDatabaseAddMetaTask(schemaName, allSourceGroup, preparedData.getSourceSql(),
                ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
                taskType.getValue(), 0);

        DdlContext ddlContext = executionContext.getDdlContext();
        DdlBackfillCostRecordTask costRecordTask = null;
        if (ddlContext != null && !ddlContext.isSubJob()) {
            costRecordTask = new DdlBackfillCostRecordTask(schemaName);
            final BalanceStats balanceStats = Balancer.collectBalanceStatsOfDatabase(schemaName);
            List<GroupStats.GroupsOfStorage> groupsOfStorages = balanceStats.getGroups();
            Long diskSize = 0L;
            Long rows = 0L;
            for (GroupStats.GroupsOfStorage groupsOfStorage : GeneralUtil.emptyIfNull(groupsOfStorages)) {
                if (groupsOfStorage == null || groupsOfStorage.getGroupDataSizeMap() == null) {
                    continue;
                }
                for (Map.Entry<String, Pair<Long, Long>> entry : groupsOfStorage.groupDataSizeMap.entrySet()) {
                    if (preparedData.getSourceTargetGroupMap().containsKey(entry.getKey())) {
                        rows += entry.getValue().getKey();
                        diskSize += entry.getValue().getValue();
                    }
                }
            }
            costRecordTask.setCostInfo(
                CostEstimableDdlTask.createCostInfo(rows, diskSize, (long) tablesPrepareData.size()));
        }

        boolean skipValidator =
            executionContext.getParamManager().getBoolean(ConnectionParams.SKIP_MOVE_DATABASE_VALIDATOR);
        if (!skipValidator) {
            if (costRecordTask != null) {
                executableDdlJob.addSequentialTasks(Lists.newArrayList(
                    /*the parent job of rebalance will acquire the Xlock of current schemaName before exec*/
                    moveDataBaseValidateTask,
                    initNewStorageInstTask,
                    costRecordTask,
                    addMetaTask
                ));
            } else {
                executableDdlJob.addSequentialTasks(Lists.newArrayList(
                    /*the parent job of rebalance will acquire the Xlock of current schemaName before exec*/
                    moveDataBaseValidateTask,
                    initNewStorageInstTask,
                    addMetaTask
                ));
            }
        } else {
            if (costRecordTask != null) {
                executableDdlJob.addSequentialTasks(Lists.newArrayList(
                    /*the parent job of rebalance will acquire the Xlock of current schemaName before exec*/
                    initNewStorageInstTask,
                    costRecordTask,
                    addMetaTask
                ));
            } else {
                executableDdlJob.addSequentialTasks(Lists.newArrayList(
                    /*the parent job of rebalance will acquire the Xlock of current schemaName before exec*/
                    initNewStorageInstTask,
                    addMetaTask
                ));
            }
        }

        executableDdlJob.labelAsTail(addMetaTask);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.SCALE_OUT_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }
        List<DdlTask> bringUpMoveDatabase =
            ComplexTaskFactory.bringUpMoveDatabase(preparedData, executionContext);

        if (stayAtPublic) {
            executableDdlJob.addSequentialTasks(bringUpMoveDatabase);
            executableDdlJob.removeTaskRelationship(addMetaTask, bringUpMoveDatabase.get(0));
            constructSubTasks(executableDdlJob, addMetaTask, bringUpMoveDatabase);
            executableDdlJob.labelAsTail(bringUpMoveDatabase.get(bringUpMoveDatabase.size() - 1));
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(executableDdlJob, addMetaTask, ImmutableList.of(pauseCurrentJobTask));
            executableDdlJob.labelAsTail(pauseCurrentJobTask);
        }
        if (GeneralUtil.isEmpty(tablesTopologyMap.entrySet())) {
            executableDdlJob.addTaskRelationship(addMetaTask, bringUpMoveDatabase.get(0));
        }
        if (skipValidator) {
            executableDdlJob.labelAsHead(initNewStorageInstTask);
        } else {
            executableDdlJob.labelAsHead(moveDataBaseValidateTask);
        }
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          MoveDatabasePreparedData preparedData,
                                          ExecutionContext executionContext) {
        MoveDatabaseBuilder moveDatabaseBuilder =
            new MoveDatabaseBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            moveDatabaseBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            moveDatabaseBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            moveDatabaseBuilder.getSourceTablesTopology();
        Map<String, MoveDatabaseItemPreparedData> moveDatabaseItemPreparedDataMap =
            moveDatabaseBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> logicalTablesPhysicalPlansMap =
            moveDatabaseBuilder.getLogicalTablesPhysicalPlansMap();
        Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap =
            moveDatabaseBuilder.getDiscardTableSpacePhysicalPlansMap();
        return new MoveDatabaseJobFactory(ddl, preparedData, moveDatabaseItemPreparedDataMap,
            logicalTablesPhysicalPlansMap, discardTableSpacePhysicalPlansMap,
            tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE,
            executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String groupName : preparedData.getGroupAndStorageInstId().keySet()) {
            resources.add(concatWithDot(preparedData.getSchemaName(), groupName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    protected Map<String, Long> getPrimaryTableVersions() {
        Map<String, Long> tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (MoveDatabaseItemPreparedData itemPreparedData : tablesPrepareData.values()) {

            String primaryTblName = itemPreparedData.getTableName();
            TableMeta tableMeta =
                executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(primaryTblName);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                primaryTblName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
            TableMeta primaryTblMeta =
                executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(primaryTblName);
            Long primaryTblVersion = primaryTblMeta.getVersion();
            tablesVersion.putIfAbsent(primaryTblName, primaryTblVersion);
        }
        return tablesVersion;
    }
}

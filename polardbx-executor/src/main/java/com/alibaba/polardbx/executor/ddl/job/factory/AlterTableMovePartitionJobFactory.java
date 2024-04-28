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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.Balancer;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableMovePartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DdlBackfillCostRecordTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ImportTableSpaceDdlNormalTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SyncLsnTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMovePartitionPreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author luoyanxin
 */
public class AlterTableMovePartitionJobFactory extends AlterTableGroupBaseJobFactory {

    final Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap;
    final Map<String, Map<String, Pair<String, String>>> tbPtbGroupMap;
    final Map<String, String> sourceAndTarDnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    final Map<String, Pair<String, String>> storageInstAndUserInfos = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public AlterTableMovePartitionJobFactory(DDL ddl, AlterTableMovePartitionPreparedData preparedData,
                                             Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                             Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                             Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap,
                                             Map<String, Map<String, Pair<String, String>>> tbPtbGroupMap,
                                             Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                             Map<String, Map<String, Set<String>>> targetTablesTopology,
                                             Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                             Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations,
                                             ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION, executionContext);
        this.discardTableSpacePhysicalPlansMap = discardTableSpacePhysicalPlansMap;
        this.tbPtbGroupMap = tbPtbGroupMap;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (preparedData.isRemainInOriginalTableGroup()) {
            return doMoveInOriginTableGroup();
        } else if (preparedData.isMoveToExistTableGroup()) {
            return doMoveAndMoveToExistTableGroup();
        } else if (preparedData.isCreateNewTableGroup()) {
            return doMoveInNewTableGroup();
        } else if (StringUtils.isNotEmpty(preparedData.getTargetImplicitTableGroupName())) {
            return withImplicitTableGroup(executionContext);
        } else {
            throw new RuntimeException("unexpected");
        }
    }

    private ExecutableDdlJob doMoveAndMoveToExistTableGroup() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();
        String schemaName = preparedData.getSchemaName();
        String targetTableGroup = preparedData.getTargetTableGroup();
        String sourceTableGroup = preparedData.getTableGroupName();
        String tableName = preparedData.getTableName();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(sourceTableGroup);

        DdlTask emptyTask = new EmptyTask(schemaName);
        DdlTask validateSourceTableGroup =
            new AlterTableGroupValidateTask(schemaName, sourceTableGroup, tablesVersion, false,
                /*todo*/null, false);
        DdlTask validateTargetTableGroup = new AlterTableGroupValidateTask(schemaName, targetTableGroup,
            preparedData.getFirstTableVersionInTargetTableGroup(), false, preparedData.getTargetPhysicalGroups(),
            false);

        executableDdlJob.addTask(emptyTask);
        executableDdlJob.addTask(validateSourceTableGroup);
        executableDdlJob.addTask(validateTargetTableGroup);
        executableDdlJob.addTaskRelationship(emptyTask, validateSourceTableGroup);
        executableDdlJob.addTaskRelationship(emptyTask, validateTargetTableGroup);

        Set<Long> outdatedPartitionGroupId = new HashSet<>();

        for (String splitPartitionName : preparedData.getOldPartitionNames()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(splitPartitionName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        List<String> newPartitions = new ArrayList<>();
        List<String> localities = new ArrayList<>();
        for (int i = 0; i < preparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(preparedData.getInvisiblePartitionGroups().get(i)
                .getPhy_db());
            newPartitions.add(preparedData.getNewPartitionNames().get(i));
            localities.add(preparedData.getInvisiblePartitionGroups().get(i)
                .getLocality());
        }
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            targetTableGroup,
            tableGroupConfig.getTableGroupRecord().getId(),
            preparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions,
            localities);

        DdlContext ddlContext = executionContext.getDdlContext();
        DdlBackfillCostRecordTask costRecordTask = null;
        if (ddlContext != null && !ddlContext.isSubJob()) {
            costRecordTask = new DdlBackfillCostRecordTask(schemaName);
            final BalanceStats balanceStats = Balancer.collectBalanceStatsOfTable(schemaName, tableName);
            List<PartitionStat> partitionStats = balanceStats.getPartitionStats();
            Long diskSize = 0L;
            Long rows = 0L;
            Set<String> partitionNamesSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            preparedData.getOldPartitionNames().forEach(o -> partitionNamesSet.add(o));
            for (PartitionStat partitionStat : partitionStats) {
                if (partitionNamesSet.contains(partitionStat.getPartitionName())) {
                    diskSize += partitionStat.getPartitionDiskSize();
                    rows += partitionStat.getPartitionRows();
                }
            }
            costRecordTask.setCostInfo(CostEstimableDdlTask.createCostInfo(rows, diskSize, 1L));
        }
        if (costRecordTask != null) {
            executableDdlJob.addTask(costRecordTask);
        }
        executableDdlJob.addTask(addMetaTask);
        if (costRecordTask != null) {
            executableDdlJob.addTaskRelationship(validateSourceTableGroup, costRecordTask);
            executableDdlJob.addTaskRelationship(validateTargetTableGroup, costRecordTask);
            executableDdlJob.addTaskRelationship(costRecordTask, addMetaTask);
        } else {
            executableDdlJob.addTaskRelationship(validateSourceTableGroup, addMetaTask);
            executableDdlJob.addTaskRelationship(validateTargetTableGroup, addMetaTask);
        }

        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableChangeTopology(schemaName, targetTableGroup, tableName, taskType,
                executionContext);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }

        if (stayAtPublic) {
            executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, bringUpAlterTableGroupTasks,
                preparedData.getOldPartitionNames().get(0));
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, ImmutableList.of(pauseCurrentJobTask),
                preparedData.getOldPartitionNames().get(0));
        }

        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        return executableDdlJob;
    }

    private ExecutableDdlJob doMoveInNewTableGroup() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();
        String schemaName = preparedData.getSchemaName();
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, preparedData.getTableGroupName(), tablesVersion, false,
                preparedData.getTargetPhysicalGroups(), false);

        SubJobTask subJobMoveTableToNewGroup =
            new SubJobTask(schemaName, String.format(SET_NEW_TABLE_GROUP, preparedData.getTableName()), null);
        SubJobTask subJobMoveTable = new SubJobTask(schemaName, preparedData.getSourceSql(), null);
        DdlContext ddlContext = executionContext.getDdlContext();
        CostEstimableDdlTask.CostInfo costInfo = null;
        if (ddlContext != null && !ddlContext.isSubJob()) {
            final BalanceStats balanceStats =
                Balancer.collectBalanceStatsOfTable(schemaName, preparedData.getTableName());
            List<PartitionStat> partitionStats = balanceStats.getPartitionStats();
            Long diskSize = 0L;
            Long rows = 0L;
            Set<String> partitionNamesSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            preparedData.getOldPartitionNames().forEach(o -> partitionNamesSet.add(o));
            for (PartitionStat partitionStat : partitionStats) {
                if (partitionNamesSet.contains(partitionStat.getPartitionName())) {
                    diskSize += partitionStat.getPartitionDiskSize();
                    rows += partitionStat.getPartitionRows();
                }
            }
            costInfo = CostEstimableDdlTask.createCostInfo(rows, diskSize, 1L);
        }
        if (costInfo != null) {
            subJobMoveTable.setCostInfo(costInfo);
        }
        subJobMoveTableToNewGroup.setParentAcquireResource(true);
        subJobMoveTable.setParentAcquireResource(true);
        executableDdlJob.addSequentialTasks(
            Lists.newArrayList(validateTask, subJobMoveTableToNewGroup, subJobMoveTable));
        return executableDdlJob;
    }

    private ExecutableDdlJob doMoveInOriginTableGroup() {
        AlterTableMovePartitionPreparedData alterTableGroupMovePartitionPreparedData =
            (AlterTableMovePartitionPreparedData) preparedData;
        String schemaName = alterTableGroupMovePartitionPreparedData.getSchemaName();
        String tableName = alterTableGroupMovePartitionPreparedData.getTableName();
        String tableGroupName = alterTableGroupMovePartitionPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, alterTableGroupMovePartitionPreparedData.getTableGroupName(),
                tablesVersion, true, alterTableGroupMovePartitionPreparedData.getTargetPhysicalGroups(), false);
        DdlContext ddlContext = executionContext.getDdlContext();
        DdlBackfillCostRecordTask costRecordTask = null;
        if (ddlContext != null && !ddlContext.isSubJob()) {
            costRecordTask = new DdlBackfillCostRecordTask(schemaName);
            final BalanceStats balanceStats = Balancer.collectBalanceStatsOfTable(schemaName, tableName);
            List<PartitionStat> partitionStats = balanceStats.getPartitionStats();
            Long diskSize = 0L;
            Long rows = 0L;
            Set<String> partitionNamesSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            preparedData.getOldPartitionNames().forEach(o -> partitionNamesSet.add(o));
            for (PartitionStat partitionStat : partitionStats) {
                if (partitionNamesSet.contains(partitionStat.getPartitionName())) {
                    diskSize += partitionStat.getPartitionDiskSize();
                    rows += partitionStat.getPartitionRows();
                }
            }
            costRecordTask.setCostInfo(CostEstimableDdlTask.createCostInfo(rows, diskSize, 1L));
        }

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupMovePartitionPreparedData.getTableGroupName());

        Set<Long> outdatedPartitionGroupId = new HashSet<>();
        for (String mergePartitionName : alterTableGroupMovePartitionPreparedData.getOldPartitionNames()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(mergePartitionName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        List<String> newPartitions = new ArrayList<>();
        List<String> localities = new ArrayList<>();
        for (int i = 0; i < preparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(preparedData.getInvisiblePartitionGroups().get(i)
                .getPhy_db());
            newPartitions.add(preparedData.getNewPartitionNames().get(i));
            localities.add(preparedData.getInvisiblePartitionGroups().get(i)
                .getLocality());
        }
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            alterTableGroupMovePartitionPreparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions,
            localities);

        if (costRecordTask != null) {
            executableDdlJob.addSequentialTasks(Lists.newArrayList(validateTask, costRecordTask, addMetaTask));
        } else {
            executableDdlJob.addSequentialTasks(Lists.newArrayList(validateTask, addMetaTask));
        }
        executableDdlJob.labelAsHead(validateTask);

        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, tableGroupName, null, taskType, executionContext);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }

        if (stayAtPublic) {
            executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, bringUpAlterTableGroupTasks,
                alterTableGroupMovePartitionPreparedData.getTargetPartitionsLocation().keySet().iterator().next());
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, ImmutableList.of(pauseCurrentJobTask),
                alterTableGroupMovePartitionPreparedData.getTargetPartitionsLocation().keySet().iterator().next());
        }

        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));

        return executableDdlJob;
    }

    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        EmptyTask emptyTask = new EmptyTask(schemaName);
        String logicalTableName = preparedData.getTableName();
        TableMeta tm = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        final boolean useChangeSet = ChangeSetUtils.isChangeSetProcedure(executionContext);
        AlterTableGroupSubTaskJobFactory subTaskJobFactory;
        if (useChangeSet && ChangeSetUtils.supportUseChangeSet(taskType, tm)) {
            subTaskJobFactory =
                new AlterTableMovePartitionChangeSetSubTaskJobFactory(ddl,
                    (AlterTableMovePartitionPreparedData) preparedData,
                    tablesPrepareData.get(preparedData.getTableName()),
                    newPartitionsPhysicalPlansMap.get(preparedData.getTableName()),
                    discardTableSpacePhysicalPlansMap.get(preparedData.getTableName()),
                    tbPtbGroupMap.get(preparedData.getTableName()),
                    sourceAndTarDnMap,
                    storageInstAndUserInfos,
                    tablesTopologyMap.get(preparedData.getTableName()),
                    targetTablesTopology.get(preparedData.getTableName()),
                    sourceTablesTopology.get(preparedData.getTableName()),
                    orderedTargetTablesLocations.get(preparedData.getTableName()), targetPartitionName, false, taskType,
                    executionContext);
        } else {
            subTaskJobFactory =
                new AlterTableMovePartitionSubTaskJobFactory(ddl, (AlterTableMovePartitionPreparedData) preparedData,
                    tablesPrepareData.get(preparedData.getTableName()),
                    newPartitionsPhysicalPlansMap.get(preparedData.getTableName()),
                    tablesTopologyMap.get(preparedData.getTableName()),
                    targetTablesTopology.get(preparedData.getTableName()),
                    sourceTablesTopology.get(preparedData.getTableName()),
                    orderedTargetTablesLocations.get(preparedData.getTableName()), targetPartitionName, false, taskType,
                    executionContext);
        }

        ExecutableDdlJob subTask = subTaskJobFactory.create();
        executableDdlJob.combineTasks(subTask);
        executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());

        SyncLsnTask syncLsnTask = null;
        int parallelism = ScaleOutUtils.getTableGroupTaskParallelism(executionContext);
        Queue<DdlTask> leavePipeLineQueue = new LinkedList<>();

        if (preparedData.isUsePhysicalBackfill()) {
            Map<String, Set<String>> sourceTableTopology = sourceTablesTopology.get(preparedData.getTableName());
            Map<String, Set<String>> targetTableTopology = targetTablesTopology.get(preparedData.getTableName());
            Map<String, String> targetGroupAndStorageIdMap = new HashMap<>();
            Map<String, String> sourceGroupAndStorageIdMap = new HashMap<>();
            for (String groupName : sourceTableTopology.keySet()) {
                sourceGroupAndStorageIdMap.put(groupName,
                    DbTopologyManager.getStorageInstIdByGroupName(schemaName, groupName));
            }
            for (String groupName : targetTableTopology.keySet()) {
                targetGroupAndStorageIdMap.put(groupName,
                    DbTopologyManager.getStorageInstIdByGroupName(schemaName, groupName));
            }

            syncLsnTask =
                new SyncLsnTask(schemaName, sourceGroupAndStorageIdMap, targetGroupAndStorageIdMap);
            executableDdlJob.addTask(syncLsnTask);

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
                    preparedData.getSchemaName(), preparedData.getTableName(),
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

        if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
            executableDdlJob.addTask(emptyTask);
            executableDdlJob.addTask(subTaskJobFactory.getCdcTableGroupDdlMarkTask());
            executableDdlJob.addTaskRelationship(subTask.getTail(), emptyTask);
            executableDdlJob.addTaskRelationship(emptyTask, subTaskJobFactory.getCdcTableGroupDdlMarkTask());
            executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(),
                bringUpAlterTableGroupTasks.get(0));
        } else {
            executableDdlJob.addTaskRelationship(subTask.getTail(), bringUpAlterTableGroupTasks.get(0));
        }

        if (bringUpAlterTableGroupTasks.size() > 1 && !(bringUpAlterTableGroupTasks.get(
            0) instanceof PauseCurrentJobTask)) {
            DdlTask dropUselessTableTask =
                ComplexTaskFactory.CreateDropUselessPhyTableTask(schemaName, preparedData.getTableName(),
                    sourceTablesTopology.get(preparedData.getTableName()), executionContext);
            executableDdlJob.addTask(dropUselessTableTask);
            executableDdlJob.labelAsTail(dropUselessTableTask);
            executableDdlJob.addTaskRelationship(
                bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1), dropUselessTableTask);
        }
        executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());

    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl, AlterTableMovePartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableMovePartitionBuilder alterTableMovePartitionBuilder =
            new AlterTableMovePartitionBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableMovePartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableMovePartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableMovePartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableMovePartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableMovePartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            alterTableMovePartitionBuilder.getOrderedTargetTablesLocations();
        Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap =
            alterTableMovePartitionBuilder.getDiscardTableSpacePhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> tbPtbGroupMap =
            alterTableMovePartitionBuilder.getTbPtbGroupMap();
        return new AlterTableMovePartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, discardTableSpacePhysicalPlansMap, tbPtbGroupMap,
            tablesTopologyMap, targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);

    }

}

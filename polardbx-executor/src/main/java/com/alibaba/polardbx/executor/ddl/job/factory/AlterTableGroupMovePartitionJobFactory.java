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
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupMovePartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AnalyzePhyTableTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DdlBackfillCostRecordTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ImportTableSpaceDdlNormalTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SyncLsnTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyExecutorInitTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyFinishTask;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
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
public class AlterTableGroupMovePartitionJobFactory extends AlterTableGroupBaseJobFactory {
    final Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap;
    final Map<String, Map<String, Pair<String, String>>> tbPtbGroupMap;
    final Map<String, String> sourceAndTarDnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    final Map<String, Pair<String, String>> storageInstAndUserInfos = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public AlterTableGroupMovePartitionJobFactory(DDL ddl, AlterTableGroupMovePartitionPreparedData preparedData,
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

        AlterTableGroupMovePartitionPreparedData alterTableGroupMovePartitionPreparedData =
            (AlterTableGroupMovePartitionPreparedData) preparedData;
        String schemaName = alterTableGroupMovePartitionPreparedData.getSchemaName();
        String tableGroupName = alterTableGroupMovePartitionPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, alterTableGroupMovePartitionPreparedData.getTableGroupName(),
                tablesVersion, true, alterTableGroupMovePartitionPreparedData.getTargetPhysicalGroups(), false);

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupMovePartitionPreparedData.getTableGroupName());

        DdlContext ddlContext = executionContext.getDdlContext();
        DdlBackfillCostRecordTask costRecordTask = null;
        if (ddlContext != null && !ddlContext.isSubJob()) {
            costRecordTask = new DdlBackfillCostRecordTask(schemaName);
            final BalanceStats balanceStats = Balancer.collectBalanceStatsOfTableGroup(schemaName, tableGroupName);
            List<PartitionGroupStat> partitionStats = balanceStats.getPartitionGroupStats();
            Long diskSize = 0L;
            Long rows = 0L;
            Set<String> partitionNamesSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            preparedData.getOldPartitionNames().forEach(o -> partitionNamesSet.add(o));
            for (PartitionGroupStat partitionStat : partitionStats) {
                if (partitionNamesSet.contains(partitionStat.getFirstPartition().getPartitionName())) {
                    diskSize += partitionStat.getTotalDiskSize();
                    rows += partitionStat.getDataRows();
                }
            }
            costRecordTask.setCostInfo(
                CostEstimableDdlTask.createCostInfo(rows, diskSize, (long) tableGroupConfig.getTableCount()));
        }
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
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, tableGroupName, null, taskType,
                preparedData.getDdlVersionId(), executionContext);

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
            List<DdlTask> parentTaskList = new ArrayList<>();
            boolean partitionSwitch =
                ComplexTaskMetaManager.ComplexTaskStatus.SOURCE_WRITE_ONLY.name().equalsIgnoreCase(finalStatus)
                    || ComplexTaskMetaManager.ComplexTaskStatus.SOURCE_DELETE_ONLY.name().equalsIgnoreCase(finalStatus);
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            if (partitionSwitch) {
                executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);
                parentTaskList.addAll(bringUpAlterTableGroupTasks);
                executableDdlJob.addTaskRelationship(
                    bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1), pauseCurrentJobTask);
            }
            parentTaskList.add(pauseCurrentJobTask);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, parentTaskList,
                alterTableGroupMovePartitionPreparedData.getTargetPartitionsLocation().keySet().iterator().next());
        }

        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        //attacheCdcFinalMarkTask(executableDdlJob); //暂时不支持复制
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl, AlterTableGroupMovePartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableGroupMovePartitionBuilder alterTableGroupMovePartitionBuilder =
            new AlterTableGroupMovePartitionBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableGroupMovePartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableGroupMovePartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableGroupMovePartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableGroupMovePartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap =
            alterTableGroupMovePartitionBuilder.getDiscardTableSpacePhysicalPlansMap();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupMovePartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupMovePartitionBuilder.getOrderedTargetTablesLocations();
        Map<String, Map<String, Pair<String, String>>> tbPtbGroup =
            alterTableGroupMovePartitionBuilder.getTbPtbGroupMap();

        return new AlterTableGroupMovePartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, discardTableSpacePhysicalPlansMap, tbPtbGroup, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            executionContext).create();
    }

    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        EmptyTask emptyTask = new EmptyTask(schemaName);
        ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask =
            new ChangeSetApplyExecutorInitTask(schemaName,
                ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        ChangeSetApplyFinishTask changeSetApplyFinishTask = new ChangeSetApplyFinishTask(preparedData.getSchemaName(),
            String.format("schema %s group %s start double write ", preparedData.getSchemaName(),
                preparedData.getTableGroupName()));
        SyncLsnTask syncLsnTask = null;
        boolean syncLsnTaskAdded = false;
        boolean emptyTaskAdded = false;

        final boolean useChangeSet = ChangeSetUtils.isChangeSetProcedure(executionContext);

        int pipelineSize = ScaleOutUtils.getTaskPipelineSize(executionContext);
        Queue<DdlTask> leavePipeLineQueue = new LinkedList<>();
        for (Map.Entry<String, Map<String, List<List<String>>>> entry : tablesTopologyMap.entrySet()) {

            AlterTableGroupSubTaskJobFactory subTaskJobFactory;
            String logicalTableName = tablesPrepareData.get(entry.getKey()).getTableName();
            TableMeta tm = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
            if (useChangeSet && tm.isHasPrimaryKey() && ChangeSetUtils.supportUseChangeSet(taskType, tm)) {
                subTaskJobFactory =
                    new AlterTableGroupChangeSetJobFactory(ddl, preparedData, tablesPrepareData.get(entry.getKey()),
                        newPartitionsPhysicalPlansMap.get(entry.getKey()),
                        discardTableSpacePhysicalPlansMap.get(entry.getKey()),
                        tbPtbGroupMap.get(entry.getKey()),
                        sourceAndTarDnMap,
                        storageInstAndUserInfos,
                        tablesTopologyMap.get(entry.getKey()),
                        targetTablesTopology.get(entry.getKey()), sourceTablesTopology.get(entry.getKey()),
                        orderedTargetTablesLocations.get(entry.getKey()), targetPartitionName, false,
                        changeSetApplyExecutorInitTask,
                        changeSetApplyFinishTask,
                        taskType, executionContext);
            } else {
                subTaskJobFactory =
                    new AlterTableGroupSubTaskJobFactory(ddl, preparedData, tablesPrepareData.get(entry.getKey()),
                        newPartitionsPhysicalPlansMap.get(entry.getKey()), tablesTopologyMap.get(entry.getKey()),
                        targetTablesTopology.get(entry.getKey()), sourceTablesTopology.get(entry.getKey()),
                        orderedTargetTablesLocations.get(entry.getKey()), targetPartitionName, false, taskType,
                        executionContext);
            }
            ExecutableDdlJob subTask = subTaskJobFactory.create();
            executableDdlJob.combineTasks(subTask);
            executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());

            if (!syncLsnTaskAdded && preparedData.isUsePhysicalBackfill()) {

                Map<String, Set<String>> sourceTableTopology = sourceTablesTopology.get(entry.getKey());
                Map<String, Set<String>> targetTableTopology = targetTablesTopology.get(entry.getKey());
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

                if (GeneralUtil.isNotEmpty(subTaskJobFactory.getPhysicalyTaskPipeLine())) {
                    syncLsnTask = new SyncLsnTask(schemaName, sourceGroupAndStorageIdMap, targetGroupAndStorageIdMap);
                    executableDdlJob.addTask(syncLsnTask);
                    syncLsnTaskAdded = true;
                }
            }

            if (preparedData.isUsePhysicalBackfill()) {
                for (List<DdlTask> pipeLine : GeneralUtil.emptyIfNull(subTaskJobFactory.getPhysicalyTaskPipeLine())) {
                    DdlTask parentLeaveNode;
                    if (leavePipeLineQueue.size() < pipelineSize) {
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
                        preparedData.getSchemaName(), entry.getKey(), targetTables);
                    for (int i = 2; i < pipeLine.size(); i++) {
                        executableDdlJob.addTaskRelationship(pipeLine.get(1),
                            pipeLine.get(i));
                        executableDdlJob.addTaskRelationship(pipeLine.get(i),
                            importTableSpaceDdlNormalTask);
                    }
                    AnalyzePhyTableTask analyzePhyTableTask = new AnalyzePhyTableTask(schemaName, tarGroupKey,
                        phyTableName);
                    executableDdlJob.addTaskRelationship(importTableSpaceDdlNormalTask, analyzePhyTableTask);
                    executableDdlJob.addTaskRelationship(analyzePhyTableTask,
                        subTaskJobFactory.getBackfillTaskEdgeNodes().get(1));
                    leavePipeLineQueue.add(analyzePhyTableTask);
                }
            }

            if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
                if (!emptyTaskAdded) {
                    executableDdlJob.addTask(emptyTask);
                    emptyTaskAdded = true;
                }
                executableDdlJob.addTask(subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTask.getTail(), emptyTask);
                executableDdlJob.addTaskRelationship(emptyTask, subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(),
                    bringUpAlterTableGroupTasks.get(0));
            } else {
                executableDdlJob.addTaskRelationship(subTask.getTail(), bringUpAlterTableGroupTasks.get(0));
            }

            DdlTask dropUselessTableTask = ComplexTaskFactory.CreateDropUselessPhyTableTask(schemaName, entry.getKey(),
                sourceTablesTopology.get(entry.getKey()), targetTablesTopology.get(entry.getKey()),
                executionContext);
            executableDdlJob.addTask(dropUselessTableTask);
            executableDdlJob.labelAsTail(dropUselessTableTask);
            executableDdlJob.addTaskRelationship(
                bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1), dropUselessTableTask);
            executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);

    }

}

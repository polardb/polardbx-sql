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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableReorgPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableReorgPartitionPreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableReorgPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableReorgPartitionJobFactory(DDL ddl, AlterTableReorgPartitionPreparedData preparedData,
                                              Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                              Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                              Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                              Map<String, Map<String, Set<String>>> targetTablesTopology,
                                              Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                              Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations,
                                              ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.REORGANIZE_PARTITION, executionContext);
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (preparedData.isRemainInOriginalTableGroup()) {
            return reorgInOriginTableGroup();
        } else if (preparedData.isMoveToExistTableGroup()) {
            return reorgAndMoveToExistTableGroup();
        } else if (preparedData.isCreateNewTableGroup()) {
            return reorgInNewTableGroup();
        } else {
            throw new RuntimeException("unexpected");
        }
    }

    private ExecutableDdlJob reorgAndMoveToExistTableGroup() {
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
            new AlterTableGroupValidateTask(schemaName,
                sourceTableGroup, tablesVersion, false,
                /*todo*/null);
        DdlTask validateTargetTableGroup =
            new AlterTableGroupValidateTask(schemaName,
                targetTableGroup, preparedData.getFirstTableVersionInTargetTableGroup(), false,
                preparedData.getTargetPhysicalGroups());

        executableDdlJob.addTask(emptyTask);
        executableDdlJob.addTask(validateSourceTableGroup);
        executableDdlJob.addTask(validateTargetTableGroup);
        executableDdlJob.addTaskRelationship(emptyTask, validateSourceTableGroup);
        executableDdlJob.addTaskRelationship(emptyTask, validateTargetTableGroup);

        Set<Long> outdatedPartitionGroupId = new HashSet<>();

        for (String oldPartGroupName : preparedData.getOldPartitionNames()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(oldPartGroupName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        List<String> newPartitions = preparedData.getNewPartitionNames();
        for (int i = 0; i < preparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(preparedData.getInvisiblePartitionGroups().get(i).getPhy_db());
        }

        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            targetTableGroup,
            tableGroupConfig.getTableGroupRecord().getId(),
            preparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions);

        executableDdlJob.addTask(addMetaTask);
        executableDdlJob.addTaskRelationship(validateSourceTableGroup, addMetaTask);
        executableDdlJob.addTaskRelationship(validateTargetTableGroup, addMetaTask);

        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableChangeTopology(schemaName, targetTableGroup, tableName,
                taskType, executionContext);

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

    private ExecutableDdlJob reorgInNewTableGroup() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        String schemaName = preparedData.getSchemaName();
        Map<String, Long> tablesVersion = getTablesVersion();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, preparedData.getTableGroupName(), tablesVersion, false,
                preparedData.getTargetPhysicalGroups());

        SubJobTask subJobMoveTableToNewGroup =
            new SubJobTask(schemaName, String.format(SET_NEW_TABLE_GROUP, preparedData.getTableName()), null);

        SubJobTask subJobReorgTable = new SubJobTask(schemaName, preparedData.getSourceSql(), null);

        subJobMoveTableToNewGroup.setParentAcquireResource(true);
        subJobReorgTable.setParentAcquireResource(true);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            subJobMoveTableToNewGroup,
            subJobReorgTable
        ));

        return executableDdlJob;
    }

    private ExecutableDdlJob reorgInOriginTableGroup() {
        AlterTableReorgPartitionPreparedData reorgPreparedData = (AlterTableReorgPartitionPreparedData) preparedData;
        String schemaName = reorgPreparedData.getSchemaName();
        String tableGroupName = reorgPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        Map<String, Long> tablesVersion = getTablesVersion();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, reorgPreparedData.getTableGroupName(), tablesVersion, true,
                reorgPreparedData.getTargetPhysicalGroups());

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(reorgPreparedData.getTableGroupName());

        Set<Long> outdatedPartitionGroupId = new HashSet<>();

        for (String oldPartGroupName : reorgPreparedData.getOldPartGroupNames()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(oldPartGroupName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }

        int targetDbCnt = reorgPreparedData.getTargetGroupDetailInfoExRecords().size();

        List<String> targetDbList = new ArrayList<>();
        List<String> newPartitions = new ArrayList<>();

        for (int i = 0; i < reorgPreparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(reorgPreparedData.getTargetGroupDetailInfoExRecords().get(i % targetDbCnt).phyDbName);
            newPartitions.add(reorgPreparedData.getNewPartitionNames().get(i));
        }

        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            reorgPreparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            addMetaTask
        ));

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
                reorgPreparedData.getNewPartitionNames().get(0));
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, ImmutableList.of(pauseCurrentJobTask),
                reorgPreparedData.getNewPartitionNames().get(0));
        }

        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));

        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableReorgPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableReorgPartitionBuilder alterTableReorgPartitionBuilder =
            new AlterTableReorgPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableReorgPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableReorgPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableReorgPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableReorgPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableReorgPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            alterTableReorgPartitionBuilder.getOrderedTargetTablesLocations();

        return new AlterTableReorgPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        EmptyTask emptyTask = new EmptyTask(schemaName);
        AlterTableReorgPartitionSubTaskJobFactory subTaskJobFactory =
            new AlterTableReorgPartitionSubTaskJobFactory(ddl,
                (AlterTableReorgPartitionPreparedData) preparedData,
                tablesPrepareData.get(preparedData.getTableName()),
                newPartitionsPhysicalPlansMap.get(preparedData.getTableName()),
                tablesTopologyMap.get(preparedData.getTableName()),
                targetTablesTopology.get(preparedData.getTableName()),
                sourceTablesTopology.get(preparedData.getTableName()),
                orderedTargetTablesLocations.get(preparedData.getTableName()), targetPartitionName, false,
                taskType, executionContext);
        ExecutableDdlJob subTask = subTaskJobFactory.create();
        executableDdlJob.combineTasks(subTask);
        executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());

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

            DdlTask dropUselessTableTask = ComplexTaskFactory
                .CreateDropUselessPhyTableTask(schemaName, preparedData.getTableName(),
                    sourceTablesTopology.get(preparedData.getTableName()),
                    executionContext);
            executableDdlJob.addTask(dropUselessTableTask);
            executableDdlJob.labelAsTail(dropUselessTableTask);
            executableDdlJob
                .addTaskRelationship(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1),
                    dropUselessTableTask);
        }
        executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());

    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
    }

}

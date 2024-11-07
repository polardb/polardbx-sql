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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableDropPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupDisableDropPartitionMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupDropPartitionRefreshMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.CleanupEmptyTableGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupsSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author luoyanxin
 */
public class AlterTableDropPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableDropPartitionJobFactory(DDL ddl, AlterTableDropPartitionPreparedData preparedData,
                                             Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                             Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                             Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                             Map<String, Map<String, Set<String>>> targetTablesTopology,
                                             Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                             Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations,
                                             ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.DROP_PARTITION, executionContext);
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (preparedData.isRemainInOriginalTableGroup()) {
            return doDropInOriginTableGroup();
        } else if (preparedData.isMoveToExistTableGroup()) {
            return doDropAndMoveToExistTableGroup();
        } else if (preparedData.isCreateNewTableGroup()) {
            return doDropInNewTableGroup();
        } else if (StringUtils.isNotEmpty(preparedData.getTargetImplicitTableGroupName())) {
            return withImplicitTableGroup(executionContext);
        } else {
            throw new RuntimeException("unexpected");
        }
    }

    protected ExecutableDdlJob doDropInOriginTableGroup() {
        AlterTableGroupDropPartitionPreparedData alterTableGroupDropPartitionPreparedData =
            (AlterTableGroupDropPartitionPreparedData) preparedData;
        String schemaName = alterTableGroupDropPartitionPreparedData.getSchemaName();
        String tableGroupName = alterTableGroupDropPartitionPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        Map<String, Long> tablesVersion = getTablesVersion();

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupDropPartitionPreparedData.getTableGroupName());
        boolean isBrdTg = tableGroupConfig.getTableGroupRecord().isBroadCastTableGroup();
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, alterTableGroupDropPartitionPreparedData.getTableGroupName(),
                tablesVersion, true,
                isBrdTg ? null : alterTableGroupDropPartitionPreparedData.getTargetPhysicalGroups(), false);

        Set<String> oldPartitionNames = new TreeSet<>(String::compareToIgnoreCase);
        oldPartitionNames.addAll(preparedData.getOldPartitionNames());

        DdlTask disableDropPartitionMetaTask = new AlterTableGroupDisableDropPartitionMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            preparedData.getTableName(),
            false,
            alterTableGroupDropPartitionPreparedData.getSourceSql(),
            oldPartitionNames,
            preparedData.isOperateOnSubPartition());

        DdlTask refreshMetaTask =
            new AlterTableGroupDropPartitionRefreshMetaTask(schemaName, tableGroupName, tableGroupName,
                preparedData.getTableName(),
                false,
                alterTableGroupDropPartitionPreparedData.getSourceSql(),
                oldPartitionNames,
                preparedData.isOperateOnSubPartition());

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            disableDropPartitionMetaTask
        ));
        executableDdlJob.labelAsHead(validateTask);
        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }

        if (stayAtPublic) {
            constructSubTasks(schemaName, executableDdlJob, disableDropPartitionMetaTask,
                ImmutableList.of(refreshMetaTask),
                null);

            BaseDdlTask tableGroupSyncTask =
                new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());

            DdlTask dropUselessTableTask =
                ComplexTaskFactory.CreateDropUselessPhyTableTask(schemaName, preparedData.getTableName(),
                    getTheDeletedPartitionsLocation((AlterTableGroupDropPartitionPreparedData) preparedData,
                        preparedData.getTableName()), null, executionContext);

            executableDdlJob.addTaskRelationship(executableDdlJob.getTail(), tableGroupSyncTask);
            executableDdlJob.addTaskRelationship(tableGroupSyncTask, dropUselessTableTask);
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, disableDropPartitionMetaTask,
                ImmutableList.of(pauseCurrentJobTask), null);
        }

        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        return executableDdlJob;
    }

    protected ExecutableDdlJob doDropAndMoveToExistTableGroup() {
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
                /*todo*/null, false);
        DdlTask validateTargetTableGroup =
            new AlterTableGroupValidateTask(schemaName,
                targetTableGroup, preparedData.getFirstTableVersionInTargetTableGroup(), false,
                preparedData.getTargetPhysicalGroups(), false);

        executableDdlJob.addTask(emptyTask);
        executableDdlJob.addTask(validateSourceTableGroup);
        executableDdlJob.addTask(validateTargetTableGroup);
        executableDdlJob.addTaskRelationship(emptyTask, validateSourceTableGroup);
        executableDdlJob.addTaskRelationship(emptyTask, validateTargetTableGroup);

        Set<String> oldPartitionNames = new TreeSet<>(String::compareToIgnoreCase);
        oldPartitionNames.addAll(preparedData.getOldPartitionNames());

        DdlTask disableDropPartitionMetaTask = new AlterTableGroupDisableDropPartitionMetaTask(schemaName,
            sourceTableGroup,
            tableGroupConfig.getTableGroupRecord().getId(),
            preparedData.getTableName(),
            false,
            preparedData.getSourceSql(),
            oldPartitionNames,
            preparedData.isOperateOnSubPartition());

        DdlTask refreshMetaTask =
            new AlterTableGroupDropPartitionRefreshMetaTask(schemaName, sourceTableGroup, targetTableGroup,
                preparedData.getTableName(),
                false,
                preparedData.getSourceSql(), oldPartitionNames,
                preparedData.isOperateOnSubPartition());

        executableDdlJob.addTaskRelationship(validateSourceTableGroup, disableDropPartitionMetaTask);
        executableDdlJob.addTaskRelationship(validateTargetTableGroup, disableDropPartitionMetaTask);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }

        if (stayAtPublic) {
            constructSubTasks(schemaName, executableDdlJob, disableDropPartitionMetaTask,
                ImmutableList.of(refreshMetaTask),
                null);

            CleanupEmptyTableGroupTask cleanupEmptyTableGroupTask = new CleanupEmptyTableGroupTask(schemaName,
                preparedData.getTableGroupName());

            List<String> tableGroups = new ArrayList<>(2);
            tableGroups.add(preparedData.getTableGroupName());
            tableGroups.add(preparedData.getTargetTableGroup());
            BaseDdlTask tableGroupsSyncTask =
                new TableGroupsSyncTask(preparedData.getSchemaName(), tableGroups);

            DdlTask dropUselessTableTask =
                ComplexTaskFactory.CreateDropUselessPhyTableTask(schemaName, preparedData.getTableName(),
                    getTheDeletedPartitionsLocation((AlterTableGroupDropPartitionPreparedData) preparedData,
                        preparedData.getTableName()), null, executionContext);
            executableDdlJob.addTaskRelationship(executableDdlJob.getTail(), cleanupEmptyTableGroupTask);
            executableDdlJob.addTaskRelationship(cleanupEmptyTableGroupTask, tableGroupsSyncTask);
            executableDdlJob.addTaskRelationship(tableGroupsSyncTask, dropUselessTableTask);
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, disableDropPartitionMetaTask,
                ImmutableList.of(pauseCurrentJobTask),
                null);
        }

        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        return executableDdlJob;
    }

    protected ExecutableDdlJob doDropInNewTableGroup() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();
        String schemaName = preparedData.getSchemaName();
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName,
                preparedData.getTableGroupName(), tablesVersion, false,
                preparedData.getTargetPhysicalGroups(), false);

        SubJobTask subJobMoveTableToNewGroup =
            new SubJobTask(schemaName, String.format(SET_NEW_TABLE_GROUP, preparedData.getTableName()), null);
        SubJobTask subJobAddPartition = new SubJobTask(schemaName, preparedData.getSourceSql(), null);
        subJobMoveTableToNewGroup.setParentAcquireResource(true);
        subJobAddPartition.setParentAcquireResource(true);
        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            subJobMoveTableToNewGroup,
            subJobAddPartition
        ));
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableDropPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {

        AlterTableDropPartitionBuilder alterTableDropPartitionBuilder =
            new AlterTableDropPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableDropPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableDropPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableDropPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableDropPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableDropPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            alterTableDropPartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableDropPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        AlterTableDropPartitionSubTaskJobFactory subTaskJobFactory =
            new AlterTableDropPartitionSubTaskJobFactory(ddl,
                (AlterTableDropPartitionPreparedData) preparedData,
                tablesPrepareData.get(preparedData.getTableName()),
                newPartitionsPhysicalPlansMap.get(preparedData.getTableName()),
                tablesTopologyMap.get(preparedData.getTableName()),
                targetTablesTopology.get(preparedData.getTableName()),
                sourceTablesTopology.get(preparedData.getTableName()),
                orderedTargetTablesLocations.get(preparedData.getTableName()),
                "",
                false,
                taskType,
                executionContext);
        ExecutableDdlJob subDdlJob = subTaskJobFactory.create();
        List<DdlTask> subTasks = subDdlJob.getAllTasks();

        executableDdlJob.getExcludeResources().addAll(subDdlJob.getExcludeResources());
        executableDdlJob.addTaskRelationship(tailTask, subTasks.get(0));
        executableDdlJob.addTaskRelationship(subTasks.get(0), subTasks.get(1));
        executableDdlJob.addTaskRelationship(subTasks.get(1), subTasks.get(2));
        if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
            executableDdlJob.addTaskRelationship(subTasks.get(subTasks.size() - 1),
                subTaskJobFactory.getCdcTableGroupDdlMarkTask());
            executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(),
                bringUpAlterTableGroupTasks.get(0));

            List<DdlTask> syncTasks =
                subTaskJobFactory.generateSyncTask(schemaName, preparedData.getTableName(), executionContext);
            executableDdlJob.addTaskRelationship(bringUpAlterTableGroupTasks.get(0), syncTasks.get(0));
            executableDdlJob.addTaskRelationship(syncTasks.get(0), syncTasks.get(1));
            executableDdlJob.labelAsTail(syncTasks.get(1));
        } else {
            executableDdlJob.addTaskRelationship(subTasks.get(subTasks.size() - 1), bringUpAlterTableGroupTasks.get(0));
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
    }

}

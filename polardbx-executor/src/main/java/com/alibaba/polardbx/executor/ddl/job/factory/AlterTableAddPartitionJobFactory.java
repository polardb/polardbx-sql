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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableAddPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateTablesVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.*;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * @author luoyanxin
 */
public class AlterTableAddPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    //todo luoyanxin consider the table with default partition, we need to "split" the default partition

    public AlterTableAddPartitionJobFactory(DDL ddl, AlterTableAddPartitionPreparedData preparedData,
                                            Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                            Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                            Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap,
                                            Map<String, Map<String, Set<String>>> targetTablesTopology,
                                            Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                            Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations,
                                            ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
                targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
                ComplexTaskMetaManager.ComplexTaskType.ADD_PARTITION, executionContext);
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (preparedData.isRemainInOriginalTableGroup()) {
            return doAddInOriginTableGroup();
        } else if (preparedData.isMoveToExistTableGroup()) {
            return doAddAndMoveToExistTableGroup();
        } else if (preparedData.isCreateNewTableGroup()) {
            return doAddInNewTableGroup();
        } else if (StringUtils.isNotEmpty(preparedData.getTargetImplicitTableGroupName())) {
            return withImplicitTableGroup(executionContext);
        } else {
            throw new RuntimeException("unexpected");
        }
    }

    protected ExecutableDdlJob doAddInOriginTableGroup() {
        AlterTableGroupAddPartitionPreparedData alterTableGroupAddPartitionPreparedData =
                (AlterTableGroupAddPartitionPreparedData) preparedData;
        String schemaName = alterTableGroupAddPartitionPreparedData.getSchemaName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        Map<String, Long> tablesVersion = getTablesVersion();
        DdlTask validateTask =
                new AlterTableGroupValidateTask(schemaName, alterTableGroupAddPartitionPreparedData.getTableGroupName(),
                        tablesVersion, true, alterTableGroupAddPartitionPreparedData.getTargetPhysicalGroups(), false);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(alterTableGroupAddPartitionPreparedData.getTableGroupName());

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

        AlterTableGroupAddPartitionGroupMetaTask addPartitionGroupMetaTask =
                new AlterTableGroupAddPartitionGroupMetaTask(schemaName, tableGroupConfig.getTableGroupRecord().id, targetDbList, newPartitions, localities);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(validateTask, addPartitionGroupMetaTask));
        List<DdlTask> bringUpAlterTableGroupTasks = generateSyncTask();

        final String finalStatus =
                executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                    StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }

        if (stayAtPublic) {
            executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);
            constructSubTasks(schemaName, executableDdlJob, addPartitionGroupMetaTask, bringUpAlterTableGroupTasks, null);
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, addPartitionGroupMetaTask, ImmutableList.of(pauseCurrentJobTask), null);
        }

        executableDdlJob.labelAsTail(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1));
        return executableDdlJob;
    }

    protected ExecutableDdlJob doAddAndMoveToExistTableGroup() {
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
        emptyTask = new EmptyTask(schemaName);
        executableDdlJob.addTaskRelationship(validateSourceTableGroup, emptyTask);
        executableDdlJob.addTaskRelationship(validateTargetTableGroup, emptyTask);

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


        List<DdlTask> bringUpAlterTableGroupTasks = generateSyncTask();

        final String finalStatus =
                executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                    StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }

        if (stayAtPublic) {
            executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);
            constructSubTasks(schemaName, executableDdlJob, emptyTask, bringUpAlterTableGroupTasks, null);
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, emptyTask, ImmutableList.of(pauseCurrentJobTask), null);
        }

        executableDdlJob.labelAsTail(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1));

        return executableDdlJob;
    }

    protected ExecutableDdlJob doAddInNewTableGroup() {
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
                                          AlterTableAddPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableAddPartitionBuilder alterTableAddPartitionBuilder =
                new AlterTableAddPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap =
                alterTableAddPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
                alterTableAddPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
                alterTableAddPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
                alterTableAddPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
                alterTableAddPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
                alterTableAddPartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableAddPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
                newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
                orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        AlterTableGroupSubTaskJobFactory subTaskJobFactory =
                new AlterTableAddPartitionSubTaskJobFactory(ddl,
                        (AlterTableAddPartitionPreparedData) preparedData,
                        tablesPrepareData.get(preparedData.getTableName()),
                        newPartitionsPhysicalPlansMap.get(preparedData.getTableName()),
                        tablesTopologyMap.get(preparedData.getTableName()),
                        targetTablesTopology.get(preparedData.getTableName()),
                        sourceTablesTopology.get(preparedData.getTableName()),
                        orderedTargetTablesLocations.get(preparedData.getTableName()),
                        targetPartitionName,
                        false,
                        taskType,
                        executionContext);
        ExecutableDdlJob subDdlJob = subTaskJobFactory.create();
        executableDdlJob.combineTasks(subDdlJob);
        executableDdlJob.addTaskRelationship(tailTask, subDdlJob.getHead());

        executableDdlJob.getExcludeResources().addAll(subDdlJob.getExcludeResources());
        AlterTableGroupAddPartitionRemoveMetaTask removeMetaTask = new AlterTableGroupAddPartitionRemoveMetaTask(schemaName, preparedData.getTableGroupName());
        if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
            executableDdlJob.addTaskRelationship(subDdlJob.getTail(),
                    subTaskJobFactory.getCdcTableGroupDdlMarkTask());
            executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(),
                    removeMetaTask);
            executableDdlJob.addTaskRelationship(removeMetaTask,
                    bringUpAlterTableGroupTasks.get(0));
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
    }

    private List<DdlTask> generateSyncTask() {
        String schemaName = preparedData.getSchemaName();
        String tableGroupName = preparedData.getTableGroupName();
        List<String> logicalTableNames = new ArrayList<>();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(preparedData.getTableName());
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            logicalTableNames.add(tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
        } else {
            logicalTableNames.add(preparedData.getTableName());
        }

        List<DdlTask> tasks = new ArrayList<>(3);
        String targetTableGroupName = null;
        if (StringUtils.isNotEmpty(preparedData.getTargetTableGroup())) {
            targetTableGroupName = preparedData.getTargetTableGroup();

        } else if (StringUtils.isNotEmpty(preparedData.getTargetImplicitTableGroupName())) {
            targetTableGroupName = preparedData.getTargetImplicitTableGroupName();
        }
        if (preparedData.isMoveToExistTableGroup()) {
            CleanupEmptyTableGroupTask cleanupEmptyTableGroupTask = new CleanupEmptyTableGroupTask(schemaName,
                    preparedData.getTableGroupName());
            tasks.add(cleanupEmptyTableGroupTask);
        }
        DdlTask tableGroupSyncTask;
        if (StringUtils.isEmpty(targetTableGroupName) || targetTableGroupName.equalsIgnoreCase(tableGroupName)) {
            tableGroupSyncTask =
                    new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());
        } else {
            tableGroupSyncTask =
                    new TableGroupsSyncTask(preparedData.getSchemaName(), Lists.newArrayList(preparedData.getTableGroupName(), targetTableGroupName));
        }
        DdlTask updateTablesVersionTask = new UpdateTablesVersionTask(schemaName, logicalTableNames);
        //not use preemptive sync to interrupt dml， just wait for the sync task to finish
        TablesSyncTask tableSyncTask = new TablesSyncTask(schemaName, logicalTableNames, true);
        tasks.add(tableGroupSyncTask);
        tasks.add(updateTablesVersionTask);
        tasks.add(tableSyncTask);
        return tasks;
    }
}

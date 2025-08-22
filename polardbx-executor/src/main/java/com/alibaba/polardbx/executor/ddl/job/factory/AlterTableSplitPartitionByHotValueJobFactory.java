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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableSplitPartitionByHotValueBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSplitPartitionByHotValuePreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author luoyanxin
 */
public class AlterTableSplitPartitionByHotValueJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableSplitPartitionByHotValueJobFactory(DDL ddl,
                                                        AlterTableGroupSplitPartitionByHotValuePreparedData preparedData,
                                                        Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                        Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap,
                                                        Map<String, Map<String, Set<String>>> targetTablesTopology,
                                                        Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                                        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations,
                                                        ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.SPLIT_HOT_VALUE, executionContext);
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (preparedData.isRemainInOriginalTableGroup()) {
            return splitInOriginTableGroup();
        } else if (preparedData.isMoveToExistTableGroup()) {
            return splitAndMoveToExistTableGroup();
        } else if (preparedData.isCreateNewTableGroup()) {
            return splitInNewTableGroup();
        } else if (org.apache.commons.lang.StringUtils.isNotEmpty(preparedData.getTargetImplicitTableGroupName())) {
            return withImplicitTableGroup(executionContext);
        } else {
            throw new RuntimeException("unexpected");
        }
    }

    private ExecutableDdlJob splitInOriginTableGroup() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();
        String schemaName = preparedData.getSchemaName();
        String tableGroupName = preparedData.getTableGroupName();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName,
                preparedData.getTableGroupName(), tablesVersion, true,
                preparedData.getTargetPhysicalGroups(), false);

        Set<Long> outdatedPartitionGroupId =
            getOldDatePartitionGroups(preparedData, preparedData.getOldPartitionNames(),
                ((AlterTableSplitPartitionByHotValuePreparedData) preparedData).isSplitSubPartition());

        List<String> targetDbList = new ArrayList<>();

        List<String> newPartitions = getNewPartitions();

        List<String> localities = new ArrayList<>();
        Map<String, String> partAndDbMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (PartitionGroupRecord partitionGroupRecord : preparedData.getInvisiblePartitionGroups()) {
            partAndDbMap.put(partitionGroupRecord.partition_name, partitionGroupRecord.phy_db);
        }
        for (int i = 0; i < newPartitions.size(); i++) {
            targetDbList.add(partAndDbMap.get(newPartitions.get(i)));
            localities.add(preparedData.getInvisiblePartitionGroups().get(i)
                .getLocality());
        }

        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            preparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions,
            localities);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            addMetaTask
        ));
        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, tableGroupName, null,
                taskType, preparedData.getDdlVersionId(), executionContext);

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

        if (((AlterTableGroupSplitPartitionByHotValuePreparedData) preparedData).isSkipSplit()) {
            return new TransientDdlJob();
        } else {

            // TODO(luoyanxin)
            executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
            return executableDdlJob;
        }
    }

    private ExecutableDdlJob splitAndMoveToExistTableGroup() {
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

        Set<Long> outdatedPartitionGroupId =
            getOldDatePartitionGroups(preparedData, preparedData.getOldPartitionNames(),
                ((AlterTableSplitPartitionByHotValuePreparedData) preparedData).isSplitSubPartition());

        List<String> targetDbList = new ArrayList<>();
        List<String> localities = new ArrayList<>();
        List<String> newPartitions = getNewPartitions();
        Map<String, String> partAndDbMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (PartitionGroupRecord partitionGroupRecord : preparedData.getInvisiblePartitionGroups()) {
            partAndDbMap.put(partitionGroupRecord.partition_name, partitionGroupRecord.phy_db);
        }
        for (int i = 0; i < newPartitions.size(); i++) {
            targetDbList.add(partAndDbMap.get(newPartitions.get(i)));
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

        if (((AlterTableGroupSplitPartitionByHotValuePreparedData) preparedData).isSkipSplit()) {
            return new TransientDdlJob();
        } else {

            // TODO(luoyanxin)
            executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
            return executableDdlJob;
        }
    }

    private ExecutableDdlJob splitInNewTableGroup() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();
        String schemaName = preparedData.getSchemaName();
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName,
                preparedData.getTableGroupName(), tablesVersion, false,
                preparedData.getTargetPhysicalGroups(), false);

        SubJobTask subJobMoveTableToNewGroup =
            new SubJobTask(schemaName, String.format(SET_NEW_TABLE_GROUP, preparedData.getTableName()), null);
        SubJobTask subJobSplitTable = new SubJobTask(schemaName, preparedData.getSourceSql(), null);
        subJobMoveTableToNewGroup.setParentAcquireResource(true);
        subJobSplitTable.setParentAcquireResource(true);
        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            subJobMoveTableToNewGroup,
            subJobSplitTable
        ));
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupSplitPartitionByHotValuePreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableSplitPartitionByHotValueBuilder alterTableSplitPartitionByHotValueBuilder =
            new AlterTableSplitPartitionByHotValueBuilder(ddl, preparedData, executionContext);
        Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap =
            alterTableSplitPartitionByHotValueBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableSplitPartitionByHotValueBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableSplitPartitionByHotValueBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableSplitPartitionByHotValueBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableSplitPartitionByHotValueBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            alterTableSplitPartitionByHotValueBuilder.getOrderedTargetTablesLocations();
        return new AlterTableSplitPartitionByHotValueJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        String logicalTableName = preparedData.getTableName();
        TableMeta tm = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        final boolean useChangeSet = ChangeSetUtils.isChangeSetProcedure(executionContext);
        AlterTableGroupSubTaskJobFactory subTaskJobFactory;
        if (useChangeSet && ChangeSetUtils.supportUseChangeSet(taskType, tm)) {
            subTaskJobFactory =
                new AlterTableSplitPartitionByHotValueChangeSetJobFactory(ddl,
                    (AlterTableSplitPartitionByHotValuePreparedData) preparedData,
                    tablesPrepareData.get(preparedData.getTableName()),
                    newPartitionsPhysicalPlansMap.get(preparedData.getTableName()),
                    tablesTopologyMap.get(preparedData.getTableName()),
                    targetTablesTopology.get(preparedData.getTableName()),
                    sourceTablesTopology.get(preparedData.getTableName()),
                    orderedTargetTablesLocations.get(preparedData.getTableName()), targetPartitionName, false,
                    taskType, executionContext);
        } else {
            subTaskJobFactory =
                new AlterTableSplitPartitionByHotValueSubTaskJobFactory(ddl,
                    (AlterTableSplitPartitionByHotValuePreparedData) preparedData,
                    tablesPrepareData.get(preparedData.getTableName()),
                    newPartitionsPhysicalPlansMap.get(preparedData.getTableName()),
                    tablesTopologyMap.get(preparedData.getTableName()),
                    targetTablesTopology.get(preparedData.getTableName()),
                    sourceTablesTopology.get(preparedData.getTableName()),
                    orderedTargetTablesLocations.get(preparedData.getTableName()), targetPartitionName, false,
                    taskType, executionContext);
        }

        ExecutableDdlJob subTask = subTaskJobFactory.create();
        if (((AlterTableGroupSplitPartitionByHotValuePreparedData) preparedData).isSkipSplit()) {
            return;
        }
        executableDdlJob.combineTasks(subTask);
        executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());
        if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
            executableDdlJob.addTask(subTaskJobFactory.getCdcTableGroupDdlMarkTask());
            executableDdlJob.addTaskRelationship(subTask.getTail(), subTaskJobFactory.getCdcTableGroupDdlMarkTask());
            executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(), bringUpAlterTableGroupTasks.get(0));
        } else {
            executableDdlJob.addTaskRelationship(subTask.getTail(), bringUpAlterTableGroupTasks.get(0));
        }
        if (bringUpAlterTableGroupTasks.size() > 1 && !(bringUpAlterTableGroupTasks.get(
            0) instanceof PauseCurrentJobTask)) {
            DdlTask dropUselessTableTask = ComplexTaskFactory
                .CreateDropUselessPhyTableTask(schemaName, preparedData.getTableName(),
                    sourceTablesTopology.get(preparedData.getTableName()),
                    targetTablesTopology.get(preparedData.getTableName()),
                    executionContext);
            executableDdlJob.addTask(dropUselessTableTask);
            executableDdlJob
                .addTaskRelationship(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1),
                    dropUselessTableTask);
        }
        executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
    }

    private List<String> getNewPartitions() {
        AlterTableSplitPartitionByHotValuePreparedData splitData =
            (AlterTableSplitPartitionByHotValuePreparedData) preparedData;

        if (splitData.isUseTemplatePart()) {
            List<String> newPartitions = new ArrayList<>();
            for (String logicalPartName : preparedData.getLogicalParts()) {
                for (String newPartName : preparedData.getNewPartitionNames()) {
                    newPartitions.add(PartitionNameUtil.autoBuildSubPartitionName(logicalPartName, newPartName));
                }
            }
            return newPartitions;
        } else {
            return preparedData.getNewPartitionNames();
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
        AlterTableGroupSplitPartitionByHotValuePreparedData splitPreparedData =
            (AlterTableGroupSplitPartitionByHotValuePreparedData) preparedData;

        if (StringUtils.isNotEmpty(splitPreparedData.getHotKeyPartitionName())) {
            resources.add(concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()),
                splitPreparedData.getHotKeyPartitionName()));
        }

    }

}

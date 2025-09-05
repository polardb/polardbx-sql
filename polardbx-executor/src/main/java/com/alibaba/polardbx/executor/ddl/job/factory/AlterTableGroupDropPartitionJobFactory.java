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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupDropPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupDisableDropPartitionMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupDropPartitionRefreshMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
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

import java.util.*;

/**
 * @author luoyanxin
 */
public class AlterTableGroupDropPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableGroupDropPartitionJobFactory(DDL ddl, AlterTableGroupDropPartitionPreparedData preparedData,
                                                  Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                  Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                  Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap,
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
            null, true,
            alterTableGroupDropPartitionPreparedData.getSourceSql(),
            oldPartitionNames,
            preparedData.isOperateOnSubPartition());

        DdlTask refreshMetaTask =
            new AlterTableGroupDropPartitionRefreshMetaTask(schemaName, tableGroupName, tableGroupName,
                null, true,
                alterTableGroupDropPartitionPreparedData.getSourceSql(), oldPartitionNames,
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
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, disableDropPartitionMetaTask,
                ImmutableList.of(pauseCurrentJobTask), null);
        }

        // TODO(luoyanxin)
        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        attacheCdcFinalMarkTask(executableDdlJob);
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl, AlterTableGroupDropPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {

        String schemaName = preparedData.getSchemaName();
        String tableGroupName = preparedData.getTableGroupName();

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);

        Set<Long> outdatedPartitionGroupId = new HashSet<>();
        for (String dropPartitionName : preparedData.getOldPartitionNames()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(dropPartitionName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }

        AlterTableGroupDropPartitionBuilder alterTableGroupDropPartitionBuilder =
            new AlterTableGroupDropPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap =
            alterTableGroupDropPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableGroupDropPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableGroupDropPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableGroupDropPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupDropPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupDropPartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableGroupDropPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {

        EmptyTask emptyTask = new EmptyTask(schemaName);
        BaseDdlTask tableGroupSyncTask = null;
        boolean emptyTaskAdded = false;

        for (Map.Entry<String, TreeMap<String, List<List<String>>>> entry : tablesTopologyMap.entrySet()) {
            AlterTableDropPartitionSubTaskJobFactory subTaskJobFactory =
                new AlterTableDropPartitionSubTaskJobFactory(ddl,
                    (AlterTableGroupDropPartitionPreparedData) preparedData,
                    tablesPrepareData.get(entry.getKey()),
                    newPartitionsPhysicalPlansMap.get(entry.getKey()),
                    tablesTopologyMap.get(entry.getKey()),
                    targetTablesTopology.get(entry.getKey()),
                    sourceTablesTopology.get(entry.getKey()),
                    orderedTargetTablesLocations.get(entry.getKey()),
                    "",
                    false,
                    taskType,
                    executionContext);
            ExecutableDdlJob subDdlJob = subTaskJobFactory.create();
            List<DdlTask> subTasks = subTaskJobFactory.getAllTaskList();

            DdlTask dropUselessTableTask =
                ComplexTaskFactory.CreateDropUselessPhyTableTask(schemaName, entry.getKey(),
                    getTheDeletedPartitionsLocation((AlterTableGroupDropPartitionPreparedData) preparedData,
                        entry.getKey()),
                    null, executionContext);
            executableDdlJob.getExcludeResources().addAll(subDdlJob.getExcludeResources());
            executableDdlJob.addTaskRelationship(tailTask, subTasks.get(0));
            DdlTask subTailTask = subTasks.get(0);
            if (executionContext.getParamManager().getBoolean(ConnectionParams.DISABLE_PARTITION_BEFORE_DROP)) {
                executableDdlJob.addTaskRelationship(subTasks.get(0), subTasks.get(1));
                executableDdlJob.addTaskRelationship(subTasks.get(1), subTasks.get(2));
                subTailTask = subTasks.get(2);
            }
            if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
                executableDdlJob.addTaskRelationship(subTailTask,
                    subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                if (!emptyTaskAdded) {
                    executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(),
                        bringUpAlterTableGroupTasks.get(0));
                }

                List<DdlTask> syncTasks =
                    subTaskJobFactory.generateSyncTask(schemaName, entry.getKey(), executionContext);
                executableDdlJob.addTaskRelationship(bringUpAlterTableGroupTasks.get(0), syncTasks.get(0));
                executableDdlJob.addTaskRelationship(syncTasks.get(0), syncTasks.get(1));
                executableDdlJob.addTaskRelationship(syncTasks.get(1), emptyTask);

                if (!emptyTaskAdded) {
                    tableGroupSyncTask =
                        new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());
                    executableDdlJob.addTaskRelationship(emptyTask, tableGroupSyncTask);
                }
                executableDdlJob.addTaskRelationship(tableGroupSyncTask, dropUselessTableTask);
                emptyTaskAdded = true;
            } else {
                executableDdlJob.addTaskRelationship(subTailTask,
                    bringUpAlterTableGroupTasks.get(0));
            }
            executableDdlJob.labelAsTail(dropUselessTableTask);
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
    }

}

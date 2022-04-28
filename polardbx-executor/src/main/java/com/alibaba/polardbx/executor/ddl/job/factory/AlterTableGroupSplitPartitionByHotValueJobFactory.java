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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupSplitPartitionByHotValueBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author luoyanxin
 */
public class AlterTableGroupSplitPartitionByHotValueJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableGroupSplitPartitionByHotValueJobFactory(DDL ddl,
                                                             AlterTableGroupSplitPartitionByHotValuePreparedData preparedData,
                                                             Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                             Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                             Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                                             Map<String, Map<String, Set<String>>> targetTablesTopology,
                                                             Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                                             Map<String, List<Pair<String, String>>> orderedTargetTablesLocations,
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
        AlterTableGroupSplitPartitionByHotValuePreparedData alterTableGroupSplitPartitionByHotValuePreparedData =
            (AlterTableGroupSplitPartitionByHotValuePreparedData) preparedData;
        String schemaName = alterTableGroupSplitPartitionByHotValuePreparedData.getSchemaName();
        String tableGroupName = alterTableGroupSplitPartitionByHotValuePreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName,
                alterTableGroupSplitPartitionByHotValuePreparedData.getTableGroupName(), tablesVersion, true,
                alterTableGroupSplitPartitionByHotValuePreparedData.getTargetPhysicalGroups());
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupSplitPartitionByHotValuePreparedData.getTableGroupName());

        Set<Long> outdatedPartitionGroupId = new HashSet<>();

        for (String splitPartitionName : alterTableGroupSplitPartitionByHotValuePreparedData.getOldPartitionNames()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(splitPartitionName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        int targetDbCnt =
            alterTableGroupSplitPartitionByHotValuePreparedData.getTargetGroupDetailInfoExRecords().size();
        List<String> newPartitions = alterTableGroupSplitPartitionByHotValuePreparedData.getNewPartitionNames();
        for (int i = 0; i < alterTableGroupSplitPartitionByHotValuePreparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(alterTableGroupSplitPartitionByHotValuePreparedData.getTargetGroupDetailInfoExRecords()
                .get(i % targetDbCnt).phyDbName);
        }
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            alterTableGroupSplitPartitionByHotValuePreparedData.getSourceSql(),
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
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, tableGroupName, null,
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
                alterTableGroupSplitPartitionByHotValuePreparedData.getOldPartitionNames().get(0));
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, ImmutableList.of(pauseCurrentJobTask),
                alterTableGroupSplitPartitionByHotValuePreparedData.getOldPartitionNames().get(0));
        }

        if (((AlterTableGroupSplitPartitionByHotValuePreparedData) preparedData).isSkipSplit()) {
            return new TransientDdlJob();
        } else {
            executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
            return executableDdlJob;
        }
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupSplitPartitionByHotValuePreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableGroupSplitPartitionByHotValueBuilder alterTableGroupSplitPartitionByHotValueBuilder =
            new AlterTableGroupSplitPartitionByHotValueBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableGroupSplitPartitionByHotValueBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableGroupSplitPartitionByHotValueBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableGroupSplitPartitionByHotValueBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableGroupSplitPartitionByHotValueBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupSplitPartitionByHotValueBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, List<Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupSplitPartitionByHotValueBuilder.getOrderedTargetTablesLocations();
        return new AlterTableGroupSplitPartitionByHotValueJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        EmptyTask emptyTask = new EmptyTask(schemaName);
        boolean emptyTaskAdded = false;

        for (Map.Entry<String, Map<String, List<List<String>>>> entry : tablesTopologyMap.entrySet()) {
            AlterTableGroupSplitPartitionByHotValueSubTaskJobFactory subTaskJobFactory =
                new AlterTableGroupSplitPartitionByHotValueSubTaskJobFactory(ddl,
                    (AlterTableGroupSplitPartitionByHotValuePreparedData) preparedData,
                    tablesPrepareData.get(entry.getKey()),
                    newPartitionsPhysicalPlansMap.get(entry.getKey()), tablesTopologyMap.get(entry.getKey()),
                    targetTablesTopology.get(entry.getKey()), sourceTablesTopology.get(entry.getKey()),
                    orderedTargetTablesLocations.get(entry.getKey()), targetPartitionName, false, executionContext);
            ExecutableDdlJob subTask = subTaskJobFactory.create();
            if (((AlterTableGroupSplitPartitionByHotValuePreparedData) preparedData).isSkipSplit()) {
                return;
            }
            executableDdlJob.combineTasks(subTask);
            executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());
            if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
                if (!emptyTaskAdded) {
                    executableDdlJob.addTask(emptyTask);
                    emptyTaskAdded = true;
                }
                executableDdlJob.addTask(subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTask.getTail(), emptyTask);
                executableDdlJob.addTaskRelationship(emptyTask, subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(), bringUpAlterTableGroupTasks.get(0));
            } else {
                executableDdlJob.addTaskRelationship(subTask.getTail(), bringUpAlterTableGroupTasks.get(0));
            }
            DdlTask dropUselessTableTask = ComplexTaskFactory
                .CreateDropUselessPhyTableTask(schemaName, entry.getKey(), sourceTablesTopology.get(entry.getKey()),
                    executionContext);
            executableDdlJob.addTask(dropUselessTableTask);
            executableDdlJob
                .addTaskRelationship(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1),
                    dropUselessTableTask);
            executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
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

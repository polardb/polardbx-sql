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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.RefreshTopologyBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InitNewStorageInstTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.RefreshTopologyAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.RefreshTopologyValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RefreshDbTopologyPreparedData;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author luoyanxin
 */
public class RefreshDbTopologyFactory extends AlterTableGroupBaseJobFactory {

    public RefreshDbTopologyFactory(DDL ddl, RefreshDbTopologyPreparedData preparedData,
                                    Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                    Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                    Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                    Map<String, Map<String, Set<String>>> targetTablesTopology,
                                    Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                    Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations,
                                    ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.REFRESH_TOPOLOGY, executionContext);
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        RefreshDbTopologyPreparedData refreshTopologyPreparedData =
            (RefreshDbTopologyPreparedData) preparedData;
        String schemaName = refreshTopologyPreparedData.getSchemaName();
        String tableGroupName = refreshTopologyPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        Map<String, Long> tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tablesPrepareData.entrySet().stream()
            .forEach(o -> tablesVersion.putIfAbsent(o.getKey(), o.getValue().getTableVersion()));

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, refreshTopologyPreparedData.getTableGroupName(), tablesVersion,
                true, null, false);
        RefreshTopologyValidateTask refreshTopologyValidateTask =
            new RefreshTopologyValidateTask(schemaName, refreshTopologyPreparedData.getInstGroupDbInfo());

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(refreshTopologyPreparedData.getTableGroupName());

        DdlTask InitNewStorageInstTask =
            new InitNewStorageInstTask(schemaName, refreshTopologyPreparedData.getInstGroupDbInfo());

        List<String> targetDbList = new ArrayList<>();
        int targetDbCnt = refreshTopologyPreparedData.getTargetGroupDetailInfoExRecords().size();
        List<String> newPartitions = refreshTopologyPreparedData.getNewPartitionNames();
        for (int i = 0; i < newPartitions.size(); i++) {
            targetDbList.add(refreshTopologyPreparedData.getTargetGroupDetailInfoExRecords()
                .get(i % targetDbCnt).phyDbName);
        }

        DdlTask addMetaTask = new RefreshTopologyAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            refreshTopologyPreparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            targetDbList,
            newPartitions);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            refreshTopologyValidateTask,
            InitNewStorageInstTask,
            addMetaTask
        ));
        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, tableGroupName, null,
                taskType, preparedData.getDdlVersionId(), executionContext);

        executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);
        constructSubTasks(schemaName, executableDdlJob, addMetaTask, bringUpAlterTableGroupTasks, null);
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1));
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          RefreshDbTopologyPreparedData preparedData,
                                          ExecutionContext executionContext) {
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
                .getBroadcastTableGroupConfig();
        if (tableGroupConfig == null || tableGroupConfig.getAllTables().size() == 0) {
            ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
            DdlTask InitNewStorageInstTask =
                new InitNewStorageInstTask(preparedData.getSchemaName(), preparedData.getInstGroupDbInfo());
            executableDdlJob.addSequentialTasks(Lists.newArrayList(
                InitNewStorageInstTask
            ));
            executableDdlJob.labelAsHead(InitNewStorageInstTask);
            executableDdlJob.labelAsTail(InitNewStorageInstTask);
            return executableDdlJob;
        }
        RefreshTopologyBuilder refreshTopologyBuilder =
            new RefreshTopologyBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            refreshTopologyBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            refreshTopologyBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            refreshTopologyBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            refreshTopologyBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            refreshTopologyBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            refreshTopologyBuilder.getOrderedTargetTablesLocations();
        return new RefreshDbTopologyFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        EmptyTask emptyTask = new EmptyTask(schemaName);
        boolean emptyTaskAdded = false;
        for (Map.Entry<String, Map<String, List<List<String>>>> entry : tablesTopologyMap.entrySet()) {
            RefreshDbTopologySubTaskJobFactory subTaskJobFactory =
                new RefreshDbTopologySubTaskJobFactory(ddl, preparedData, tablesPrepareData.get(entry.getKey()),
                    newPartitionsPhysicalPlansMap.get(entry.getKey()), tablesTopologyMap.get(entry.getKey()),
                    targetTablesTopology.get(entry.getKey()), sourceTablesTopology.get(entry.getKey()),
                    orderedTargetTablesLocations.get(entry.getKey()), targetPartitionName, false, executionContext);
            ExecutableDdlJob subTask = subTaskJobFactory.create();
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

            executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
        RefreshDbTopologyPreparedData refreshTopologyPreparedData =
            (RefreshDbTopologyPreparedData) preparedData;
        if (refreshTopologyPreparedData.getInstGroupDbInfo() != null) {
            for (Map.Entry<String, List<Pair<String, String>>> entry : refreshTopologyPreparedData.getInstGroupDbInfo()
                .entrySet()) {
                for (Pair<String, String> groupAndDb : entry.getValue()) {
                    resources.add(concatWithDot(preparedData.getSchemaName(), groupAndDb.getKey()));
                }
            }
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
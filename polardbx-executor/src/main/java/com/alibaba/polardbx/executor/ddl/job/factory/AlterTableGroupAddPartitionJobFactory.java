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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupAddPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateTablesVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.*;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupAddPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * @author luoyanxin
 */
public class AlterTableGroupAddPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    //todo luoyanxin consider the table with default partition, we need to "split" the default partition

    public AlterTableGroupAddPartitionJobFactory(DDL ddl, AlterTableGroupAddPartitionPreparedData preparedData,
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
        AlterTableGroupAddPartitionPreparedData alterTableGroupAddPartitionPreparedData =
                (AlterTableGroupAddPartitionPreparedData) preparedData;
        String schemaName = alterTableGroupAddPartitionPreparedData.getSchemaName();
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

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        Map<String, Long> tablesVersion = getTablesVersion();
        DdlTask validateTask =
                new AlterTableGroupValidateTask(schemaName, alterTableGroupAddPartitionPreparedData.getTableGroupName(),
                        tablesVersion, true, alterTableGroupAddPartitionPreparedData.getTargetPhysicalGroups(), false);
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
        attacheCdcFinalMarkTask(executableDdlJob);
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl, AlterTableGroupAddPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableGroupAddPartitionBuilder alterTableGroupAddPartitionBuilder =
                new AlterTableGroupAddPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap =
                alterTableGroupAddPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
                alterTableGroupAddPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
                alterTableGroupAddPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
                alterTableGroupAddPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
                alterTableGroupAddPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
                alterTableGroupAddPartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableGroupAddPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
                newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
                orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        boolean firstTable = true;

        AlterTableGroupAddPartitionRemoveMetaTask removeMetaTask = new AlterTableGroupAddPartitionRemoveMetaTask(schemaName, preparedData.getTableGroupName());
        for (Map.Entry<String, TreeMap<String, List<List<String>>>> entry : tablesTopologyMap.entrySet()) {
            AlterTableGroupAddPartitionSubTaskJobFactory subTaskJobFactory =
                    new AlterTableGroupAddPartitionSubTaskJobFactory(ddl,
                            (AlterTableGroupAddPartitionPreparedData) preparedData,
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
            executableDdlJob.combineTasks(subDdlJob);
            executableDdlJob.addTaskRelationship(tailTask, subDdlJob.getHead());

            executableDdlJob.getExcludeResources().addAll(subDdlJob.getExcludeResources());
            if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
                executableDdlJob.addTaskRelationship(subDdlJob.getTail(),
                        subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(), removeMetaTask);
                if (firstTable) {
                    executableDdlJob.addTaskRelationship(removeMetaTask,
                            bringUpAlterTableGroupTasks.get(0));
                }
                firstTable = false;
            }
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
    }

    private List<DdlTask> generateSyncTask() {
        String schemaName = preparedData.getSchemaName();
        String tableGroupName = preparedData.getTableGroupName();
        Set<String> primaryLogicalTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<String> logicalTableNames = new ArrayList<>();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);
        for (String logicalTable : tableGroupConfig.getAllTables()) {
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTable);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                        tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                logicalTable = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
            if (!primaryLogicalTables.contains(logicalTable)) {
                logicalTableNames.add(logicalTable);
                primaryLogicalTables.add(logicalTable);
            }
        }
        List<DdlTask> tasks = new ArrayList<>(3);
        DdlTask tableGroupSyncTask =
                new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());

        DdlTask updateTablesVersionTask = new UpdateTablesVersionTask(schemaName, logicalTableNames);
        //not use preemptive sync to interrupt dml， just wait for the sync task to finish
        TablesSyncTask tableSyncTask = new TablesSyncTask(schemaName, logicalTableNames, true);
        tasks.add(tableGroupSyncTask);
        tasks.add(updateTablesVersionTask);
        tasks.add(tableSyncTask);
        return tasks;
    }

}

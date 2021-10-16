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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupSplitPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author luoyanxin
 */
public class AlterTableGroupSplitPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableGroupSplitPartitionJobFactory(DDL ddl, AlterTableGroupSplitPartitionPreparedData preparedData,
                                                   Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                   Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                   Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                                   Map<String, Map<String, Set<String>>> targetTablesTopology,
                                                   Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                                   Map<String, List<Pair<String, String>>> orderedTargetTablesLocations,
                                                   ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION, executionContext);
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        AlterTableGroupSplitPartitionPreparedData alterTableGroupSplitPartitionPreparedData =
            (AlterTableGroupSplitPartitionPreparedData) preparedData;
        String schemaName = alterTableGroupSplitPartitionPreparedData.getSchemaName();
        String tableName = alterTableGroupSplitPartitionPreparedData.getTableName();
        String tableGroupName = alterTableGroupSplitPartitionPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, alterTableGroupSplitPartitionPreparedData.getTableGroupName());
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupSplitPartitionPreparedData.getTableGroupName());

        Set<Long> outdatedPartitionGroupId = new HashSet<>();
        for (String splitPartitionName : alterTableGroupSplitPartitionPreparedData.getSplitPartitions()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(splitPartitionName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        int targetDbCnt = alterTableGroupSplitPartitionPreparedData.getTargetGroupDetailInfoExRecords().size();
        List<String> newPartitions = new ArrayList<>();
        for (int i = 0; i < alterTableGroupSplitPartitionPreparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(alterTableGroupSplitPartitionPreparedData.getTargetGroupDetailInfoExRecords()
                .get(i % targetDbCnt).phyDbName);
            newPartitions.add(alterTableGroupSplitPartitionPreparedData.getNewPartitionNames().get(i));
        }
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            alterTableGroupSplitPartitionPreparedData.getSourceSql(),
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

        executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);
        constructSubTasks(schemaName, executableDdlJob, addMetaTask, bringUpAlterTableGroupTasks,
            alterTableGroupSplitPartitionPreparedData.getSplitPartitions().get(0));
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupSplitPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableGroupSplitPartitionBuilder alterTableGroupSplitPartitionBuilder =
            new AlterTableGroupSplitPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableGroupSplitPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableGroupSplitPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableGroupSplitPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableGroupSplitPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupSplitPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, List<Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupSplitPartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableGroupSplitPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String splitPartName : preparedData.getOldPartitionNames()) {
            resources.add(concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()),
                splitPartName));
        }

    }

}
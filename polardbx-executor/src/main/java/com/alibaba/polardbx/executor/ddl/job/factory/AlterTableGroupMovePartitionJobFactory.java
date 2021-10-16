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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupMovePartitionBuilder;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
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
public class AlterTableGroupMovePartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableGroupMovePartitionJobFactory(DDL ddl, AlterTableGroupMovePartitionPreparedData preparedData,
                                                  Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                  Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                  Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                                  Map<String, Map<String, Set<String>>> targetTablesTopology,
                                                  Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                                  Map<String, List<Pair<String, String>>> orderedTargetTablesLocations,
                                                  ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION, executionContext);
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        AlterTableGroupMovePartitionPreparedData alterTableGroupMovePartitionPreparedData =
            (AlterTableGroupMovePartitionPreparedData) preparedData;
        String schemaName = alterTableGroupMovePartitionPreparedData.getSchemaName();
        String tableName = alterTableGroupMovePartitionPreparedData.getTableName();
        String tableGroupName = alterTableGroupMovePartitionPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, alterTableGroupMovePartitionPreparedData.getTableGroupName());
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupMovePartitionPreparedData.getTableGroupName());

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
        int targetDbCnt = alterTableGroupMovePartitionPreparedData.getTargetGroupDetailInfoExRecords().size();
        List<String> newPartitions = new ArrayList<>();
        for (int i = 0; i < alterTableGroupMovePartitionPreparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(alterTableGroupMovePartitionPreparedData.getTargetGroupDetailInfoExRecords()
                .get(i % targetDbCnt).phyDbName);
            newPartitions.add(alterTableGroupMovePartitionPreparedData.getNewPartitionNames().get(i));
        }
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            alterTableGroupMovePartitionPreparedData.getSourceSql(),
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
        //only support move to one partition, not to multiple
        constructSubTasks(schemaName, executableDdlJob, addMetaTask, bringUpAlterTableGroupTasks,
            alterTableGroupMovePartitionPreparedData.getTargetPartitionsLocation().keySet().iterator().next());
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupMovePartitionPreparedData preparedData,
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
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupMovePartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, List<Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupMovePartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableGroupMovePartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String partName : preparedData.getOldPartitionNames()) {
            resources.add(concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()),
                partName));
        }

    }

}
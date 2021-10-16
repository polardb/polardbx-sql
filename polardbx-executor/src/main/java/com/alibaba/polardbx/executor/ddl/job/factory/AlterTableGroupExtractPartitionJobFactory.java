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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupExtractPartitionBuilder;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
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
public class AlterTableGroupExtractPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableGroupExtractPartitionJobFactory(DDL ddl, AlterTableGroupExtractPartitionPreparedData preparedData,
                                                     Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                     Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                     Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                                     Map<String, Map<String, Set<String>>> targetTablesTopology,
                                                     Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                                     Map<String, List<Pair<String, String>>> orderedTargetTablesLocations,
                                                     ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.EXTRACT_PARTITION, executionContext);
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        AlterTableGroupExtractPartitionPreparedData alterTableGroupExtractPartitionPreparedData =
            (AlterTableGroupExtractPartitionPreparedData) preparedData;
        String schemaName = alterTableGroupExtractPartitionPreparedData.getSchemaName();
        String tableGroupName = alterTableGroupExtractPartitionPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName,
                alterTableGroupExtractPartitionPreparedData.getTableGroupName());
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupExtractPartitionPreparedData.getTableGroupName());

        Set<Long> outdatedPartitionGroupId = new HashSet<>();
        for (String splitPartitionName : alterTableGroupExtractPartitionPreparedData.getOldPartitionNames()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(splitPartitionName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        int targetDbCnt = alterTableGroupExtractPartitionPreparedData.getTargetGroupDetailInfoExRecords().size();
        List<String> newPartitions = new ArrayList<>();
        for (int i = 0; i < alterTableGroupExtractPartitionPreparedData.getNewPartitions().size(); i++) {
            targetDbList.add(alterTableGroupExtractPartitionPreparedData.getTargetGroupDetailInfoExRecords()
                .get(i % targetDbCnt).phyDbName);
            newPartitions
                .add(alterTableGroupExtractPartitionPreparedData.getNewPartitions().get(i).getName().toString());
        }
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            alterTableGroupExtractPartitionPreparedData.getSourceSql(),
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
            alterTableGroupExtractPartitionPreparedData.getOldPartitionNames().get(0));
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupExtractPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableGroupExtractPartitionBuilder alterTableGroupExtractPartitionBuilder =
            new AlterTableGroupExtractPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableGroupExtractPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableGroupExtractPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableGroupExtractPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableGroupExtractPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupExtractPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, List<Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupExtractPartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableGroupExtractPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
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
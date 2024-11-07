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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupReorgPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupReorgPartitionPreparedData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableGroupReorgPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableGroupReorgPartitionJobFactory(DDL ddl,
                                                   AlterTableGroupReorgPartitionPreparedData preparedData,
                                                   Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                   Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                   Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                                   Map<String, Map<String, Set<String>>> targetTablesTopology,
                                                   Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                                   Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations,
                                                   ExecutionContext executionContext) {
        super(ddl, preparedData, tablesPrepareData, newPartitionsPhysicalPlansMap, tablesTopologyMap,
            targetTablesTopology, sourceTablesTopology, orderedTargetTablesLocations,
            ComplexTaskMetaManager.ComplexTaskType.REORGANIZE_PARTITION, executionContext);
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        AlterTableGroupReorgPartitionPreparedData reorgPreparedData =
            (AlterTableGroupReorgPartitionPreparedData) preparedData;

        String schemaName = reorgPreparedData.getSchemaName();
        String tableGroupName = reorgPreparedData.getTableGroupName();

        OptimizerContext oc = OptimizerContext.getContext(schemaName);
        TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigByName(tableGroupName);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        Map<String, Long> tablesVersion = getTablesVersion();

        DdlTask validateTask = new AlterTableGroupValidateTask(schemaName, tableGroupName, tablesVersion, true,
            reorgPreparedData.getTargetPhysicalGroups(), false);

        Set<Long> outdatedPartitionGroupId = new HashSet<>();
        List<String> outdatedPartitionGroupLocalities = new ArrayList<>();

        for (String oldPartGroupName : reorgPreparedData.getOldPartGroupNames()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(oldPartGroupName)) {
                    outdatedPartitionGroupId.add(record.id);
                    outdatedPartitionGroupLocalities.add(record.locality);
                    break;
                }
            }
        }

        int targetDbCnt = reorgPreparedData.getTargetGroupDetailInfoExRecords().size();

        List<String> localities = new ArrayList<>();
        List<String> targetDbList = new ArrayList<>();
        List<String> newPartitions = new ArrayList<>();

        for (int i = 0; i < reorgPreparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(reorgPreparedData.getTargetGroupDetailInfoExRecords().get(i % targetDbCnt).phyDbName);
            newPartitions.add(reorgPreparedData.getNewPartitionNames().get(i));

            int indexLocality =
                i < outdatedPartitionGroupLocalities.size() ? i : outdatedPartitionGroupLocalities.size() - 1;

            String partitionLocality =
                StringUtils.isEmpty(outdatedPartitionGroupLocalities.get(indexLocality)) ? StringUtils.EMPTY :
                    outdatedPartitionGroupLocalities.get(indexLocality);

            Boolean isIdentical = outdatedPartitionGroupLocalities.stream().allMatch(o -> o.equals(partitionLocality));
            LocalityDesc targetLocality = isIdentical ? LocalityDesc.parse(partitionLocality) : new LocalityDesc();
            localities.add(targetLocality.toString());
        }

        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            reorgPreparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions,
            localities
        );

        DdlTask syncTableGroupTask = new TableGroupSyncTask(schemaName, tableGroupName);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            addMetaTask
        ));

        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, tableGroupName, null, taskType,
                preparedData.getDdlVersionId(), executionContext);

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
                reorgPreparedData.getNewPartitionNames().get(0));
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, ImmutableList.of(pauseCurrentJobTask),
                reorgPreparedData.getNewPartitionNames().get(0));
        }

        executableDdlJob.addSequentialTasksAfter(
            executableDdlJob.getTail(),
            Lists.newArrayList(
                syncTableGroupTask
            ));

        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        attacheCdcFinalMarkTask(executableDdlJob);
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupReorgPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableGroupReorgPartitionBuilder alterTableGroupReorgPartitionBuilder =
            new AlterTableGroupReorgPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableGroupReorgPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableGroupReorgPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableGroupReorgPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableGroupReorgPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupReorgPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupReorgPartitionBuilder.getOrderedTargetTablesLocations();

        return new AlterTableGroupReorgPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
    }

}

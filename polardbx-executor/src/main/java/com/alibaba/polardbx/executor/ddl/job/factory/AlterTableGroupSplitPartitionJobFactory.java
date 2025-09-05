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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupSplitPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
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
public class AlterTableGroupSplitPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    public AlterTableGroupSplitPartitionJobFactory(DDL ddl, AlterTableGroupSplitPartitionPreparedData preparedData,
                                                   Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                   Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                   Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap,
                                                   Map<String, Map<String, Set<String>>> targetTablesTopology,
                                                   Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                                   Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations,
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
        Map<String, Long> tablesVersion = getTablesVersion();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, alterTableGroupSplitPartitionPreparedData.getTableGroupName(),
                tablesVersion, true, alterTableGroupSplitPartitionPreparedData.getTargetPhysicalGroups(), false);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupSplitPartitionPreparedData.getTableGroupName());
        String firstTbInTg = tableGroupConfig.getAllTables().get(0);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(firstTbInTg);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        Set<Long> outdatedPartitionGroupId =
            getOldDatePartitionGroups(preparedData,
                ((AlterTableGroupSplitPartitionPreparedData) preparedData).getSplitPartitions(),
                ((AlterTableGroupSplitPartitionPreparedData) preparedData).isSplitSubPartition());
        String locality = "";
        for (String splitPartitionName : alterTableGroupSplitPartitionPreparedData.getSplitPartitions()) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(splitPartitionName)) {
                    locality = (record.locality == null) ? "" : record.locality;
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        List<String> newPartitions = getNewPartitions(partitionInfo);

        Map<String, String> partAndDbMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (PartitionGroupRecord partitionGroupRecord : preparedData.getInvisiblePartitionGroups()) {
            partAndDbMap.put(partitionGroupRecord.partition_name, partitionGroupRecord.phy_db);
        }
        List<String> localities = new ArrayList<>();
        for (int i = 0; i < newPartitions.size(); i++) {
            targetDbList.add(partAndDbMap.get(newPartitions.get(i)));
            localities.add(alterTableGroupSplitPartitionPreparedData.getInvisiblePartitionGroups().get(i)
                .getLocality());
        }

        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            alterTableGroupSplitPartitionPreparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions,
            localities);

        DdlTask syncTableGroupTask = new TableGroupSyncTask(schemaName, tableGroupName);

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
                alterTableGroupSplitPartitionPreparedData.getSplitPartitions().get(0));
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, ImmutableList.of(pauseCurrentJobTask),
                alterTableGroupSplitPartitionPreparedData.getSplitPartitions().get(0));
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

    private List<String> getNewPartitions(PartitionInfo partitionInfo) {
        AlterTableGroupSplitPartitionPreparedData splitData = (AlterTableGroupSplitPartitionPreparedData) preparedData;

        List<String> newPartitions = preparedData.getNewPartitionNames();
        PartitionByDefinition subPartBy = partitionInfo.getPartitionBy().getSubPartitionBy();
        boolean splitTemplateSubPartition =
            splitData.isSplitSubPartition() && (subPartBy != null && subPartBy.isUseSubPartTemplate());
        if (splitTemplateSubPartition) {
            newPartitions = new ArrayList<>();
            for (String newPartName : preparedData.getNewPartitionNames()) {
                for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                    newPartitions.add(
                        PartitionNameUtil.autoBuildSubPartitionName(partitionSpec.getName(), newPartName));
                }
            }
        }
        return newPartitions;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupSplitPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableGroupSplitPartitionBuilder alterTableGroupSplitPartitionBuilder =
            new AlterTableGroupSplitPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap =
            alterTableGroupSplitPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableGroupSplitPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableGroupSplitPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableGroupSplitPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupSplitPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, Map<String, Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupSplitPartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableGroupSplitPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
    }

}

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
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupModifyPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PauseCurrentJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupRemoveTempPartitionTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author luoyanxin
 */
public class AlterTableGroupModifyPartitionJobFactory extends AlterTableGroupBaseJobFactory {

    private static String MODIFY_PARTITION_LOCK = "MODIFY_PARTITION_LOCK";

    public AlterTableGroupModifyPartitionJobFactory(DDL ddl, AlterTableGroupModifyPartitionPreparedData preparedData,
                                                    Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                                    Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                                    Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                                    Map<String, Map<String, Set<String>>> targetTablesTopology,
                                                    Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                                    Map<String, List<Pair<String, String>>> orderedTargetTablesLocations,
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
        AlterTableGroupModifyPartitionPreparedData alterTableGroupModifyPartitionPreparedData =
            (AlterTableGroupModifyPartitionPreparedData) preparedData;
        String schemaName = alterTableGroupModifyPartitionPreparedData.getSchemaName();
        String tableGroupName = alterTableGroupModifyPartitionPreparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tablesVersion = getTablesVersion();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, alterTableGroupModifyPartitionPreparedData.getTableGroupName(),
                tablesVersion, true, alterTableGroupModifyPartitionPreparedData.getTargetPhysicalGroups());
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(alterTableGroupModifyPartitionPreparedData.getTableGroupName());

        Set<Long> outdatedPartitionGroupId = new HashSet<>();
        List<String> outdatedPartitionNames = new ArrayList();
        //the newPartitionNames must include the oldPartitionNames
        outdatedPartitionNames.addAll(preparedData.getNewPartitionNames());
        for (String oldPartitionName : outdatedPartitionNames) {
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(oldPartitionName)) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        int targetDbCnt = alterTableGroupModifyPartitionPreparedData.getTargetGroupDetailInfoExRecords().size();
        List<String> newPartitions = new ArrayList<>();
        for (int i = 0; i < alterTableGroupModifyPartitionPreparedData.getNewPartitionNames().size(); i++) {
            targetDbList.add(alterTableGroupModifyPartitionPreparedData.getTargetGroupDetailInfoExRecords()
                .get(i % targetDbCnt).phyDbName);
            newPartitions.add(alterTableGroupModifyPartitionPreparedData.getNewPartitionNames().get(i));
        }
        List<LocalityDesc> oldPartitionLocalities = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> outdatedPartitionNames.contains(o.partition_name)).
            map(o -> LocalityDesc.parse(o.getLocality())).collect(Collectors.toList());
        List<String> localties = Collections.nCopies(newPartitions.size(), oldPartitionLocalities.get(0).toString());
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            tableGroupName,
            tableGroupConfig.getTableGroupRecord().getId(),
            alterTableGroupModifyPartitionPreparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            taskType.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions,
            localties);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            addMetaTask
        ));

        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, tableGroupName, null,
                taskType, executionContext);

        if (((AlterTableGroupModifyPartitionPreparedData) preparedData).isDropVal()) {
            AlterTableGroupRemoveTempPartitionTask alterTableGroupRemoveTempPartitionTask =
                new AlterTableGroupRemoveTempPartitionTask(schemaName,
                    ((AlterTableGroupModifyPartitionPreparedData) preparedData).getTempPartition(),
                    tableGroupConfig.getTableGroupRecord().getId());
            bringUpAlterTableGroupTasks.add(0, alterTableGroupRemoveTempPartitionTask);
        }

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
                null);
        } else {
            PauseCurrentJobTask pauseCurrentJobTask = new PauseCurrentJobTask(schemaName);
            constructSubTasks(schemaName, executableDdlJob, addMetaTask, ImmutableList.of(pauseCurrentJobTask),
                null);
        }

        // TODO(luoyanxin)
        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupModifyPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        AlterTableGroupModifyPartitionBuilder alterTableGroupModifyPartitionBuilder =
            new AlterTableGroupModifyPartitionBuilder(ddl, preparedData, executionContext);
        Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
            alterTableGroupModifyPartitionBuilder.build().getTablesTopologyMap();
        Map<String, Map<String, Set<String>>> targetTablesTopology =
            alterTableGroupModifyPartitionBuilder.getTargetTablesTopology();
        Map<String, Map<String, Set<String>>> sourceTablesTopology =
            alterTableGroupModifyPartitionBuilder.getSourceTablesTopology();
        Map<String, AlterTableGroupItemPreparedData> tableGroupItemPreparedDataMap =
            alterTableGroupModifyPartitionBuilder.getTablesPreparedData();
        Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
            alterTableGroupModifyPartitionBuilder.getNewPartitionsPhysicalPlansMap();
        Map<String, List<Pair<String, String>>> orderedTargetTablesLocations =
            alterTableGroupModifyPartitionBuilder.getOrderedTargetTablesLocations();
        return new AlterTableGroupModifyPartitionJobFactory(ddl, preparedData, tableGroupItemPreparedDataMap,
            newPartitionsPhysicalPlansMap, tablesTopologyMap, targetTablesTopology, sourceTablesTopology,
            orderedTargetTablesLocations, executionContext).create();
    }

    @Override
    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        EmptyTask emptyTask = new EmptyTask(schemaName);
        boolean emptyTaskAdded = false;

        for (Map.Entry<String, Map<String, List<List<String>>>> entry : tablesTopologyMap.entrySet()) {
            AlterTableGroupModifyPartitionSubTaskJobFactory subTaskJobFactory =
                new AlterTableGroupModifyPartitionSubTaskJobFactory(ddl,
                    (AlterTableGroupModifyPartitionPreparedData) preparedData, tablesPrepareData.get(entry.getKey()),
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
                tryRewriteCdcTopology(subTaskJobFactory);
                executableDdlJob.addTask(subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTask.getTail(), emptyTask);
                executableDdlJob.addTaskRelationship(emptyTask, subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(), bringUpAlterTableGroupTasks.get(0));
            } else {
                executableDdlJob.addTaskRelationship(subTask.getTail(), bringUpAlterTableGroupTasks.get(0));
            }
            //todo delete physcial tables of temp_partition
            DdlTask dropUselessTableTask = ComplexTaskFactory
                .CreateDropUselessPhyTableTask(schemaName, entry.getKey(),
                    getTheDeletedPartitionsLocation(preparedData.getSchemaName(), entry.getKey(),
                        subTaskJobFactory.getTempPartitionInfo()), executionContext);
            executableDdlJob.addTask(dropUselessTableTask);
            executableDdlJob
                .addTaskRelationship(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1),
                    dropUselessTableTask);
            executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
        }
    }

    public Map<String, Set<String>> getTheDeletedPartitionsLocation(String schemaName, String tableName,
                                                                    PartitionSpec tempPartitionSpec) {
        Map<String, Set<String>> deletedPhyTables = new HashMap<>();
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                .getPartitionInfo(tableName);
        int num = 0;
        List<String> outdatedPartitionNames = new ArrayList();
        outdatedPartitionNames.addAll(preparedData.getNewPartitionNames());
        for (String oldPartitionName : outdatedPartitionNames) {
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                if (partitionSpec.getName().equalsIgnoreCase(oldPartitionName)) {
                    PartitionLocation location = partitionSpec.getLocation();
                    deletedPhyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>())
                        .add(location.getPhyTableName());
                    num++;
                    break;
                }
            }
        }
        assert num == preparedData.getNewPartitionNames().size();

        if (((AlterTableGroupModifyPartitionPreparedData) preparedData).isDropVal() && tempPartitionSpec != null) {
            PartitionLocation location = tempPartitionSpec.getLocation();
            deletedPhyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>())
                .add(location.getPhyTableName());
        }
        return deletedPhyTables;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);
        //because AlterTableGroupRemoveTempPartitionTask will call deleteTablePartitionByGidAndPartNameFromDelta
        //while group_id is always 0 for new partition in delta, so modify partition sure be executed serially
        resources.add(concatWithDot(preparedData.getSchemaName(), MODIFY_PARTITION_LOCK));
    }

    private void tryRewriteCdcTopology(AlterTableGroupModifyPartitionSubTaskJobFactory subTaskJobFactory) {
        if (preparedData.isDropVal()) {
            CdcTableGroupDdlMarkTask cdcTableGroupDdlMarkTask =
                (CdcTableGroupDdlMarkTask) subTaskJobFactory.getCdcTableGroupDdlMarkTask();
            PartitionSpec tempPartitionInfo = subTaskJobFactory.getTempPartitionInfo();
            Map<String, Set<String>> topology = cdcTableGroupDdlMarkTask.getTargetTableTopology();
            topology.forEach((k, v) -> v.remove(tempPartitionInfo.getLocation().getPhyTableName()));
        }
    }

}

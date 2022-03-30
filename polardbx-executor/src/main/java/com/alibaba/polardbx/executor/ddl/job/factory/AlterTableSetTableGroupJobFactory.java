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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateTablesVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableSetTableGroupChangeMetaOnlyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.ReloadTableMetaAfterChangeTableGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * @author luoyanxin
 */
public class AlterTableSetTableGroupJobFactory extends DdlJobFactory {

    @Deprecated
    private final DDL ddl;
    private final AlterTableSetTableGroupPreparedData preparedData;
    private final PhysicalPlanData physicalPlanData;
    private final Map<String, Set<String>> sourceTableTopology;
    private final Map<String, Set<String>> targetTableTopology;
    private final List<PartitionGroupRecord> newPartitionRecords;
    protected final ExecutionContext executionContext;

    public AlterTableSetTableGroupJobFactory(DDL ddl, AlterTableSetTableGroupPreparedData preparedData,
                                             PhysicalPlanData physicalPlanData,
                                             Map<String, Set<String>> sourceTableTopology,
                                             Map<String, Set<String>> targetTableTopology,
                                             List<PartitionGroupRecord> newPartitionRecords,
                                             ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.physicalPlanData = physicalPlanData;
        this.sourceTableTopology = sourceTableTopology;
        this.targetTableTopology = targetTableTopology;
        this.newPartitionRecords = newPartitionRecords;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        String schemaName = preparedData.getSchemaName();
        String targetTableGroupName = preparedData.getTableGroupName();
        String tableName = preparedData.getTableName();

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(tableName);
        TableGroupConfig curTableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
        boolean[] flag = {false, false};//0:changeSchemaOnly? 1:do nothing?
        changeMetaInfoCheck(flag);
        if (flag[1]) {
            return new TransientDdlJob();
        } else if (flag[0]) {
            AlterTableSetTableGroupChangeMetaOnlyTask tableSetTableGroupChangeMetaOnlyTask =
                new AlterTableSetTableGroupChangeMetaOnlyTask(preparedData.getSchemaName(), preparedData.getTableName(),
                    curTableGroupConfig.getTableGroupRecord().getTg_name(), preparedData.getTableGroupName(), false,
                    false);

            ReloadTableMetaAfterChangeTableGroupTask reloadTargetTableGroup =
                new ReloadTableMetaAfterChangeTableGroupTask(schemaName, preparedData.getTableGroupName());

            BaseDdlTask syncSourceTableGroup =
                new TableGroupSyncTask(schemaName, curTableGroupConfig.getTableGroupRecord().getTg_name());

            DdlTask updateTablesVersionTask = new UpdateTablesVersionTask(schemaName, ImmutableList.of(tableName));

            Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
            Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
            // make sure the tablegroup is reload before table, we can't update table version inside TablesSyncTask
            DdlTask syncTable =
                new TableSyncTask(schemaName, tableName, true, initWait, interval, TimeUnit.MILLISECONDS);

            executableDdlJob.addSequentialTasks(Lists.newArrayList(
                tableSetTableGroupChangeMetaOnlyTask,
                reloadTargetTableGroup,
                syncSourceTableGroup,
                updateTablesVersionTask,
                syncTable
            ));
            return executableDdlJob;
        }

        Map<String, Long> tableVersions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, targetTableGroupName, tableVersions, false, null);

        Set<Long> outdatedPartitionGroupId = new HashSet<>();
        // the old and new partition name is identical, so here use newPartitionRecords
        for (PartitionGroupRecord newRecord : newPartitionRecords) {
            for (PartitionGroupRecord record : curTableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(newRecord.getPartition_name())) {
                    outdatedPartitionGroupId.add(record.id);
                    break;
                }
            }
        }
        List<String> targetDbList = new ArrayList<>();
        List<String> newPartitions = new ArrayList<>();
        for (PartitionGroupRecord newRecord : newPartitionRecords) {
            targetDbList.add(newRecord.getPhy_db());
            newPartitions.add(newRecord.getPartition_name());
        }
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName,
            targetTableGroupName,
            curTableGroupConfig.getTableGroupRecord().getId(),
            preparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP.getValue(),
            outdatedPartitionGroupId,
            targetDbList,
            newPartitions);

        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            addMetaTask
        ));
        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, targetTableGroupName, tableName,
                ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP, executionContext);

        executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);

        constructSubTasks(schemaName, partitionInfo, executableDdlJob, addMetaTask, bringUpAlterTableGroupTasks);

        // TODO(luoyanxin)
        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getOriginalTableGroup()));
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableName()));

    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    protected void constructSubTasks(String schemaName, PartitionInfo curPartitionInfo,
                                     ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                     List<DdlTask> bringUpAlterTableGroupTasks) {
        String tableName = preparedData.getTableName();

        PartitionInfo newPartitionInfo = generateNewPartitionInfo();
        TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
        logTableRec.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC;
        List<TablePartitionRecord> partRecList =
            PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
            .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                newPartitionInfo.getPartitionBy().getPartitions());

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig curTableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        //DdlTask validateTask = new AlterTableGroupValidateTask(schemaName, preparedData.getTableGroupName());
        DdlTask addMetaTask =
            new AlterTableGroupAddSubTaskMetaTask(schemaName, tableName,
                curTableGroupConfig.getTableGroupRecord().tg_name, curPartitionInfo.getTableGroupId(), "",
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING.getValue(), 0, logTableRec, partRecList,
                subPartRecInfos);

        List<DdlTask> taskList = new ArrayList<>();
        //1. validate
        //taskList.add(validateTask);

        //2. create physical table
        //2.1 insert meta to complex_task_outline
        taskList.add(addMetaTask);
        //2.2 create partitioned physical table
        DdlTask phyDdlTask =
            new CreateTablePhyDdlTask(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData);
        taskList.add(phyDdlTask);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        final boolean stayAtCreating =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.CREATING.name(), finalStatus);
        final boolean stayAtDeleteOnly =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY.name(), finalStatus);
        final boolean stayAtWriteOnly =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY.name(), finalStatus);
        final boolean stayAtWriteReOrg =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG.name(), finalStatus);

        List<DdlTask> bringUpNewPartitions = ComplexTaskFactory
            .addPartitionTasks(schemaName, tableName, sourceTableTopology, targetTableTopology, stayAtCreating,
                stayAtDeleteOnly, stayAtWriteOnly, stayAtWriteReOrg, false,
                executionContext);
        //3.2 status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> READY_TO_PUBLIC
        taskList.addAll(bringUpNewPartitions);

        final ExecutableDdlJob subTask = new ExecutableDdlJob();
        subTask.addSequentialTasks(taskList);
        subTask.labelAsHead(addMetaTask);

        if (!stayAtCreating) {
            executableDdlJob.labelAsTail(bringUpNewPartitions.get(bringUpNewPartitions.size() - 1));
        } else {
            executableDdlJob.labelAsTail(phyDdlTask);
        }

        executableDdlJob.combineTasks(subTask);
        executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());
        executableDdlJob.addTaskRelationship(executableDdlJob.getTail(), bringUpAlterTableGroupTasks.get(0));
        DdlTask dropUselessTableTask = ComplexTaskFactory
            .CreateDropUselessPhyTableTask(schemaName, tableName, sourceTableTopology, executionContext);
        executableDdlJob.addTask(dropUselessTableTask);
        executableDdlJob.addTaskRelationship(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1),
            dropUselessTableTask);
        executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
    }

    private PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        SqlNode sqlAlterTableSetTableGroup = ddl.getSqlNode();

        return AlterTableGroupSnapShotUtils
            .getNewPartitionInfoForSetTableGroup(curPartitionInfo, sqlAlterTableSetTableGroup);
    }

    private void changeMetaInfoCheck(boolean[] flag) {
        String schemaName = preparedData.getSchemaName();
        String logicTableName = preparedData.getTableName();
        String targetTableGroup = preparedData.getTableGroupName();
        final SchemaManager schemaManager = executionContext.getSchemaManager();
        PartitionInfo sourcePartitionInfo = schemaManager.getTable(logicTableName).getPartitionInfo();
        if (StringUtils.isEmpty(targetTableGroup)) {
            flag[0] = true;
            flag[1] = false;
            return;
        }
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig targetTableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(targetTableGroup);
        TableGroupConfig sourceTableGroupInfo =
            tableGroupInfoManager.getTableGroupConfigById(sourcePartitionInfo.getTableGroupId());
        if (sourceTableGroupInfo.getTableGroupRecord().tg_name.equalsIgnoreCase(targetTableGroup)) {
            // do nothing;
            flag[0] = false;
            flag[1] = true;
        } else if (GeneralUtil.isEmpty(targetTableGroupConfig.getAllTables())) {
            flag[0] = true;
            flag[1] = false;
        } else {
            TablePartRecordInfoContext tablePartRecordInfoContext =
                targetTableGroupConfig.getAllTables().get(0);
            String tableInTbGrp = tablePartRecordInfoContext.getLogTbRec().tableName;
            PartitionInfo targetPartitionInfo = schemaManager.getTable(tableInTbGrp).getPartitionInfo();

            PartitionStrategy strategy = sourcePartitionInfo.getPartitionBy().getStrategy();
            boolean isVectorStrategy =
                (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.RANGE_COLUMNS);
            boolean match = false;
            if (isVectorStrategy) {
                List<String> actualPartCols = PartitionInfoUtil.getActualPartitionColumns(sourcePartitionInfo);
                if (PartitionInfoUtil
                    .partitionEquals(sourcePartitionInfo, targetPartitionInfo, actualPartCols.size())) {
                    match = true;
                }
            } else if (sourcePartitionInfo.equals(targetPartitionInfo)) {
                match = true;
            }

            if (!match) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "the partition policy of tablegroup:" + targetTableGroup + " is not match to table: "
                        + logicTableName);
            }
            for (PartitionSpec partitionSpec : sourcePartitionInfo.getPartitionBy().getPartitions()) {
                PartitionGroupRecord partitionGroupRecord = targetTableGroupConfig.getPartitionGroupRecords().stream()
                    .filter(o -> o.getPartition_name().equalsIgnoreCase(partitionSpec.getName())).findFirst()
                    .orElse(null);
                assert partitionGroupRecord != null;

                if (!partitionSpec.getLocation().getGroupKey()
                    .equalsIgnoreCase(GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.getPhy_db()))) {
                    flag[0] = false;
                    flag[1] = false;
                    return;
                }
            }
            flag[0] = true;
            flag[1] = false;
        }

    }

}

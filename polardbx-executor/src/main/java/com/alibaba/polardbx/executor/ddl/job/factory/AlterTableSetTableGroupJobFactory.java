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

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateTablesVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableSetTableGroupMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableSetGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableSetTableGroupChangeMetaOnlyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.CleanupEmptyTableGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.JoinGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.ReloadTableMetaAfterChangeTableGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlKind;
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

        if (preparedData.isAlignPartitionNameFirst()) {
            return alignPartitionNameAndSetTableGroup();
        }

        if (preparedData.isRepartition()) {
            return repartitionWithTableGroup();
        }

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(tableName);
        TableGroupConfig curTableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
        boolean[] flag = {false, false};//0:changeSchemaOnly? 1:do nothing?
        changeMetaInfoCheck(flag);

        JoinGroupValidateTask joinGroupValidateTask =
            new JoinGroupValidateTask(schemaName, ImmutableList.of(targetTableGroupName), tableName, false);

        Map<String, Long> tableVersions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());

        if (flag[1]) {
            return new TransientDdlJob();
        } else if (flag[0]) {

            DdlTask validateTask = null;

            if (StringUtils.isNotEmpty(preparedData.getTableGroupName()) && !preparedData.isImplicit()) {
                validateTask =
                    new AlterTableGroupValidateTask(schemaName, preparedData.getTableGroupName(), tableVersions, false,
                        null, true);
            } else {
                validateTask =
                    new ValidateTableVersionTask(schemaName, tableVersions);
            }

            AlterTableSetTableGroupChangeMetaOnlyTask tableSetTableGroupChangeMetaOnlyTask =
                new AlterTableSetTableGroupChangeMetaOnlyTask(preparedData.getSchemaName(), preparedData.getTableName(),
                    curTableGroupConfig.getTableGroupRecord().getTg_name(), preparedData.getTableGroupName(),
                    false,
                    false,
                    preparedData.getOriginalJoinGroup(),
                    preparedData.isImplicit());

            CleanupEmptyTableGroupTask cleanupEmptyTableGroupTask =
                new CleanupEmptyTableGroupTask(schemaName, curTableGroupConfig.getTableGroupRecord().getTg_name());

            ReloadTableMetaAfterChangeTableGroupTask reloadTargetTableGroup =
                new ReloadTableMetaAfterChangeTableGroupTask(schemaName, preparedData.getTableGroupName());

            BaseDdlTask syncSourceTableGroup =
                new TableGroupSyncTask(schemaName, curTableGroupConfig.getTableGroupRecord().getTg_name());

            DdlTask updateTablesVersionTask =
                new UpdateTablesVersionTask(schemaName, ImmutableList.of(preparedData.getPrimaryTableName()));

            boolean enablePreemptiveMdl =
                executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PREEMPTIVE_MDL);
            Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
            Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
            // make sure the tablegroup is reload before table, we can't update table version inside TablesSyncTask
            DdlTask syncTable =
                new TableSyncTask(schemaName, preparedData.getPrimaryTableName(), enablePreemptiveMdl, initWait,
                    interval, TimeUnit.MILLISECONDS);

            if (!executionContext.getDdlContext().isSubJob()) {
                boolean isGsi = isGsi(schemaName, preparedData.getPrimaryTableName(), preparedData.getTableName());
                CdcAlterTableSetTableGroupMarkTask cdcAlterTableSetTableGroupMarkTask =
                    new CdcAlterTableSetTableGroupMarkTask(schemaName, preparedData.getPrimaryTableName(),
                        preparedData.getTableName(), isGsi);
                executableDdlJob.addSequentialTasks(Lists.newArrayList(
                    validateTask,
                    tableSetTableGroupChangeMetaOnlyTask,
                    cleanupEmptyTableGroupTask,
                    reloadTargetTableGroup,
                    syncSourceTableGroup,
                    updateTablesVersionTask,
                    cdcAlterTableSetTableGroupMarkTask,
                    syncTable
                ));
            } else {
                executableDdlJob.addSequentialTasks(Lists.newArrayList(
                    validateTask,
                    tableSetTableGroupChangeMetaOnlyTask,
                    cleanupEmptyTableGroupTask,
                    reloadTargetTableGroup,
                    syncSourceTableGroup,
                    updateTablesVersionTask,
                    syncTable
                ));
            }

            return executableDdlJob;
        }

        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, targetTableGroupName, tableVersions, false, null, false);

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
        DdlTask addMetaTask = new AlterTableGroupAddMetaTask(schemaName, targetTableGroupName,
            curTableGroupConfig.getTableGroupRecord().getId(), preparedData.getSourceSql(),
            ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG.getValue(),
            ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP.getValue(), outdatedPartitionGroupId, targetDbList,
            newPartitions);

        boolean skipValidator =
            executionContext.getParamManager().getBoolean(ConnectionParams.SKIP_TABLEGROUP_VALIDATOR);
        if (skipValidator) {
            executableDdlJob.addSequentialTasks(Lists.newArrayList(joinGroupValidateTask, addMetaTask));
        } else {
            executableDdlJob.addSequentialTasks(Lists.newArrayList(validateTask, joinGroupValidateTask, addMetaTask));
        }
        List<DdlTask> bringUpAlterTableGroupTasks =
            ComplexTaskFactory.bringUpAlterTableGroup(schemaName, targetTableGroupName, tableName,
                ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP, executionContext);

        executableDdlJob.addSequentialTasks(bringUpAlterTableGroupTasks);

        constructSubTasks(schemaName, partitionInfo, executableDdlJob, addMetaTask, bringUpAlterTableGroupTasks);

        // TODO(luoyanxin)
        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        return executableDdlJob;
    }

    private ExecutableDdlJob repartitionWithTableGroup() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tableVersions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());
        String targetTableGroupName = preparedData.getTableGroupName();
        ParamManager.setBooleanVal(executionContext.getParamManager().getProps(), ConnectionParams.DDL_ON_GSI, true,
            false);
        String schemaName = preparedData.getSchemaName();
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, targetTableGroupName, tableVersions, false, null, false);

        // this sql of sub-job will be routed to LogicalAlterTableRepartitionHandler
        // and the changed topology meta info will be notified to cdc by LogicalAlterTableRepartitionHandler
        SubJobTask reparitionSubJob = new SubJobTask(schemaName,
            String.format("alter table %s partition align to %s", preparedData.getTableName(), targetTableGroupName),
            null);
        reparitionSubJob.setParentAcquireResource(true);
        executableDdlJob.addSequentialTasks(Lists.newArrayList(validateTask, reparitionSubJob));

        if (!executionContext.getDdlContext().isSubJob()) {
            boolean isGsi = isGsi(schemaName, preparedData.getPrimaryTableName(), preparedData.getTableName());

            CdcAlterTableSetTableGroupMarkTask cdcAlterTableSetTableGroupMarkTask =
                new CdcAlterTableSetTableGroupMarkTask(schemaName, preparedData.getPrimaryTableName(),
                    preparedData.getTableName(), isGsi);
            executableDdlJob.appendTask(cdcAlterTableSetTableGroupMarkTask);
        }

        return executableDdlJob;
    }

    private boolean isGsi(String schemaName, String primaryTableName, String tableName) {
        return !StringUtils.equalsIgnoreCase(preparedData.getPrimaryTableName(), preparedData.getTableName())
            && CBOUtil.isGsi(schemaName, tableName);
    }

    private ExecutableDdlJob alignPartitionNameAndSetTableGroup() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        Map<String, Long> tableVersions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());
        String targetTableGroupName = preparedData.getTableGroupName();

        String schemaName = preparedData.getSchemaName();
        DdlTask validateTask =
            new AlterTableGroupValidateTask(schemaName, targetTableGroupName, tableVersions, false, null, false);

        if (GeneralUtil.isEmpty(preparedData.getPartitionNamesMap())) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, "unexpect error");
        }

        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> entry : preparedData.getPartitionNamesMap().entrySet()) {
            if (i > 0) {
                sb.append(",`");
            } else {
                sb.append("`");
            }
            sb.append(entry.getKey());
            sb.append("` to `");
            sb.append(entry.getValue());
            sb.append("`");
            i++;
        }

        // this sql of sub-job will be routed to LogicalAlterTableRenamePartitionHandler
        // and will not trigger cdc ddl mark
        SubJobTask subJobAlignPartitionName = new SubJobTask(schemaName,
            String.format("alter table %s rename partition %s", preparedData.getTableName(), sb), null);

        // this sql of sub-job will be routed to LogicalAlterTableSetTableGroupHandler
        // and the changed topology meta info will be notified to cdc by LogicalAlterTableSetTableGroupHandler
        SubJobTask subJobSetTableGroup = new SubJobTask(schemaName, preparedData.getSourceSql(), null);

        subJobAlignPartitionName.setParentAcquireResource(true);
        subJobSetTableGroup.setParentAcquireResource(true);
        executableDdlJob.addSequentialTasks(
            Lists.newArrayList(validateTask, subJobAlignPartitionName, subJobSetTableGroup));

        if (!executionContext.getDdlContext().isSubJob()) {
            boolean isGsi = isGsi(schemaName, preparedData.getPrimaryTableName(), preparedData.getTableName());

            CdcAlterTableSetTableGroupMarkTask cdcAlterTableSetTableGroupMarkTask =
                new CdcAlterTableSetTableGroupMarkTask(schemaName, preparedData.getPrimaryTableName(),
                    preparedData.getTableName(), isGsi);
            executableDdlJob.appendTask(cdcAlterTableSetTableGroupMarkTask);
        }

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getOriginalTableGroup()));
        if (!StringUtils.isEmpty(preparedData.getTableGroupName())) {
            resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
        }
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getPrimaryTableName()));
        if (StringUtils.isNotEmpty(preparedData.getOriginalJoinGroup())) {
            resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getOriginalJoinGroup()));
        }

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
        List<TablePartitionRecord> partRecList = PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos =
            PartitionInfoUtil.prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                newPartitionInfo.getPartitionBy().getPartitions());

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig curTableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        //DdlTask validateTask = new AlterTableGroupValidateTask(schemaName, preparedData.getTableGroupName());
        DdlTask addMetaTask = new AlterTableSetGroupAddSubTaskMetaTask(schemaName, tableName,
            curTableGroupConfig.getTableGroupRecord().tg_name, curPartitionInfo.getTableGroupId(), "",
            ComplexTaskMetaManager.ComplexTaskStatus.CREATING.getValue(), 0, logTableRec, partRecList, subPartRecInfos,
            preparedData.getTableGroupName(), preparedData.getOriginalJoinGroup());

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

        List<DdlTask> bringUpNewPartitions =
            ComplexTaskFactory.addPartitionTasks(schemaName, tableName, sourceTableTopology, targetTableTopology,
                stayAtCreating, stayAtDeleteOnly, stayAtWriteOnly, stayAtWriteReOrg, false, executionContext, false,
                ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP);
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

        SqlKind sqlKind = ddl.kind();
        DdlContext dc = executionContext.getDdlContext();

        Map<String, Set<String>> newTopology = newPartitionInfo.getTopology();

        CdcDdlMarkVisibility cdcDdlMarkVisibility =
            (executionContext.getDdlContext().isSubJob()) ? CdcDdlMarkVisibility.Private :
                CdcDdlMarkVisibility.Protected;
        CdcTableGroupDdlMarkTask cdcTableGroupDdlMarkTask =
            new CdcTableGroupDdlMarkTask(preparedData.getTableGroupName(), schemaName, tableName, sqlKind, newTopology,
                dc.getDdlStmt(), cdcDdlMarkVisibility);

        executableDdlJob.addTask(cdcTableGroupDdlMarkTask);
        executableDdlJob.addTaskRelationship(taskList.get(taskList.size() - 1), cdcTableGroupDdlMarkTask);
        executableDdlJob.addTaskRelationship(cdcTableGroupDdlMarkTask, bringUpAlterTableGroupTasks.get(0));

        DdlTask dropUselessTableTask =
            ComplexTaskFactory.CreateDropUselessPhyTableTask(schemaName, tableName, sourceTableTopology,
                executionContext);
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

        return AlterTableGroupSnapShotUtils.getNewPartitionInfo(null, curPartitionInfo, false, ddl.getSqlNode(), null,
            null, null, null, null, null, executionContext);
    }

    private void changeMetaInfoCheck(boolean[] flag) {
        String schemaName = preparedData.getSchemaName();
        String logicTableName = preparedData.getTableName();
        String targetTableGroup = preparedData.getTableGroupName();
        final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        PartitionInfo sourcePartitionInfo = schemaManager.getTable(logicTableName).getPartitionInfo();

        if (StringUtils.isEmpty(targetTableGroup)) {
//            LocalityDesc localityDesc = LocalityDesc.parse(sourcePartitionInfo.getLocality());
////            if(localityDesc.getBalanceSingleTable()){
////                flag[0] = true;
////                flag[1] = false;
////            }else {
            flag[0] = true;
            flag[1] = false;
//            }
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
        } else if ((targetTableGroupConfig != null && GeneralUtil.isEmpty(targetTableGroupConfig.getAllTables()))
            || (preparedData.isImplicit() && targetTableGroupConfig == null)) {
            flag[0] = true;
            flag[1] = false;
        } else {
            String tableInTbGrp = targetTableGroupConfig.getAllTables().get(0);
            PartitionInfo targetPartitionInfo = schemaManager.getTable(tableInTbGrp).getPartitionInfo();

            PartitionStrategy strategy = sourcePartitionInfo.getPartitionBy().getStrategy();
            boolean isVectorStrategy =
                (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.RANGE_COLUMNS);
            boolean match = false;
            if (isVectorStrategy) {
                if (PartitionInfoUtil.actualPartColsEquals(sourcePartitionInfo, targetPartitionInfo,
                    PartitionInfoUtil.fetchAllLevelMaxActualPartColsFromPartInfos(sourcePartitionInfo,
                        targetPartitionInfo))) {
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

            for (PartitionGroupRecord partitionGroupRecord : targetTableGroupConfig.getPartitionGroupRecords()) {
                PartitionSpec partitionSpec =
                    AlterTableGroupUtils.findPartitionSpec(sourcePartitionInfo, partitionGroupRecord);

                assert partitionSpec != null;

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

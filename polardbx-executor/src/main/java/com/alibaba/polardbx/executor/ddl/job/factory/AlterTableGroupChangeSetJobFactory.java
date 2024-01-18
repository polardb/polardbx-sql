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
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.AlterTableGroupBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.AlterTableGroupMovePartitionsCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyExecutorInitTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyFinishTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetCatchUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CHANGE_SET_APPLY_OPTIMIZATION;
import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genChangeSetCatchUpTasks;
import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genTargetTableLocations;

public class AlterTableGroupChangeSetJobFactory extends AlterTableGroupSubTaskJobFactory {
    private ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask;

    private ChangeSetApplyFinishTask changeSetApplyFinishTask;

    protected final AlterTableGroupBasePreparedData parentPrepareData;

    public AlterTableGroupChangeSetJobFactory(DDL ddl, AlterTableGroupBasePreparedData parentPrepareData,
                                              AlterTableGroupItemPreparedData preparedData,
                                              List<PhyDdlTableOperation> phyDdlTableOperations,
                                              Map<String, List<List<String>>> tableTopology,
                                              Map<String, Set<String>> targetTableTopology,
                                              Map<String, Set<String>> sourceTableTopology,
                                              //List<Pair<String, String>> orderedTargetTableLocations,
                                              Map<String, Pair<String, String>> orderedTargetTableLocations,
                                              String targetPartition, boolean skipBackfill,
                                              ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask,
                                              ChangeSetApplyFinishTask changeSetApplyFinishTask,
                                              ComplexTaskMetaManager.ComplexTaskType taskType,
                                              ExecutionContext executionContext) {
        super(ddl,
            parentPrepareData,
            preparedData,
            phyDdlTableOperations,
            tableTopology,
            targetTableTopology,
            sourceTableTopology,
            orderedTargetTableLocations,
            targetPartition,
            skipBackfill,
            taskType,
            executionContext);
        this.parentPrepareData = parentPrepareData;
        this.changeSetApplyExecutorInitTask = changeSetApplyExecutorInitTask;
        this.changeSetApplyFinishTask = changeSetApplyFinishTask;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (skipBackfill) {
            return super.doCreate();
        }

        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        String tableGroupName = preparedData.getTableGroupName();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);

        PartitionInfo newPartitionInfo = generateNewPartitionInfo();
        TablePartitionRecord logTableRec = PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
        logTableRec.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC;
        List<TablePartitionRecord> partRecList =
            PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
            .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                newPartitionInfo.getPartitionBy().getPartitions());

        //DdlTask validateTask = new AlterTableGroupValidateTask(schemaName, preparedData.getTableGroupName());
        DdlTask addMetaTask =
            new AlterTableGroupAddSubTaskMetaTask(schemaName, tableName,
                tableGroupConfig.getTableGroupRecord().getTg_name(),
                tableGroupConfig.getTableGroupRecord().getId(), "",
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING.getValue(), 0, logTableRec, partRecList,
                subPartRecInfos);

        List<DdlTask> taskList = new ArrayList<>();
        //1. validate
        //taskList.add(validateTask);
        if (changeSetApplyExecutorInitTask != null) {
            taskList.add(changeSetApplyExecutorInitTask);
        }

        //2. create physical table
        //2.1 insert meta to complex_task_outline
        taskList.add(addMetaTask);
        //2.2 create partitioned physical table
        phyDdlTableOperations.forEach(o -> o.setPartitionInfo(newPartitionInfo));
        if (!tableTopology.isEmpty()) {
            PhysicalPlanData physicalPlanData =
                DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations);
            DdlTask phyDdlTask =
                new CreateTablePhyDdlTask(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData);
            taskList.add(phyDdlTask);
        }

        List<String> relatedTables = new ArrayList<>();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            relatedTables.add(tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
        } else {
            relatedTables.add(tableName);
        }

        AlterTableGroupBackFillTask alterTableGroupBackFillTask =
            new AlterTableGroupBackFillTask(schemaName, tableName, sourceTableTopology, targetTableTopology,
                isBroadcast(),
                ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION == taskType, true);

        Map<String, String> targetTableLocations = genTargetTableLocations(orderedTargetTableLocations);
        Long changeSetId = ChangeSetManager.getChangeSetId();

        ChangeSetStartTask changeSetStartTask =
            new ChangeSetStartTask(schemaName, tableName, sourceTableTopology, taskType, changeSetId);

        Map<String, ChangeSetCatchUpTask> catchUpTasks = genChangeSetCatchUpTasks(
            schemaName,
            tableName,
            sourceTableTopology,
            targetTableLocations,
            taskType,
            changeSetId
        );

        final boolean useApplyOpt = changeSetApplyFinishTask != null
            && executionContext.getParamManager().getBoolean(CHANGE_SET_APPLY_OPTIMIZATION);
        AlterTableGroupMovePartitionsCheckTask changeSetCheckTask =
            new AlterTableGroupMovePartitionsCheckTask(schemaName, tableName, sourceTableTopology, targetTableTopology,
                useApplyOpt, relatedTables);
        AlterTableGroupMovePartitionsCheckTask changeSetCheckTwiceTask =
            new AlterTableGroupMovePartitionsCheckTask(schemaName, tableName, sourceTableTopology, targetTableTopology,
                false, relatedTables);

        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG);
        boolean stayAtPublic = true;
        if (StringUtils.isNotEmpty(finalStatus)) {
            stayAtPublic =
                StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name(), finalStatus);
        }

        List<DdlTask> movePartitionTasks = ChangeSetUtils.genChangeSetOnlineSchemaChangeTasks(
            schemaName, tableName,
            relatedTables,
            finalStatus,
            changeSetStartTask,
            catchUpTasks,
            alterTableGroupBackFillTask,
            changeSetCheckTask,
            changeSetCheckTwiceTask,
            changeSetApplyFinishTask,
            executionContext);

        taskList.addAll(movePartitionTasks);
        executableDdlJob.addSequentialTasks(taskList);

        //cdc ddl mark task
        SqlKind sqlKind = ddl.kind();
        DdlContext dc = executionContext.getDdlContext();

        Map<String, Set<String>> newTopology = newPartitionInfo.getTopology();
        if (stayAtPublic) {
            cdcTableGroupDdlMarkTask =
                new CdcTableGroupDdlMarkTask(tableGroupName, schemaName, tableName, sqlKind, newTopology,
                    dc.getDdlStmt());
        }

        if (changeSetApplyExecutorInitTask != null) {
            executableDdlJob.labelAsHead(changeSetApplyExecutorInitTask);
        } else {
            executableDdlJob.labelAsHead(addMetaTask);
        }
        executableDdlJob.labelAsTail(taskList.get(taskList.size() - 1));
        return executableDdlJob;
    }

    public AlterTableGroupBasePreparedData getParentPrepareData() {
        return parentPrepareData;
    }
}

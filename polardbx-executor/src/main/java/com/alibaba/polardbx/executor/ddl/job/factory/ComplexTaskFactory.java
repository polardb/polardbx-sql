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
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPhyTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.AlterTableGroupBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.MoveTableBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseCleanupTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseSwitchDataSourcesTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateTablesVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMoveDatabaseDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterComplexTaskUpdateJobStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupCleanupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupMovePartitionRefreshMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupRefreshMetaBaseTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableSetTableGroupRefreshMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabasePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class ComplexTaskFactory {
    /**
     * for
     * alter tablegroup
     */
    public static List<DdlTask> addPartitionTasks(String schemaName,
                                                  String logicalTableName,
                                                  Map<String, Set<String>> sourcePhyTables,
                                                  Map<String, Set<String>> targetPhyTables,
                                                  boolean stayAtCreating,
                                                  boolean stayAtDeleteOnly,
                                                  boolean stayAtWriteOnly,
                                                  boolean stayAtWriteReorg,
                                                  boolean skipBackFill, ExecutionContext executionContext) {
        List<DdlTask> taskList = new ArrayList<>();

        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);
        List<String> relatedTables = new ArrayList<>();
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert
                tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            relatedTables.add(tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
        } else {
            relatedTables.add(logicalTableName);
        }

        AlterComplexTaskUpdateJobStatusTask deleteOnlyTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                logicalTableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask writeOnlyTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                logicalTableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask writeReOrgTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                logicalTableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask readyToPublicTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                logicalTableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
                null,
                null);

        //sync for creating status
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));
        if (stayAtCreating) {
            return taskList;
        }
        taskList.add(deleteOnlyTask);
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));
        if (stayAtDeleteOnly) {
            return taskList;
        }

        taskList.add(writeOnlyTask);
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));

        if (stayAtWriteOnly) {
            return taskList;
        }

        if (!skipBackFill) {
            taskList
                .add(new AlterTableGroupBackFillTask(schemaName, logicalTableName, sourcePhyTables, targetPhyTables));
        }
        taskList.add(writeReOrgTask);
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));

        if (stayAtWriteReorg) {
            return taskList;
        }

        taskList.add(readyToPublicTask);
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));
        return taskList;
    }

    public static List<DdlTask> bringUpAlterTableGroup(String schemaName,
                                                       String tableGroupName,
                                                       String tableName,
                                                       ComplexTaskMetaManager.ComplexTaskType complexTaskType,
                                                       ExecutionContext executionContext) {

        List<String> logicalTableNames = new ArrayList<>();
        // not include GSI tables
        Set<String> primaryLogicalTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        if (complexTaskType != ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP) {
            for (TablePartRecordInfoContext tablePartRecordInfoContext : tableGroupConfig.getAllTables()) {
                String logicalTable = tablePartRecordInfoContext.getLogTbRec().getTableName();
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
        } else {
            // for alter table set tableGroup, only need to care about the table in "alter table" only
            if (!primaryLogicalTables.contains(tableName)) {
                logicalTableNames.add(tableName);
                primaryLogicalTables.add(tableName);
            }
        }

        List<DdlTask> taskList = new ArrayList<>();
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        AlterComplexTaskUpdateJobStatusTask DoingReorgTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableGroupName,
                logicalTableNames,
                false,
                ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG,
                ComplexTaskMetaManager.ComplexTaskStatus.SOURCE_WRITE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY);
        AlterComplexTaskUpdateJobStatusTask sourceWriteOnlyTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableGroupName,
                logicalTableNames,
                false,
                ComplexTaskMetaManager.ComplexTaskStatus.SOURCE_WRITE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.SOURCE_DELETE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY);

        AlterTableGroupRefreshMetaBaseTask alterTableGroupRefreshTableGroupMetaTask;
        List<BaseDdlTask> synTableGroupTasks = new ArrayList<>();
        if (complexTaskType == ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION) {
            alterTableGroupRefreshTableGroupMetaTask =
                new AlterTableGroupMovePartitionRefreshMetaTask(schemaName, tableGroupName);
            BaseDdlTask synTableGroup =
                new TableGroupSyncTask(schemaName, tableGroupName);
            synTableGroupTasks.add(synTableGroup);
        } else if (complexTaskType == ComplexTaskMetaManager.ComplexTaskType.SET_TABLEGROUP) {
            PartitionInfo partitionInfo =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
            TableGroupConfig sourceTableGroupConfigInfo =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            assert sourceTableGroupConfigInfo != null && sourceTableGroupConfigInfo.getTableGroupRecord() != null;

            alterTableGroupRefreshTableGroupMetaTask =
                new AlterTableSetTableGroupRefreshMetaTask(schemaName, tableGroupName, partitionInfo.getTableGroupId(),
                    tableName);
            BaseDdlTask synTargetTableGroup =
                new TableGroupSyncTask(schemaName, tableGroupName);
            BaseDdlTask synSourceTableGroup =
                new TableGroupSyncTask(schemaName, sourceTableGroupConfigInfo.getTableGroupRecord().getTg_name());
            synTableGroupTasks.add(synTargetTableGroup);
            synTableGroupTasks.add(synSourceTableGroup);
        } else {
            alterTableGroupRefreshTableGroupMetaTask =
                new AlterTableGroupRefreshMetaBaseTask(schemaName, tableGroupName);
            BaseDdlTask synTableGroup =
                new TableGroupSyncTask(schemaName, tableGroupName);
            synTableGroupTasks.add(synTableGroup);
        }

        AlterTableGroupCleanupTask alterTableGroupCleanupTask = new AlterTableGroupCleanupTask(schemaName);

        taskList.add(DoingReorgTask);
        taskList
            .add(new TablesSyncTask(schemaName, logicalTableNames, true, initWait, interval, TimeUnit.MILLISECONDS));
        taskList.add(sourceWriteOnlyTask);
        taskList
            .add(new TablesSyncTask(schemaName, logicalTableNames, true, initWait, interval, TimeUnit.MILLISECONDS));

        taskList.add(alterTableGroupRefreshTableGroupMetaTask);
        taskList.addAll(synTableGroupTasks);
        DdlTask updateTablesVersionTask = new UpdateTablesVersionTask(schemaName, logicalTableNames);
        taskList.add(updateTablesVersionTask);

        // make sure the tablegroup is reload before table, we can't update table version inside TablesSyncTask
        taskList
            .add(new TablesSyncTask(schemaName, logicalTableNames, true, initWait, interval, TimeUnit.MILLISECONDS));

        taskList.add(alterTableGroupCleanupTask);

        return taskList;
    }

    public static List<DdlTask> bringUpMoveDatabase(MoveDatabasePreparedData preparedData,
                                                    ExecutionContext executionContext) {

        String schemaName = preparedData.getSchemaName();

        List<String> logicalTableNames = ScaleOutPlanUtil.getLogicalTables(schemaName);
        List<DdlTask> taskList = new ArrayList<>();
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        AlterComplexTaskUpdateJobStatusTask DoingReorgTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                "-",
                logicalTableNames,
                false,
                ComplexTaskMetaManager.ComplexTaskStatus.DOING_REORG,
                ComplexTaskMetaManager.ComplexTaskStatus.FINISH_DB_MIG,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC);
        AlterComplexTaskUpdateJobStatusTask readOnlyTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                "-",
                logicalTableNames,
                false,
                ComplexTaskMetaManager.ComplexTaskStatus.FINISH_DB_MIG,
                ComplexTaskMetaManager.ComplexTaskStatus.DB_READONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC);

        MoveDatabaseSwitchDataSourcesTask moveDatabaseSwitchDataSourcesTask =
            new MoveDatabaseSwitchDataSourcesTask(schemaName, preparedData.getGroupAndStorageInstId(),
                preparedData.getSourceTargetGroupMap());

        CdcMoveDatabaseDdlMarkTask cdcMoveDatabaseDdlMarkTask =
            new CdcMoveDatabaseDdlMarkTask(schemaName, SqlKind.MOVE_DATABASE, preparedData.getSourceSql());

        AlterComplexTaskUpdateJobStatusTask toPublicTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                "-",
                logicalTableNames,
                false,
                ComplexTaskMetaManager.ComplexTaskStatus.DB_READONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
                ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC);

        MoveDatabaseCleanupTask moveDatabaseCleanupTask =
            new MoveDatabaseCleanupTask(schemaName,
                preparedData.getSourceTargetGroupMap());

        taskList.add(DoingReorgTask);
        taskList
            .add(new TablesSyncTask(schemaName, logicalTableNames, true, initWait, interval, TimeUnit.MILLISECONDS));
        taskList.add(readOnlyTask);
        taskList
            .add(new TablesSyncTask(schemaName, logicalTableNames, true, initWait, interval, TimeUnit.MILLISECONDS));
        taskList.add(moveDatabaseSwitchDataSourcesTask);
        taskList
            .add(new TablesSyncTask(schemaName, logicalTableNames, true, initWait, interval, TimeUnit.MILLISECONDS));
        taskList.add(cdcMoveDatabaseDdlMarkTask);
        taskList.add(toPublicTask);
        taskList
            .add(new TablesSyncTask(schemaName, logicalTableNames, true, initWait, interval, TimeUnit.MILLISECONDS));
        taskList.add(moveDatabaseCleanupTask);

        return taskList;
    }

    public static List<DdlTask> moveTableTasks(String schemaName,
                                               String logicalTableName,
                                               Map<String, Set<String>> sourcePhyTables,
                                               Map<String, Set<String>> targetPhyTables,
                                               Map<String, String> sourceAndTargetGroupMap,
                                               boolean stayAtCreating,
                                               boolean stayAtDeleteOnly,
                                               boolean stayAtWriteOnly,
                                               boolean stayAtWriteReorg,
                                               ExecutionContext executionContext) {
        List<DdlTask> taskList = new ArrayList<>();
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);
        List<String> relatedTables = new ArrayList<>();
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert
                tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            relatedTables.add(tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
        } else {
            relatedTables.add(logicalTableName);
        }
        AlterComplexTaskUpdateJobStatusTask deleteOnlyTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                logicalTableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask writeOnlyTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                logicalTableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask writeReOrgTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                logicalTableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask readyToPublicTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                logicalTableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
                null,
                null);

        //sync for creating status
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));
        if (stayAtCreating) {
            return taskList;
        }
        taskList.add(deleteOnlyTask);
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));
        if (stayAtDeleteOnly) {
            return taskList;
        }
        taskList.add(writeOnlyTask);
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));
        if (stayAtWriteOnly) {
            return taskList;
        }

        taskList
            .add(new MoveTableBackFillTask(schemaName, logicalTableName, sourcePhyTables, targetPhyTables,
                sourceAndTargetGroupMap));
        taskList.add(writeReOrgTask);
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));

        if (stayAtWriteReorg) {
            return taskList;
        }
        taskList.add(readyToPublicTask);
        taskList.add(
            new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval, TimeUnit.MILLISECONDS));
        return taskList;
    }

    public static DdlTask CreateDropUselessPhyTableTask(String schemaName, String logicalTableName,
                                                        Map<String, Set<String>> sourceTables,
                                                        ExecutionContext executionContext) {

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        Map<String, List<List<String>>> tableTopology = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : sourceTables.entrySet()) {
            for (String val : entry.getValue()) {
                List<String> phyTable = new ArrayList<>();
                phyTable.add(val);
                tableTopology.computeIfAbsent(entry.getKey(), o -> new ArrayList<>()).add(phyTable);
            }
        }
        DdlPhyPlanBuilder
            dropPhyTableBuilder = DropPhyTableBuilder.createBuilder(schemaName, logicalTableName, true, tableTopology,
            executionContext).build();
        List<PhyDdlTableOperation> physicalPlans = dropPhyTableBuilder.getPhysicalPlans();
        physicalPlans.forEach(o -> o.setPartitionInfo(partitionInfo));
        PhysicalPlanData physicalPlanData = DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, physicalPlans);

        return new DropTablePhyDdlTask(schemaName, physicalPlanData);
    }

}

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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@Getter
@TaskName(name = "AlterTableGroupRefreshMetaBaseTask")
public class AlterTableGroupRefreshMetaBaseTask extends BaseDdlTask {

    protected String tableGroupName;

    @JSONCreator
    public AlterTableGroupRefreshMetaBaseTask(String schemaName, String tableGroupName) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        refreshTableGroupMeta(metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
        updateAllTablesVersion(metaDbConnection);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        reloadTables(executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        reloadTables(executionContext);
    }

    protected void reloadTables(ExecutionContext executionContext) {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .reloadTableGroupByGroupName(schemaName, tableGroupName);

        for (TablePartRecordInfoContext infoContext : tableGroupConfig.getAllTables()) {
            String tableName = infoContext.getLogTbRec().tableName;
            schemaName = infoContext.getLogTbRec().tableSchema;
            ((GmsTableMetaManager) (executionContext.getSchemaManager(schemaName)))
                .tonewversion(tableName, true, 500L, 500L, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 1、delete the outdated partition group;
     * 2、delete the outdated table partition;
     * 3、insert the new partition group from partition_group_delta table to partition_group;
     * 4、insert the new table partition by upsert stmt;
     * 5、cleanup partition_group_delta
     * 6、cleanup table_partition_delta
     */
    public void refreshTableGroupMeta(Connection metaDbConnection) {

        boolean isUpsert = true;
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();

        tablePartitionAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        dbGroupInfoAccessor.setConnection(metaDbConnection);

        updateTaskStatus(metaDbConnection);

        long tableGroupId = tableGroupConfig.getTableGroupRecord().id;

        List<PartitionGroupRecord> outDatedPartRecords =
            partitionGroupAccessor.getOutDatedPartitionGroupsByTableGroupIdFromDelta(tableGroupId);
        for (PartitionGroupRecord record : outDatedPartRecords) {

            // 1、delete the outdated partition group
            partitionGroupAccessor.deletePartitionGroupById(record.id);

            // 2、delete the outdated table partitions
            List<TablePartitionRecord> partitionRecords =
                tablePartitionAccessor.getTablePartitionsByDbNamePartGroupId(schemaName, record.id);
            for (TablePartitionRecord partitionRecord : partitionRecords) {
                tablePartitionAccessor.deleteTablePartitionsById(partitionRecord.id);
            }
        }

        List<PartitionGroupRecord> newPartitionGroups = partitionGroupAccessor
            .getPartitionGroupsByTableGroupId(tableGroupId, true);
        Map<String, Long> newPartitionGroupsInfo = new TreeMap(String.CASE_INSENSITIVE_ORDER);
        if (GeneralUtil.isNotEmpty(newPartitionGroups)) {
            for (PartitionGroupRecord record : newPartitionGroups) {
                // 3、insert the new partition group from partition_group_delta table to partition_group
                record.visible = 1;
                //reset to version=1;
                record.meta_version = 1L;
                Long partitionGroupId = partitionGroupAccessor.addNewPartitionGroup(record, false, true);
                newPartitionGroupsInfo.putIfAbsent(record.partition_name, partitionGroupId);
            }
        }
        for (TablePartRecordInfoContext infoContext : tableGroupConfig.getAllTables()) {
            String tableName = infoContext.getLogTbRec().tableName;
            schemaName = infoContext.getLogTbRec().tableSchema;
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);

            /**
             * At this time, the old partInfo and the new partInfo has been switched,
             * so the new partInfo should be got from tableMeta.getPartitionInfo()
             */

            PartitionInfo newPartitionInfo = tableMeta.getPartitionInfo();
            if (newPartitionInfo == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("Failed to get new partition info for table[%s]", tableName));
            }

            /**
             * Use the partition Info of table_partitions_delta to replace the partitionInfo of
             * table_partitions
             */
            TablePartitionRecord logTableRec =
                PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
            List<TablePartitionRecord> partRecList =
                PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
            for (TablePartitionRecord record : partRecList) {
                if (newPartitionGroupsInfo.containsKey(record.partName)) {
                    // update the groupId with according to new partitionGroup
                    record.groupId = newPartitionGroupsInfo.get(record.partName);
                }
            }
            Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
                .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                    newPartitionInfo.getPartitionBy().getPartitions());

            for (Map.Entry<String, List<TablePartitionRecord>> entry : subPartRecInfos.entrySet()) {
                List<TablePartitionRecord> subpartRecList = entry.getValue();
                for (TablePartitionRecord record : subpartRecList) {
                    if (newPartitionGroupsInfo.containsKey(record.partName)) {
                        // update the groupId with according to new partitionGroup
                        record.groupId = newPartitionGroupsInfo.get(record.partName);
                    }
                }
            }

            TablePartRecordInfoContext tablePartRecordInfoContext = new TablePartRecordInfoContext();
            tablePartRecordInfoContext.setLogTbRec(logTableRec);
            tablePartRecordInfoContext.setPartitionRecList(partRecList);
            tablePartRecordInfoContext.setSubPartitionRecMap(subPartRecInfos);
            List<TablePartRecordInfoContext> tablePartRecordInfoContexts = new ArrayList<>();
            tablePartRecordInfoContexts.add(tablePartRecordInfoContext);

            // 4、insert the new table partition
            tablePartitionAccessor.addNewTablePartitionConfigs(tablePartRecordInfoContext.getLogTbRec(),
                tablePartRecordInfoContext.getPartitionRecList(),
                tablePartRecordInfoContext.getSubPartitionRecMap(),
                isUpsert, false);

        }
        // 5、cleanup partition_group_delta
        partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupId, true);

        // 6、cleanup table_partition_delta
        //todo luoyanxin only delete the related records
        tablePartitionAccessor
            .deleteTablePartitionConfigsForDeltaTable(schemaName, null);

    }

    protected void updateTaskStatus(Connection metaDbConnection) {
        ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
        complexTaskOutlineAccessor.setConnection(metaDbConnection);

        ComplexTaskMetaManager.updateAllSubTasksStatusByJobId(this.getJobId(), schemaName,
            ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC, metaDbConnection);

        ComplexTaskMetaManager.updateParentComplexTaskStatusByJobId(getJobId(),
            schemaName,
            ComplexTaskMetaManager.ComplexTaskStatus.SOURCE_DELETE_ONLY,
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC,
            metaDbConnection);
    }

    protected void updateAllTablesVersion(Connection metaDbConnection) {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        for (TablePartRecordInfoContext infoContext : tableGroupConfig.getAllTables()) {
            updateTableVersion(metaDbConnection, infoContext.getLogTbRec().tableSchema,
                infoContext.getLogTbRec().tableName);
        }
    }

    protected void updateTableVersion(Connection metaDbConnection, String schemaName, String logicalTableName) {
        try {
            TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

}

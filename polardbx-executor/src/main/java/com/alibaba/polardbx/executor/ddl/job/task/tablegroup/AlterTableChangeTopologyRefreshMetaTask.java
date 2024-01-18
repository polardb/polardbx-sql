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
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
@TaskName(name = "AlterTableChangeTopologyRefreshMetaTask")
public class AlterTableChangeTopologyRefreshMetaTask extends BaseDdlTask {

    protected String sourceTableGroup;
    protected String targetTableGroup;
    protected String tableName;

    @JSONCreator
    public AlterTableChangeTopologyRefreshMetaTask(String schemaName, String sourceTableGroup, String targetTableGroup,
                                                   String tableName) {
        super(schemaName);
        this.sourceTableGroup = sourceTableGroup;
        this.targetTableGroup = targetTableGroup;
        this.tableName = tableName;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        refreshTableMeta(metaDbConnection);
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
        updateTableVersion(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    /**
     * 1、delete the outdated table partition;
     * 2、insert the new partition group from partition_group_delta table to partition_group;
     * 3、insert the new table partition by upsert stmt;
     * 4、cleanup partition_group_delta
     * 5、cleanup table_partition_delta
     */
    public void refreshTableMeta(Connection metaDbConnection) {
        boolean isUpsert = false;
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(targetTableGroup);
        TableGroupConfig sourceTableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(sourceTableGroup);
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();

        tablePartitionAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);

        updateTaskStatus(metaDbConnection);

        long targetTableGroupId = tableGroupConfig.getTableGroupRecord().id;
        long sourceTableGroupId = sourceTableGroupConfig.getTableGroupRecord().id;

        tablePartitionAccessor.deleteTablePartitionConfigs(schemaName, tableName);

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
        newPartitionInfo = newPartitionInfo.copy();
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        for (PartitionSpec partitionSpec : newPartitionInfo.getPartitionBy().getPhysicalPartitions()) {
            if (!partitionSpec.getLocation().isVisiable()) {
                Optional<PartitionGroupRecord> partitionGroupRecord = partitionGroupRecords.stream()
                    .filter(o -> o.partition_name.equalsIgnoreCase(partitionSpec.getName())).findFirst();
                if (!partitionGroupRecord.isPresent()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                        "the partition:" + partitionSpec.getName() + " is not exists this current table group:"
                            + targetTableGroup);
                }
                partitionSpec.getLocation().setPartitionGroupId(partitionGroupRecord.get().id);
            }
        }
        SQLRecorderLogger.ddlMetaLogger.info(MessageFormat.format(
            "AlterTableChangeTopologyRefreshMetaTask-PartitionInfo:{0}",
            tableMeta.getPartitionInfo().getDigest(tableMeta.getVersion())));

        if (tableMeta.getNewPartitionInfo() != null) {
            SQLRecorderLogger.ddlMetaLogger.info(MessageFormat.format(
                "AlterTableChangeTopologyRefreshMetaTask-newPartitionInfo:{0}",
                tableMeta.getNewPartitionInfo().getDigest(tableMeta.getVersion())));
        }
        /**
         * Use the partition Info of table_partitions_delta to replace the partitionInfo of
         * table_partitions
         */
        TablePartitionRecord logTableRec =
            PartitionInfoUtil.prepareRecordForLogicalTable(newPartitionInfo);
        logTableRec.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC;
        List<TablePartitionRecord> partRecList =
            PartitionInfoUtil.prepareRecordForAllPartitions(newPartitionInfo);
        Map<String, List<TablePartitionRecord>> subPartRecInfos = PartitionInfoUtil
            .prepareRecordForAllSubpartitions(partRecList, newPartitionInfo,
                newPartitionInfo.getPartitionBy().getPartitions());

        assert newPartitionInfo.getTableGroupId().longValue() == targetTableGroupId;

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

        // 5、cleanup table_partition_delta
        //only delete the related records
        tablePartitionAccessor
            .deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);

        // 6、cleanup partition_group_delta
        partitionGroupAccessor.deletePartitionGroupsByTableGroupId(targetTableGroupId, true);
        partitionGroupAccessor.deletePartitionGroupsByTableGroupId(sourceTableGroupId, true);

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

    protected void updateTableVersion(Connection metaDbConnection, ExecutionContext executionContext) {
        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        TableMeta tableMeta = schemaManager.getTable(tableName);
        String primaryTable = tableName;
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert
                tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            primaryTable = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        }
        updateTableVersion(metaDbConnection, schemaName, primaryTable);
    }

    protected void updateTableVersion(Connection metaDbConnection, String schemaName, String logicalTableName) {
        try {
            TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

}

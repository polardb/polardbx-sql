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
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Getter
@TaskName(name = "AlterTableGroupRefreshMetaBaseTask")
public class AlterTableGroupRefreshMetaBaseTask extends BaseDdlTask {

    protected String tableGroupName;
    protected Long versionId;

    @JSONCreator
    public AlterTableGroupRefreshMetaBaseTask(String schemaName, String tableGroupName, Long versionId) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.versionId = versionId;
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
        updateAllTablesVersion(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
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
        ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
        ColumnarTableEvolutionAccessor columnarTableEvolutionAccessor = new ColumnarTableEvolutionAccessor();
        ColumnarPartitionEvolutionAccessor columnarPartitionEvolutionAccessor =
            new ColumnarPartitionEvolutionAccessor();

        tablePartitionAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        dbGroupInfoAccessor.setConnection(metaDbConnection);
        columnarTableMappingAccessor.setConnection(metaDbConnection);
        columnarTableEvolutionAccessor.setConnection(metaDbConnection);
        columnarPartitionEvolutionAccessor.setConnection(metaDbConnection);

        updateTaskStatus(metaDbConnection);

        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        long tableGroupId = tableGroupRecord.id;

        /**
         * Fetch all pg that are to be deleted from partition_group_delta and
         * remove these partitions from table_partitions by id
         */
        List<PartitionGroupRecord> outDatedPartRecords =
            partitionGroupAccessor.getOutDatedPartitionGroupsByTableGroupIdFromDelta(tableGroupId);
        Set<Long> outDatedPartGroupIds = new HashSet<>();
        for (PartitionGroupRecord record : outDatedPartRecords) {

            // 1、delete the outdated partition group
            partitionGroupAccessor.deletePartitionGroupById(record.id);

            // 2、delete the outdated table partitions
            List<TablePartitionRecord> partitionRecords =
                tablePartitionAccessor.getTablePartitionsByDbNamePartGroupId(schemaName, record.id);
            for (TablePartitionRecord partitionRecord : partitionRecords) {
                tablePartitionAccessor.deleteTablePartitionsById(partitionRecord.id);
            }
            outDatedPartGroupIds.add(record.id);
        }

        /**
         * Fetch new pg from partition_group_delta and put them into the partition_group as final result
         */
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

        for (String tableName : tableGroupConfig.getAllTables()) {
            schemaName = tableGroupRecord.getSchema();
            TableMeta tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
            SQLRecorderLogger.ddlMetaLogger.info(
                "AlterTableGroupRefreshMetaBaseTask-LatestSchemaManager:" + System
                    .identityHashCode(OptimizerContext.getContext(schemaName).getLatestSchemaManager()));

            /**
             * At this time, the old partInfo and the new partInfo has been switched,
             * so the new partInfo should be got from tableMeta.getPartitionInfo()
             */
            PartitionInfo newPartitionInfo = tableMeta.getPartitionInfo();
            PartitionInfo partitionInfo = tableMeta.getNewPartitionInfo();
            if (newPartitionInfo == null || partitionInfo == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("Failed to get partition info for table[%s]", tableName));
            }

            SQLRecorderLogger.ddlMetaLogger.info(MessageFormat.format(
                "AlterTableGroupRefreshMetaBaseTask-PartitionInfo:{0}",
                tableMeta.getPartitionInfo().getDigest(tableMeta.getVersion())));

            if (tableMeta.getNewPartitionInfo() != null) {
                SQLRecorderLogger.ddlMetaLogger.info(MessageFormat.format(
                    "AlterTableGroupRefreshMetaBaseTask-newPartitionInfo:{0}",
                    tableMeta.getNewPartitionInfo().getDigest(tableMeta.getVersion())));
            }

            /**
             *
             */
            if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                    boolean deleteLogicalPart = true;
                    for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                        if (!outDatedPartGroupIds.contains(subPartSpec.getLocation().getPartitionGroupId())) {
                            deleteLogicalPart = false;
                            break;
                        }
                    }
                    if (deleteLogicalPart) {
                        tablePartitionAccessor.deleteTablePartitionsById(partitionSpec.getId());
                    }
                }
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
            tablePartRecordInfoContext.setSubPartitionRecList(
                TablePartRecordInfoContext.buildAllSubPartitionRecList(subPartRecInfos));

            List<TablePartRecordInfoContext> tablePartRecordInfoContexts = new ArrayList<>();
            tablePartRecordInfoContexts.add(tablePartRecordInfoContext);

            // 4、insert the new table partition
            tablePartitionAccessor.addNewTablePartitionConfigs(tablePartRecordInfoContext.getLogTbRec(),
                tablePartRecordInfoContext.getPartitionRecList(),
                tablePartRecordInfoContext.getSubPartitionRecMap(),
                isUpsert, false);

            // 4.5 insert new columnar table partition to columnar evolution
            updateColumnarEvolutionSysTables(tableMeta, tableName, tablePartitionAccessor, columnarTableMappingAccessor,
                columnarPartitionEvolutionAccessor, columnarTableEvolutionAccessor, versionId, jobId);

            // 5、cleanup table_partition_delta
            //only delete the related records
            tablePartitionAccessor
                .deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);

        }
        // 6、cleanup partition_group_delta
        partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupId, true);

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

    protected void updateAllTablesVersion(Connection metaDbConnection, ExecutionContext executionContext) {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        for (String tableName : tableGroupConfig.getAllTables()) {
            TableMeta tableMeta = schemaManager.getTable(tableName);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                tableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
            updateTableVersion(metaDbConnection, schemaName, tableName);
        }
    }

    protected void updateTableVersion(Connection metaDbConnection, String schemaName, String logicalTableName) {
        try {
            TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void updateColumnarEvolutionSysTables(TableMeta tableMeta, String tableName,
                                                 TablePartitionAccessor tablePartitionAccessor,
                                                 ColumnarTableMappingAccessor columnarTableMappingAccessor,
                                                 ColumnarPartitionEvolutionAccessor columnarPartitionEvolutionAccessor,
                                                 ColumnarTableEvolutionAccessor columnarTableEvolutionAccessor,
                                                 long versionId, long jobId) {
        if (tableMeta.isColumnar()) {
            // Insert column evolution records
            List<TablePartitionRecord> partitionRecords =
                tablePartitionAccessor.getTablePartitionsByDbNameTbName(schemaName, tableName, false);

            List<ColumnarTableMappingRecord> records =
                columnarTableMappingAccessor.querySchemaIndex(schemaName, tableName);
            if (records == null || records.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format("Failed to get columnar table mapping for table[%s]", tableName));
            }

            // get columnar table mapping
            ColumnarTableMappingRecord record = records.get(0);
            // get latest columnar table evolution
            ColumnarTableEvolutionRecord columnarTableEvolutionRecord =
                columnarTableEvolutionAccessor.queryTableIdLatest(record.tableId).get(0);

            List<Long> partitions = columnarTableEvolutionRecord.partitions;

            List<ColumnarPartitionEvolutionRecord> columnarPartitionEvolutionRecords =
                columnarPartitionEvolutionAccessor.queryIdsWithOrder(columnarTableEvolutionRecord.partitions);

            // find first changed partition(except logic partition)
            int pos = getPos(partitionRecords, columnarPartitionEvolutionRecords);

            List<ColumnarPartitionEvolutionRecord> partitionEvolutionRecords = new ArrayList<>();

            // old logic partition
//            columnarPartitionEvolutionRecords.get(0).status = ColumnarPartitionStatus.ABSENT.getValue();
//            partitionEvolutionRecords.add(columnarPartitionEvolutionRecords.get(0));
            // new logic partition
            partitionEvolutionRecords.add(
                new ColumnarPartitionEvolutionRecord(record.tableId, partitionRecords.get(0).partName,
                    versionId, jobId, partitionRecords.get(0), ColumnarPartitionStatus.PUBLIC.getValue()));

            // other level partitions
            for (int i = pos; i < partitionRecords.size(); i++) {
                partitionEvolutionRecords.add(
                    new ColumnarPartitionEvolutionRecord(record.tableId, partitionRecords.get(i).partName,
                        versionId, jobId, partitionRecords.get(i), ColumnarPartitionStatus.PUBLIC.getValue()));
            }
//            for (int i = pos; i < columnarPartitionEvolutionRecords.size(); i++) {
//                columnarPartitionEvolutionRecords.get(i).status = ColumnarPartitionStatus.ABSENT.getValue();
//                partitionEvolutionRecords.add(columnarPartitionEvolutionRecords.get(i));
//            }

            if (!partitionEvolutionRecords.isEmpty()) {
                columnarPartitionEvolutionAccessor.insert(partitionEvolutionRecords);
                columnarPartitionEvolutionAccessor.updatePartitionIdAsId(record.tableId, versionId);
            }

            partitionEvolutionRecords =
                columnarPartitionEvolutionAccessor.queryTableIdAndNotInStatus(record.tableId, versionId,
                    ColumnarPartitionStatus.ABSENT.getValue());

            // first partition must be logic partition
            partitions = partitions.subList(1, Math.min(pos, partitionRecords.size()));
            partitions.add(0, partitionEvolutionRecords.get(0).id);
            // other partitions
            for (int i = 1; i < partitionEvolutionRecords.size(); i++) {
                partitions.add(partitionEvolutionRecords.get(i).id);
            }

            ColumnarTableEvolutionRecord latest =
                columnarTableEvolutionAccessor.queryTableIdLatest(record.tableId).get(0);
            latest.versionId = versionId;
            latest.commitTs = Long.MAX_VALUE;
            latest.ddlType = DdlType.ALTER_TABLE.name();
            latest.ddlJobId = jobId;
            latest.partitions = partitions;
            columnarTableEvolutionAccessor.insert(ImmutableList.of(latest));
            columnarTableMappingAccessor.updateVersionId(versionId, record.tableId);
        }
    }

    private static int getPos(List<TablePartitionRecord> partitionRecords,
                              List<ColumnarPartitionEvolutionRecord> columnarPartitionEvolutionRecords) {
        int pos = -1;
        // first is logic partition
        for (int i = 1; i < Math.min(columnarPartitionEvolutionRecords.size(), partitionRecords.size()); i++) {
            ColumnarPartitionEvolutionRecord evolutionRecord = columnarPartitionEvolutionRecords.get(i);
            TablePartitionRecord partitionRecord = partitionRecords.get(i);
            if (!TablePartitionRecord.isPartitionRecordEqual(partitionRecord, evolutionRecord.partitionRecord)) {
                pos = i;
                break;
            }
        }
        // add/drop column last
        if (pos == -1) {
            pos = columnarPartitionEvolutionRecords.size();
        }
        return pos;
    }
}

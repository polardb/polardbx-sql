package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import javax.servlet.http.Part;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "AlterTableGroupAddPartitionMetaTask")
public class AlterTableGroupAddPartitionMetaTask extends BaseDdlTask {

    protected String targetTableGroup;
    protected String tableName;
    protected String sourceSql;
    protected TablePartitionRecord logicalTableRecord;
    protected List<TablePartitionRecord> partitionRecords;
    protected Map<String, List<TablePartitionRecord>> subPartitionInfos;

    @JSONCreator
    public AlterTableGroupAddPartitionMetaTask(String schemaName,
                                               String targetTableGroup,
                                               String tableName,
                                               TablePartitionRecord logicalTableRecord,
                                               List<TablePartitionRecord> partitionRecords,
                                               Map<String, List<TablePartitionRecord>> subPartitionInfos,
                                               String sourceSql) {
        super(schemaName);
        this.targetTableGroup = targetTableGroup;
        this.tableName = tableName;
        this.sourceSql = sourceSql;
        this.logicalTableRecord = logicalTableRecord;
        this.partitionRecords = partitionRecords;
        this.subPartitionInfos = subPartitionInfos;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateAndInsertTablePartitions(metaDbConnection, executionContext);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private void updateAndInsertTablePartitions(Connection metaDbConnection, ExecutionContext executionContext) {
        boolean isUpsert = true;
        boolean toDeltatable = false;
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(targetTableGroup);
        TablePartitionAccessor tpAccess = new TablePartitionAccessor();
        PartitionGroupAccessor pgAccess = new PartitionGroupAccessor();
        tpAccess.setConnection(metaDbConnection);
        pgAccess.setConnection(metaDbConnection);
        List<PartitionGroupRecord> pgRecords = pgAccess.getPartitionGroupsByTableGroupId(tableGroupConfig.getTableGroupRecord().id, false);
        int phySubPartitionNum = 0;
        if (GeneralUtil.isNotEmpty(subPartitionInfos)) {
            for (List<TablePartitionRecord> records : subPartitionInfos.values()) {
                phySubPartitionNum += records.size();
            }
        }
        if (GeneralUtil.isEmpty(pgRecords) || GeneralUtil.isEmpty(subPartitionInfos) && pgRecords.size() != partitionRecords.size()
                || GeneralUtil.isNotEmpty(subPartitionInfos) && phySubPartitionNum != pgRecords.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "table group partition number not match");
        }
        logicalTableRecord.groupId = pgRecords.get(0).tg_id;

        Map<String, PartitionGroupRecord> pgMap = pgRecords.stream().collect(Collectors.toMap(pg -> pg.partition_name, pg -> pg));
        if (GeneralUtil.isEmpty(subPartitionInfos)) {
            for (TablePartitionRecord record : partitionRecords) {
                PartitionGroupRecord pgRecord = pgMap.get(record.partName);
                if (pgRecord == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "partition group name " + record.partName + " not find");
                }
                record.groupId = pgRecord.id;
            }
        } else {
            for (Map.Entry<String, List<TablePartitionRecord>> entry : subPartitionInfos.entrySet()) {
                List<TablePartitionRecord> subPartRecList = entry.getValue();
                for (TablePartitionRecord record : subPartRecList) {
                    PartitionGroupRecord pgRecord = pgMap.get(record.partName);
                    if (pgRecord == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "partition group name " + record.partName + " not find");
                    }
                    record.groupId = pgRecord.id;
                }
            }
        }
        tpAccess.addNewTablePartitionConfigs(logicalTableRecord, partitionRecords, subPartitionInfos, isUpsert, toDeltatable);
        // here is add meta to partition_partitions_delta table for CDC mark usage only
        tpAccess.addNewTablePartitionConfigs(logicalTableRecord, partitionRecords, subPartitionInfos, isUpsert, true);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        //do not support rollback
        updateSupportedCommands(true, false, metaDbConnection);
        executeImpl(metaDbConnection, executionContext);
    }

}

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ColumnarConfig;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.polardbx.statistics.SQLRecorderLogger.ddlLogger;

@Getter
@TaskName(name = "UpdateColumnarConfigTask")
public class UpdateColumnarConfigTask extends BaseGmsTask {
    private final String indexName;
    private final Map<String, String> options;

    public UpdateColumnarConfigTask(String schemaName, String logicalTableName, String indexName,
                                    Map<String, String> options) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.options = options;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
        updateTableVersion(metaDbConnection);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (MapUtils.isEmpty(options)) {
            return;
        }
        List<ColumnarConfigRecord> configRecords = getColumnarConfigRecords(metaDbConnection);

        // If options contains 'type', update `columnar_table_mapping`.
        ColumnarConfigRecord record;
        if (null != (record = getRecordWith(configRecords, ColumnarOptions.TYPE))) {
            ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
            tableMappingAccessor.setConnection(metaDbConnection);
            long tableId = record.tableId;
            tableMappingAccessor.updateTypeByTableId(record.tableId, record.configValue);
            tableMappingAccessor.UpdateExtraByTableId(record.tableId, null);

            if (ColumnarConfig.SNAPSHOT.equalsIgnoreCase(record.configValue)) {
                // This columnar index is a snapshot.
                Map<String, Map<String, String>> records = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                Map<String, String> globalConfig = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                MetaDbUtil.generateColumnarConfig(schemaName, logicalTableName, records, globalConfig);

                // If SNAPSHOT_RETENTION_DAYS not set, set a default one.
                if (null == getRecordWith(configRecords, ColumnarOptions.SNAPSHOT_RETENTION_DAYS)) {
                    ColumnarConfigRecord tmpRecord = new ColumnarConfigRecord();
                    tmpRecord.tableId = tableId;
                    tmpRecord.configKey = ColumnarOptions.SNAPSHOT_RETENTION_DAYS;
                    tmpRecord.configValue = ColumnarConfig.getValue(tmpRecord.configKey, null, globalConfig);
                    String columnarPurgeSaveMs = globalConfig.get(ColumnarOptions.COLUMNAR_PURGE_SAVE_MS);
                    // SNAPSHOT_RETENTION_DAYS should >= COLUMNAR_PURGE_SAVE_MS
                    if (null != columnarPurgeSaveMs) {
                        long columnarPurgeSaveDays = 1 + Long.parseLong(columnarPurgeSaveMs) / 1000 / 60 / 60 / 24;
                        if (columnarPurgeSaveDays > Long.parseLong(tmpRecord.configValue)) {
                            tmpRecord.configValue = Long.toString(columnarPurgeSaveDays);
                        }
                    }
                    configRecords.add(tmpRecord);
                }
                // If AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL not set, set a default one.
                if (null == getRecordWith(configRecords, ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL)) {
                    ColumnarConfigRecord tmpRecord = new ColumnarConfigRecord();
                    tmpRecord.tableId = tableId;
                    tmpRecord.configKey = ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL;
                    tmpRecord.configValue = ColumnarConfig.getValue(tmpRecord.configKey, null, globalConfig);
                    configRecords.add(tmpRecord);
                }
                // Force backup.
                {
                    ColumnarConfigRecord tmpRecord = new ColumnarConfigRecord();
                    tmpRecord.tableId = tableId;
                    tmpRecord.configKey = ColumnarOptions.COLUMNAR_BACKUP_ENABLE;
                    tmpRecord.configValue = "true";
                    configRecords.add(tmpRecord);
                }
            }
        }

        ColumnarConfigAccessor accessor = new ColumnarConfigAccessor();
        accessor.setConnection(metaDbConnection);
        accessor.insert(configRecords);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
        updateTableVersion(metaDbConnection);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (MapUtils.isEmpty(options)) {
            return;
        }
        try {
            // Find table id.
            List<ColumnarConfigRecord> configRecords = getColumnarConfigRecords(metaDbConnection);
            ColumnarConfigAccessor accessor = new ColumnarConfigAccessor();
            accessor.setConnection(metaDbConnection);
            accessor.deleteByTableIdAndKeyValue(configRecords);

            // If options contains 'type', update `columnar_table_mapping`.
            ColumnarConfigRecord record;
            if (null != (record = getRecordWith(configRecords, ColumnarOptions.TYPE))) {
                ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
                tableMappingAccessor.setConnection(metaDbConnection);
                tableMappingAccessor.updateTypeByTableId(record.tableId, null);
            }
        } catch (Exception e) {
            ddlLogger.error("Rollback impl for update columnar config task failed.", e);
        }
    }

    @NotNull
    private List<ColumnarConfigRecord> getColumnarConfigRecords(Connection metaDbConnection) {
        // Find table id.
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        List<ColumnarTableMappingRecord> records =
            tableInfoManager.queryColumnarTable(schemaName, logicalTableName, indexName);
        if (records.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Columnar table mapping record not found.");
        }
        long tableId = records.get(0).tableId;
        List<ColumnarConfigRecord> configRecords = new ArrayList<>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            ColumnarConfigRecord record = new ColumnarConfigRecord();
            record.tableId = tableId;
            record.configKey = format(option.getKey());
            record.configValue = format(option.getValue());
            configRecords.add(record);
        }
        return configRecords;
    }

    private ColumnarConfigRecord getRecordWith(List<ColumnarConfigRecord> records, String key) {
        for (ColumnarConfigRecord record : records) {
            if (key.equalsIgnoreCase(record.configKey)) {
                return record;
            }
        }
        return null;
    }

    protected static String format(String str) {
        return str.replaceAll("^[ '\"]*|[ '\"]*$", "").trim();
    }
}

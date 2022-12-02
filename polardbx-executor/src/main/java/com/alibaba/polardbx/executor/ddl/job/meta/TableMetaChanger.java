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

package com.alibaba.polardbx.executor.ddl.job.meta;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.common.utils.timezone.TimestampUtils;
import com.alibaba.polardbx.executor.gms.TableRuleManager;
import com.alibaba.polardbx.executor.gms.util.SequenceUtil;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import static com.alibaba.polardbx.gms.metadb.table.TableInfoManager.PhyInfoSchemaContext;

public class TableMetaChanger {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetaChanger.class);

    private static final ConfigManager CONFIG_MANAGER = MetaDbConfigManager.getInstance();

    private static final String VARIABLE_TIME_ZONE = "time_zone";
    private static final String QUERY_PHY_TIME_ZONE = "show variables like '" + VARIABLE_TIME_ZONE + "'";

    public static void addTableExt(Connection metaDbConn, TablesExtRecord tablesExtRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.addTableExt(tablesExtRecord);
    }

    public static void removeTableExt(Connection metaDbConn, String schemaName, String logicalTableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.removeTableExt(schemaName, logicalTableName);
    }

    public static void addTableMeta(Connection metaDbConn, PhyInfoSchemaContext phyInfoSchemaContext,
                                    boolean hasTimestampColumnDefault, ExecutionContext executionContext,
                                    Map<String, String> binaryColumnDefaultValues) {
        String schemaName = phyInfoSchemaContext.tableSchema;
        String tableName = phyInfoSchemaContext.tableName;

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        long newSeqCacheSize = executionContext.getParamManager().getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE);
        newSeqCacheSize = newSeqCacheSize < 1 ? 0 : newSeqCacheSize;
        tableInfoManager.addTable(phyInfoSchemaContext, newSeqCacheSize);

        if (hasTimestampColumnDefault) {
            Map<String, String> convertedColumnDefaults =
                convertColumnDefaults(phyInfoSchemaContext, null, executionContext);
            tableInfoManager.resetColumnDefaults(schemaName, tableName, null, convertedColumnDefaults);
        }

        // Update binary default value as hex string
        if (!binaryColumnDefaultValues.isEmpty()) {
            tableInfoManager.updateBinaryColumnDefaults(schemaName, tableName, binaryColumnDefaultValues);
        }
    }

    public static void addOssTableMeta(Connection metaDbConn, PhyInfoSchemaContext phyInfoSchemaContext,
                                       Engine tableEngine) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.addOssTable(phyInfoSchemaContext, tableEngine);
    }

    public static void addOssFileMeta(Connection metaDbConn, String tableSchema, String tableName,
                                      FilesRecord filesRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.addOssFile(tableSchema, tableName, filesRecord);
    }

    public static void changeOssFile(Connection metaDbConn, Long primaryKey, Long fileSize) {
        changeOssFile(metaDbConn, primaryKey, new byte[] {}, fileSize, 0L);
    }

    public static void changeOssFile(Connection metaDbConn, Long primaryKey, byte[] fileMeta, Long fileSize,
                                     Long rowCount) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.changeOssFile(primaryKey, fileMeta, fileSize, rowCount);
    }

    public static Long addOssFileAndReturnLastInsertId(Connection metaDbConn, String tableSchema, String tableName,
                                                       FilesRecord filesRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        return tableInfoManager.addOssFileAndReturnLastInsertId(tableSchema, tableName, filesRecord);
    }

    public static void changeTableEngine(Connection metaDbConn, String tableSchema, String tableName, Engine engine) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.changeTableEngine(tableSchema, tableName, engine.name());
    }

    public static List<FilesRecord> lockOssFileMeta(Connection metaDbConn, Long taskId, String tableSchema,
                                                    String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        return tableInfoManager.lockOssFile(taskId, tableSchema, tableName);
    }

    public static void deleteOssFileMeta(Connection metaDbConn, Long taskId, String tableSchema, String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.deleteOssFile(taskId, tableSchema, tableName);
    }

    public static void validOssFileMeta(Connection metaDbConn, Long taskId, String tableSchema, String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.validOssFile(taskId, tableSchema, tableName);
    }

    public static void addOssColumnMeta(Connection metaDbConn, String tableSchema, String tableName,
                                        ColumnMetasRecord columnMetasRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.addOSSColumnsMeta(tableSchema, tableName, columnMetasRecord);
    }

    public static List<ColumnMetasRecord> lockOssColumnMeta(Connection metaDbConn, Long taskId,
                                                            String tableSchema, String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        return tableInfoManager.queryOSSColumnsMeta(taskId, tableSchema, tableName);
    }

    public static void deleteOssColumnMeta(Connection metaDbConn, Long taskId, String tableSchema, String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.deleteOSSColumnsMeta(taskId, tableSchema, tableName);
    }

    public static void validOssColumnMeta(Connection metaDbConn, Long taskId, String tableSchema, String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.validOSSColumnsMeta(taskId, tableSchema, tableName);
    }

    public static PhyInfoSchemaContext buildPhyInfoSchemaContext(String schemaName, String logicalTableName,
                                                                 String dbIndex, String phyTableName,
                                                                 SequenceBean sequenceBean,
                                                                 TablesExtRecord tablesExtRecord, boolean isPartitioned,
                                                                 boolean ifNotExists, SqlKind sqlKind,
                                                                 ExecutionContext executionContext) {
        PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        sequenceBean = SequenceMetaChanger.createSequenceIfExists(schemaName, logicalTableName, sequenceBean,
            tablesExtRecord, isPartitioned, ifNotExists, sqlKind, executionContext);

        if (sequenceBean != null) {
            SequenceBaseRecord sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
            phyInfoSchemaContext.sequenceRecord = sequenceRecord;
        }

        return phyInfoSchemaContext;
    }

    public static void triggerSchemaChange(Connection metaDbConn, String schemaName, String tableName,
                                           SequenceBaseRecord sequenceRecord, TableInfoManager tableInfoManager) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, tableName);

        // Show all table meta.
        tableInfoManager.showTable(schemaName, tableName, sequenceRecord);

        // Register new table data id.
        CONFIG_MANAGER.register(tableDataId, metaDbConn);

        // update table list data id
        CONFIG_MANAGER.notify(tableListDataId, metaDbConn);
    }

    public static void afterNewTableMeta(String schemaName, String logicalTableName) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName);

        CommonMetaChanger.sync(tableListDataId);

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            CONFIG_MANAGER.notify(tableDataId, metaDbConn);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }

        CommonMetaChanger.sync(tableDataId);
    }

    public static void hideTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.hideTable(schemaName, logicalTableName);
    }

    public static void showTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.showTable(schemaName, logicalTableName, null);
    }

    public static void removeTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                       boolean withTablesExtOrPartition, ExecutionContext executionContext) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName);

        // Remove sequence meta if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean = SequenceMetaChanger.dropSequenceIfExists(schemaName, logicalTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }
        final SequenceBaseRecord finalSequenceRecord = sequenceRecord;

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        // Remove all table meta.
        tableInfoManager.removeTable(schemaName, logicalTableName, finalSequenceRecord, withTablesExtOrPartition);

        if (withTablesExtOrPartition) {
            tableInfoManager.removeTableExt(schemaName, logicalTableName);
        }

        // Unregister the table data id.
        CONFIG_MANAGER.unregister(tableDataId, metaDbConnection);

        CONFIG_MANAGER.notify(tableListDataId, metaDbConnection);

        if (sequenceRecord != null) {
            SequenceManagerProxy.getInstance().invalidate(schemaName, sequenceBean.getName());
        }
    }

    public static void afterRemovingTableMeta(String schemaName, String logicalTableName) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        CommonMetaChanger.sync(tableListDataId);

        TableRuleManager.reload(schemaName, logicalTableName);
        PartitionInfoManager.reload(schemaName, logicalTableName);
    }

    public static void addNewTableName(Connection metaDbConnection, String schemaName, String logicalTableName,
                                       String newLogicalTableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.addNewTableName(schemaName, logicalTableName, newLogicalTableName);
    }

    public static void removeNewTableName(Connection metaDbConnection, String schemaName, String logicalTableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.addNewTableName(schemaName, logicalTableName, DdlConstants.EMPTY_CONTENT);
    }

    public static void renameTableMeta(Connection metaDbConn, String schemaName, String logicalTableName,
                                       String newLogicalTableName, ExecutionContext executionContext) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName);
        String newTableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, newLogicalTableName);
        String newTbNamePattern =
            buildNewTbNamePattern(executionContext, schemaName, logicalTableName, newLogicalTableName);

        // Rename sequence if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean =
            SequenceMetaChanger.renameSequenceIfExists(schemaName, logicalTableName, newLogicalTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        // Replace with new table name
        tableInfoManager
            .renameTable(schemaName, logicalTableName, newLogicalTableName, newTbNamePattern, sequenceRecord);

        // Unregister the old table data id.
        CONFIG_MANAGER.unregister(tableDataId, metaDbConn);

        // Register new table data id.
        CONFIG_MANAGER.register(newTableDataId, metaDbConn);

        CONFIG_MANAGER.notify(tableListDataId, metaDbConn);
    }

    public static void renamePartitionTableMeta(Connection metaDbConn, String schemaName, String logicalTableName,
                                                String newLogicalTableName, ExecutionContext executionContext) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName);
        String newTableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, newLogicalTableName);

        // Rename sequence if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean =
            SequenceMetaChanger.renameSequenceIfExists(schemaName, logicalTableName, newLogicalTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        // Replace with new table name
        tableInfoManager
            .renamePartitionTable(schemaName, logicalTableName, newLogicalTableName, sequenceRecord);

        // Unregister the old table data id.
        CONFIG_MANAGER.unregister(tableDataId, metaDbConn);

        // Register new table data id.
        CONFIG_MANAGER.register(newTableDataId, metaDbConn);

        CONFIG_MANAGER.notify(tableListDataId, metaDbConn);
    }

    public static void afterRenamingTableMeta(String schemaName, String newLogicalTableName) {
        afterNewTableMeta(schemaName, newLogicalTableName);
    }

    public static void hideTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                     List<String> columnNames, List<String> indexNames) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.hideColumns(schemaName, logicalTableName, columnNames);
        tableInfoManager.hideIndexes(schemaName, logicalTableName, indexNames);
    }

    public static void showTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                     List<String> columnNames, List<String> indexNames) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.showColumns(schemaName, logicalTableName, columnNames);
        tableInfoManager.showIndexes(schemaName, logicalTableName, indexNames);
    }

    public static void changeTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                       String dbIndex, String phyTableName, SqlKind sqlKind, boolean isPartitioned,
                                       List<String> droppedColumns, List<String> addedColumns,
                                       List<String> updatedColumns, Map<String, String> changedColumns,
                                       boolean hasTimestampColumnDefault, Map<String, String> binaryColumnDefaultValues,
                                       List<String> droppedIndexes, List<String> addedIndexes,
                                       List<String> addedIndexesWithoutNames, Map<String, String> renamedIndexes,
                                       boolean primaryKeyDropped, List<String> addedPrimaryKeyColumns,
                                       List<Pair<String, String>> columnAfterAnother, boolean requireLogicalColumnOrder,
                                       String tableComment, String tableRowFormat, SequenceBean sequenceBean,
                                       boolean onlineModifyColumnIndexTask, ExecutionContext executionContext) {
        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        sequenceBean = SequenceMetaChanger.alterSequenceIfExists(schemaName, logicalTableName, sequenceBean,
            null, isPartitioned, false, sqlKind, executionContext);
        if (sequenceBean != null) {
            phyInfoSchemaContext.sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        Map<String, Map<String, Object>> columnJdbcExtInfo = tableInfoManager.fetchColumnJdbcExtInfo(
            phyInfoSchemaContext.phyTableSchema, phyInfoSchemaContext.phyTableName, phyInfoSchemaContext.dataSource);

        // Remove dropped column meta if exist.
        if (GeneralUtil.isNotEmpty(droppedColumns)) {
            tableInfoManager.removeColumns(schemaName, logicalTableName, droppedColumns);
        }

        // Update existing column meta if exist and column name may be changed as well.
        if (GeneralUtil.isNotEmpty(changedColumns)) {
            tableInfoManager.changeColumns(phyInfoSchemaContext, columnJdbcExtInfo, changedColumns);
            // Reset binary default value flag
            for (String columnName : changedColumns.keySet()) {
                tableInfoManager.resetColumnBinaryDefaultFlag(schemaName, logicalTableName, columnName);
            }
        }

        // Add new column meta if exist.
        if (GeneralUtil.isNotEmpty(addedColumns)) {
            tableInfoManager.addColumns(phyInfoSchemaContext, columnJdbcExtInfo, addedColumns);
        }

        // Update existing column meta if exist.
        if (GeneralUtil.isNotEmpty(updatedColumns)) {
            tableInfoManager.updateColumns(phyInfoSchemaContext, columnJdbcExtInfo, updatedColumns);
            // Clear binary default value flag
            for (String columnName : updatedColumns) {
                tableInfoManager.resetColumnBinaryDefaultFlag(schemaName, logicalTableName, columnName);
            }
        }

        // Refresh related column order.
        refreshColumnOrder(schemaName, logicalTableName, columnAfterAnother, requireLogicalColumnOrder,
            tableInfoManager);

        // Update column defaults with particular time zone if needed.
        if (hasTimestampColumnDefault) {
            List<String> allChangedColumns = new ArrayList<>();

            if (GeneralUtil.isNotEmpty(changedColumns)) {
                allChangedColumns.addAll(GeneralUtil.emptyIfNull(changedColumns.keySet()));
            }
            allChangedColumns.addAll(GeneralUtil.emptyIfNull(addedColumns));
            allChangedColumns.addAll(GeneralUtil.emptyIfNull(updatedColumns));

            Map<String, String> convertedColumnDefaults =
                convertColumnDefaults(phyInfoSchemaContext, allChangedColumns, executionContext);

            tableInfoManager.resetColumnDefaults(schemaName, logicalTableName, allChangedColumns,
                convertedColumnDefaults);
        }

        // Update binary default value as hex string
        if (!binaryColumnDefaultValues.isEmpty()) {
            tableInfoManager.updateBinaryColumnDefaults(schemaName, logicalTableName, binaryColumnDefaultValues);
        }

        // Remove existing index meta.
        if (GeneralUtil.isNotEmpty(droppedIndexes)) {
            tableInfoManager.removeIndexes(schemaName, logicalTableName, droppedIndexes);
        }

        // Rename existing index meta.
        if (GeneralUtil.isNotEmpty(renamedIndexes)) {
            if (onlineModifyColumnIndexTask) {
                Set<String> indexSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                indexSet.addAll(renamedIndexes.keySet());
                indexSet.addAll(renamedIndexes.values());
                List<String> indexes = new ArrayList<>(indexSet);
                tableInfoManager.removeIndexes(schemaName, logicalTableName, indexes);
                tableInfoManager.addIndexes(phyInfoSchemaContext, indexes);
            } else {
                tableInfoManager.renameIndexes(schemaName, logicalTableName, renamedIndexes);
            }
        }

        // Add new index meta if existed.
        if (GeneralUtil.isNotEmpty(addedIndexes) || GeneralUtil.isNotEmpty(addedIndexesWithoutNames)) {
            tableInfoManager.addIndexes(phyInfoSchemaContext, addedIndexes, addedIndexesWithoutNames);
        }

        // Drop existing primary key.
        if (primaryKeyDropped) {
            tableInfoManager.dropPrimaryKey(schemaName, logicalTableName);
        }

        // Add new primary key.
        if (GeneralUtil.isNotEmpty(addedPrimaryKeyColumns)) {
            tableInfoManager.addPrimaryKey(phyInfoSchemaContext);
        }

        // Sequence meta if needed.
        SequenceBaseRecord sequenceRecord = phyInfoSchemaContext.sequenceRecord;
        if (sequenceRecord != null) {
            if (sequenceBean.isNew()) {
                long newSeqCacheSize = executionContext.getParamManager().getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE);
                newSeqCacheSize = newSeqCacheSize < 1 ? 0 : newSeqCacheSize;
                tableInfoManager.createSequence(sequenceRecord, newSeqCacheSize);
            } else {
                tableInfoManager.alterSequence(sequenceRecord);
            }
        }

        if (tableComment != null) {
            tableInfoManager.updateTableComment(schemaName, logicalTableName, tableComment);
        }

        if (tableRowFormat != null) {
            tableInfoManager.updateTableRowFormat(schemaName, logicalTableName, tableRowFormat);
        }

        tableInfoManager.showTable(schemaName, logicalTableName, sequenceRecord, !onlineModifyColumnIndexTask);
    }

    public static void changeTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                       String dbIndex, String phyTableName, List<String> addedColumns,
                                       List<String> droppedColumns) {

        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        Map<String, Map<String, Object>> columnJdbcExtInfo = tableInfoManager.fetchColumnJdbcExtInfo(
            phyInfoSchemaContext.phyTableSchema, phyInfoSchemaContext.phyTableName, phyInfoSchemaContext.dataSource);

        // Remove dropped column meta if exist.
        if (GeneralUtil.isNotEmpty(droppedColumns)) {
            tableInfoManager.removeColumns(schemaName, logicalTableName, droppedColumns);
        }

        // Add new column meta if exist.
        if (GeneralUtil.isNotEmpty(addedColumns)) {
            tableInfoManager.addColumns(phyInfoSchemaContext, columnJdbcExtInfo, addedColumns);
        }

        tableInfoManager.showTable(schemaName, logicalTableName, phyInfoSchemaContext.sequenceRecord);
    }

    protected static void refreshColumnOrder(String tableSchema, String tableName,
                                             List<Pair<String, String>> columnAfterAnother,
                                             boolean requireLogicalColumnOrder,
                                             TableInfoManager tableInfoManager) {
        // Reorganize related columns to ensure the correct logical column order.
        if (!columnAfterAnother.isEmpty()) {
            tableInfoManager.reorgColumnOrder(tableSchema, tableName, columnAfterAnother);
            // Update flag for logical column expansion during inserting only if
            // logical and physical column order are different.
            if (requireLogicalColumnOrder) {
                tableInfoManager.updateLogicalColumnOrderFlag(tableSchema, tableName);
            }
        }
        // Reset column positions to make them continuous.
        tableInfoManager.resetColumnOrder(tableSchema, tableName);
    }

    // Set column multi-write source column, which will not trigger column multi-write since target column does not
    // exist yet
    public static void onlineModifyColumnInitColumn(Connection metaDbConnection, String schemaName,
                                                    String logicalTableName, String sourceColumn) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.setMultiWriteSourceColumn(schemaName, logicalTableName, sourceColumn);
    }

    public static void onlineModifyColumnInitColumnRollBack(Connection metaDbConnection, String schemaName,
                                                            String logicalTableName, String sourceColumn) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.showColumns(schemaName, logicalTableName, ImmutableList.of(sourceColumn));
    }

    // Add new column and set column multi-write target column, which will trigger column multi-write
    public static void onlineModifyColumnAddColumn(Connection metaDbConnection, String schemaName,
                                                   String logicalTableName, String dbIndex, String phyTableName,
                                                   String addedColumn, String sourceColumn, String afterColumn,
                                                   List<String> coveringGsi, List<String> gsiDbIndex,
                                                   List<String> gsiPhyTableName) {
        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        Map<String, Map<String, Object>> columnJdbcExtInfo = tableInfoManager.fetchColumnJdbcExtInfo(
            phyInfoSchemaContext.phyTableSchema, phyInfoSchemaContext.phyTableName, phyInfoSchemaContext.dataSource);

        tableInfoManager.addColumns(phyInfoSchemaContext, columnJdbcExtInfo, ImmutableList.of(addedColumn));
        tableInfoManager.showColumns(schemaName, logicalTableName, ImmutableList.of(addedColumn));
        tableInfoManager.setMultiWriteSourceColumn(schemaName, logicalTableName, sourceColumn);
        tableInfoManager.setMultiWriteTargetColumn(schemaName, logicalTableName, addedColumn);

        List<Pair<String, String>> columnAfterAnother = new ArrayList<>();
        columnAfterAnother.add(new Pair<>(addedColumn, afterColumn));
        refreshColumnOrder(schemaName, logicalTableName, columnAfterAnother, true, tableInfoManager);

        for (int i = 0; i < coveringGsi.size(); i++) {
            TableInfoManager.PhyInfoSchemaContext gsiPhyInfoSchemaContext =
                CommonMetaChanger.getPhyInfoSchemaContext(schemaName, coveringGsi.get(i), gsiDbIndex.get(i),
                    gsiPhyTableName.get(i));
            Map<String, Map<String, Object>> gsiColumnJdbcExtInfo =
                tableInfoManager.fetchColumnJdbcExtInfo(gsiPhyInfoSchemaContext.phyTableSchema,
                    gsiPhyInfoSchemaContext.phyTableName, gsiPhyInfoSchemaContext.dataSource);
            tableInfoManager.addColumns(gsiPhyInfoSchemaContext, gsiColumnJdbcExtInfo, ImmutableList.of(addedColumn));
            // We check whether doing column multi-write only based on primary table
            tableInfoManager.setMultiWriteTargetColumn(schemaName, coveringGsi.get(i), addedColumn);
        }
    }

    public static void onlineModifyColumnAddColumnRollback(Connection metaDbConnection, String schemaName,
                                                           String logicalTableName, String addedColumn,
                                                           String sourceColumn, List<String> coveringGsi) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        tableInfoManager.removeColumns(schemaName, logicalTableName, ImmutableList.of(addedColumn));
        tableInfoManager.setMultiWriteSourceColumn(schemaName, logicalTableName, sourceColumn);

        for (String gsiName : coveringGsi) {
            tableInfoManager.removeColumns(schemaName, gsiName, ImmutableList.of(addedColumn));
        }
        refreshColumnOrder(schemaName, logicalTableName, Collections.emptyList(), true, tableInfoManager);
    }

    public static void onlineModifyColumnDropColumn(Connection metaDbConnection, String schemaName,
                                                    String logicalTableName, String dbIndex, String phyTableName,
                                                    String droppedColumn, List<String> coveringGsi,
                                                    List<String> gsiDbIndex, List<String> gsiPhyTableName,
                                                    boolean unique, String uniqueKeyName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        tableInfoManager.removeColumns(schemaName, logicalTableName, ImmutableList.of(droppedColumn));
        for (String gsiName : coveringGsi) {
            tableInfoManager.removeColumns(schemaName, gsiName, ImmutableList.of(droppedColumn));
        }
        refreshColumnOrder(schemaName, logicalTableName, Collections.emptyList(), true, tableInfoManager);
    }

    public static void onlineModifyColumnSwapColumn(Connection metaDbConnection, String schemaName,
                                                    String logicalTableName, boolean isChange, String dbIndex,
                                                    String phyTableName, String newColumnName, String oldColumnName,
                                                    String afterColumnName, Map<String, List<String>> localIndexes,
                                                    Map<String, String> uniqueIndexMap, List<String> coveringGsi,
                                                    List<String> gsiDbIndex, List<String> gsiPhyTableName) {
        Map<String, TableInfoManager.PhyInfoSchemaContext> phyInfoMap = new HashMap<>();

        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);
        phyInfoMap.put(logicalTableName, phyInfoSchemaContext);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        Map<String, Map<String, Object>> columnJdbcExtInfo = tableInfoManager.fetchColumnJdbcExtInfo(
            phyInfoSchemaContext.phyTableSchema, phyInfoSchemaContext.phyTableName, phyInfoSchemaContext.dataSource);

        List<String> changedColumns = ImmutableList.of(newColumnName, oldColumnName);
        List<Pair<String, String>> columnAfterAnother = new ArrayList<>();
        String beforeColumnName = null;
        if (!isChange) {
            beforeColumnName = tableInfoManager.getBeforeColumnName(schemaName, logicalTableName, oldColumnName);
            if (beforeColumnName.equalsIgnoreCase(newColumnName)) {
                beforeColumnName = tableInfoManager.getBeforeColumnName(schemaName, logicalTableName, newColumnName);
            }
        }

        // Need to update column info since we may have changed column nullable
        tableInfoManager.updateColumns(phyInfoSchemaContext, columnJdbcExtInfo, changedColumns);
        // Do not support swap column names directly, so we reload column info from DN

        String droppedColumnName = isChange ? oldColumnName : newColumnName;
        String keptColumnName = isChange ? newColumnName : oldColumnName;

        tableInfoManager.setMultiWriteSourceColumn(schemaName, logicalTableName, keptColumnName);
        tableInfoManager.setMultiWriteTargetColumn(schemaName, logicalTableName, droppedColumnName);

        List<Pair<String, String>> gsiColumnAfterAnother = new ArrayList<>();
        gsiColumnAfterAnother.add(new Pair<>(oldColumnName, newColumnName));
        for (int i = 0; i < coveringGsi.size(); i++) {
            TableInfoManager.PhyInfoSchemaContext gsiPhyInfoSchemaContext =
                CommonMetaChanger.getPhyInfoSchemaContext(schemaName, coveringGsi.get(i), gsiDbIndex.get(i),
                    gsiPhyTableName.get(i));
            phyInfoMap.put(coveringGsi.get(i), gsiPhyInfoSchemaContext);
            Map<String, Map<String, Object>> gsiColumnJdbcExtInfo =
                tableInfoManager.fetchColumnJdbcExtInfo(gsiPhyInfoSchemaContext.phyTableSchema,
                    gsiPhyInfoSchemaContext.phyTableName, gsiPhyInfoSchemaContext.dataSource);
            tableInfoManager.updateColumns(gsiPhyInfoSchemaContext, gsiColumnJdbcExtInfo, changedColumns);

            // Move kept column to the end, same as physical order
            refreshColumnOrder(schemaName, coveringGsi.get(i), gsiColumnAfterAnother, true, tableInfoManager);

            tableInfoManager.showColumns(schemaName, coveringGsi.get(i), changedColumns);
            tableInfoManager.setMultiWriteTargetColumn(schemaName, coveringGsi.get(i), droppedColumnName);
        }

        if (!isChange) {
            // We only need to make sure new column is in right position
            if (afterColumnName.equalsIgnoreCase(oldColumnName)) {
                // Add to same position
                columnAfterAnother.add(new Pair<>(oldColumnName, beforeColumnName));
            } else {
                // Add to specified position
                columnAfterAnother.add(new Pair<>(oldColumnName, afterColumnName));
            }

            // Reset local index info
            if (!localIndexes.isEmpty()) {
                for (Map.Entry<String, List<String>> entry : localIndexes.entrySet()) {
                    String tableName = entry.getKey();
                    tableInfoManager.removeIndexes(schemaName, tableName, entry.getValue());
                    tableInfoManager.addIndexes(phyInfoMap.get(tableName), entry.getValue());
                    tableInfoManager.showIndexes(schemaName, tableName, entry.getValue());
                }
            }

            // Reset unique index info
            if (!uniqueIndexMap.isEmpty()) {
                for (Map.Entry<String, String> entry : uniqueIndexMap.entrySet()) {
                    String tableName = entry.getKey();
                    tableInfoManager.removeIndex(schemaName, tableName, entry.getValue());
                    tableInfoManager.addIndex(phyInfoMap.get(tableName), entry.getValue());
                    tableInfoManager.showIndex(schemaName, tableName, entry.getValue());
                }
            }
        }

        refreshColumnOrder(schemaName, logicalTableName, columnAfterAnother, true, tableInfoManager);
    }

    public static void onlineModifyColumnStopMultiWrite(Connection metaDbConnection, String schemaName,
                                                        String logicalTableName, String sourceColumnName,
                                                        String targetColumnName) {
        List<String> changedColumns = ImmutableList.of(sourceColumnName, targetColumnName);
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.showColumns(schemaName, logicalTableName, changedColumns);
        tableInfoManager.setMultiWriteTargetColumn(schemaName, logicalTableName, targetColumnName);
    }

    public static void addIndexMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                    String indexName, String dbIndex, String phyTableName) {
        TableInfoManager.PhyInfoSchemaContext context =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        if (TStringUtil.isNotEmpty(logicalTableName) && TStringUtil.isNotEmpty(indexName)) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            tableInfoManager.addIndex(context, indexName);
            tableInfoManager.showTable(schemaName, logicalTableName, null);
        }
    }

    public static void hideIndexMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                     String indexName) {
        if (TStringUtil.isNotEmpty(logicalTableName) && TStringUtil.isNotEmpty(indexName)) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            tableInfoManager.hideIndex(schemaName, logicalTableName, indexName);
        }
    }

    public static void showIndexMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                     String indexName) {
        if (TStringUtil.isNotEmpty(logicalTableName) && TStringUtil.isNotEmpty(indexName)) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            tableInfoManager.showIndex(schemaName, logicalTableName, indexName);
        }
    }

    public static void removeIndexMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                       String indexName) {
        if (TStringUtil.isNotEmpty(logicalTableName) && TStringUtil.isNotEmpty(indexName)) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            tableInfoManager.removeIndex(schemaName, logicalTableName, indexName);
        }
    }

    public static String buildNewTbNamePattern(ExecutionContext executionContext, String schemaName,
                                               String logicalTableName, String newLogicalTableName) {
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        TableRule tableRule = tddlRuleManager.getTableRule(logicalTableName);

        if (tableRule == null) {
            // The table may have been renamed and we are in an idempotent recovery process,
            // so let's try to get table name pattern from new table rule.
            tableRule = tddlRuleManager.getTableRule(newLogicalTableName);
        }
        if (tableRule == null) {
            // Failed since we can't find the table rule. And we don't check it from meta db for now.
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "not found table rule for table '" + logicalTableName + "'");
        }

        String newTbNamePattern = tableRule.getTbNamePattern();

        if (executionContext.needToRenamePhyTables()) {
            newTbNamePattern =
                TStringUtil.replaceWithIgnoreCase(newTbNamePattern, logicalTableName, newLogicalTableName);
        }

        return newTbNamePattern;
    }

    public static void setAutoPartitionFlag(Connection metaDbConnection,
                                            String schemaName,
                                            String primaryTableName,
                                            boolean isAutoPartition) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        TablesExtRecord sourceTableExt = tableInfoManager.queryTableExt(schemaName, primaryTableName, false);
        if (isAutoPartition) {
            sourceTableExt.setAutoPartition();
        } else {
            sourceTableExt.clearAutoPartition();
        }
        tableInfoManager.alterTableExtFlag(schemaName, primaryTableName, sourceTableExt.flag);
    }

    public static void addPartitionInfoMeta(Connection metaDbConnection,
                                            TableGroupConfig tableGroupConfig,
                                            ExecutionContext executionContext,
                                            boolean isUpsert) {

        TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.addTablePartitionInfos(tableGroupConfig, isUpsert);
    }

    public static void removePartitionInfoMeta(Connection metaDbConn, String schemaName, String logicalTableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.deletePartitionInfo(schemaName, logicalTableName);
    }

    public static void truncateTableWithRecycleBin(String schemaName, String tableName, String binTableName,
                                                   String tmpTableName, Connection metaDbConn,
                                                   ExecutionContext executionContext) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, tableName);
        String binTableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, binTableName);
        String tmpTableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, tmpTableName);

        TableRule tableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(tableName);
        TableRule tmpTableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(tmpTableName);
        String tbNamePattern = tableRule.getTbNamePattern();
        String tmpTbNamePattern = tmpTableRule.getTbNamePattern();

        // Rename sequence if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean = SequenceMetaChanger.renameSequenceIfExists(schemaName, tableName, binTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        // Switch the tables.
        tableInfoManager.renameTable(schemaName, tableName, binTableName, tbNamePattern, sequenceRecord);
        tableInfoManager.renameTable(schemaName, tmpTableName, tableName, tmpTbNamePattern, null);

        CONFIG_MANAGER.unregister(tmpTableDataId, metaDbConn);
        CONFIG_MANAGER.register(binTableDataId, metaDbConn);

        try {
            TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConn);
            TableInfoManager.updateTableVersion(schemaName, binTableName, metaDbConn);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, e.getMessage());
        }

        CONFIG_MANAGER.notify(tableDataId, metaDbConn);
        CONFIG_MANAGER.notify(binTableDataId, metaDbConn);

        CONFIG_MANAGER.notify(tableListDataId, metaDbConn);
    }

    public static void afterTruncatingTableWithRecycleBin(String schemaName, String tableName, String binTableName) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, tableName);
        String binTableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, binTableName);

        CommonMetaChanger.sync(tableListDataId);

        CommonMetaChanger.sync(tableDataId);
        CommonMetaChanger.sync(binTableDataId);
    }

    public static Map<String, String> convertColumnDefaults(PhyInfoSchemaContext context, List<String> columnNames,
                                                            ExecutionContext executionContext) {
        try (Connection phyDbConn = context.dataSource.getConnection()) {
            TimeZone sessionTimeZone = getSessionTimeZone(phyDbConn, executionContext);

            if (sessionTimeZone == null) {
                // No conversion needed.
                return null;
            }

            TableInfoManager tableInfoManager = new TableInfoManager();
            List<ColumnsInfoSchemaRecord> columnRecords =
                tableInfoManager.fetchColumnInfoSchema(context.phyTableSchema, context.phyTableName, columnNames,
                    phyDbConn);

            if (GeneralUtil.isEmpty(columnRecords)) {
                LOGGER.error("Cannot find column records from physical info schema");
                return null;
            }

            Map<String, String> columnDefaults = new HashMap<>();
            for (ColumnsInfoSchemaRecord record : columnRecords) {
                if (TStringUtil.equalsIgnoreCase(TimestampUtils.TYPE_NAME_TIMESTAMP, record.dataType)) {
                    String convertedTimestamp =
                        TimestampUtils.convertToGMT8(record.columnDefault, sessionTimeZone, record.datetimePrecision);
                    columnDefaults.put(record.columnName, convertedTimestamp);
                }
            }

            return columnDefaults;
        } catch (Exception e) {
            LOGGER.error("Failed to convert column defaults with time zone. Caused by: " + e.getMessage(), e);
            // Don't convert column defaults instead of throwing an exception.
            return null;
        }
    }

    private static TimeZone getSessionTimeZone(Connection phyDbConn, ExecutionContext executionContext)
        throws Exception {
        String mysqlTimeZoneId = null;

        if (executionContext.getServerVariables() != null) {
            mysqlTimeZoneId = (String) executionContext.getServerVariables().get(VARIABLE_TIME_ZONE);
        }

        if (phyDbConn instanceof TGroupDirectConnection) {
            ((TGroupDirectConnection) phyDbConn).setServerVariables(executionContext.getServerVariables());
        }

        if (TStringUtil.isBlank(mysqlTimeZoneId)) {
            try (Statement stmt = phyDbConn.createStatement();
                ResultSet rs = stmt.executeQuery(QUERY_PHY_TIME_ZONE)) {
                if (rs.next()) {
                    mysqlTimeZoneId = rs.getString(2);
                }
            }
        }

        if (!TimestampUtils.needTimeZoneConversion(mysqlTimeZoneId)) {
            return null;
        }

        InternalTimeZone internalTimeZone = TimeZoneUtils.convertFromMySqlTZ(mysqlTimeZoneId);
        return internalTimeZone != null ? internalTimeZone.getTimeZone() :
            TimeZone.getTimeZone("GMT" + mysqlTimeZoneId);
    }

    public static void beginAlterColumnDefaultValue(Connection metaDbConn, String schema, String table, String column) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.beginUpdateColumnDefaultVal(schema, table, column);
    }

    public static void addLocalPartitionMeta(Connection metaDbConnection,
                                             TableLocalPartitionRecord localPartitionRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.addLocalPartitionRecord(localPartitionRecord);
    }

    public static void addScheduledJob(Connection metaDbConnection,
                                       ScheduledJobsRecord scheduledJobsRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.addScheduledJob(scheduledJobsRecord);
    }

    public static void replaceScheduledJob(Connection metaDbConnection,
                                           ScheduledJobsRecord scheduledJobsRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.replaceScheduledJob(scheduledJobsRecord);
    }

    public static void removeLocalPartitionMeta(Connection metaDbConnection,
                                                String schemaName,
                                                String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.removeLocalPartitionRecord(schemaName, tableName);
    }

    public static void removeScheduledJobs(Connection metaDbConnection,
                                           String schemaName,
                                           String tableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.removeScheduledJobRecord(schemaName, tableName);
    }

    public static void endAlterColumnDefaultValue(Connection metaDbConn, String schema, String table, String column) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.endUpdateColumnDefaultVal(schema, table, column);
    }

    public static void updateArchiveTable(Connection metaDbConnection,
                                          String schemaName, String tableName,
                                          String archiveTableSchema, String archiveTableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.updateArchiveTable(schemaName, tableName, archiveTableSchema, archiveTableName);
    }
}

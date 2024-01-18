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
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
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
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.sync.ClearPlanCacheSyncAction;
import com.alibaba.polardbx.executor.gms.TableRuleManager;
import com.alibaba.polardbx.executor.gms.util.SequenceUtil;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.foreign.ForeignRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gms.GmsTableMetaManager.getForeignKeys;
import static com.alibaba.polardbx.executor.gms.GmsTableMetaManager.getReferencedForeignKeys;
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
                                    Map<String, String> specialDefaultValues,
                                    Map<String, Long> specialDefaultValueFlags,
                                    List<ForeignKeyData> addedForeignKeys) {
        String schemaName = phyInfoSchemaContext.tableSchema;
        String tableName = phyInfoSchemaContext.tableName;

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        long newSeqCacheSize = executionContext.getParamManager().getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE);
        newSeqCacheSize = newSeqCacheSize < 1 ? 0 : newSeqCacheSize;
        tableInfoManager.addTable(phyInfoSchemaContext, newSeqCacheSize,
            SequenceUtil.buildFailPointInjector(executionContext), addedForeignKeys);

        //add foreign key table meta
        if (GeneralUtil.isNotEmpty(addedForeignKeys)) {
            try {
                for (ForeignKeyData addedForeignKey : addedForeignKeys) {
                    TableInfoManager.updateTableVersion(addedForeignKey.refSchema, addedForeignKey.refTableName,
                        metaDbConn);
                    Map<String, Set<String>> fkTables =
                        ForeignKeyUtils.getAllForeignKeyRelatedTables(addedForeignKey.refSchema,
                            addedForeignKey.refTableName);
                    for (Map.Entry<String, Set<String>> entry : fkTables.entrySet()) {
                        for (String table : entry.getValue()) {
                            TableInfoManager.updateTableVersion(entry.getKey(), table, metaDbConn);
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        if (hasTimestampColumnDefault) {
            Map<String, String> convertedColumnDefaults =
                convertColumnDefaults(phyInfoSchemaContext, null, executionContext);
            tableInfoManager.resetColumnDefaults(schemaName, tableName, null, convertedColumnDefaults);
        }

        if (!specialDefaultValues.isEmpty()) {
            tableInfoManager.updateSpecialColumnDefaults(schemaName, tableName, specialDefaultValues,
                specialDefaultValueFlags);
        }
    }

    public static void addOssTableMeta(Connection metaDbConn, PhyInfoSchemaContext phyInfoSchemaContext,
                                       Engine tableEngine, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.addOssTable(phyInfoSchemaContext, tableEngine,
            SequenceUtil.buildFailPointInjector(executionContext));
    }

    public static void addOssFileMeta(Connection metaDbConn, String tableSchema, String tableName,
                                      FilesRecord filesRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.addOssFile(tableSchema, tableName, filesRecord);
    }

    public static void addOssFileWithTso(Connection metaDbConn, String tableSchema, String tableName,
                                         FilesRecord filesRecord) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.addOssFileWithTso(tableSchema, tableName, filesRecord);
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
                                                                 long ts,
                                                                 ExecutionContext executionContext) {
        PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        boolean needToCreate = SequenceMetaChanger.createSequenceIfExists(schemaName, logicalTableName, sequenceBean,
            tablesExtRecord, isPartitioned, ifNotExists, sqlKind, executionContext);

        if (needToCreate) {
            SequenceBaseRecord sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
            phyInfoSchemaContext.sequenceRecord = sequenceRecord;
        }

        phyInfoSchemaContext.ts = ts;

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

        // Change foreign logical if contains child table
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(logicalTableName);
        if (tableMeta != null && !tableMeta.getReferencedForeignKeys().values().isEmpty()) {
            try {
                TableMetaChanger.addLogicalForeignKeyMeta(metaDbConnection, schemaName, logicalTableName,
                    new ArrayList<>(tableMeta.getReferencedForeignKeys().values()));
                for (Map.Entry<String, ForeignKeyData> entry : tableMeta.getReferencedForeignKeys().entrySet()) {
                    TableInfoManager.updateTableVersion(entry.getValue().schema, entry.getValue().tableName,
                        metaDbConnection);
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        // Clean foreign key table meta
        try {
            Map<String, Set<String>> fkTables =
                ForeignKeyUtils.getAllForeignKeyRelatedTables(schemaName,
                    logicalTableName);
            for (Map.Entry<String, Set<String>> entry : fkTables.entrySet()) {
                for (String table : entry.getValue()) {
                    TableInfoManager.updateTableVersion(entry.getKey(), table, metaDbConnection);
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

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

    public static void notifyTableListDataId(Connection metaDbConn, String schemaName) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        CONFIG_MANAGER.notify(tableListDataId, metaDbConn);
    }

    public static void syncTableListDataId(String schemaName) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);

        CommonMetaChanger.sync(tableListDataId);
    }

    public static void renameTableMeta(Connection metaDbConn, String schemaName, String logicalTableName,
                                       String newLogicalTableName, ExecutionContext executionContext) {
        renameTableMeta(metaDbConn, schemaName, logicalTableName, newLogicalTableName, executionContext, true);
    }

    public static void renameTableMeta(Connection metaDbConn, String schemaName, String logicalTableName,
                                       String newLogicalTableName, ExecutionContext executionContext,
                                       boolean notifyTableListDataId) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName);
        String newTableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, newLogicalTableName);
        String newTbNamePattern =
            buildNewTbNamePattern(executionContext, schemaName, logicalTableName, newLogicalTableName);
        boolean isGsi = TableValidator.checkTableIsGsi(schemaName, logicalTableName);

        // Rename sequence if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean =
            SequenceMetaChanger.renameSequenceIfExists(metaDbConn, schemaName, logicalTableName, newLogicalTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        // replace indexes meta data
        if (isGsi) {
            tableInfoManager.renameIndexes(schemaName, logicalTableName, newLogicalTableName);
        }

        // Replace with new table name
        if (!executionContext.needToRenamePhyTables()) {
            newTbNamePattern = null;
        }
        tableInfoManager
            .renameTable(schemaName, logicalTableName, newLogicalTableName, newTbNamePattern, sequenceRecord);

        // update foreign key and table meta
        renameForeignKeyTable(metaDbConn, schemaName, logicalTableName, newLogicalTableName, tableInfoManager);

        // Unregister the old table data id.
        CONFIG_MANAGER.unregister(tableDataId, metaDbConn);

        // Register new table data id.
        CONFIG_MANAGER.register(newTableDataId, metaDbConn);

        if (notifyTableListDataId) {
            CONFIG_MANAGER.notify(tableListDataId, metaDbConn);
        }
    }

    public static void renamePartitionTableMeta(Connection metaDbConn, String schemaName, String logicalTableName,
                                                String newLogicalTableName, ExecutionContext executionContext) {
        renamePartitionTableMeta(metaDbConn, schemaName, logicalTableName, newLogicalTableName, executionContext, true);
    }

    public static void renamePartitionTableMeta(Connection metaDbConn, String schemaName, String logicalTableName,
                                                String newLogicalTableName, ExecutionContext executionContext,
                                                boolean notifyTableListDataId) {
        String tableListDataId = MetaDbDataIdBuilder.getTableListDataId(schemaName);
        String tableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName);
        String newTableDataId = MetaDbDataIdBuilder.getTableDataId(schemaName, newLogicalTableName);
        boolean renamePhyTable = executionContext.needToRenamePhyTables();
        boolean isGsi = TableValidator.checkTableIsGsi(schemaName, logicalTableName);

        // Rename sequence if exists.
        SequenceBaseRecord sequenceRecord = null;
        SequenceBean sequenceBean =
            SequenceMetaChanger.renameSequenceIfExists(metaDbConn, schemaName, logicalTableName, newLogicalTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        // for ttl
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(logicalTableName);
        if (tableMeta != null && tableMeta.getLocalPartitionDefinitionInfo() != null) {
            tableInfoManager.renameLocalPartitionInfo(schemaName, logicalTableName, newLogicalTableName);
        }

        // Replace with new physical table name
        if (renamePhyTable) {
            tableInfoManager.renamePartitionTablePhyTable(schemaName, logicalTableName, newLogicalTableName);
        }

        // replace indexes meta data
        if (isGsi) {
            tableInfoManager.renameIndexes(schemaName, logicalTableName, newLogicalTableName);
        }

        // Replace with new table name
        tableInfoManager.renamePartitionTable(schemaName, logicalTableName, newLogicalTableName, sequenceRecord);

        // update foreign key indexesAccessor and table meta
        renameForeignKeyTable(metaDbConn, schemaName, logicalTableName, newLogicalTableName, tableInfoManager);

        // Unregister the old table data id.
        CONFIG_MANAGER.unregister(tableDataId, metaDbConn);

        // Register new table data id.
        CONFIG_MANAGER.register(newTableDataId, metaDbConn);

        if (notifyTableListDataId) {
            CONFIG_MANAGER.notify(tableListDataId, metaDbConn);
        }
    }

    private static void renameForeignKeyTable(Connection metaDbConn, String schemaName, String logicalTableName,
                                              String newLogicalTableName, TableInfoManager tableInfoManager) {
        final List<ForeignRecord> referencedFkRecords =
            tableInfoManager.queryReferencedForeignKeys(schemaName, logicalTableName);
        final List<ForeignRecord> fkRecords =
            tableInfoManager.queryForeignKeys(schemaName, logicalTableName);

        final Map<String, ForeignKeyData> foreignKeys = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, ForeignKeyData> referencedForeignKeys = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        getReferencedForeignKeys(referencedForeignKeys, referencedFkRecords, tableInfoManager);
        getForeignKeys(foreignKeys, fkRecords, tableInfoManager);

        try {
            if (!referencedFkRecords.isEmpty()) {
                tableInfoManager
                    .updateReferencedForeignKeyTable(schemaName, logicalTableName, newLogicalTableName);

                for (Map.Entry<String, ForeignKeyData> e : referencedForeignKeys.entrySet()) {
                    // referenced table -> table
                    String referencedSchemaName = e.getValue().schema;
                    String referencedTableName = e.getValue().tableName;
                    TableInfoManager.updateTableVersion(referencedSchemaName, referencedTableName, metaDbConn);
                }
            }

            if (!foreignKeys.isEmpty()) {
                List<ForeignRecord> records = tableInfoManager.queryForeignKeys(schemaName, logicalTableName);
                for (ForeignRecord record : records) {
                    String constraintName = record.constraintName;
                    if (constraintName.contains(logicalTableName + "_ibfk_")) {
                        constraintName = newLogicalTableName + "_ibfk_" + constraintName.substring(
                            constraintName.lastIndexOf("_") + 1);
                    }

                    TableValidator.validateTableNameLength(constraintName);
                    tableInfoManager
                        .updateForeignKeyTable(schemaName, newLogicalTableName, record.indexName, constraintName,
                            logicalTableName);
                    tableInfoManager
                        .updateForeignKeyColsTable(schemaName, newLogicalTableName, logicalTableName);
                }

                Map<String, Set<String>> fkTables =
                    ForeignKeyUtils.getAllForeignKeyRelatedTables(schemaName, logicalTableName);
                for (Map.Entry<String, Set<String>> entry : fkTables.entrySet()) {
                    for (String table : entry.getValue()) {
                        TableInfoManager.updateTableVersion(entry.getKey(), table, metaDbConn);
                    }
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void afterRenamingTableMeta(String schemaName, String newLogicalTableName) {
        afterNewTableMeta(schemaName, newLogicalTableName);
    }

    public static void hideTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                     List<String> columnNames, List<String> indexNames) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.hideColumns(schemaName, logicalTableName, columnNames);
        tableInfoManager.hideIndexesColumns(schemaName, logicalTableName, columnNames);
        tableInfoManager.hideIndexes(schemaName, logicalTableName, indexNames);
    }

    public static void showTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                     List<String> columnNames, List<String> indexNames) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.showColumns(schemaName, logicalTableName, columnNames);
        tableInfoManager.showIndexesColumns(schemaName, logicalTableName, columnNames);
        tableInfoManager.showIndexes(schemaName, logicalTableName, indexNames);
    }

    public static void changeTableMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                       String dbIndex, String phyTableName, SqlKind sqlKind, boolean isPartitioned,
                                       List<String> droppedColumns, List<String> addedColumns,
                                       List<String> updatedColumns, List<Pair<String, String>> changedColumns,
                                       boolean hasTimestampColumnDefault, Map<String, String> specialDefaultValues,
                                       Map<String, Long> specialDefaultValueFlags, List<String> droppedIndexes,
                                       List<String> addedIndexes, List<String> addedIndexesWithoutNames,
                                       List<Pair<String, String>> renamedIndexes, boolean primaryKeyDropped,
                                       List<String> addedPrimaryKeyColumns,
                                       List<Pair<String, String>> columnAfterAnother, boolean requireLogicalColumnOrder,
                                       String tableComment, String tableRowFormat, SequenceBean sequenceBean,
                                       boolean onlineModifyColumnIndexTask,
                                       boolean changeFileStore,
                                       ExecutionContext executionContext) {
        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        sequenceBean = SequenceMetaChanger.alterSequenceIfExists(schemaName, logicalTableName, sequenceBean,
            null, isPartitioned, false, sqlKind, executionContext);
        if (sequenceBean != null) {
            phyInfoSchemaContext.sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }

        if (changeFileStore) {
            final ITimestampOracle timestampOracle =
                executionContext.getTransaction().getTransactionManagerUtil().getTimestampOracle();
            if (null == timestampOracle) {
                throw new UnsupportedOperationException("Do not support timestamp oracle");
            }
            phyInfoSchemaContext.ts = timestampOracle.nextTimestamp();
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
            for (Pair<String, String> changeColumn : changedColumns) {
                String columnName = changeColumn.getKey();
                tableInfoManager.resetColumnBinaryDefaultFlag(schemaName, logicalTableName, columnName);
            }

            // update foreign key column and table meta
            Map<String, String> reverseChangeColumns = new HashMap<>();
            for (Pair<String, String> entry : changedColumns) {
                reverseChangeColumns.put(entry.getValue(), entry.getKey());
            }

            try {
                TableMeta tableMeta =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(logicalTableName);
                if (tableMeta != null) {
                    Map<String, ForeignKeyData> referencedForeignKeys = tableMeta.getReferencedForeignKeys();
                    if (!referencedForeignKeys.isEmpty()) {
                        for (Map.Entry<String, ForeignKeyData> e : referencedForeignKeys.entrySet()) {
                            boolean sync = false;
                            for (int i = 0; i < e.getValue().columns.size(); ++i) {
                                String oldColumn = e.getValue().refColumns.get(i);
                                if (reverseChangeColumns.containsKey(oldColumn)) {
                                    String columnName = reverseChangeColumns.get(oldColumn);
                                    String indexName = e.getValue().indexName;
                                    tableInfoManager
                                        .updateReferencedForeignKeyColumn(e.getValue().schema,
                                            e.getValue().tableName, indexName,
                                            columnName, oldColumn);
                                    sync = true;
                                }
                            }

                            // referenced table -> table
                            if (sync) {
                                String referencedSchemaName = e.getValue().schema;
                                String referredTableName = e.getValue().tableName;
                                TableInfoManager.updateTableVersion(referencedSchemaName, referredTableName,
                                    metaDbConnection);
                            }
                        }
                    }

                    Map<String, ForeignKeyData> foreignKeys = tableMeta.getForeignKeys();
                    if (!foreignKeys.isEmpty()) {
                        for (Map.Entry<String, ForeignKeyData> e : foreignKeys.entrySet()) {
                            boolean sync = false;
                            for (int i = 0; i < e.getValue().columns.size(); ++i) {
                                String oldColumn = e.getValue().columns.get(i);
                                if (reverseChangeColumns.containsKey(oldColumn)) {
                                    String columnName = reverseChangeColumns.get(oldColumn);
                                    String indexName = e.getValue().indexName;
                                    tableInfoManager
                                        .updateForeignKeyColumn(schemaName, logicalTableName, indexName, columnName,
                                            oldColumn);
                                    sync = true;
                                }
                            }
                            // referencing table -> table
                            if (sync) {
                                Map<String, Set<String>> fkTables =
                                    ForeignKeyUtils.getAllForeignKeyRelatedTables(e.getValue().refSchema,
                                        e.getValue().refTableName);
                                for (Map.Entry<String, Set<String>> entry : fkTables.entrySet()) {
                                    for (String table : entry.getValue()) {
                                        TableInfoManager.updateTableVersion(entry.getKey(), table, metaDbConnection);
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
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
                allChangedColumns.addAll(
                    GeneralUtil.emptyIfNull(changedColumns.stream().map(p -> p.getKey()).collect(Collectors.toList())));
            }
            allChangedColumns.addAll(GeneralUtil.emptyIfNull(addedColumns));
            allChangedColumns.addAll(GeneralUtil.emptyIfNull(updatedColumns));

            Map<String, String> convertedColumnDefaults =
                convertColumnDefaults(phyInfoSchemaContext, allChangedColumns, executionContext);

            tableInfoManager.resetColumnDefaults(schemaName, logicalTableName, allChangedColumns,
                convertedColumnDefaults);
        }

        // Update binary default value as hex string
        if (!specialDefaultValueFlags.isEmpty()) {
            tableInfoManager.updateSpecialColumnDefaults(schemaName, logicalTableName, specialDefaultValues,
                specialDefaultValueFlags);
        }

        // Remove existing index meta.
        if (GeneralUtil.isNotEmpty(droppedIndexes)) {
            tableInfoManager.removeIndexes(schemaName, logicalTableName, droppedIndexes);
        }

        // Rename existing index meta.
        if (GeneralUtil.isNotEmpty(renamedIndexes)) {
            if (onlineModifyColumnIndexTask) {
                Set<String> indexSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                indexSet.addAll(renamedIndexes.stream().map(p -> p.getKey()).collect(Collectors.toSet()));
                indexSet.addAll(renamedIndexes.stream().map(p -> p.getValue()).collect(Collectors.toSet()));
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
            long newSeqCacheSize = executionContext.getParamManager().getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE);
            newSeqCacheSize = newSeqCacheSize < 1 ? 0 : newSeqCacheSize;
            if (sequenceBean.isNew()) {
                tableInfoManager.createSequence(sequenceRecord, newSeqCacheSize,
                    SequenceUtil.buildFailPointInjector(executionContext));
            } else {
                tableInfoManager.alterSequence(sequenceRecord, newSeqCacheSize);
            }
        }

        if (tableComment != null) {
            tableInfoManager.updateTableComment(schemaName, logicalTableName, tableComment);
        }

        if (tableRowFormat != null) {
            tableInfoManager.updateTableRowFormat(schemaName, logicalTableName, tableRowFormat);
        }

        tableInfoManager.updateTableCollation(phyInfoSchemaContext);

        // Add generated column will not be mixed with other alters
        boolean isAddLogicalGeneratedColumn =
            specialDefaultValueFlags.values().stream().anyMatch(l -> l == ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN);

        tableInfoManager.showTable(schemaName, logicalTableName, sequenceRecord,
            !(onlineModifyColumnIndexTask || isAddLogicalGeneratedColumn));
    }

    public static void addForeignKeyMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                         String dbIndex, String phyTableName,
                                         List<ForeignKeyData> addedForeignKeys) {
        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        // Add foreign key.
        try {
            if (GeneralUtil.isNotEmpty(addedForeignKeys)) {
                // create foreign key constraints symbol
                String symbol = ForeignKeyUtils.getForeignKeyConstraintName(schemaName, logicalTableName);

                tableInfoManager.addForeignKeys(phyInfoSchemaContext, addedForeignKeys, symbol);

                // update table meta
                ForeignKeyData data = addedForeignKeys.get(0);
                TableInfoManager.updateTableVersion(data.refSchema, data.refTableName, metaDbConnection);

                Map<String, Set<String>> fkTables =
                    ForeignKeyUtils.getAllForeignKeyRelatedTables(schemaName, logicalTableName);
                for (Map.Entry<String, Set<String>> entry : fkTables.entrySet()) {
                    for (String table : entry.getValue()) {
                        TableInfoManager.updateTableVersion(entry.getKey(), table, metaDbConnection);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropForeignKeyMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                          String dbIndex, String phyTableName,
                                          List<String> droppedForeignKeys) {
        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        // Drop foreign key.
        try {
            if (GeneralUtil.isNotEmpty(droppedForeignKeys)) {
                tableInfoManager.dropForeignKeys(schemaName, logicalTableName, droppedForeignKeys);

                Map<String, Set<String>> fkTables =
                    ForeignKeyUtils.getAllForeignKeyRelatedTables(schemaName, logicalTableName);
                for (Map.Entry<String, Set<String>> entry : fkTables.entrySet()) {
                    for (String table : entry.getValue()) {
                        TableInfoManager.updateTableVersion(entry.getKey(), table, metaDbConnection);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void addLogicalForeignKeyMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                                List<ForeignKeyData> addedForeignKeys) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        // Add logical foreign key.
        try {
            for (ForeignKeyData fk : addedForeignKeys) {
                long pushDown = fk.pushDown |= 2;
                tableInfoManager.updateForeignKeyPushDown(fk.schema, fk.tableName, fk.indexName, pushDown);

                // update table meta
                TableInfoManager.updateTableVersion(fk.refSchema, fk.refTableName, metaDbConnection);

                Map<String, Set<String>> fkTables =
                    ForeignKeyUtils.getAllForeignKeyRelatedTables(schemaName, logicalTableName);
                for (Map.Entry<String, Set<String>> entry : fkTables.entrySet()) {
                    for (String table : entry.getValue()) {
                        TableInfoManager.updateTableVersion(entry.getKey(), table, metaDbConnection);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropLogicalForeignKeyMeta(Connection metaDbConnection, String schemaName,
                                                 String logicalTableName,
                                                 List<ForeignKeyData> droppedForeignKeys) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        // drop logical foreign key.
        try {
            for (ForeignKeyData fk : droppedForeignKeys) {
                long pushDown = fk.pushDown &= 1;
                tableInfoManager.updateForeignKeyPushDown(fk.schema, fk.tableName, fk.indexName, pushDown);

                // update table meta
                TableInfoManager.updateTableVersion(fk.refSchema, fk.refTableName, metaDbConnection);

                Map<String, Set<String>> fkTables =
                    ForeignKeyUtils.getAllForeignKeyRelatedTables(schemaName, logicalTableName);
                for (Map.Entry<String, Set<String>> entry : fkTables.entrySet()) {
                    for (String table : entry.getValue()) {
                        TableInfoManager.updateTableVersion(entry.getKey(), table, metaDbConnection);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public static void addGeneratedColumnMeta(Connection metaDbConnection, String schemaName, String logicalTableName,
                                              String dbIndex, String phyTableName, List<String> sourceExprs,
                                              List<String> targetColumns, List<Pair<String, String>> columnAfterAnother,
                                              boolean requireLogicalColumnOrder,
                                              List<String> addedIndexesWithoutNames) {
        Map<String, String> logicalGeneratedColumnExprs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Long> logicalGeneratedColumnFlags = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < targetColumns.size(); i++) {
            logicalGeneratedColumnExprs.put(targetColumns.get(i), sourceExprs.get(i));
            logicalGeneratedColumnFlags.put(targetColumns.get(i), ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN);
        }

        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        Map<String, Map<String, Object>> columnJdbcExtInfo =
            tableInfoManager.fetchColumnJdbcExtInfo(phyInfoSchemaContext.phyTableSchema,
                phyInfoSchemaContext.phyTableName, phyInfoSchemaContext.dataSource);

        tableInfoManager.addColumns(phyInfoSchemaContext, columnJdbcExtInfo, targetColumns);
        // Hide columns from select, but still eval expression for DML in CN
        tableInfoManager.hideColumns(schemaName, logicalTableName, targetColumns);
        tableInfoManager.updateColumnsStatus(schemaName, ImmutableList.of(logicalTableName), targetColumns,
            ColumnStatus.ABSENT.getValue(), ColumnStatus.WRITE_ONLY.getValue());
        tableInfoManager.updateSpecialColumnDefaults(schemaName, logicalTableName, logicalGeneratedColumnExprs,
            logicalGeneratedColumnFlags);

        refreshColumnOrder(schemaName, logicalTableName, columnAfterAnother, requireLogicalColumnOrder,
            tableInfoManager);

        if (GeneralUtil.isNotEmpty(addedIndexesWithoutNames)) {
            tableInfoManager.addIndexes(phyInfoSchemaContext, Collections.emptyList(), addedIndexesWithoutNames);
        }
    }

    public static void addGeneratedColumnMetaRollback(Connection metaDbConnection, String schemaName,
                                                      String logicalTableName, List<String> targetColumns) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.removeColumns(schemaName, logicalTableName, targetColumns);
        refreshColumnOrder(schemaName, logicalTableName, Collections.emptyList(), true, tableInfoManager);
    }

    public static void changeColumnStatus(Connection metaDbConn, String schema, String table,
                                          List<String> targetColumns, ColumnStatus status) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.changeColumnStatus(schema, table, targetColumns, status);
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
        SequenceBean sequenceBean =
            SequenceMetaChanger.renameSequenceIfExists(metaDbConn, schemaName, tableName, binTableName);
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

    private static void clearPlancache() {
        Set<String> activeSchemas = OptimizerContext.getActiveSchemaNames();
        for (String schema : activeSchemas) {
            OptimizerContext optimizerContext = OptimizerContext.getContext(schema);
            if (optimizerContext != null) {
                // TODO: find more precise way
                SyncManagerHelper.sync(new ClearPlanCacheSyncAction(schema), schema);
            }
        }
    }

    public static void updateArchiveTable(Connection metaDbConnection,
                                          String schemaName, String tableName,
                                          String archiveTableSchema, String archiveTableName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.updateArchiveTable(schemaName, tableName, archiveTableSchema, archiveTableName);
    }
}

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

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gms.TableRuleManager;
import com.alibaba.polardbx.executor.gms.util.SequenceUtil;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.table.TableInfoManager.PhyInfoSchemaContext;

public class TableMetaChanger {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetaChanger.class);

    private static final ConfigManager CONFIG_MANAGER = MetaDbConfigManager.getInstance();

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

    public static void addTableMeta(Connection metaDbConn, PhyInfoSchemaContext phyInfoSchemaContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.addTable(phyInfoSchemaContext);
    }

    public static PhyInfoSchemaContext buildPhyInfoSchemaContext(String schemaName, String logicalTableName,
                                                                 String dbIndex, String phyTableName,
                                                                 SequenceBean sequenceBean,
                                                                 TablesExtRecord tablesExtRecord, boolean isPartitioned,
                                                                 boolean ifNotExists, SqlKind sqlKind,
                                                                 ExecutionContext executionContext) {
        PhyInfoSchemaContext phyInfoSchemaContext =
            CommonMetaChanger.getPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName);

        // Add sequence meta if needed.
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        sequenceBean = SequenceMetaChanger.createSequenceIfExists(schemaName, logicalTableName, sequenceBean,
            tablesExtRecord, isPartitioned, ifNotExists, sqlKind, executionContext);

        if (sequenceBean != null) {
            SystemTableRecord sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
            phyInfoSchemaContext.sequenceRecord = sequenceRecord;
        }

        return phyInfoSchemaContext;
    }

    public static void triggerSchemaChange(Connection metaDbConn, String schemaName, String tableName,
                                           SystemTableRecord sequenceRecord, TableInfoManager tableInfoManager) {
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
        SystemTableRecord sequenceRecord = null;
        SequenceBean sequenceBean = SequenceMetaChanger.dropSequenceIfExists(schemaName, logicalTableName);
        if (sequenceBean != null) {
            sequenceRecord = SequenceUtil.convert(sequenceBean, schemaName, executionContext);
        }
        final SystemTableRecord finalSequenceRecord = sequenceRecord;

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
            SequenceManagerProxy.getInstance().invalidate(schemaName, sequenceBean.getSequenceName());
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
        SystemTableRecord sequenceRecord = null;
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
        SystemTableRecord sequenceRecord = null;
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
                                       boolean columnReorder, List<String> droppedIndexes,
                                       List<String> addedIndexes, List<String> addedIndexesWithoutNames,
                                       Map<String, String> renamedIndexes, boolean primaryKeyDropped,
                                       List<String> addedPrimaryKeyColumns, String tableComment,
                                        SequenceBean sequenceBean,
                                       ExecutionContext executionContext) {
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
        }

        // Add new column meta if exist.
        if (GeneralUtil.isNotEmpty(addedColumns)) {
            tableInfoManager.addColumns(phyInfoSchemaContext, columnJdbcExtInfo, addedColumns);
        }

        // Update existing column meta if exist.
        if (GeneralUtil.isNotEmpty(updatedColumns)) {
            tableInfoManager.updateColumns(phyInfoSchemaContext, columnJdbcExtInfo, updatedColumns);
        }

        // Need to refresh all column to ensure the correct order of column.
        if (columnReorder) {
            tableInfoManager.refreshColumnOrder(phyInfoSchemaContext, columnJdbcExtInfo);
        }

        // Remove existing index meta.
        if (GeneralUtil.isNotEmpty(droppedIndexes)) {
            tableInfoManager.removeIndexes(schemaName, logicalTableName, droppedIndexes);
        }

        // Rename existing index meta.
        if (GeneralUtil.isNotEmpty(renamedIndexes)) {
            tableInfoManager.renameIndexes(schemaName, logicalTableName, renamedIndexes);
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
        SystemTableRecord sequenceRecord = phyInfoSchemaContext.sequenceRecord;
        if (sequenceRecord != null) {
            if (sequenceBean.isNew()) {
                tableInfoManager.createSequence(sequenceRecord);
            } else {
                tableInfoManager.alterSequence(sequenceRecord);
            }
        }

        if (tableComment != null) {
            tableInfoManager.updateTableComment(schemaName, logicalTableName, tableComment);
        }

        tableInfoManager.showTable(schemaName, logicalTableName, sequenceRecord);
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
        SystemTableRecord sequenceRecord = null;
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

    public static void beginAlterColumnDefaultValue(Connection metaDbConn, String schema, String table, String column) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.beginUpdateColumnDefaultVal(schema, table, column);
    }

    public static void endAlterColumnDefaultValue(Connection metaDbConn, String schema, String table, String column) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        tableInfoManager.endUpdateColumnDefaultVal(schema, table, column);
    }
}

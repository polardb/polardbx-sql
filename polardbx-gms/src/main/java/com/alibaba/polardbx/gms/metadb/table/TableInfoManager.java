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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnMappingAccessor;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnMappingRecord;
import com.alibaba.polardbx.gms.metadb.foreign.ForeignAccessor;
import com.alibaba.polardbx.gms.metadb.foreign.ForeignColsAccessor;
import com.alibaba.polardbx.gms.metadb.foreign.ForeignColsRecord;
import com.alibaba.polardbx.gms.metadb.foreign.ForeignRecord;
import com.alibaba.polardbx.gms.metadb.record.RecordConverter;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionAccessor;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupTableDetailAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.tablegroup.TableGroupRecord.TG_TYPE_PARTITION_TBL_TG;

public class TableInfoManager extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableInfoManager.class);

    private final SchemataAccessor schemataAccessor;
    private final TablesAccessor tablesAccessor;
    private final TablesExtAccessor tablesExtAccessor;
    private final ColumnsAccessor columnsAccessor;
    private final IndexesAccessor indexesAccessor;
    private final SequencesAccessor sequencesAccessor;
    private final TablePartitionAccessor tablePartitionAccessor;
    private final TableGroupAccessor tableGroupAccessor;
    private final PartitionGroupAccessor partitionGroupAccessor;
    private final FilesAccessor filesAccessor;
    private final ColumnMetaAccessor columnMetaAccessor;
    private final ForeignAccessor foreignAccessor;
    private final ForeignColsAccessor foreignColsAccessor;

    private final ColumnMappingAccessor columnMappingAccessor;

    private final ColumnEvolutionAccessor columnEvolutionAccessor;
    private final TableLocalPartitionAccessor localPartitionAccessor;
    private final ScheduledJobsAccessor scheduledJobsAccessor;
    private final FiredScheduledJobsAccessor firedScheduledJobsAccessor;
    private final JoinGroupTableDetailAccessor joinGroupTableDetailAccessor;
    private final ColumnarTableMappingAccessor columnarTableMappingAccessor;
    private final ColumnarColumnEvolutionAccessor columnarColumnEvolutionAccessor;
    private final ColumnarTableEvolutionAccessor columnarTableEvolutionAccessor;

    public TableInfoManager() {
        schemataAccessor = new SchemataAccessor();
        tablesAccessor = new TablesAccessor();
        tablesExtAccessor = new TablesExtAccessor();
        columnsAccessor = new ColumnsAccessor();
        indexesAccessor = new IndexesAccessor();
        sequencesAccessor = new SequencesAccessor();
        tablePartitionAccessor = new TablePartitionAccessor();
        tableGroupAccessor = new TableGroupAccessor();
        partitionGroupAccessor = new PartitionGroupAccessor();
        filesAccessor = new FilesAccessor();
        columnMetaAccessor = new ColumnMetaAccessor();
        columnMappingAccessor = new ColumnMappingAccessor();
        columnEvolutionAccessor = new ColumnEvolutionAccessor();
        localPartitionAccessor = new TableLocalPartitionAccessor();
        scheduledJobsAccessor = new ScheduledJobsAccessor();
        firedScheduledJobsAccessor = new FiredScheduledJobsAccessor();
        joinGroupTableDetailAccessor = new JoinGroupTableDetailAccessor();
        columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
        columnarTableEvolutionAccessor = new ColumnarTableEvolutionAccessor();
        columnarColumnEvolutionAccessor = new ColumnarColumnEvolutionAccessor();
        foreignAccessor = new ForeignAccessor();
        foreignColsAccessor = new ForeignColsAccessor();
    }

    private static final String SQL_UPDATE_TABLE_VERSION = "UPDATE "
        + GmsSystemTables.TABLES
        + " SET VERSION=last_insert_id(VERSION+1) WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

    private static final String SQL_UPDATE_TABLE_NEW_VERSION = "UPDATE "
        + GmsSystemTables.TABLES
        + " SET VERSION=? WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

    private static final String SQL_SELECT_TABLE_VERSION = "SELECT version from "
        + GmsSystemTables.TABLES
        + " WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

    private static final String SQL_UPDATE_TABLE_EXT_NEW_VERSION = "UPDATE "
        + GmsSystemTables.TABLES_EXT
        + " SET VERSION=? WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

    private static final String SQL_SELECT_TABLE_EXT_VERSION = "SELECT version from "
        + GmsSystemTables.TABLES_EXT
        + " WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

    public static long updateTableVersion(String schema, String table, Connection conn) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(SQL_UPDATE_TABLE_VERSION)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, table);
            pstmt.executeUpdate();
        }

        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schema);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, table);
        DdlMetaLogUtil.logSql(SQL_UPDATE_TABLE_VERSION, params);

        long newVersion;
        try (PreparedStatement pstmt = conn.prepareStatement("select last_insert_id()")) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            newVersion = rs.getLong(1);
        }

        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getTableDataId(schema, table), conn);
        return newVersion;
    }

    public static long updateTableVersionWithoutDataId(String schema, String table, Connection conn)
        throws SQLException {
        return updateTableVersion4Repartition(schema, table, conn);
    }

    public static long updateTableVersion4Repartition(String schema, String table, Connection conn)
        throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(SQL_UPDATE_TABLE_VERSION)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, table);
            pstmt.executeUpdate();
        }

        DdlMetaLogUtil.logSql(SQL_UPDATE_TABLE_VERSION);

        long newVersion;
        try (PreparedStatement pstmt = conn.prepareStatement("select last_insert_id()")) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            newVersion = rs.getLong(1);
        }

        return newVersion;
    }

    public static long updateTableVersion4Rename(String schema, String table, long version, Connection conn)
        throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(SQL_UPDATE_TABLE_NEW_VERSION)) {
            pstmt.setLong(1, version);
            pstmt.setString(2, schema);
            pstmt.setString(3, table);
            pstmt.executeUpdate();
        }

        DdlMetaLogUtil.logSql(SQL_UPDATE_TABLE_NEW_VERSION);

        return version;
    }

    public static long getTableVersion4Rename(String schema, String table, Connection conn)
        throws SQLException {
        long newVersion;
        try (PreparedStatement pstmt = conn.prepareStatement(SQL_SELECT_TABLE_VERSION)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, table);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                newVersion = rs.getLong(1);
            } else {
                newVersion = 1;
            }
        }

        DdlMetaLogUtil.logSql(SQL_SELECT_TABLE_VERSION);

        return newVersion;
    }

    public static long updateTableExtVersion4Rename(String schema, String table, long version, Connection conn)
        throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(SQL_UPDATE_TABLE_EXT_NEW_VERSION)) {
            pstmt.setLong(1, version);
            pstmt.setString(2, schema);
            pstmt.setString(3, table);
            pstmt.executeUpdate();
        }

        DdlMetaLogUtil.logSql(SQL_UPDATE_TABLE_EXT_NEW_VERSION);

        return version;
    }

    public static long getTableExtVersion4Rename(String schema, String table, Connection conn)
        throws SQLException {
        long newVersion;
        try (PreparedStatement pstmt = conn.prepareStatement(SQL_SELECT_TABLE_EXT_VERSION)) {
            pstmt.setString(1, schema);
            pstmt.setString(2, table);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                newVersion = rs.getLong(1);
            } else {
                newVersion = 1;
            }
        }

        DdlMetaLogUtil.logSql(SQL_SELECT_TABLE_EXT_VERSION);

        return newVersion;
    }

    @Override
    public void setConnection(Connection connection) {
        super.setConnection(connection);
        schemataAccessor.setConnection(connection);
        tablesAccessor.setConnection(connection);
        tablesExtAccessor.setConnection(connection);
        columnsAccessor.setConnection(connection);
        indexesAccessor.setConnection(connection);
        sequencesAccessor.setConnection(connection);
        tablePartitionAccessor.setConnection(connection);
        tableGroupAccessor.setConnection(connection);
        partitionGroupAccessor.setConnection(connection);
        filesAccessor.setConnection(connection);
        columnMetaAccessor.setConnection(connection);
        columnMappingAccessor.setConnection(connection);
        columnEvolutionAccessor.setConnection(connection);
        localPartitionAccessor.setConnection(connection);
        scheduledJobsAccessor.setConnection(connection);
        firedScheduledJobsAccessor.setConnection(connection);
        joinGroupTableDetailAccessor.setConnection(connection);
        columnarTableMappingAccessor.setConnection(connection);
        columnarTableEvolutionAccessor.setConnection(connection);
        columnarColumnEvolutionAccessor.setConnection(connection);
        foreignAccessor.setConnection(connection);
        foreignColsAccessor.setConnection(connection);
    }

    public String getDefaultDbIndex(String schemaName) {
        SchemataRecord record = schemataAccessor.query(schemaName);
        return record != null ? record.defaultDbIndex : null;
    }

    /**
     * Retrieve default group for all schema
     *
     * @return map of schema and group_name
     */
    public static Map<String, String> getAllDefaultDbIndex() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(true);
            return getAllDefaultDbIndex(metaDbConn);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    public static Map<String, String> getAllDefaultDbIndex(Connection metaDbConn) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);
        return tableInfoManager.getAllSchemaDefaultDbIndex();
    }

    public List<TablesExtRecord> queryVisibleTableExts(String tableSchema) {
        List<TablesExtRecord> visibleRecords = new ArrayList<>();
        List<TablesExtRecord> records = queryTableExts(tableSchema);
        for (TablesExtRecord record : records) {
            if (record.status == TableStatus.PUBLIC.getValue()) {
                visibleRecords.add(record);
            }
        }
        return visibleRecords;
    }

    public List<TablesExtRecord> queryTableExts(String tableSchema) {
        return tablesExtAccessor.query(tableSchema);
    }

    public TablesExtRecord queryVisibleTableExt(String tableSchema, String tableName, boolean isNewTableForRename) {
        TablesExtRecord record = queryTableExt(tableSchema, tableName, isNewTableForRename);
        return record != null && record.status == TableStatus.PUBLIC.getValue() ? record : null;
    }

    public TablesExtRecord queryTableExt(String tableSchema, String tableName, boolean isNewTableForRename) {
        return tablesExtAccessor.query(tableSchema, tableName, isNewTableForRename);
    }

    public List<TablesRecord> queryVisibleTables(String tableSchema) {
        List<TablesRecord> visibleRecords = new ArrayList<>();
        List<TablesRecord> records = queryTables(tableSchema);
        for (TablesRecord record : records) {
            if (record.status == TableStatus.PUBLIC.getValue()) {
                visibleRecords.add(record);
            }
        }
        return visibleRecords;
    }

    public List<TableNamesRecord> queryVisibleTableNames(String tableSchema) {
        List<TableNamesRecord> records = tablesAccessor.queryVisibleTableNames(tableSchema);
        return records;
    }

    public List<TablesRecord> queryTables(String tableSchema) {
        return tablesAccessor.query(tableSchema);
    }

    public Set<String> queryTablesName(String tableSchema) {
        Set<String> tablesName = new TreeSet<>(String::compareToIgnoreCase);
        List<TablesRecord> records = queryTables(tableSchema);
        for (TablesRecord record : records) {
            tablesName.add(record.tableName);
        }
        return tablesName;
    }

    public List<IndexesRecord> queryColumnarIndexes(String tableSchema) {
        List<IndexesRecord> indexesRecords = indexesAccessor.query(tableSchema);
        List<IndexesRecord> columnarIndexes = new ArrayList<>();
        for (IndexesRecord indexesRecord : indexesRecords) {
            if (indexesRecord.isColumnar()) {
                columnarIndexes.add(indexesRecord);
            }
        }
        return columnarIndexes;
    }

    public TablesRecord queryVisibleTable(String tableSchema, String tableName, boolean isNewTableForRename) {
        TablesRecord record = queryTable(tableSchema, tableName, isNewTableForRename);
        return record != null && record.status == TableStatus.PUBLIC.getValue() ? record : null;
    }

    public TablesRecord queryTable(String tableSchema, String tableName, boolean isNewTableForRename) {
        return tablesAccessor.query(tableSchema, tableName, isNewTableForRename);
    }

    public TablesRecord queryTable(long tableId) {
        return tablesAccessor.query(tableId);
    }

    public List<TablesRecord> queryTablesByEngine(String engine) {
        return tablesAccessor.queryByEngine(engine);
    }

    public List<TablesRecord> queryTablesByEngineAndTableType(String engine, String tableType) {
        return tablesAccessor.queryByEngineAndTableType(engine, tableType);
    }

    public List<ColumnsRecord> queryVisibleColumns(String tableSchema, String tableName) {
        List<ColumnsRecord> visibleRecords = new ArrayList<>();
        List<ColumnsRecord> records = queryColumns(tableSchema, tableName);
        for (ColumnsRecord record : records) {
            if (record.status != ColumnStatus.ABSENT.getValue()) {
                visibleRecords.add(record);
            }
        }
        return visibleRecords;
    }

    public Map<String, List<ColumnsRecord>> queryVisibleColumns(String tableSchema) {
        Map<String, List<ColumnsRecord>> visibleRecords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<ColumnsRecord> records = queryColumns(tableSchema);
        for (ColumnsRecord record : records) {
            if (record.status != ColumnStatus.ABSENT.getValue()) {
                visibleRecords.computeIfAbsent(record.tableName, k -> new ArrayList<>()).add(record);
            }
        }
        return visibleRecords;
    }

    public List<ColumnsRecord> queryColumns(String tableSchema) {
        return columnsAccessor.query(tableSchema);
    }

    public List<ColumnsRecord> queryOneColumn(String tableSchema, String tableName, String columnName) {
        return columnsAccessor.query(tableSchema, tableName, columnName);
    }

    public List<ColumnsRecord> queryColumns(String tableSchema, String tableName) {
        return columnsAccessor.query(tableSchema, tableName);
    }

    public Map<String, List<ColumnMappingRecord>> queryColumnMappings(String tableSchema) {
        Map<String, List<ColumnMappingRecord>> visibleRecords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<ColumnMappingRecord> records = columnMappingAccessor.querySchema(tableSchema);
        for (ColumnMappingRecord record : records) {
            visibleRecords.computeIfAbsent(record.tableName, k -> new ArrayList<>()).add(record);
        }
        return visibleRecords;
    }

    public List<ColumnMappingRecord> queryColumnMappings(String tableSchema, String tableName) {
        return columnMappingAccessor.querySchemaTable(tableSchema, tableName);
    }

    public List<IndexesRecord> queryVisibleIndexes(String tableSchema, String tableName) {
        List<IndexesRecord> visibleRecords = new ArrayList<>();
        List<IndexesRecord> records = queryIndexes(tableSchema, tableName);
        for (IndexesRecord record : records) {
            if (record.indexStatus == IndexStatus.PUBLIC.getValue()) {
                visibleRecords.add(record);
            }
        }
        return visibleRecords;
    }

    public Map<String, List<IndexesRecord>> queryVisibleIndexes(String tableSchema) {
        Map<String, List<IndexesRecord>> visibleRecords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<IndexesRecord> records = queryIndexes(tableSchema);
        for (IndexesRecord record : records) {
            if (record.indexStatus == IndexStatus.PUBLIC.getValue()) {
                visibleRecords.computeIfAbsent(record.tableName, k -> new ArrayList<>()).add(record);
            }
        }
        return visibleRecords;
    }

    public List<IndexesRecord> queryIndexes(String tableSchema) {
        return indexesAccessor.query(tableSchema);
    }

    public List<IndexesRecord> queryIndexes(String tableSchema, String tableName) {
        return indexesAccessor.query(tableSchema, tableName);
    }

    public Map<String, List<ForeignRecord>> queryReferencedForeignKeys(String tableSchema) {
        Map<String, List<ForeignRecord>> allReferencedFkRecords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<ForeignRecord> records = foreignAccessor.queryReferencedForeignKeys(tableSchema);
        for (ForeignRecord record : records) {
            allReferencedFkRecords.computeIfAbsent(record.refTableName, k -> new ArrayList<>()).add(record);
        }
        return allReferencedFkRecords;
    }

    public List<ForeignRecord> queryReferencedForeignKeys(String tableSchema, String tableName) {
        return foreignAccessor.queryReferencedForeignKeys(tableSchema, tableName);
    }

    public Map<String, List<ForeignRecord>> queryForeignKeys(String tableSchema) {
        Map<String, List<ForeignRecord>> allFkRecords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<ForeignRecord> records = foreignAccessor.queryForeignKeys(tableSchema);
        for (ForeignRecord record : records) {
            allFkRecords.computeIfAbsent(record.tableName, k -> new ArrayList<>()).add(record);
        }
        return allFkRecords;
    }

    public List<ForeignRecord> queryReferencedTable(String tableSchema, String tableName) {
        return foreignAccessor.queryRefTable(tableSchema, tableName);
    }

    public List<ForeignRecord> queryForeignKeys(String tableSchema, String tableName) {
        return foreignAccessor.queryForeignKeys(tableSchema, tableName);
    }

    public int[] insertForeignKeyCols(List<ForeignColsRecord> foreignColsRecords) {
        return foreignColsAccessor.insert(foreignColsRecords);
    }

    public int deleteForeignKeyCols(String tableSchema, String tableName, String indexName,
                                    List<String> columnNames) {
        return foreignColsAccessor.deleteForeignKeyCols(tableSchema, tableName, indexName, columnNames);
    }

    public List<ForeignColsRecord> queryForeignKeysCols(String tableSchema, String tableName, String indexName) {
        return foreignColsAccessor.queryForeignKeyCols(tableSchema, tableName, indexName);
    }

    public int updateForeignKeyTable(String tableSchema, String tableName, String indexName, String constraintName,
                                     String originalTableName) {
        return foreignAccessor.updateForeignKeyTable(tableSchema, tableName, indexName, constraintName,
            originalTableName);
    }

    public int updateForeignKeyColsTable(String tableSchema, String tableName, String originalTableName) {
        return foreignColsAccessor.updateForeignKeyColsTable(tableSchema, tableName, originalTableName);
    }

    public int updateForeignKeyColumn(String tableSchema, String tableName, String indexName, String columnName,
                                      String originColumnName) {
        return foreignColsAccessor.updateForeignKeyColumn(tableSchema, tableName, indexName, columnName,
            originColumnName);
    }

    public int updateReferencedForeignKeyTable(String tableSchema, String tableName, String newTableName) {
        return foreignAccessor.updateReferencedForeignKeyTable(tableSchema, tableName, newTableName);
    }

    public int updateReferencedForeignKeyColumn(String refTableSchema, String tableName, String indexName,
                                                String columnName,
                                                String originColumnName) {
        return foreignColsAccessor.updateReferencedForeignKeyColumn(refTableSchema, tableName, indexName, columnName,
            originColumnName);
    }

    public int updateForeignKeyPushDown(String tableSchema, String tableName, String indexName, long pushDown) {
        return foreignAccessor.updateForeignKeyPushDown(tableSchema, tableName, indexName, pushDown);
    }

    public List<IndexesRecord> queryIndexes(String tableSchema, String tableName, String indexName) {
        return indexesAccessor.query(tableSchema, tableName, indexName);
    }

    public List<TablePartitionRecord> queryTablePartitions(String tableSchema, String tableName, boolean fromDelta) {
        return tablePartitionAccessor.getTablePartitionsByDbNameTbName(tableSchema, tableName, fromDelta);
    }

    public List<TablePartitionRecord> queryTablePartitions(String tableSchema, boolean fromDelta) {
        return tablePartitionAccessor.getTablePartitionsByDbNameTbName(tableSchema, null, fromDelta);
    }

    public void alterTablePartitionName(String tableSchema, String originName, String newName) {
        tablePartitionAccessor.rename(tableSchema, originName, newName);
    }

    public void updateTablePartitionVersion(String tableSchema, String tableName, long newVersion) {
        tablePartitionAccessor.updateVersion(tableSchema, tableName, newVersion);
    }

    public boolean checkIfIndexExists(String tableSchema, String tableName, String indexName) {
        return indexesAccessor.checkIfExists(tableSchema, tableName, indexName);
    }

    public boolean checkIfColumnarIndexExists(String tableSchema, String tableName) {
        return indexesAccessor.checkIfColumnarExists(tableSchema, tableName);
    }

    public long getColumnarIndexNum(String tableSchema, String tableName) {
        return indexesAccessor.getColumnarIndexNum(tableSchema, tableName);
    }

    public boolean checkIfTableExists(String tableSchema, String tableName) {
        TablesRecord tablesRecord = tablesAccessor.query(tableSchema, tableName, false);
        TablesExtRecord tablesExtRecord = tablesExtAccessor.query(tableSchema, tableName, false);
        if (tablesRecord != null && tablesRecord.status == TableStatus.PUBLIC.getValue() &&
            tablesExtRecord != null && tablesExtRecord.status == TableStatus.PUBLIC.getValue()) {
            return true;
        }
        List<TablePartitionRecord> tablePartitionsRows =
            tablePartitionAccessor.getTablePartitionsByDbNameTbName(tableSchema, tableName, false);
        if (tablesRecord != null && tablePartitionsRows.size() > 0) {
            return true;
        }
        return false;
    }

    public boolean checkIfTableExistsWithAnyStatus(String tableSchema, String tableName) {
        TablesRecord tablesRecord = tablesAccessor.query(tableSchema, tableName, false);
        TablesExtRecord tablesExtRecord = tablesExtAccessor.query(tableSchema, tableName, false);
        return tablesRecord != null || tablesExtRecord != null;
    }

    public int addTableExt(TablesExtRecord record) {
        return tablesExtAccessor.insert(record);
    }

    public int removeTableExt(String tableSchema, String tableName) {
        return tablesExtAccessor.delete(tableSchema, tableName);
    }

    public int updateTableExt(TablesExtRecord record) {
        return tablesExtAccessor.alter(record);
    }

    public int alterTableType(String tableSchema, String targetTableName, int tableType) {
        return tablesExtAccessor.alterTableType(tableSchema, targetTableName, tableType);
    }

    public int alterTableExtName(String tableSchema, String originName, String newName) {
        return tablesExtAccessor.alterName(tableSchema, originName, newName);
    }

    public void addShardColumns4RepartitionKey(String tableSchema, String tableName, List<String> changeShardColumns) {
        tablePartitionAccessor.addColumnForPartExprAndPartDesc(tableSchema, tableName, changeShardColumns);
    }

    public int alterTableExtNameAndTypeAndFlag(String tableSchema, String originName, String newName, int tableType,
                                               long flag) {
        indexesAccessor.renameLocalIndexes(tableSchema, originName, newName);
        return tablesExtAccessor.alterNameAndTypeAndFlag(tableSchema, originName, newName, tableType, flag);
    }

    public void repartitionCutOver(String tableSchema, String originName, String newName, int tableType) {
        tablePartitionAccessor.alterNameAndType(tableSchema, originName, newName, tableType);
        indexesAccessor.renameLocalIndexes(tableSchema, originName, newName);
    }

    public void alterPartitionCountCutOver(String tableSchema, String originIndexTableName, String newIndexTableName) {
        indexesAccessor.cutOverGlobalIndex(tableSchema, originIndexTableName, newIndexTableName);
    }

    public void alterModifyColumnCutOver(String tableSchema, String originTableName, String newTableName) {
        columnsAccessor.rename(tableSchema, originTableName, newTableName);
    }

    public void changeTablePartitionsPartFlag(String tableSchema, String tableName, Long partFlag) {
        tablePartitionAccessor.updateTablePartitionsPartFlag(tableSchema, tableName, partFlag);
    }

    public int alterTableExtFlag(String tableSchema, String tableName, long flag) {
        return tablesExtAccessor.alterTableExtFlag(tableSchema, tableName, flag);
    }

    public List<ColumnsRecord> addColumnarTable(String schemaName, String primaryTableName, String columnarTableName,
                                                Engine engine) {
        // Build Table Meta
        TablesRecord tablesRecord = new TablesRecord();
        tablesRecord.tableSchema = schemaName;
        tablesRecord.tableName = columnarTableName;
        tablesRecord.tableType = "COLUMNAR TABLE";
        tablesRecord.engine = engine.name();
        tablesRecord.version = 0;
        tablesRecord.rowFormat = "Dynamic";
        tablesRecord.tableRows = 0;
        tablesRecord.avgRowLength = 0;
        tablesRecord.dataLength = 0;
        tablesRecord.maxDataLength = 0;
        tablesRecord.indexLength = 0;
        tablesRecord.dataFree = 0;
        tablesRecord.autoIncrement = 0;
        tablesRecord.tableCollation = "utf8mb4_general_ci";
        tablesRecord.checkSum = 0;
        tablesRecord.createOptions = "";
        tablesRecord.tableComment = "THIS IS A COLUMNAR TABLE";

        tablesAccessor.insert(tablesRecord);

        // Fill primary table column meta into columnar table
        List<ColumnsRecord> columnsRecords = columnsAccessor.query(schemaName, primaryTableName);
        for (ColumnsRecord columnsRecord : columnsRecords) {
            columnsRecord.tableName = columnarTableName;
        }
        columnsAccessor.insert(columnsRecords, schemaName, columnarTableName);

        // No index meta for columnar table

        return columnsRecords;
    }

    public ColumnarTableMappingRecord addCreateCciSchemaEvolutionMeta(String schemaName,
                                                                      String primaryTableName,
                                                                      String columnarTableName,
                                                                      Map<String, String> options,
                                                                      long versionId,
                                                                      long ddlJobId) {

        final List<ColumnsRecord> columnRecords = columnsAccessor.query(schemaName, columnarTableName);

        return addCreateCciSchemaEvolutionMeta(schemaName,
            primaryTableName,
            columnarTableName,
            options,
            versionId,
            ddlJobId,
            columnRecords);
    }

    public ColumnarTableMappingRecord addCreateCciSchemaEvolutionMeta(String schemaName,
                                                                      String primaryTableName,
                                                                      String columnarTableName,
                                                                      Map<String, String> options,
                                                                      long versionId,
                                                                      long ddlJobId,
                                                                      List<ColumnsRecord> columnsRecords) {
        final ColumnarTableMappingRecord tableMappingRecord = addColumnarTableMappingRecord(schemaName,
            primaryTableName,
            columnarTableName,
            versionId,
            ColumnarTableStatus.CREATING);
        long tableId = tableMappingRecord.tableId;

        // Insert column evolution records
        addCciColumnEvolutionRecords(columnsRecords, tableId, versionId, ddlJobId);

        // Insert table evolution record
        addCreateCciTableEvolutionRecord(tableMappingRecord, options, versionId, ddlJobId, DdlType.CREATE_INDEX);

        return tableMappingRecord;
    }

    public ColumnarTableMappingRecord addColumnarTableMappingRecord(String schemaName, String primaryTableName,
                                                                    String columnarTableName,
                                                                    long versionId,
                                                                    ColumnarTableStatus status) {
        final ColumnarTableMappingRecord tableMappingRecord = new ColumnarTableMappingRecord(schemaName,
            primaryTableName,
            columnarTableName,
            versionId,
            status.name());
        columnarTableMappingAccessor.insert(ImmutableList.of(tableMappingRecord));

        return columnarTableMappingAccessor
            .querySchemaTableIndex(schemaName, primaryTableName, columnarTableName)
            .get(0);
    }

    public void addCciColumnEvolutionRecords(List<ColumnsRecord> columnsRecords, long tableId, long versionId,
                                             long ddlJobId) {
        // Insert column evolution records
        List<ColumnarColumnEvolutionRecord> columnEvolutionRecords = new ArrayList<>(columnsRecords.size());
        for (ColumnsRecord columnsRecord : columnsRecords) {
            columnEvolutionRecords.add(
                new ColumnarColumnEvolutionRecord(tableId, columnsRecord.columnName,
                    versionId, ddlJobId, columnsRecord));
        }
        columnarColumnEvolutionAccessor.insert(columnEvolutionRecords);
        columnarColumnEvolutionAccessor.updateFieldIdAsId(tableId, versionId);
    }

    public void addCciTableEvolutionRecord(String schemaName, String primaryTableName,
                                           String columnarTableName,
                                           Map<String, String> options,
                                           long tableId, long versionId,
                                           long ddlJobId, DdlType ddlType,
                                           List<Long> columns) {
        final ColumnarTableEvolutionRecord columnarTableEvolutionRecord = new ColumnarTableEvolutionRecord(
            versionId,
            tableId,
            schemaName,
            primaryTableName,
            columnarTableName,
            options,
            ddlJobId,
            ddlType.name(),
            Long.MAX_VALUE,
            columns);

        columnarTableEvolutionAccessor.insert(ImmutableList.of(columnarTableEvolutionRecord));
    }

    public void addCciTableEvolutionRecord(ColumnarTableMappingRecord tableMappingRecord, long versionId, long ddlJobId,
                                           DdlType ddlType, List<Long> columns, Map<String, String> options) {
        addCciTableEvolutionRecord(tableMappingRecord.tableSchema,
            tableMappingRecord.tableName,
            tableMappingRecord.indexName,
            options,
            tableMappingRecord.tableId,
            versionId,
            ddlJobId,
            ddlType,
            columns);
    }

    /**
     * Add cci table evolution record with all columns in ColumnarColumnEvolution.
     * For create cci use only.
     */
    private void addCreateCciTableEvolutionRecord(ColumnarTableMappingRecord tableMappingRecord,
                                                  Map<String, String> options,
                                                  long versionId,
                                                  long ddlJobId,
                                                  DdlType ddlType) {
        final long tableId = tableMappingRecord.tableId;

        // Get field id, should be in order
        final List<ColumnarColumnEvolutionRecord> columnEvolutionRecords =
            columnarColumnEvolutionAccessor.queryTableIdVersionIdOrderById(tableId, versionId);
        final List<Long> columns = columnEvolutionRecords.stream().map(r -> r.id).collect(Collectors.toList());

        addCciTableEvolutionRecord(tableMappingRecord, versionId, ddlJobId, ddlType, columns, options);
    }

    /**
     * Add cci table evolution record with columns same as latest table evolution record
     * For schema change actions not modify column set of cci.
     */
    public ColumnarTableMappingRecord addCciTableEvolutionRecordWithLatestColumns(String schemaName,
                                                                                  String primaryTableName,
                                                                                  String columnarTableName,
                                                                                  long versionId,
                                                                                  long ddlJobId,
                                                                                  DdlType ddlType) {
        // Get table mapping record
        final ColumnarTableMappingRecord tableMappingRecord =
            columnarTableMappingAccessor.querySchemaTableIndex(schemaName, primaryTableName, columnarTableName).get(0);

        final long tableId = tableMappingRecord.tableId;
        final long latestVersionId = tableMappingRecord.latestVersionId;
        final ColumnarTableEvolutionRecord latestTableEvolution =
            queryColumnarTableEvolution(tableId, latestVersionId).get(0);

        addCciTableEvolutionRecord(tableMappingRecord, versionId, ddlJobId, ddlType,
            latestTableEvolution.columns, latestTableEvolution.options);

        return tableMappingRecord;
    }

    /**
     * Add cci table evolution record with columns same as specified table evolution record
     * For schema change actions not modify column set of cci.
     */
    public ColumnarTableMappingRecord addCciTableEvolutionRecordWithSpecifiedColumns(
        @NotNull ColumnarTableMappingRecord fromTableMappingRecord,
        long versionId,
        long ddlJobId,
        DdlType ddlType) {
        final long tableId = fromTableMappingRecord.tableId;
        final long fromVersionId = fromTableMappingRecord.latestVersionId;
        final ColumnarTableEvolutionRecord fromTableEvolution =
            queryColumnarTableEvolution(tableId, fromVersionId).get(0);

        addCciTableEvolutionRecord(fromTableMappingRecord, versionId, ddlJobId, ddlType,
            fromTableEvolution.columns, fromTableEvolution.options);

        return fromTableMappingRecord;
    }

    public void addDropCciSchemaEvolutionMeta(String schemaName, String primaryTableName, String columnarTableName,
                                              long versionId, long ddlJobId) {
        final ColumnarTableMappingRecord tableMappingRecord = addDropCciTableEvolutionRecord(
            schemaName,
            primaryTableName,
            columnarTableName,
            versionId,
            ddlJobId);

        updateStatusByTableId(
            tableMappingRecord.tableId,
            ColumnarTableStatus.DROP.name(),
            versionId);
    }

    public void addDropCciSchemaEvolutionMeta(@NotNull ColumnarTableMappingRecord tableMappingRecord,
                                              long dropCciVersionId,
                                              long ddlJobId) {
        addDropCciTableEvolutionRecord(
            tableMappingRecord,
            dropCciVersionId,
            ddlJobId);

        updateStatusByTableId(
            tableMappingRecord.tableId,
            ColumnarTableStatus.DROP.name(),
            dropCciVersionId);
    }

    public ColumnarTableMappingRecord addDropCciTableEvolutionRecord(String schemaName, String primaryTableName,
                                                                     String columnarTableName, long versionId,
                                                                     long ddlJobId) {
        return addCciTableEvolutionRecordWithLatestColumns(schemaName,
            primaryTableName,
            columnarTableName,
            versionId,
            ddlJobId,
            DdlType.DROP_INDEX);
    }

    public ColumnarTableMappingRecord addDropCciTableEvolutionRecord(
        @NotNull ColumnarTableMappingRecord tableMappingRecord,
        long versionId,
        long ddlJobId) {
        return addCciTableEvolutionRecordWithSpecifiedColumns(
            tableMappingRecord,
            versionId,
            ddlJobId,
            DdlType.DROP_INDEX);
    }

    public Set<Pair<Long, String>> queryCci(String schemaName, String primaryTableName) {
        // set of <tableId, indexName>
        List<ColumnarTableMappingRecord> columnarTableMappingRecords =
            columnarTableMappingAccessor.querySchemaTable(schemaName, primaryTableName);
        Set<Pair<Long, String>> indexes =
            columnarTableMappingRecords.stream()
                .filter(record -> !record.status.equals(ColumnarTableStatus.DROP.name()))
                .map(record -> new Pair<>(record.tableId, record.indexName))
                .collect(Collectors.toSet());
        return indexes;
    }

    public void alterColumnarTableColumns(String schemaName, String primaryTableName, Set<Pair<Long, String>> indexes,
                                          long versionId, long ddlJobId, DdlType ddlType,
                                          List<Pair<String, String>> changeColumns) {
        for (Pair<Long, String> index : indexes) {
            Long tableId = index.getKey();
            String indexName = index.getValue();
            final ColumnarTableMappingRecord tableMapping =
                columnarTableMappingAccessor.querySchemaTableIndex(schemaName, primaryTableName, indexName).get(0);
            if (tableMapping.status.equals(ColumnarTableStatus.DROP.name())) {
                continue;
            }
            long latestVersionId = tableMapping.latestVersionId;

            // Fill primary table column meta into columns system table
            List<ColumnsRecord> columnsRecords = columnsAccessor.query(schemaName, primaryTableName);

            // Insert column evolution records
            ColumnarTableEvolutionRecord columnarTableEvolutionRecord =
                columnarTableEvolutionAccessor.queryByVersionIdLatest(latestVersionId).get(0);
            List<Long> columns =
                reorderColumns(columnarTableEvolutionRecord, columnsRecords, versionId, ddlJobId,
                    indexName, changeColumns);

            columnarTableEvolutionRecord =
                new ColumnarTableEvolutionRecord(versionId, tableId, schemaName, primaryTableName, indexName,
                    columnarTableEvolutionRecord.options,
                    ddlJobId, ddlType.name(), Long.MAX_VALUE, columns);

            columnarTableEvolutionAccessor.insert(ImmutableList.of(columnarTableEvolutionRecord));
            columnarTableMappingAccessor.updateVersionId(versionId, tableId);
        }
    }

    public void dropColumnarTableColumns(String schemaName, String primaryTableName, Set<Pair<Long, String>> indexes,
                                         List<String> droppedColumns, long versionId, long ddlJobId) {
        if (GeneralUtil.isEmpty(droppedColumns)) {
            return;
        }

        for (Pair<Long, String> index : indexes) {
            Long tableId = index.getKey();
            String indexName = index.getValue();
            final ColumnarTableMappingRecord tableMapping =
                columnarTableMappingAccessor.querySchemaTableIndex(schemaName, primaryTableName, indexName).get(0);
            if (tableMapping.status.equals(ColumnarTableStatus.DROP.name())) {
                continue;
            }
            long latestVersionId = tableMapping.latestVersionId;

            columnsAccessor.delete(schemaName, indexName, droppedColumns);

            List<ColumnarColumnEvolutionRecord> columnarColumnEvolutionRecords =
                columnarColumnEvolutionAccessor.queryTableIdAndNotInStatus(tableId, ColumnStatus.ABSENT.getValue());

            for (ColumnarColumnEvolutionRecord columnarColumnEvolutionRecord : columnarColumnEvolutionRecords) {
                if (droppedColumns.contains(columnarColumnEvolutionRecord.columnName)) {

                    columnarColumnEvolutionRecord.columnsRecord.status = ColumnStatus.ABSENT.getValue();
                    ColumnarColumnEvolutionRecord columnEvolutionRecord =
                        new ColumnarColumnEvolutionRecord(tableId, columnarColumnEvolutionRecord.fieldId,
                            columnarColumnEvolutionRecord.columnName,
                            versionId, ddlJobId, columnarColumnEvolutionRecord.columnsRecord);
                    columnarColumnEvolutionAccessor.insert(ImmutableList.of(columnEvolutionRecord));
                }
            }

            List<ColumnsRecord> columnsRecords = columnsAccessor.query(schemaName, primaryTableName);

            ColumnarTableEvolutionRecord columnarTableEvolutionRecord =
                columnarTableEvolutionAccessor.queryByVersionIdLatest(latestVersionId).get(0);
            List<Long> columns =
                reorderColumns(columnarTableEvolutionRecord, columnsRecords, versionId, ddlJobId,
                    indexName, new ArrayList<>());

            columnarTableEvolutionRecord =
                new ColumnarTableEvolutionRecord(versionId, tableId, schemaName, primaryTableName, indexName,
                    columnarTableEvolutionRecord.options,
                    ddlJobId, DdlType.ALTER_TABLE_DROP_COLUMN.name(), Long.MAX_VALUE, columns);

            columnarTableEvolutionAccessor.insert(ImmutableList.of(columnarTableEvolutionRecord));

            columnarTableMappingAccessor.updateVersionId(versionId, tableId);
        }
    }

    public void changeColumnarIndexTableColumns(List<String> indexTableNames, String schemaName,
                                                String primaryTableName, List<Pair<String, String>> changeColumns) {
        // for alter table change column -> rename column
        Map<String, String> changeColumnsMap = new HashMap<>();
        for (Pair<String, String> changeColumn : changeColumns) {
            // <new name, old name>
            changeColumnsMap.put(changeColumn.getKey(), changeColumn.getValue());
        }

        for (String indexTableName : indexTableNames) {
            final ColumnarTableMappingRecord tableMapping =
                columnarTableMappingAccessor.querySchemaTableIndex(schemaName, primaryTableName, indexTableName).get(0);
            if (tableMapping.status.equals(ColumnarTableStatus.DROP.name())) {
                continue;
            }
            long latestVersionId = tableMapping.latestVersionId;

            ColumnarTableEvolutionRecord columnarTableEvolutionRecord =
                columnarTableEvolutionAccessor.queryByVersionIdLatest(latestVersionId).get(0);

            List<ColumnsRecord> columnsRecords = columnsAccessor.query(schemaName, primaryTableName);

            List<ColumnarColumnEvolutionRecord> columnarColumnEvolutionRecords =
                columnarColumnEvolutionAccessor.queryIdsWithOrder(columnarTableEvolutionRecord.columns);
            final int pos = getPos(columnsRecords, columnarColumnEvolutionRecords);
            List<ColumnsRecord> newRecords = new ArrayList<>();
            for (int i = pos; i < columnsRecords.size(); i++) {
                ColumnsRecord record = columnsRecords.get(i);
                record.tableName = indexTableName;
                newRecords.add(record);
            }

            // insert/update index table column records in columns
            if (!newRecords.isEmpty()) {
                List<String> deleteColumns = new ArrayList<>();
                for (ColumnsRecord record : newRecords) {
                    if (changeColumnsMap.containsKey(record.columnName)) {
                        deleteColumns.add(changeColumnsMap.get(record.columnName));
                    } else {
                        deleteColumns.add(record.columnName);
                    }
                }
                columnsAccessor.delete(schemaName, indexTableName, deleteColumns);
                columnsAccessor.insert(newRecords, schemaName, indexTableName);
            }
        }
    }

    public List<Long> reorderColumns(ColumnarTableEvolutionRecord columnarTableEvolutionRecord,
                                     List<ColumnsRecord> columnsRecords, Long versionId, Long ddlJobId,
                                     String indexTableName, List<Pair<String, String>> changeColumns) {
        List<ColumnarColumnEvolutionRecord> columnarColumnEvolutionRecords =
            columnarColumnEvolutionAccessor.queryIdsWithOrder(columnarTableEvolutionRecord.columns);

        // for alter table change column -> rename column
        Map<String, String> changeColumnsMap = new HashMap<>();
        for (Pair<String, String> changeColumn : changeColumns) {
            // <old name, new name>
            changeColumnsMap.put(changeColumn.getValue(), changeColumn.getKey());
        }

        Map<String, Long> columnMap = new HashMap<>();
        for (ColumnarColumnEvolutionRecord columnarColumnEvolutionRecord : columnarColumnEvolutionRecords) {
            if (changeColumnsMap.containsKey(columnarColumnEvolutionRecord.columnName)) {
                columnMap.put(changeColumnsMap.get(columnarColumnEvolutionRecord.columnName),
                    columnarColumnEvolutionRecord.fieldId);
            } else {
                columnMap.put(columnarColumnEvolutionRecord.columnName, columnarColumnEvolutionRecord.fieldId);
            }
        }

        long tableId = columnarTableEvolutionRecord.tableId;
        List<Long> columns = columnarTableEvolutionRecord.columns;
        final int pos = getPos(columnsRecords, columnarColumnEvolutionRecords);
        List<ColumnarColumnEvolutionRecord> records = new ArrayList<>();
        for (int i = pos; i < columnsRecords.size(); i++) {
            ColumnsRecord record = columnsRecords.get(i);
            record.tableName = indexTableName;
            if (columnMap.containsKey(record.columnName)) {
                records.add(
                    new ColumnarColumnEvolutionRecord(tableId, columnMap.get(record.columnName), record.columnName,
                        versionId, ddlJobId, record));
            } else {
                records.add(new ColumnarColumnEvolutionRecord(tableId, record.columnName,
                    versionId, ddlJobId, record));
            }
        }

        if (!records.isEmpty()) {
            columnarColumnEvolutionAccessor.insert(records);
            columnarColumnEvolutionAccessor.updateFieldIdAsId(tableId, versionId);
        }

        columnarColumnEvolutionRecords =
            columnarColumnEvolutionAccessor.queryTableIdAndNotInStatus(tableId, versionId,
                ColumnStatus.ABSENT.getValue());

        columns = columns.subList(0, Math.min(pos, columnsRecords.size()));

        for (ColumnarColumnEvolutionRecord columnarColumnEvolutionRecord : columnarColumnEvolutionRecords) {
            columns.add(columnarColumnEvolutionRecord.id);
        }

        return columns;
    }

    private static int getPos(List<ColumnsRecord> columnsRecords,
                              List<ColumnarColumnEvolutionRecord> columnarColumnEvolutionRecords) {
        int pos = -1;
        for (int i = 0; i < Math.min(columnarColumnEvolutionRecords.size(), columnsRecords.size()); i++) {
            ColumnarColumnEvolutionRecord evolutionRecord = columnarColumnEvolutionRecords.get(i);
            ColumnsRecord columnsRecord = columnsRecords.get(i);
            if (!isColumnRecordEqual(columnsRecord, evolutionRecord.columnsRecord)) {
                pos = i;
                break;
            }
        }
        // add/drop column last
        if (pos == -1) {
            pos = columnarColumnEvolutionRecords.size();
        }
        return pos;
    }

    static boolean isColumnRecordEqual(ColumnsRecord record1, ColumnsRecord record2) {
        return record1.columnName.equals(record2.columnName) &&
            Objects.equals(record1.columnDefault, record2.columnDefault) &&
            record1.equals(record2);
    }

    public void changeColumnarIndexColumnMeta(PhyInfoSchemaContext context,
                                              Map<String, Map<String, Object>> columnsJdbcExtInfo,
                                              List<Pair<String, String>> changeColumns, String indexName) {
        List<String> newColumnNames =
            changeColumns.stream().map(Pair::getKey).collect(Collectors.toList());

        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, newColumnNames,
                context.dataSource);

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

        Map<String, String> columnNameMap = new HashMap<>();
        for (Pair<String, String> pair : changeColumns) {
            columnNameMap.put(pair.getKey(), pair.getValue());
        }

        // Adjust the columns to keep the original order.
        for (String newColumnName : newColumnNames) {
            // indexes system table
            indexesAccessor.updateColumnName(context.tableSchema, context.tableName, newColumnName,
                columnNameMap.get(newColumnName));
        }
    }

    /**
     * Query columnar table evolution by tableId
     */
    public List<ColumnarTableEvolutionRecord> queryColumnarTableEvolutionLatest(Long tableId) {
        return columnarTableEvolutionAccessor.queryTableIdLatest(tableId);
    }

    /**
     * Query columnar table evolution by ddlJobId
     */
    public List<ColumnarTableEvolutionRecord> queryColumnarTableEvolutionLatestByDdlJobId(Long jobId) {
        return columnarTableEvolutionAccessor.queryDdlJobIdLatest(jobId);
    }

    /**
     * Query columnar table evolution by ddlJobId
     */
    public List<ColumnarTableEvolutionRecord> queryColumnarTableEvolutionByDdlJobId(Long jobId) {
        return columnarTableEvolutionAccessor.queryDdlJobId(jobId);
    }

    /**
     * Query columnar table evolution by tableId
     */
    public List<ColumnarTableEvolutionRecord> queryColumnarTableEvolution(Long tableId, Long versionId) {
        return columnarTableEvolutionAccessor.queryTableIdVersionId(tableId, versionId);
    }

    /**
     * Query columnar column evolution by versionId and fieldId list
     */
    public List<ColumnarColumnEvolutionRecord> queryColumnarColumnEvolution(List<Long> fieldList) {
        return columnarColumnEvolutionAccessor.queryIds(fieldList);
    }

    /**
     * Query columnar table mapping by schema, primary and columnar table name
     */
    public List<ColumnarTableMappingRecord> queryColumnarTableMapping(
        String schemaName,
        String primaryTableName,
        String columnarTableName) {

        return columnarTableMappingAccessor.querySchemaTableIndex(schemaName, primaryTableName, columnarTableName);
    }

    /**
     * Query columnar table mapping by schema and columnar table name
     */
    public List<ColumnarTableMappingRecord> queryColumnarTableMapping(
        String schemaName,
        String columnarTableName) {

        return columnarTableMappingAccessor.querySchemaIndex(schemaName, columnarTableName);
    }

    /**
     * Query columnar table mapping by tableId
     */
    public List<ColumnarTableMappingRecord> queryColumnarTableMapping(Long tableId) {
        return columnarTableMappingAccessor.queryTableId(tableId);
    }

    public void renameColumnarTable(String schemaName, String primaryTableName, String newPrimaryTableName,
                                    long versionId, long ddlJobID) {
        List<ColumnarTableMappingRecord> tableMappingRecords =
            columnarTableMappingAccessor.querySchemaTable(schemaName, primaryTableName);
        for (ColumnarTableMappingRecord tableMappingRecord : tableMappingRecords) {
            long tableId = tableMappingRecord.tableId;
            ColumnarTableEvolutionRecord latest = columnarTableEvolutionAccessor.queryTableIdLatest(tableId).get(0);
            latest.versionId = versionId;
            latest.commitTs = Long.MAX_VALUE;
            latest.tableName = newPrimaryTableName;
            latest.ddlType = DdlType.RENAME_TABLE.name();
            latest.ddlJobId = ddlJobID;
            columnarTableEvolutionAccessor.insert(ImmutableList.of(latest));
            columnarTableMappingAccessor.updateTableNameId(newPrimaryTableName, versionId, tableId);
        }
    }

    public List<ColumnarTableMappingRecord> queryColumnarTable(String schemaName, String tableName,
                                                               String indexName) {
        return columnarTableMappingAccessor.querySchemaTableIndex(schemaName, tableName, indexName);
    }

    public void addOssTable(PhyInfoSchemaContext context, Engine tableEngine, Supplier<?> failPointInjector) {
        // Used to fetch table collation
        TablesInfoSchemaRecord tablesInfoSchemaRecord =
            fetchTableMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, context.dataSource);

        // Build Table Meta
        TablesRecord tablesRecord = new TablesRecord();
        tablesRecord.tableSchema = context.tableSchema;
        tablesRecord.tableName = context.tableName;
        tablesRecord.tableType = "ORC TABLE";
        tablesRecord.engine = tableEngine.name();
        tablesRecord.version = 0;
        tablesRecord.rowFormat = "Dynamic";
        tablesRecord.tableRows = 0;
        tablesRecord.avgRowLength = 0;
        tablesRecord.dataLength = 0;
        tablesRecord.maxDataLength = 0;
        tablesRecord.indexLength = 0;
        tablesRecord.dataFree = 0;
        tablesRecord.autoIncrement = 0;
        tablesRecord.tableCollation = tablesInfoSchemaRecord.tableCollation;
        tablesRecord.checkSum = 0;
        tablesRecord.createOptions = "";
        tablesRecord.tableComment = "THIS IS OSS TABLE";

        tablesAccessor.insert(tablesRecord);

        // Column Meta
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, null, context.dataSource);

        Map<String, Map<String, Object>> columnJdbcExtInfo =
            fetchColumnJdbcExtInfo(context.phyTableSchema, context.phyTableName, context.dataSource);

        List<ColumnsRecord> columnsRecords = RecordConverter.convertColumn(
            columnsInfoSchemaRecords, columnJdbcExtInfo, context.tableSchema, context.tableName);

        columnsAccessor.insert(columnsRecords, context.tableSchema, context.tableName);

        columnMappingAccessor.insert(ColumnMappingRecord.fromColumnRecord(columnsRecords));
        addColumnEvolution(context, columnsRecords, context.ts);

        // Index Meta
        List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords =
            fetchIndexMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, context.dataSource);

        if (indexesInfoSchemaRecords != null && !indexesInfoSchemaRecords.isEmpty()) {
            List<IndexesRecord> indexesRecords =
                RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);

            indexesAccessor.insert(indexesRecords, context.tableSchema, context.tableName);
        }

        if (context.sequenceRecord != null) {
            sequencesAccessor.insert(context.sequenceRecord, 0, failPointInjector);
        }
    }

    public void addOssFile(String tableSchema, String tableName, FilesRecord filesRecord) {
        filesAccessor.insert(ImmutableList.of(filesRecord), tableSchema, tableName);
    }

    public void addOssFileWithTso(String tableSchema, String tableName, FilesRecord filesRecord) {
        filesAccessor.insertWithTso(ImmutableList.of(filesRecord));
    }

    public List<FilesRecord> queryFilesByLogicalSchemaTable(String logicalSchemaName, String logicalTableName) {
        return filesAccessor.queryByLogicalSchemaTable(logicalSchemaName, logicalTableName);
    }

    public List<FilesRecord> queryTableFormatByLogicalSchemaTable(String logicalSchemaName, String logicalTableName) {
        return filesAccessor.queryTableFormatByLogicalSchemaTable(logicalSchemaName, logicalTableName);
    }

    public FilesRecord queryLatestFileByLogicalSchemaTable(String logicalSchemaName, String logicalTableName) {
        List<FilesRecord> filesRecordList =
            filesAccessor.queryLatestFileByLogicalSchemaTable(logicalSchemaName, logicalTableName);
        if (filesRecordList.isEmpty()) {
            return null;
        } else {
            return filesRecordList.get(0);
        }
    }

    public List<FilesRecord> queryFilesByEngine(Engine engine) {
        return filesAccessor.queryByEngine(engine);
    }

    public void updateFilesCommitTs(Long ts, String logicalSchemaName, String logicalTableName, Long taskId) {
        filesAccessor.updateFilesCommitTs(ts, logicalSchemaName, logicalTableName, taskId);
    }

    public void updateFilesRemoveTs(Long ts, String logicalSchemaName, String logicalTableName, List<String> files) {
        filesAccessor.updateFilesRemoveTs(ts, logicalSchemaName, logicalTableName, files);
    }

    public void updateTableRemoveTs(Long ts, String logicalSchemaName, String logicalTableName) {
        filesAccessor.updateTableRemoveTs(ts, logicalSchemaName, logicalTableName);
    }

    public List<FilesRecord> queryFilesByLogicalSchema(String logicalSchemaName) {
        return filesAccessor.queryByLogicalSchema(logicalSchemaName);
    }

    public List<FilesRecord> queryFilesByFileName(String fileName) {
        return filesAccessor.queryByFileName(fileName);
    }

    public Long addOssFileAndReturnLastInsertId(String tableSchema, String tableName, FilesRecord filesRecord) {
        return filesAccessor.insertAndReturnLastInsertId(filesRecord, tableSchema, tableName);
    }

    public void changeOssFile(Long primaryKey, byte[] fileMeta, Long fileSize, Long rowCount) {
        filesAccessor.validFile(primaryKey, fileMeta, fileSize, rowCount);
    }

    public void changeTableEngine(String tableSchema, String tableName, String engine) {
        tablesAccessor.updateEngine(tableSchema, tableName, engine);
    }

    public List<FilesRecord> lockOssFile(Long taskId, String tableSchema, String tableName) {
        return filesAccessor.queryByIdAndSchemaAndTable(taskId, tableSchema, tableName);
    }

    public void deleteOssFile(Long taskId, String tableSchema, String tableName) {
        filesAccessor.delete(taskId, tableSchema, tableName);
    }

    public void validOssFile(Long taskId, String tableSchema, String tableName) {
        filesAccessor.ready(taskId, tableSchema, tableName);
    }

    public void addOSSColumnsMeta(String tableSchema, String tableName, ColumnMetasRecord columnMetasRecord) {
        columnMetaAccessor.insert(ImmutableList.of(columnMetasRecord));
    }

    public List<ColumnMetasRecord> queryOSSColumnsMeta(Long taskId, String tableSchema, String tableName) {
        return columnMetaAccessor.queryByIdAndSchemaAndTable(taskId, tableSchema, tableName);
    }

    public void deleteOSSColumnsMeta(Long taskId, String tableSchema, String tableName) {
        columnMetaAccessor.delete(taskId, tableSchema, tableName);
    }

    public void validOSSColumnsMeta(Long taskId, String tableSchema, String tableName) {
        columnMetaAccessor.ready(taskId, tableSchema, tableName);
    }

    public void addTable(PhyInfoSchemaContext context, long newSeqCacheSize, Supplier<?> failPointInjector,
                         List<ForeignKeyData> addedForeignKeys, Map<String, String> columnMapping,
                         List<String> addNewColumns) {
        // Table Meta
        TablesInfoSchemaRecord tablesInfoSchemaRecord =
            fetchTableMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, context.dataSource);

        TablesRecord tablesRecord =
            RecordConverter.convertTable(tablesInfoSchemaRecord, context.tableSchema, context.tableName);

        int tableId = tablesAccessor.insert(tablesRecord);

        // Column Meta
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, null, context.dataSource);

        Map<String, Map<String, Object>> columnJdbcExtInfo =
            fetchColumnJdbcExtInfo(context.phyTableSchema, context.phyTableName, context.dataSource);

        List<ColumnsRecord> columnsRecords = RecordConverter.convertColumn(
            columnsInfoSchemaRecords, columnJdbcExtInfo, context.tableSchema, context.tableName);

        for (ColumnsRecord columnsRecord : columnsRecords) {
            if (MapUtils.isNotEmpty(columnMapping) && columnMapping.containsKey(
                columnsRecord.columnName.toLowerCase())) {
                columnsRecord.setColumnMappingName(columnMapping.get(columnsRecord.columnName.toLowerCase()));
            } else if (addNewColumns != null && addNewColumns.contains(columnsRecord.columnName.toLowerCase())) {
                // 新增的列 映射为空
                columnsRecord.setColumnMappingName("");
            }
        }

        columnsAccessor.insert(columnsRecords, context.tableSchema, context.tableName);

        // Index Meta
        List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords =
            fetchIndexMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, context.dataSource);

        if (indexesInfoSchemaRecords != null && !indexesInfoSchemaRecords.isEmpty()) {
            // foreign key
            if (GeneralUtil.isNotEmpty(addedForeignKeys)) {
                if (!addedForeignKeys.get(0).isPushDown()) {
                    insertLogicForeignKeyIndexes(addedForeignKeys, context, indexesInfoSchemaRecords);
                } else {
                    for (ForeignKeyData data : addedForeignKeys) {
                        data.indexName = data.constraint;
                    }
                    List<IndexesRecord> indexesRecords =
                        RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);
                    indexesAccessor.insert(indexesRecords, context.tableSchema, context.tableName);
                }
                convertForeignKey(addedForeignKeys, context);
            } else {
                List<IndexesRecord> indexesRecords =
                    RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);
                indexesAccessor.insert(indexesRecords, context.tableSchema, context.tableName);
            }
        }

        if (context.sequenceRecord != null) {
            sequencesAccessor.insert(context.sequenceRecord, newSeqCacheSize, failPointInjector);
        }
    }

    public void insertLogicForeignKeyIndexes(List<ForeignKeyData> addedForeignKeys, PhyInfoSchemaContext context,
                                             List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords) {
        List<IndexesRecord> indexesRecordsFKs = new ArrayList<>();
        List<IndexesRecord> indexesRecordsOthers = new ArrayList<>();
        List<String> indexNames = new ArrayList<>();

        for (ForeignKeyData data : addedForeignKeys) {
            if (StringUtils.isEmpty(data.indexName)) {
                indexNames.add(data.constraint);
                data.indexName = data.constraint;
            } else {
                indexNames.add(data.indexName);
            }
        }

        List<IndexesInfoSchemaRecord> fkIndexesInfoSchemaRecords =
            fetchIndexMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, indexNames,
                context.dataSource);
        indexesRecordsFKs.addAll(
            RecordConverter.convertIndex(fkIndexesInfoSchemaRecords, context.tableSchema, context.tableName));

        List<IndexesInfoSchemaRecord> otherIndexesInfoSchemaRecords = new ArrayList<>();
        for (IndexesInfoSchemaRecord indexesInfoSchemaRecord : indexesInfoSchemaRecords) {
            if (!indexNames.contains(indexesInfoSchemaRecord.indexName)) {
                otherIndexesInfoSchemaRecords.add(indexesInfoSchemaRecord);
            }
        }

        indexesRecordsOthers.addAll(
            RecordConverter.convertIndex(otherIndexesInfoSchemaRecords, context.tableSchema,
                context.tableName));

        Map<String, List<IndexesRecord>> indexesRecordsFKsMap = new HashMap<>();
        for (IndexesRecord record : indexesRecordsFKs) {
            indexesRecordsFKsMap.computeIfAbsent(record.indexName, k -> new ArrayList<>()).add(record);
        }

        // Add mark and record ref table data.
        for (IndexesRecord record : indexesRecordsFKs) {
            record.indexComment = "ForeignKey";
        }

        if (!addedForeignKeys.get(0).isPushDown()) {
            indexesAccessor.insert(indexesRecordsFKs, context.tableSchema, context.tableName);
        }

        if (!indexesRecordsOthers.isEmpty()) {
            indexesAccessor.insert(indexesRecordsOthers, context.tableSchema, context.tableName);
        }
    }

    public void convertForeignKey(List<ForeignKeyData> addedForeignKeys, PhyInfoSchemaContext context) {
        for (ForeignKeyData data : addedForeignKeys) {
            ForeignRecord foreignRecord = new ForeignRecord();
            List<IndexesInfoSchemaRecord> indexesRecords =
                queryForeignKeyRefIndexes(data.refSchema, data.refTableName, data.refColumns);
            foreignRecord.schemaName = context.tableSchema;
            foreignRecord.tableName = context.tableName;
            foreignRecord.indexName = data.indexName;
            foreignRecord.constraintName = data.constraint;
            foreignRecord.refSchemaName = data.refSchema;
            foreignRecord.refTableName = data.refTableName;
            foreignRecord.refIndexName = indexesRecords.isEmpty() ? "" : indexesRecords.get(0).indexName;
            foreignRecord.nCols = data.columns.size();
            foreignRecord.updateRule = data.convertReferenceOption(data.onUpdate);
            foreignRecord.deleteRule = data.convertReferenceOption(data.onDelete);
            foreignRecord.pushDown = data.pushDown;
            foreignAccessor.insert(foreignRecord);
            for (int i = 0; i < data.columns.size(); i++) {
                ForeignColsRecord foreignColsRecord = new ForeignColsRecord();
                foreignColsRecord.schemaName = context.tableSchema;
                foreignColsRecord.tableName = context.tableName;
                foreignColsRecord.indexName = data.indexName;
                foreignColsRecord.forColName = data.columns.get(i);
                foreignColsRecord.refColName = data.refColumns.get(i);
                foreignColsRecord.pos = i;
                foreignColsAccessor.insert(foreignColsRecord);
            }
        }
    }

    public List<IndexesInfoSchemaRecord> queryForeignKeyRefIndexes(String refSchema, String refTableName,
                                                                   List<String> refColumns) {
        return indexesAccessor.queryForeignKeyRefIndexes(refSchema, refTableName, refColumns);
    }

    public int updateForeignKeyRefIndex(String tableSchema, String tableName, String indexName, String refIndex) {
        return foreignAccessor.updateForeignKeyRefIndex(tableSchema, tableName, indexName, refIndex);
    }

    public void showTable(String tableSchema, String tableName, SequenceBaseRecord sequenceRecord) {
        updateStatus(tableSchema, tableName, sequenceRecord, TableStatus.PUBLIC, true);
        tablePartitionAccessor.updateStatusForPartitionedTable(tableSchema, tableName,
            TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);
    }

    public void showTable(String tableSchema, String tableName, SequenceBaseRecord sequenceRecord,
                          boolean refreshColumnStatus) {
        updateStatus(tableSchema, tableName, sequenceRecord, TableStatus.PUBLIC, refreshColumnStatus);
        tablePartitionAccessor.updateStatusForPartitionedTable(tableSchema, tableName,
            TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);
    }

    public void showTableWithoutChangeGsiStatus(String tableSchema, String tableName,
                                                SequenceBaseRecord sequenceRecord) {
        updateStatusExceptGsi(tableSchema, tableName, sequenceRecord, TableStatus.PUBLIC, true);
        tablePartitionAccessor.updateStatusForPartitionedTable(tableSchema, tableName,
            TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);
    }

    public void hideTable(String tableSchema, String tableName) {
        updateStatus(tableSchema, tableName, null, TableStatus.ABSENT, true);
        tablePartitionAccessor.updateStatusForPartitionedTable(tableSchema, tableName,
            TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_ABSENT);
    }

    public void updatePartitionBoundDesc(String tableSchema, String tableName, String partitionName, String boundDesc) {
        tablePartitionAccessor.updatePartBoundDescForOnePartition(tableSchema, tableName, partitionName, boundDesc);

    }

    private void updateStatus(String tableSchema, String tableName, SequenceBaseRecord sequenceRecord,
                              TableStatus newTableStatus, boolean refreshColumnStatus) {
        int newStatus = newTableStatus.getValue();
        tablesAccessor.updateStatus(tableSchema, tableName, newStatus);
        tablesExtAccessor.updateStatus(tableSchema, tableName, newStatus);

        if (refreshColumnStatus) {
            columnsAccessor.updateStatus(tableSchema, tableName, newStatus);
        }

        int newUpdateStatus = IndexStatus.ABSENT.getValue();
        if (newTableStatus == TableStatus.PUBLIC) {
            newUpdateStatus = IndexStatus.PUBLIC.getValue();
        }
        indexesAccessor.updateStatus(tableSchema, tableName, newUpdateStatus);

        if (sequenceRecord != null) {
            sequencesAccessor.updateStatus(sequenceRecord, newStatus);
        }
    }

    private void updateStatusExceptGsi(String tableSchema, String tableName, SequenceBaseRecord sequenceRecord,
                                       TableStatus newTableStatus, boolean refreshColumnStatus) {
        int newStatus = newTableStatus.getValue();
        tablesAccessor.updateStatus(tableSchema, tableName, newStatus);
        tablesExtAccessor.updateStatus(tableSchema, tableName, newStatus);

        if (refreshColumnStatus) {
            columnsAccessor.updateStatus(tableSchema, tableName, newStatus);
        }

        int newUpdateStatus = IndexStatus.ABSENT.getValue();
        if (newTableStatus == TableStatus.PUBLIC) {
            newUpdateStatus = IndexStatus.PUBLIC.getValue();
        }
        indexesAccessor.updateStatusExceptGsi(tableSchema, tableName, newUpdateStatus);

        if (sequenceRecord != null) {
            sequencesAccessor.updateStatus(sequenceRecord, newStatus);
        }
    }

    public void removeColumnarTable(String schemaName, String columnarTableName) {
        // clean up columnar table meta
        removeTable(schemaName, columnarTableName, null, true);
    }

    public void removeTable(String tableSchema, String tableName, SequenceBaseRecord sequenceRecord,
                            boolean withTablesExtOrPartition) {
        tablesAccessor.delete(tableSchema, tableName);
        columnsAccessor.delete(tableSchema, tableName);
        indexesAccessor.delete(tableSchema, tableName);
        filesAccessor.delete(tableSchema, tableName);
        columnMetaAccessor.delete(tableSchema, tableName);
        foreignAccessor.delete(tableSchema, tableName);
        foreignColsAccessor.delete(tableSchema, tableName);

        columnEvolutionAccessor.delete(tableSchema, tableName);
        columnMappingAccessor.delete(tableSchema, tableName);

        if (sequenceRecord != null) {
            sequencesAccessor.delete(sequenceRecord);
        }
        if (withTablesExtOrPartition && DbInfoManager.getInstance().isNewPartitionDb(tableSchema)) {
            this.deletePartitionInfo(tableSchema, tableName);
            this.removeLocalPartitionRecord(tableSchema, tableName);
            this.removeScheduledJobRecord(tableSchema, tableName);
            this.joinGroupTableDetailAccessor.deleteJoinGroupTableDetailBySchemaTable(tableSchema, tableName);
        }
    }

    public void updateColumnarTableEvolutionDdlType(String ddlType, long ddlId) {
        columnarTableEvolutionAccessor.updateDdlType(ddlType, ddlId);
    }

    public void updateColumnarTableStatusBySchema(String schema, String status) {
        columnarTableMappingAccessor.updateStatusBySchema(schema, status);
    }

    public void updateColumnarTableStatus(String schema, String tableName, String indexName, String status,
                                          Long versionId) {
        columnarTableMappingAccessor.updateStatusByName(schema, tableName, indexName, status, versionId);
    }

    public void updateStatusByTableId(long tableId, String status, Long versionId) {
        columnarTableMappingAccessor.updateStatusAndVersionIdByTableId(tableId, versionId, status);
    }

    public long getVersionForUpdate(String tableSchema, String tableName) {
        return tablesAccessor.getTableMetaVersionForUpdate(tableSchema, tableName);
    }

    /**
     * Update the table meta version
     * <p>
     * Notice: caller must make sure the value of auto_commit of connection is false
     */
    public long updateVersionAndNotify(String tableSchema, String tableName) {
        // Update version for all related objects.
        long tableMetaVersion = getVersionForUpdate(tableSchema, tableName);
        long newVersion = tableMetaVersion + 1;
        tablesAccessor.updateVersion(tableSchema, tableName, newVersion);
        tablesExtAccessor.updateVersion(tableSchema, tableName, newVersion);
        columnsAccessor.updateVersion(tableSchema, tableName, newVersion);
        indexesAccessor.updateVersion(tableSchema, tableName, newVersion);
        tablePartitionAccessor.updateVersion(tableSchema, tableName, newVersion);
        // MetaDbConfigManager.getInstance()
        //    .notify(MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName), this.connection);
        return newVersion;
    }

    /**
     * Update the table meta version
     * <p>
     * Notice: caller must make sure the value of auto_commit of connection is false
     */
    public long updateTableVersion(String tableSchema, String tableName, long newVersion) {
        // Update version for all related objects.
        tablesAccessor.updateVersion(tableSchema, tableName, newVersion);
        tablesExtAccessor.updateVersion(tableSchema, tableName, newVersion);
        columnsAccessor.updateVersion(tableSchema, tableName, newVersion);
        indexesAccessor.updateVersion(tableSchema, tableName, newVersion);
        tablePartitionAccessor.updateVersion(tableSchema, tableName, newVersion);
        return newVersion;
    }

    public void updateTablesExtVersion(String tableSchema, String tableName, long newVersion) {
        tablesExtAccessor.updateVersion(tableSchema, tableName, newVersion);
    }

    public void updateTablePartitionsVersion(String tableSchema, String tableName, long newVersion) {
        tablePartitionAccessor.updateVersion(tableSchema, tableName, newVersion);
    }

    public void updateIndexesVersion(String tableSchema, String indexName, long newVersion) {
        indexesAccessor.updateIndexVersion(tableSchema, indexName, newVersion);
    }

    public void updateIndexesStatus(String tableSchema, String indexName, long newStatus) {
        indexesAccessor.updateStatusByIndexName(tableSchema, indexName, newStatus);
    }

    public void updateIndexesFlag(String tableSchema, String indexName, long newFlag) {
        indexesAccessor.updateFlagByIndexName(tableSchema, indexName, newFlag);
    }

    public void addNewTableName(String tableSchema, String tableName, String newTableName) {
        tablesAccessor.updateNewName(tableSchema, tableName, newTableName);
        tablesExtAccessor.updateNewName(tableSchema, tableName, newTableName);
    }

    public void updateTableComment(String tableSchema, String tableName, String comment) {
        tablesAccessor.updateComment(tableSchema, tableName, comment);
    }

    public void updateTableRowFormat(String tableSchema, String tableName, String rowFormat) {
        tablesAccessor.updateRowFormat(tableSchema, tableName, rowFormat);
    }

    public void renameTable(String tableSchema, String tableName, String newTableName, String newTbNamePattern,
                            SequenceBaseRecord sequenceRecord) {
        tablesAccessor.rename(tableSchema, tableName, newTableName);
        if (newTbNamePattern == null) {
            tablesExtAccessor.rename(tableSchema, tableName, newTableName);
        } else {
            tablesExtAccessor.rename(tableSchema, tableName, newTableName, newTbNamePattern);
        }
        columnsAccessor.rename(tableSchema, tableName, newTableName);
        indexesAccessor.rename(tableSchema, tableName, newTableName);
        filesAccessor.rename(tableSchema, tableName, newTableName);
        columnMetaAccessor.rename(tableSchema, tableName, newTableName);
        if (sequenceRecord != null) {
            sequencesAccessor.rename(sequenceRecord);
        }
    }

    public void renamePartitionTable(String tableSchema, String tableName, String newTableName,
                                     SequenceBaseRecord sequenceRecord) {
        // TODO : fix local partition archive when used in recycle bin
        tablesAccessor.rename(tableSchema, tableName, newTableName);
        tablePartitionAccessor.rename(tableSchema, tableName, newTableName);
        columnsAccessor.rename(tableSchema, tableName, newTableName);
        indexesAccessor.rename(tableSchema, tableName, newTableName);
        filesAccessor.rename(tableSchema, tableName, newTableName);
        columnMetaAccessor.rename(tableSchema, tableName, newTableName);
        if (sequenceRecord != null) {
            sequencesAccessor.rename(sequenceRecord);
        }
        columnMappingAccessor.rename(tableSchema, tableName, newTableName);
    }

    public void renamePartitionTablePhyTable(String tableSchema, String tableName, String newTableName) {
        TablePartitionConfig tablePartitionConfig =
            tablePartitionAccessor.getTablePartitionConfig(tableSchema, tableName, false);
        for (TablePartitionSpecConfig item : tablePartitionConfig.getPartitionSpecConfigs()) {
            String phyTableName = item.getSpecConfigInfo().phyTable;
            String newPhyTableName = TStringUtil.replaceWithIgnoreCase(phyTableName, tableName, newTableName);
            tablePartitionAccessor.renamePhyTableName(item.getSpecConfigInfo().id, newPhyTableName);
        }
    }

    public void renameIndexes(String tableSchema, String indexName, String newIndexName) {
        indexesAccessor.renameGsiIndexes(tableSchema, indexName, newIndexName);
    }

    public void renameLocalPartitionInfo(String tableSchema, String tableName, String newTableName) {
        String newScheduleName = "LOCAL_PARTITION:" + tableSchema + "." + newTableName;
        String oldScheduleName = "LOCAL_PARTITION:" + tableSchema + "." + tableName;
        scheduledJobsAccessor.rename(newTableName, newScheduleName, tableSchema, oldScheduleName);
        localPartitionAccessor.rename(newTableName, tableSchema, tableName);
    }

    public void setMultiWriteSourceColumn(String tableSchema, String tableName, String columnsName) {
        columnsAccessor.updateStatus(tableSchema, tableName, ImmutableList.of(columnsName),
            ColumnStatus.MULTI_WRITE_SOURCE.getValue());
    }

    public void setMultiWriteTargetColumn(String tableSchema, String tableName, String columnsName) {
        columnsAccessor.updateStatus(tableSchema, tableName, ImmutableList.of(columnsName),
            ColumnStatus.MULTI_WRITE_TARGET.getValue());
    }

    public void hideColumns(String tableSchema, String tableName, List<String> columnsNames) {
        changeColumnStatus(tableSchema, tableName, columnsNames, ColumnStatus.ABSENT);
    }

    public void showColumns(String tableSchema, String tableName, List<String> columnsNames) {
        changeColumnStatus(tableSchema, tableName, columnsNames, ColumnStatus.PUBLIC);
    }

    public void changeColumnStatus(String tableSchema, String tableName, List<String> columnsNames,
                                   ColumnStatus newStatus) {
        columnsAccessor.updateStatus(tableSchema, tableName, columnsNames, newStatus.getValue());
    }

    public void removeColumns(String tableSchema, String tableName, List<String> columnNames) {
        columnsAccessor.delete(tableSchema, tableName, columnNames);
        indexesAccessor.deleteColumns(tableSchema, tableName, columnNames);
        columnEvolutionAccessor.delete(tableSchema, tableName, columnNames);
        columnMappingAccessor.deleteAbsent(tableSchema, tableName);
        columnMappingAccessor.hide(tableSchema, tableName, columnNames);
    }

    public void hideIndexesColumns(String tableSchema, String tableName, List<String> columnsNames) {
        indexesAccessor.updateColumnStatus(tableSchema, tableName, columnsNames, IndexStatus.ABSENT.getValue());
    }

    public void showIndexesColumns(String tableSchema, String tableName, List<String> columnsNames) {
        indexesAccessor.updateColumnStatus(tableSchema, tableName, columnsNames, IndexStatus.PUBLIC.getValue());
    }

    public Map<String, String> getIsNullable(PhyInfoSchemaContext context,
                                             Map<String, Map<String, Object>> columnsJdbcExtInfo,
                                             List<String> columnNames) {
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, columnNames,
                context.dataSource);

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

        Map<String, String> isNullable = new HashMap<>();
        for (ColumnsRecord columnsRecord : columnsRecords) {
            isNullable.put(columnsRecord.columnName, columnsRecord.isNullable);
        }
        return isNullable;
    }

    public void addColumns(PhyInfoSchemaContext context, Map<String, Map<String, Object>> columnsJdbcExtInfo,
                           List<String> columnNames) {
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, columnNames,
                context.dataSource);

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

        // We assume that all column was added at the last place.
        long maxColumnPosition = columnsAccessor.queryMaxColumnPosition(context.tableSchema, context.tableName);

        columnsAccessor.insert(columnsRecords, context.tableSchema, context.tableName);

        // Reset logical column position
        for (ColumnsRecord record : columnsRecords) {
            columnsAccessor.updateNewColumnPosition(context.tableSchema, context.tableName, record.columnName,
                ++maxColumnPosition);
        }

        TablesRecord tablesRecord = tablesAccessor.query(context.tableSchema, context.tableName, false);
        if (Engine.isFileStore(tablesRecord.engine)) {
            columnMappingAccessor.insert(ColumnMappingRecord.fromColumnRecord(columnsRecords));
            addColumnEvolution(context, columnsRecords, context.ts);
        }
    }

    private void addColumnEvolution(PhyInfoSchemaContext context, List<ColumnsRecord> columnsRecords, Long ts) {
        Map<String, ColumnsRecord> columnsRecordMap = columnsRecords.stream()
            .collect(Collectors.toMap(x -> x.columnName.toLowerCase(), x -> x, (k1, k2) -> k1));

        //add column evolution
        List<ColumnEvolutionRecord> columnEvolutionRecords = new ArrayList<>();
        for (ColumnMappingRecord mapping : columnMappingAccessor.querySchemaTableColumns(context.tableSchema,
            context.tableName,
            Lists.newArrayList(columnsRecordMap.keySet()))) {
            columnEvolutionRecords.add(new ColumnEvolutionRecord(mapping.getFieldId(), ts,
                columnsRecordMap.get(mapping.columnName.toLowerCase())));
        }
        columnEvolutionAccessor.insert(columnEvolutionRecords);
    }

    public void updateColumns(PhyInfoSchemaContext context, Map<String, Map<String, Object>> columnsJdbcExtInfo,
                              List<String> updatedColumnNames) {
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, updatedColumnNames,
                context.dataSource);

        // recover the old ordinalPostion.
        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

        List<ColumnsRecord> oldColumnsRecords = queryColumns(context.tableSchema, context.tableName);
        Map<String, ColumnsRecord> oldColumnRecordMap =
            oldColumnsRecords.stream().collect(Collectors.toMap(o -> o.columnName, o -> o));
        for (ColumnsRecord columnsRecord : columnsRecords) {
            String columnName = columnsRecord.columnName;
            columnsRecord.ordinalPosition = oldColumnRecordMap.get(columnName).ordinalPosition;
        }
        columnsAccessor.update(columnsRecords);

        // Clear binary/expr default value flag
        for (String columnName : updatedColumnNames) {
            resetColumnBinaryDefaultFlag(context.tableSchema, context.tableName, columnName);
            resetColumnDefaultExprFlag(context.tableSchema, context.tableName, columnName);
        }

        // finally update.
        TablesRecord tablesRecord = tablesAccessor.query(context.tableSchema, context.tableName, false);
        if (Engine.isFileStore(tablesRecord.engine)) {
            addColumnEvolution(context, columnsRecords, context.ts);
        }
    }

    public void changeColumns(PhyInfoSchemaContext context, Map<String, Map<String, Object>> columnsJdbcExtInfo,
                              List<Pair<String, String>> columnNamePairs) {
        // columnNamePairs: new_column_name => old_column_name
        Map<String, String> New2OldColumnNameMap = new HashMap<>();
        Map<String, String> old2NewColumnNameMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Pair<String, String> pair : columnNamePairs) {
            New2OldColumnNameMap.put(pair.getKey(), pair.getValue());
            old2NewColumnNameMap.put(pair.getValue(), pair.getKey());
        }

        List<String> newColumnNames = new ArrayList<>();
        newColumnNames.addAll(columnNamePairs.stream().map(p -> p.getKey()).collect(Collectors.toList()));
        // get changed column def from information_schema.columns
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, newColumnNames,
                context.dataSource);

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);
        // recover all the changed column ordinalPosition by oldColumn meta.
        List<ColumnsRecord> oldColumnsRecords = queryColumns(context.tableSchema, context.tableName);
        Map<String, ColumnsRecord> oldColumnRecordMap =
            oldColumnsRecords.stream().collect(Collectors.toMap(o -> o.columnName, o -> o));
        for (ColumnsRecord columnsRecord : columnsRecords) {
            String columnName = columnsRecord.columnName;
            if (New2OldColumnNameMap.containsKey(columnName)) {
                ColumnsRecord oldColumnsRecord = oldColumnRecordMap.get(New2OldColumnNameMap.get(columnName));
                ColumnsAccessor.copyFromOldToNewColumnRecord(oldColumnsRecord, columnsRecord);
            }
        }

        // Adjust the changed columns to keep the original order.
        List<ColumnsRecord> columnsRecordsInOrder = new ArrayList<>();
        for (String newColumnName : newColumnNames) {
            Optional<ColumnsRecord> record =
                columnsRecords.stream().filter(r -> r.columnName.equalsIgnoreCase(newColumnName)).findFirst();
            if (record.isPresent()) {
                columnsRecordsInOrder.add(record.get());
            }
        }

        // change column def by columnsRecordsInOrder.
        columnsAccessor.change(context.tableSchema, context.tableName, columnsRecordsInOrder, New2OldColumnNameMap);

        // change index def by old2NewColumnNameMap.
        List<IndexesRecord> indexesRecords =
            indexesAccessor.queryTableIndexes(context.tableSchema, context.tableName);

        for (IndexesRecord indexesRecord : indexesRecords) {
            if (old2NewColumnNameMap.containsKey(indexesRecord.columnName)) {
                indexesRecord.columnName = old2NewColumnNameMap.get(indexesRecord.columnName);
            }
        }

        // Must rewrite the corresponding column names in indexes.
        List<String> indexesNames = indexesRecords.stream().map(o -> o.indexName).collect(Collectors.toList());
        indexesAccessor.updateIndexesByRewrite(context.tableSchema, context.tableName, indexesRecords, indexesNames);

        // reset by default flag.
        for (String columnName : columnNamePairs.stream().map(Pair::getKey).collect(Collectors.toList())) {
            resetColumnBinaryDefaultFlag(context.tableSchema, context.tableName, columnName);
            resetColumnDefaultExprFlag(context.tableSchema, context.tableName, columnName);
        }

        columnMappingAccessor.change(columnsRecords, New2OldColumnNameMap);
        TablesRecord tablesRecord = tablesAccessor.query(context.tableSchema, context.tableName, false);
        if (Engine.isFileStore(tablesRecord.engine)) {
            addColumnEvolution(context, columnsRecords, context.ts);
        }
    }

    public void refreshColumnOrder(PhyInfoSchemaContext context, Map<String, Map<String, Object>> columnsJdbcExtInfo) {
        // Fetch all.
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, null,
                context.dataSource);

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

        columnsAccessor.update(columnsRecords);
    }

    public void reorgColumnOrder(String tableSchema, String tableName, List<Pair<String, String>> columnAfterAnother) {
        for (Pair<String, String> pair : columnAfterAnother) {
            columnsAccessor.adjustColumnPosition(tableSchema, tableName, pair.getKey(), pair.getValue());
        }
    }

    public void updateLogicalColumnOrderFlag(String tableSchema, String tableName) {
        TablesRecord record = tablesAccessor.query(tableSchema, tableName, false);
        if (record != null) {
            record.setLogicalColumnOrder();
            tablesAccessor.updateFlag(tableSchema, tableName, record.flag);
        }
    }

    public void updateRebuildingTableFlag(String tableSchema, String tableName, boolean rollback) {
        TablesRecord record = tablesAccessor.query(tableSchema, tableName, false);
        if (record != null) {
            if (!rollback) {
                record.setRebuildingTable();
            } else {
                record.resetRebuildingTable();
            }
            tablesAccessor.updateFlag(tableSchema, tableName, record.flag);
        }
    }

    public void resetColumnOrder(String tableSchema, String tableName) {
        List<ColumnsRecord> columnsRecords = columnsAccessor.query(tableSchema, tableName);
        int position = 1;
        for (ColumnsRecord record : columnsRecords) {
            columnsAccessor.updateNewColumnPosition(tableSchema, tableName, record.columnName, position++);
        }
    }

    public String getBeforeColumnName(String tableSchema, String tableName, String columnName) {
        List<ColumnsRecord> columnsRecords = columnsAccessor.query(tableSchema, tableName);
        int index = 0;
        for (int i = 0; i < columnsRecords.size(); i++) {
            if (columnsRecords.get(i).columnName.equalsIgnoreCase(columnName)) {
                index = i - 1;
                break;
            }
        }
        return index == -1 ? "" : columnsRecords.get(index).columnName;
    }

    public void resetColumnDefaults(String tableSchema, String tableName, List<String> columnNames,
                                    Map<String, String> convertedColumnDefaults) {
        if (GeneralUtil.isEmpty(convertedColumnDefaults)) {
            return;
        }

        List<ColumnsRecord> columnsToUpdate = new ArrayList<>();
        List<ColumnsRecord> columnsRecords;

        if (GeneralUtil.isNotEmpty(columnNames)) {
            columnsRecords = columnsAccessor.query(tableSchema, tableName, columnNames);
        } else {
            columnsRecords = columnsAccessor.query(tableSchema, tableName);
        }

        for (ColumnsRecord record : columnsRecords) {
            String convertedColumnDefault = convertedColumnDefaults.get(record.columnName);
            if (TStringUtil.isNotBlank(convertedColumnDefault)) {
                record.columnDefault = convertedColumnDefault;
                columnsToUpdate.add(record);
            }
        }

        if (GeneralUtil.isNotEmpty(columnsToUpdate)) {
            columnsAccessor.update(columnsToUpdate);
        }
    }

    public void updateSpecialColumnDefaults(String tableSchema, String tableName,
                                            Map<String, String> specialDefaultValues,
                                            Map<String, Long> specialDefaultValueFlags) {
        if (GeneralUtil.isEmpty(specialDefaultValues)) {
            return;
        }

        List<ColumnsRecord> columnsRecords;
        columnsRecords = columnsAccessor.query(tableSchema, tableName);

        // binaryColumnDefaults may include columns that do not exist in table
        for (ColumnsRecord record : columnsRecords) {
            String specialColumnDefault = specialDefaultValues.get(record.columnName);
            if (TStringUtil.isNotBlank(specialColumnDefault)) {
                setColumnDefaultFlag(tableSchema, tableName, record.columnName,
                    specialDefaultValueFlags.get(record.columnName));
                columnsAccessor.updateColumnDefault(tableSchema, tableName, record.columnName, specialColumnDefault);
            }
        }
    }

    public void updateColumnsStatus(String tableSchema, List<String> tableNames, List<String> columnNames,
                                    int oldStatus, int newStatus) {
        columnsAccessor.updateStatus(tableSchema, tableNames, columnNames, oldStatus, newStatus);
    }

    public void setColumnDefaultFlag(String tableSchema, String tableName, String columnName, long flag) {
        columnsAccessor.setColumnFlag(tableSchema, tableName, columnName, flag);
    }

    public void updateColumnMappingName(String tableSchema, String tableName, String columnName, String columnMapping) {
        columnsAccessor.updateColumnMappingName(tableSchema, tableName, columnName, columnMapping);
    }

    public void setColumnBinaryDefaultFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.setColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
    }

    public void resetColumnBinaryDefaultFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.resetColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
    }

    public void resetColumnDefaultExprFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.resetColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_DEFAULT_EXPR);
    }

    public void setLogicalGeneratedColumnFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.setColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN);
    }

    public void resetLogicalGeneratedColumnFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.resetColumnFlag(tableSchema, tableName, columnName,
            ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN);
    }

    public void setGeneratedColumnFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.setColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_GENERATED_COLUMN);
    }

    public void resetGeneratedColumnFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.resetColumnFlag(tableSchema, tableName, columnName,
            ColumnsRecord.FLAG_GENERATED_COLUMN);
    }

    public void beginUpdateColumnDefaultVal(String tableSchema, String tableName, String columnName) {
        final int affected =
            columnsAccessor.setColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_FILL_DEFAULT);
        if (affected != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "update",
                "Failed to begin update " + wrap(tableName) + "." + wrap(tableName) + " " + wrap(columnName)
                    + " column default val.");
        }
    }

    public void endUpdateColumnDefaultVal(String tableSchema, String tableName, String columnName) {
        final int affected =
            columnsAccessor.clearColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_FILL_DEFAULT);
        if (affected != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "update",
                "Failed to end update " + wrap(tableName) + "." + wrap(tableName) + " " + wrap(columnName)
                    + " column default val.");
        }
    }

    public void addIndex(PhyInfoSchemaContext context, String indexName) {
        List<String> indexNames = new ArrayList<>(1);
        indexNames.add(indexName);
        addIndexes(context, indexNames);
    }

    public void addIndexes(PhyInfoSchemaContext context, List<String> indexNames) {
        List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords =
            fetchIndexMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, indexNames, context.dataSource);
        List<IndexesRecord> indexesRecords =
            RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);
        indexesAccessor.insert(indexesRecords, context.tableSchema, context.tableName);
    }

    public void addIndexes(PhyInfoSchemaContext context, List<String> indexNamesSpecified,
                           List<String> firstColumnsWithoutIndexNames) {
        if (CollectionUtils.isNotEmpty(indexNamesSpecified)) {
            // Fetch physical info directly with specified index names.
            addIndexes(context, indexNamesSpecified);
        }

        if (CollectionUtils.isNotEmpty(firstColumnsWithoutIndexNames)) {
            // User didn't specify an index name, so we have to check physical table to avoid conflict.
            List<String> indexNamesGenerated = new ArrayList<>();
            for (String firstColumnName : firstColumnsWithoutIndexNames) {
                if (TStringUtil.isNotEmpty(firstColumnName)) {
                    indexNamesGenerated.addAll(fetchIndexNamesFromInfoSchema(context, firstColumnName));
                }
            }
            addIndexes(context, indexNamesGenerated);
        }
    }

    public void addForeignKeys(PhyInfoSchemaContext context, List<ForeignKeyData> foreignKeyData, String symbol,
                               boolean withoutIndex) {
        if (foreignKeyData.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "check",
                "Only support one alter specification when add foreign key exists.");
        }

        final ForeignKeyData data = foreignKeyData.get(0);
        if (data.columns.isEmpty() || data.columns.size() != data.refColumns.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "check",
                "Bad foreign key with incorrect column number.");
        }

        if (data.constraint == null) {
            data.constraint = symbol;
        }

        if (!data.isPushDown() && !withoutIndex) {
            insertLogicForeignKeyIndex(data, context);
        } else {
            data.indexName = data.constraint;
        }

        convertForeignKey(foreignKeyData, context);
    }

    public void insertLogicForeignKeyIndex(ForeignKeyData data, PhyInfoSchemaContext context) {
        final List<IndexesRecord> indexesRecords;
        if (StringUtils.isEmpty(data.indexName)) {
            data.indexName = data.constraint;
            List<String> indexNames = new ArrayList<>();
            indexNames.add(data.constraint);
            List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords =
                fetchIndexMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, indexNames,
                    context.dataSource);
            indexesRecords =
                RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);
        } else {
            List<String> indexNames = new ArrayList<>();
            indexNames.add(data.indexName);
            List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords =
                fetchIndexMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, indexNames,
                    context.dataSource);
            indexesRecords =
                RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);
        }

        // Add mark and record ref table data.
        for (IndexesRecord record : indexesRecords) {
            record.indexComment = "ForeignKey";
        }
        indexesAccessor.insert(indexesRecords, context.tableSchema, context.tableName);
    }

    public void dropForeignKeys(String tableSchema, String tableName, List<String> indexNames) {
        if (indexNames.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "check",
                "Only support one alter specification when drop foreign key.");
        }
        foreignColsAccessor.deleteForeignKey(tableSchema, tableName, indexNames.get(0));
        foreignAccessor.deleteForeignKey(tableSchema, tableName, indexNames.get(0));
        indexesAccessor.delete(tableSchema, tableName, indexNames);
    }

    public void updateTableCollation(PhyInfoSchemaContext context) {
        String collation = fetchCollationFromInfoSchema(context);
        tablesAccessor.updateCollation(context.tableSchema, context.tableName, collation);
    }

    public void renameIndexes(String tableSchema, String tableName, List<Pair<String, String>> indexNamePairs) {
        List<IndexesRecord> indexesRecords =
            indexesAccessor.queryTableIndexes(tableSchema, tableName);

        Map<String, String> old2NewIndexNameMap = indexNamePairs.stream().collect(Collectors.toMap(Pair::getValue,
            Pair::getKey));
        List<String> indexNames = indexesRecords.stream().map(o -> o.indexName).collect(Collectors.toList());
        for (IndexesRecord indexesRecord : indexesRecords) {
            if (old2NewIndexNameMap.containsKey(indexesRecord.indexName)) {
                indexesRecord.indexName = old2NewIndexNameMap.get(indexesRecord.indexName);
            }
        }

        // Must change the corresponding column names in indexes.
        indexesAccessor.updateIndexesByRewrite(tableSchema, tableName, indexesRecords, indexNames);
    }

    public void hideIndex(String tableSchema, String tableName, String indexName) {
        List<String> indexNames = new ArrayList<>(1);
        indexNames.add(indexName);
        hideIndexes(tableSchema, tableName, indexNames);
    }

    public void showIndex(String tableSchema, String tableName, String indexName) {
        List<String> indexNames = new ArrayList<>(1);
        indexNames.add(indexName);
        showIndexes(tableSchema, tableName, indexNames);
    }

    public void hideIndexes(String tableSchema, String tableName, List<String> indexNames) {
        indexesAccessor.updateStatus(tableSchema, tableName, indexNames, IndexStatus.ABSENT.getValue());
    }

    public void showIndexes(String tableSchema, String tableName, List<String> indexNames) {
        indexesAccessor.updateStatus(tableSchema, tableName, indexNames, IndexStatus.PUBLIC.getValue());
    }

    /**
     * Identical to com.alibaba.polardbx.columnar.meta.utils.ColumnarTableInfoManager#notifyColumnarListener()
     * Only change columnar table status.
     * CCI is available iff both columnar table status and index status are PUBLIC.
     */
    public static void updateColumnarTableStatus(Connection metaDbConnection,
                                                 @NotNull String tableSchema,
                                                 @NotNull String tableName,
                                                 @NotNull String indexName,
                                                 @NotNull ColumnarTableStatus before,
                                                 @NotNull ColumnarTableStatus after) {

        final ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
        columnarTableMappingAccessor.setConnection(metaDbConnection);
        final ColumnarTableMappingRecord tableMapping =
            columnarTableMappingAccessor.querySchemaTableIndex(tableSchema, tableName, indexName).get(0);
        columnarTableMappingAccessor.updateStatusByTableIdAndStatus(
            tableMapping.tableId,
            before.name(),
            after.name());
    }

    public static void updateIndexStatus(Connection metaDbConnection,
                                         @NotNull String tableSchema,
                                         @NotNull String tableName,
                                         @NotNull String indexName,
                                         @NotNull IndexStatus after) {

        final IndexesAccessor indexesAccessor = new IndexesAccessor();
        indexesAccessor.setConnection(metaDbConnection);
        indexesAccessor.updateStatus(
            tableSchema,
            tableName,
            ImmutableList.of(indexName),
            after.getValue());
    }

    public void removeIndex(String tableSchema, String tableName, String indexName) {
        List<String> indexNames = new ArrayList<>(1);
        indexNames.add(indexName);
        removeIndexes(tableSchema, tableName, indexNames);
    }

    public void removeIndexes(String tableSchema, String tableName, List<String> indexNames) {
        indexesAccessor.delete(tableSchema, tableName, indexNames);
    }

    public void dropPrimaryKey(String tableSchema, String tableName) {
        indexesAccessor.deletePrimaryKey(tableSchema, tableName);
    }

    public void addPrimaryKey(PhyInfoSchemaContext context) {
        List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords =
            fetchIndexMetaFromInfoSchemaForPrimaryKey(context.phyTableSchema, context.phyTableName, context.dataSource);

        List<IndexesRecord> indexesRecords =
            RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);

        indexesAccessor.insert(indexesRecords, context.tableSchema, context.tableName);
    }

    public void updateIndexesColumnsStatus(String tableSchema, String tableName, List<String> indexNames,
                                           List<String> columnNames, int oldStatus, int newStatus) {
        indexesAccessor.updateStatus(tableSchema, tableName, indexNames, columnNames, oldStatus, newStatus);
    }

    public void createSequence(SequenceBaseRecord sequenceRecord, long newSeqCacheSize, Supplier<?> failPointInjector) {
        sequencesAccessor.insert(sequenceRecord, newSeqCacheSize, failPointInjector);
    }

    public void alterSequence(SequenceBaseRecord sequenceRecord, long newSeqCacheSize) {
        sequencesAccessor.update(sequenceRecord, newSeqCacheSize);
    }

    public SequenceBaseRecord fetchSequence(String schemaName, String seqName) {
        return sequencesAccessor.query(schemaName, seqName);
    }

    public Map<String, Map<String, Object>> fetchColumnJdbcExtInfo(String phyTableSchema, String phyTableName,
                                                                   DataSource dataSource) {
        return columnsAccessor.queryColumnJdbcExtInfo(phyTableSchema, phyTableName, dataSource);
    }

    public List<ColumnsInfoSchemaRecord> fetchColumnInfoSchema(String phyTableSchema, String phyTableName,
                                                               List<String> columnNames, Connection phyDbConn) {
        return columnsAccessor.queryInfoSchema(phyTableSchema, phyTableName, columnNames, phyDbConn);
    }

    private TablesInfoSchemaRecord fetchTableMetaFromInfoSchema(String phyTableSchema, String phyTableName,
                                                                DataSource dataSource) {
        TablesInfoSchemaRecord infoSchemaRecord =
            tablesAccessor.queryInfoSchema(phyTableSchema, phyTableName, dataSource);

        if (infoSchemaRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch",
                "Not found any information_schema.tables record for " + wrap(phyTableSchema) + "." + wrap(
                    phyTableName));
        }

        return infoSchemaRecord;
    }

    private List<ColumnsInfoSchemaRecord> fetchColumnMetaFromInfoSchema(String phyTableSchema, String phyTableName,
                                                                        List<String> columnNames,
                                                                        DataSource dataSource) {
        List<ColumnsInfoSchemaRecord> infoSchemaRecords;
        if (columnNames != null && columnNames.size() > 0) {
            infoSchemaRecords = columnsAccessor.queryInfoSchema(phyTableSchema, phyTableName, columnNames, dataSource);
        } else {
            infoSchemaRecords = columnsAccessor.queryInfoSchema(phyTableSchema, phyTableName, dataSource);
        }

        if (infoSchemaRecords == null || infoSchemaRecords.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch",
                "Not found any information_schema.columns record for " + wrap(phyTableSchema) + "." + wrap(
                    phyTableName));
        }

        return infoSchemaRecords;
    }

    private List<IndexesInfoSchemaRecord> fetchIndexMetaFromInfoSchemaForPrimaryKey(String phyTableSchema,
                                                                                    String phyTableName,
                                                                                    DataSource dataSource) {
        return fetchIndexMetaFromInfoSchema(phyTableSchema, phyTableName, null, dataSource, true);
    }

    private List<IndexesInfoSchemaRecord> fetchIndexMetaFromInfoSchema(String phyTableSchema, String phyTableName,
                                                                       DataSource dataSource) {
        return fetchIndexMetaFromInfoSchema(phyTableSchema, phyTableName, null, dataSource, false);
    }

    private List<IndexesInfoSchemaRecord> fetchIndexMetaFromInfoSchema(String phyTableSchema, String phyTableName,
                                                                       List<String> indexNames, DataSource dataSource) {
        return fetchIndexMetaFromInfoSchema(phyTableSchema, phyTableName, indexNames, dataSource, false);
    }

    private List<IndexesInfoSchemaRecord> fetchIndexMetaFromInfoSchema(String phyTableSchema, String phyTableName,
                                                                       List<String> indexNames, DataSource dataSource,
                                                                       boolean onlyForPrimaryKey) {
        List<IndexesInfoSchemaRecord> infoSchemaRecords;
        if (CollectionUtils.isNotEmpty(indexNames)) {
            infoSchemaRecords = indexesAccessor.queryInfoSchema(phyTableSchema, phyTableName, indexNames, dataSource);
        } else if (onlyForPrimaryKey) {
            infoSchemaRecords = indexesAccessor.queryInfoSchemaForPrimaryKey(phyTableSchema, phyTableName, dataSource);
        } else {
            infoSchemaRecords = indexesAccessor.queryInfoSchema(phyTableSchema, phyTableName, dataSource);
        }

        if (infoSchemaRecords == null || infoSchemaRecords.isEmpty()) {
            String message = String.format("Not found any information_schema.statistics record for %s.%s index: %s",
                wrap(phyTableSchema), wrap(phyTableName), indexNames);
            if (indexNames != null && !indexNames.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch", message);
            } else {
                LOGGER.warn(message);
            }
        }

        return infoSchemaRecords;
    }

    private List<String> fetchIndexNamesFromInfoSchema(PhyInfoSchemaContext context, String firstColumnName) {
        List<String> indexNames = new ArrayList<>();

        List<IndexesInfoSchemaRecord> infoSchemaRecordsByFirstColumn =
            fetchIndexMetaByFirstColumnFromInfoSchema(context.phyTableSchema, context.phyTableName, firstColumnName,
                context.dataSource);

        List<IndexesRecord> existingIndexesRecords =
            fetchIndexMetaByFirstColumn(context.tableSchema, context.tableName, firstColumnName);

        if (CollectionUtils.isEmpty(existingIndexesRecords)) {
            for (IndexesInfoSchemaRecord infoSchemaRecord : infoSchemaRecordsByFirstColumn) {
                indexNames.add(infoSchemaRecord.indexName);
            }
        } else {
            List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords = new ArrayList<>();
            for (IndexesInfoSchemaRecord infoSchemaRecord : infoSchemaRecordsByFirstColumn) {
                if (!containIndex(existingIndexesRecords, infoSchemaRecord)) {
                    indexNames.add(infoSchemaRecord.indexName);
                }
            }
        }

        return indexNames;
    }

    private String fetchCollationFromInfoSchema(PhyInfoSchemaContext context) {
        TablesInfoSchemaRecord tablesInfoSchemaRecord =
            fetTablesInfoSchemaRecordFromInfoSchema(context.phyTableSchema, context.phyTableName, context.dataSource);
        return tablesInfoSchemaRecord.tableCollation;
    }

    private boolean containIndex(List<IndexesRecord> indexesRecords, IndexesInfoSchemaRecord infoSchemaRecord) {
        if (CollectionUtils.isEmpty(indexesRecords)) {
            return true;
        }
        for (IndexesRecord indexesRecord : indexesRecords) {
            if (TStringUtil.equalsIgnoreCase(indexesRecord.indexName, infoSchemaRecord.indexName)) {
                return true;
            }
        }
        return false;
    }

    private static final String SHOW_VARIABLE = "show variables like '%s'";
    private static final String CHECK_XDB_VARIABLE =
        String.format(SHOW_VARIABLE, Attribute.XDB_VARIABLE_INSTANT_ADD_COLUMN);
    private static final String CHECK_MYSQL_VERSION = String.format(SHOW_VARIABLE, Attribute.MYSQL_VARIABLE_VERSION);

    public static boolean isInstantAddColumnSupportedByPhyDb(DataSource dataSource, String dbIndex) {
        try (Connection phyDbConn = dataSource.getConnection();
            Statement stmt = phyDbConn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery(CHECK_XDB_VARIABLE)) {
                if (rs.next()) {
                    String value = rs.getString("Value");
                    return TStringUtil.equalsIgnoreCase(value, "ON");
                }
            }

            try (ResultSet rs = stmt.executeQuery(CHECK_MYSQL_VERSION)) {
                if (rs.next()) {
                    String version = rs.getString("Value");
                    int majorVersion = extractMajorVersion(version);
                    if (majorVersion >= 8) {
                        // MySQL 8.0 starts support Instant Add Column by default.
                        return true;
                    }
                }
            }
        } catch (Throwable t) {
            LOGGER.error(
                "Failed to check if Instant Add Column is supported in '" + dbIndex + "'. Caused by: " + t.getMessage(),
                t);
        }

        return false;
    }

    public static boolean isXdbInstantAddColumnSupported() {
        try (Connection metaDbConn = MetaDbUtil.getConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(CHECK_XDB_VARIABLE)) {
            if (rs.next()) {
                return true;
            }
        } catch (Throwable t) {
            LOGGER.error("Failed to check if the 'XDB Instant Add Column' variable exists in MetaDB");
        }
        return false;
    }

    private static int extractMajorVersion(String version) {
        int dotIndex = TStringUtil.indexOf(version, ".");
        if (dotIndex > 0) {
            String majorVersion = TStringUtil.substring(version, 0, dotIndex);
            try {
                return Integer.valueOf(majorVersion);
            } catch (Exception ignored) {
            }
        }
        return 0;
    }

    private TablesInfoSchemaRecord fetTablesInfoSchemaRecordFromInfoSchema(String phyTableSchema,
                                                                           String phyTableName,
                                                                           DataSource dataSource) {
        TablesInfoSchemaRecord infoSchemaRecords =
            tablesAccessor.queryInfoSchema(phyTableSchema, phyTableName, dataSource);

        if (infoSchemaRecords == null) {
            String message =
                String.format("Not found any information_schema.tables record for %s.%s",
                    wrap(phyTableSchema), wrap(phyTableName));
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch", message);
        }

        return infoSchemaRecords;
    }

    private List<IndexesInfoSchemaRecord> fetchIndexMetaByFirstColumnFromInfoSchema(String phyTableSchema,
                                                                                    String phyTableName,
                                                                                    String firstColumnName,
                                                                                    DataSource dataSource) {
        List<IndexesInfoSchemaRecord> infoSchemaRecords =
            indexesAccessor.queryInfoSchemaByFirstColumn(phyTableSchema, phyTableName, firstColumnName, dataSource);

        if (infoSchemaRecords == null || infoSchemaRecords.isEmpty()) {
            String message =
                String.format("Not found any information_schema.statistics record for %s.%s first column: %s",
                    wrap(phyTableSchema), wrap(phyTableName), firstColumnName);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "fetch", message);
        }

        return infoSchemaRecords;
    }

    private List<IndexesRecord> fetchIndexMetaByFirstColumn(String tableSchema, String tableName,
                                                            String firstColumnName) {
        return indexesAccessor.queryByFirstColumn(tableSchema, tableName, firstColumnName);
    }

    public static class PhyInfoSchemaContext {

        public DataSource dataSource = null;
        public String tableSchema = null;
        public String tableName = null;
        public String phyTableSchema = null;
        public String phyTableName = null;
        public SequenceBaseRecord sequenceRecord = null;

        public Long ts = null;
    }

    public Set<String> queryAllSequenceNames(String schemaName) {
        return sequencesAccessor.queryNames(schemaName);
    }

    public void removeAll(String schemaName) {
        schemataAccessor.delete(schemaName);
        tablesAccessor.delete(schemaName);
        tablesExtAccessor.delete(schemaName);
        columnsAccessor.delete(schemaName);
        indexesAccessor.delete(schemaName);
        sequencesAccessor.deleteAll(schemaName);
        tablePartitionAccessor.deleteTablePartitionConfigsByDbName(schemaName);
        localPartitionAccessor.deleteAll(schemaName);
        filesAccessor.delete(schemaName);
        columnMetaAccessor.delete(schemaName);
        columnEvolutionAccessor.delete(schemaName);
        columnMappingAccessor.delete(schemaName);
        foreignAccessor.delete(schemaName);
        foreignColsAccessor.delete(schemaName);
    }

    public void addTablePartitionInfos(TableGroupDetailConfig tableGroupConfig, boolean isUpsert) {

        try {
            TablePartRecordInfoContext tablePartRecordInfoContext =
                tableGroupConfig.getTablesPartRecordInfoContext().get(0);
            boolean mayEmptyTableGroup = false;
            boolean isEmptyTableGroup = false;

            List<TablePartitionRecord> phyPartSpecList =
                TablePartRecordInfoContext.buildAllPhysicalPartitionRecList(tablePartRecordInfoContext);
            Map<String, TablePartitionRecord> partNameToPartRecMap =
                new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            for (int i = 0; i < phyPartSpecList.size(); i++) {
                partNameToPartRecMap.put(phyPartSpecList.get(i).getPartName(), phyPartSpecList.get(i));
            }

            if (tableGroupConfig.getTableGroupRecord() != null) {
                Long lastInsertId = tableGroupAccessor.addNewTableGroup(tableGroupConfig.getTableGroupRecord());
                //int i = 0;

                if (lastInsertId != null) {
                    tablePartRecordInfoContext.getLogTbRec().groupId = lastInsertId;

                    List<PartitionGroupRecord> allPgList = tableGroupConfig.getPartitionGroupRecords();
                    for (int j = 0; j < allPgList.size(); j++) {
                        PartitionGroupRecord pgRec = allPgList.get(j);
                        pgRec.tg_id = lastInsertId;
                        Long pgId = partitionGroupAccessor.addNewPartitionGroup(pgRec, false);
                        TablePartitionRecord targetPhyPartSpecRec = partNameToPartRecMap.get(pgRec.getPartition_name());
                        targetPhyPartSpecRec.groupId = pgId;
                    }

                    String finalTgName = TableGroupNameUtil.autoBuildTableGroupName(lastInsertId,
                        tableGroupConfig.getTableGroupRecord().tg_type);
                    List<TableGroupRecord> tableGroupRecords =
                        tableGroupAccessor
                            .getTableGroupsBySchemaAndName(tableGroupConfig.getTableGroupRecord().schema, finalTgName,
                                false);
                    if (GeneralUtil.isNotEmpty(tableGroupRecords)
                        && tableGroupConfig.getTableGroupRecord().tg_type == TG_TYPE_PARTITION_TBL_TG) {
                        finalTgName = "tg" + String.valueOf(System.currentTimeMillis());
                    }
                    tableGroupAccessor.updateTableGroupName(lastInsertId, finalTgName);
                }
            } else {
                mayEmptyTableGroup = tableGroupConfig.getAllTables().size() == 1;
            }

            List<TablePartitionRecord> phyPartRecList =
                TablePartRecordInfoContext.buildAllPhysicalPartitionRecList(tablePartRecordInfoContext);
            List<PartitionGroupRecord> pgRecList = tableGroupConfig.getPartitionGroupRecords();
            Map<String, PartitionGroupRecord> partNameToPgRecMap =
                new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            for (int i = 0; i < pgRecList.size(); i++) {
                PartitionGroupRecord pgRec = pgRecList.get(i);
                partNameToPgRecMap.put(pgRec.getPartition_name(), pgRec);
            }
            for (int i = 0; i < GeneralUtil.emptyIfNull(phyPartRecList).size(); i++) {
                TablePartitionRecord partition = phyPartRecList.get(i);
                String partName = partition.getPartName();
                if (partition.groupId == null || partition.groupId == -1) {
                    //PartitionGroupRecord pgRecord = tableGroupConfig.getPartitionGroupRecords().get(i);
                    PartitionGroupRecord pgRecord = partNameToPgRecMap.get(partName);
                    if (pgRecord == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                            "no found target partition group " + partName);
                    }
                    pgRecord.tg_id = tablePartRecordInfoContext.getLogTbRec().groupId;
                    Long pgId = partitionGroupAccessor.addNewPartitionGroup(pgRecord, false);
                    partition.groupId = pgId;
                    isEmptyTableGroup = mayEmptyTableGroup;
                }
            }

            if (isEmptyTableGroup) {
                int tableType = tablePartRecordInfoContext.getLogTbRec().getTblType();
                if (tableType == TablePartitionRecord.PARTITION_TABLE_TYPE_SINGLE_TABLE) {
                    int tableGroupType = TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG;
                    tableGroupAccessor
                        .updateTableGroupType(tablePartRecordInfoContext.getLogTbRec().groupId, tableGroupType);
                }
            }
            tablePartitionAccessor.addNewTablePartitionConfigs(tablePartRecordInfoContext.getLogTbRec(),
                tablePartRecordInfoContext.getPartitionRecList(), tablePartRecordInfoContext.getSubPartitionRecMap(),
                isUpsert, false);
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }

        return;
    }

    public List<TableGroupRecord> queryTableGroupById(Long id) {
        return tableGroupAccessor.getTableGroupsByID(id);
    }

    public void addTableGroupInfo(TableGroupRecord tableGroupRecord) throws SQLException {
        tableGroupAccessor.addNewTableGroup(tableGroupRecord);
        return;
    }

    public void addPartitionGroupInfo(List<PartitionGroupRecord> partitionGroupRecords) throws SQLException {
        partitionGroupAccessor.addNewPartitionGroups(partitionGroupRecords);
        return;
    }

    public PartitionGroupRecord queryPartitionGroupById(Long id) {
        return partitionGroupAccessor.getPartitionGroupById(id);
    }

    public List<PartitionGroupRecord> queryPartitionGroupByTgId(Long tgId) {
        return partitionGroupAccessor.getPartitionGroupsByTableGroupId(tgId, false);
    }

    public static String getSchemaDefaultDbIndex(String schemaName) {
        String defaultDbIndexGroup = null;
        TableInfoManager tableInfoManager = new TableInfoManager();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(true);
            tableInfoManager.setConnection(metaDbConn);
            defaultDbIndexGroup = tableInfoManager.getDefaultDbIndex(schemaName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
        return defaultDbIndexGroup;
    }

    public Map<String, String> getAllSchemaDefaultDbIndex() {
        List<SchemataRecord> records = schemataAccessor.queryAll();
        if (CollectionUtils.isEmpty(records)) {
            return MapUtils.EMPTY_MAP;
        }
        return records.stream().collect(Collectors.toMap(x -> x.schemaName, x -> x.defaultDbIndex));
    }

    public void deletePartitionInfo(String tableSchema, String tableName) {
        List<TablePartitionRecord> tablePartitionRecords =
            tablePartitionAccessor.getTablePartitionsByDbNameTbNameLevel(tableSchema, tableName,
                TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE, false);
        assert tablePartitionRecords.size() <= 1;
        if (tablePartitionRecords.size() == 1) {
            tablePartitionAccessor.deleteTablePartitionConfigs(tableSchema, tableName);
            List<TablePartitionRecord> allTablePartitionRecordsInCurGroup = tablePartitionAccessor
                .getTablePartitionsByDbNameGroupId(tableSchema, tablePartitionRecords.get(0).groupId);
            if (GeneralUtil.isEmpty(allTablePartitionRecordsInCurGroup)) {
                TableGroupUtils
                    .deleteEmptyTableGroupInfo(tableSchema, tablePartitionRecords.get(0).groupId, getConnection());
            }
        }
    }

    public List<TableLocalPartitionRecord> getLocalPartitionRecordBySchema(String schemaName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(schemaName));
        List<TableLocalPartitionRecord> recordList = localPartitionAccessor.queryBySchema(schemaName);
        return recordList;
    }

    public List<TableLocalPartitionRecord> getAllLocalPartitionRecord() {
        List<TableLocalPartitionRecord> recordList = localPartitionAccessor.query();
        return recordList;
    }

    public TableLocalPartitionRecord getLocalPartitionRecord(String schemaName, String tableName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(schemaName));
        TableLocalPartitionRecord record = localPartitionAccessor.queryByTableName(schemaName, tableName);
        return record;
    }

    public TableLocalPartitionRecord getLocalPartitionRecordByArchiveTable(String archiveSchemaName,
                                                                           String archiveTableName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(archiveSchemaName));
        TableLocalPartitionRecord record =
            localPartitionAccessor.queryByArchiveTableName(archiveSchemaName, archiveTableName);
        return record;
    }

    public int addLocalPartitionRecord(TableLocalPartitionRecord tableLocalPartitionRecord) {
        Preconditions.checkNotNull(tableLocalPartitionRecord);
        return localPartitionAccessor.insert(tableLocalPartitionRecord);
    }

    public void updateArchiveTable(String schemaName, String tableName, String archiveTableSchema,
                                   String archiveTableName) {
        localPartitionAccessor.updateArchiveTable(schemaName, tableName, archiveTableSchema, archiveTableName);
    }

    public void unBindingByArchiveTableName(String archiveTableSchema, String archiveTableName) {
        localPartitionAccessor.unBindingByArchiveTableName(archiveTableSchema, archiveTableName);
    }

    public void unBindingByArchiveSchemaName(String archiveTableSchema) {
        localPartitionAccessor.unBindingByArchiveSchemaName(archiveTableSchema);
    }

    public int addScheduledJob(ScheduledJobsRecord scheduledJobsRecord) {
        Preconditions.checkNotNull(scheduledJobsRecord);
        return scheduledJobsAccessor.insert(scheduledJobsRecord);
    }

    public int replaceScheduledJob(ScheduledJobsRecord scheduledJobsRecord) {
        Preconditions.checkNotNull(scheduledJobsRecord);
        return scheduledJobsAccessor.replace(scheduledJobsRecord);
    }

    public int removeLocalPartitionRecord(String schemaName, String tableName) {
        Preconditions.checkNotNull(schemaName);
        Preconditions.checkNotNull(tableName);
        return localPartitionAccessor.delete(schemaName, tableName);
    }

    public int removeScheduledJobRecord(String schemaName, String tableName) {
        Preconditions.checkNotNull(schemaName);
        Preconditions.checkNotNull(tableName);
        List<ScheduledJobsRecord> recordList = scheduledJobsAccessor.query(schemaName, tableName);
        int count = 0;
        if (CollectionUtils.isEmpty(recordList)) {
            return count;
        }
        for (ScheduledJobsRecord record : recordList) {
            count += scheduledJobsAccessor.deleteById(record.getScheduleId());
            firedScheduledJobsAccessor.deleteById(record.getScheduleId());
        }
        return count;
    }
}

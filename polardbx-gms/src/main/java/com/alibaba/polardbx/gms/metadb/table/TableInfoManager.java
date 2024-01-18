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
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
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
            allReferencedFkRecords.computeIfAbsent(record.tableName, k -> new ArrayList<>()).add(record);
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

    public List<ForeignRecord> queryForeignKeys(String tableSchema, String tableName) {
        return foreignAccessor.queryForeignKeys(tableSchema, tableName);
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
                         List<ForeignKeyData> addedForeignKeys) {
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
                indexesAccessor.queryForeignKeyRefIndexes(data.refSchema, data.refTableName, data.refColumns);
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

    public void updateTablesExtVersion(String tableSchema, String tableName, long newVersion) {
        tablesExtAccessor.updateVersion(tableSchema, tableName, newVersion);
    }

    public void updateTablePartitionsVersion(String tableSchema, String tableName, long newVersion) {
        tablePartitionAccessor.updateVersion(tableSchema, tableName, newVersion);
    }

    public void updateIndexesVersion(String tableSchema, String indexName, long newVersion) {
        indexesAccessor.updateVersion(tableSchema, indexName, newVersion);
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
        String newScheduleName = tableSchema + "." + tableName;
        scheduledJobsAccessor.rename(newTableName, newScheduleName, tableSchema, tableName);
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

    public void addColumns(PhyInfoSchemaContext context, Map<String, Map<String, Object>> columnsJdbcExtInfo,
                           List<String> columnNames) {
        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, columnNames,
                context.dataSource);

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

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

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

        columnsAccessor.update(columnsRecords);

        TablesRecord tablesRecord = tablesAccessor.query(context.tableSchema, context.tableName, false);
        if (Engine.isFileStore(tablesRecord.engine)) {
            addColumnEvolution(context, columnsRecords, context.ts);
        }
    }

    public void changeColumns(PhyInfoSchemaContext context, Map<String, Map<String, Object>> columnsJdbcExtInfo,
                              List<Pair<String, String>> columnNamePairs) {
        List<String> newColumnNames = new ArrayList<>();
        newColumnNames.addAll(columnNamePairs.stream().map(p -> p.getKey()).collect(Collectors.toList()));

        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, newColumnNames,
                context.dataSource);

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

        Map<String, String> columnNameMap = new HashMap<>();
        for (Pair<String, String> pair : columnNamePairs) {
            columnNameMap.put(pair.getKey(), pair.getValue());
        }

        // Adjust the columns to keep the original order.
        List<ColumnsRecord> columnsRecordsInOrder = new ArrayList<>();
        for (String newColumnName : newColumnNames) {
            Optional<ColumnsRecord> record =
                columnsRecords.stream().filter(r -> r.columnName.equalsIgnoreCase(newColumnName)).findFirst();
            if (record.isPresent()) {
                columnsRecordsInOrder.add(record.get());
            }
        }

        columnsAccessor.change(columnsRecordsInOrder, columnNameMap);

        for (String newColumnName : newColumnNames) {
            // Must change the corresponding column names in indexes.
            indexesAccessor.updateColumnName(context.tableSchema, context.tableName, newColumnName,
                columnNameMap.get(newColumnName));
        }
        for (String columnName : columnNamePairs.stream().map(p -> p.getKey()).collect(Collectors.toList())) {
            resetColumnBinaryDefaultFlag(context.tableSchema, context.tableName, columnName);
        }

        columnMappingAccessor.change(columnsRecords, columnNameMap);
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

    public void setColumnBinaryDefaultFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.setColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
    }

    public void resetColumnBinaryDefaultFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.resetColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
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

    public void addForeignKeys(PhyInfoSchemaContext context, List<ForeignKeyData> foreignKeyData, String symbol) {
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

        if (!data.isPushDown()) {
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
        indexesAccessor.rename(tableSchema, tableName, indexNamePairs);
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

    public void addTablePartitionInfos(TableGroupConfig tableGroupConfig, boolean isUpsert) {

        try {
            TablePartRecordInfoContext tablePartRecordInfoContext =
                tableGroupConfig.getTables().get(0);
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
                mayEmptyTableGroup = tableGroupConfig.getTables().size() == 1;
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

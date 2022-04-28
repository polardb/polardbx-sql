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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.record.RecordConverter;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionAccessor;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
    private final TableLocalPartitionAccessor localPartitionAccessor;
    private final ScheduledJobsAccessor scheduledJobsAccessor;
    private final FiredScheduledJobsAccessor firedScheduledJobsAccessor;

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
        localPartitionAccessor = new TableLocalPartitionAccessor();
        scheduledJobsAccessor = new ScheduledJobsAccessor();
        firedScheduledJobsAccessor = new FiredScheduledJobsAccessor();
    }

    private static final String SQL_UPDATE_TABLE_VERSION = "UPDATE "
        + GmsSystemTables.TABLES
        + " SET VERSION=last_insert_id(VERSION+1) WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

    public static long updateTableVersion(String schema, String table, Connection conn) throws SQLException {
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

        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getTableDataId(schema, table), conn);
        return newVersion;
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
        localPartitionAccessor.setConnection(connection);
        scheduledJobsAccessor.setConnection(connection);
        firedScheduledJobsAccessor.setConnection(connection);
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
            if (record.status != TableStatus.ABSENT.getValue()) {
                visibleRecords.add(record);
            }
        }
        return visibleRecords;
    }

    public Map<String, List<ColumnsRecord>> queryVisibleColumns(String tableSchema) {
        Map<String, List<ColumnsRecord>> visibleRecords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<ColumnsRecord> records = queryColumns(tableSchema);
        for (ColumnsRecord record : records) {
            if (record.status != TableStatus.ABSENT.getValue()) {
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

    public void alterTablePartitionsCurOver(String tableSchema, String originName, String newName, int tableType) {
        tablePartitionAccessor.alterNameAndType(tableSchema, originName, newName, tableType);
        indexesAccessor.renameLocalIndexes(tableSchema, originName, newName);
    }

    public int alterTableExtFlag(String tableSchema, String tableName, long flag) {
        return tablesExtAccessor.alterTableExtFlag(tableSchema, tableName, flag);
    }

    public void addOssTable(PhyInfoSchemaContext context, Engine tableEngine) {
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
        tablesRecord.tableCollation = "utf8mb4_general_ci";
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

        // Index Meta
        List<IndexesInfoSchemaRecord> indexesInfoSchemaRecords =
            fetchIndexMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, context.dataSource);

        if (indexesInfoSchemaRecords != null && !indexesInfoSchemaRecords.isEmpty()) {
            List<IndexesRecord> indexesRecords =
                RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);

            indexesAccessor.insert(indexesRecords, context.tableSchema, context.tableName);
        }

        if (context.sequenceRecord != null) {
            sequencesAccessor.insert(context.sequenceRecord);
        }
    }

    public void addOssFile(String tableSchema, String tableName, FilesRecord filesRecord) {
        filesAccessor.insert(ImmutableList.of(filesRecord), tableSchema, tableName);
    }

    public List<FilesRecord> queryFilesByLogicalSchemaTable(String logicalSchemaName, String logicalTableName) {
        return filesAccessor.queryByLogicalSchemaTable(logicalSchemaName, logicalTableName);
    }

    public List<FilesRecord> queryTableFormatByLogicalSchemaTable(String logicalSchemaName, String logicalTableName) {
        return filesAccessor.queryTableFormatByLogicalSchemaTable(logicalSchemaName, logicalTableName);
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
        filesAccessor.changeFile(primaryKey, fileMeta, fileSize, rowCount);
    }

    public void changeTableEngine(String tableSchema, String tableName, String engine) {
        tablesAccessor.updateEngine(tableSchema, tableName, engine);
    }

    public List<FilesRecord> lockOssFile(Long taskId, String tableSchema, String tableName) {
        return filesAccessor.lock(taskId, tableSchema, tableName);
    }

    public void deleteOssFile(Long taskId, String tableSchema, String tableName) {
        filesAccessor.delete(taskId, tableSchema, tableName);
    }

    public void validOssFile(Long taskId, String tableSchema, String tableName) {
        filesAccessor.valid(taskId, tableSchema, tableName);
    }

    public void addOSSColumnsMeta(String tableSchema, String tableName, ColumnMetasRecord columnMetasRecord) {
        columnMetaAccessor.insert(ImmutableList.of(columnMetasRecord));
    }

    public List<ColumnMetasRecord> lockOSSColumnsMeta(Long taskId, String tableSchema, String tableName) {
        return columnMetaAccessor.lock(taskId, tableSchema, tableName);
    }

    public void deleteOSSColumnsMeta(Long taskId, String tableSchema, String tableName) {
        columnMetaAccessor.delete(taskId, tableSchema, tableName);
    }

    public void validOSSColumnsMeta(Long taskId, String tableSchema, String tableName) {
        columnMetaAccessor.valid(taskId, tableSchema, tableName);
    }

    public void addTable(PhyInfoSchemaContext context) {
        // Table Meta
        TablesInfoSchemaRecord tablesInfoSchemaRecord =
            fetchTableMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, context.dataSource);

        TablesRecord tablesRecord =
            RecordConverter.convertTable(tablesInfoSchemaRecord, context.tableSchema, context.tableName);

        tablesAccessor.insert(tablesRecord);

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
            List<IndexesRecord> indexesRecords =
                RecordConverter.convertIndex(indexesInfoSchemaRecords, context.tableSchema, context.tableName);

            indexesAccessor.insert(indexesRecords, context.tableSchema, context.tableName);
        }

        if (context.sequenceRecord != null) {
            sequencesAccessor.insert(context.sequenceRecord);
        }
    }

    public void showTable(String tableSchema, String tableName, SystemTableRecord sequenceRecord) {
        updateStatus(tableSchema, tableName, sequenceRecord, TableStatus.PUBLIC);
        tablePartitionAccessor.updateStatusForPartitionedTable(tableSchema, tableName,
            TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC);
    }

    public void hideTable(String tableSchema, String tableName) {
        updateStatus(tableSchema, tableName, null, TableStatus.ABSENT);
        tablePartitionAccessor.updateStatusForPartitionedTable(tableSchema, tableName,
            TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_ABSENT);
    }

    public void updatePartitionBoundDesc(String tableSchema, String tableName, String partitionName, String boundDesc) {
        tablePartitionAccessor.updatePartBoundDescForOnePartition(tableSchema, tableName, partitionName, boundDesc);

    }

    private void updateStatus(String tableSchema, String tableName, SystemTableRecord sequenceRecord,
                              TableStatus newTableStatus) {
        int newStatus = newTableStatus.getValue();
        tablesAccessor.updateStatus(tableSchema, tableName, newStatus);
        tablesExtAccessor.updateStatus(tableSchema, tableName, newStatus);
        columnsAccessor.updateStatus(tableSchema, tableName, newStatus);

        int newUpdateStatus = IndexStatus.ABSENT.getValue();
        if (newTableStatus == TableStatus.PUBLIC) {
            newUpdateStatus = IndexStatus.PUBLIC.getValue();
        }
        indexesAccessor.updateStatus(tableSchema, tableName, newUpdateStatus);

        if (sequenceRecord != null) {
            sequencesAccessor.updateStatus(sequenceRecord, newStatus);
        }
    }

    public void removeTable(String tableSchema, String tableName, SystemTableRecord sequenceRecord,
                            boolean withTablesExtOrPartition) {
        tablesAccessor.delete(tableSchema, tableName);
        columnsAccessor.delete(tableSchema, tableName);
        indexesAccessor.delete(tableSchema, tableName);
        filesAccessor.delete(tableSchema, tableName);
        columnMetaAccessor.delete(tableSchema, tableName);
        if (sequenceRecord != null) {
            sequencesAccessor.delete(sequenceRecord);
        }
        if (withTablesExtOrPartition && DbInfoManager.getInstance().isNewPartitionDb(tableSchema)) {
            this.deletePartitionInfo(tableSchema, tableName);
            this.removeLocalPartitionRecord(tableSchema, tableName);
            this.removeScheduledJobRecord(tableSchema, tableName);
        }
    }

    /**
     * when drop/add partition, we will:
     * 1、create a new table group the this new table schema
     * 2、create new partition groups, for drop partition,
     * the location of each partition group ,is inherit from table_partitions table
     * for add partition, the location of a those partition groups which could map to
     * the existing partitions is inherit from table_partitions table, for the location
     * of a those partition groups which map to the new partitions is decided by the allocation algorithm
     * 3、renew the table group id and partition group id of the related record in table_partitions
     */
    public Long rebuildPartitionMetaWhenDropPartition(String tableSchema, String tableName, String partitionName,
                                                      TableGroupRecord tableGroupRecord,
                                                      Map<Long, PartitionGroupRecord> partitionGroupRecordMap) {
        //in case can't insert into table_group due to unique key conflict
        tableGroupRecord.tg_name = String.valueOf(System.currentTimeMillis());
        Long newTableGroupID = tableGroupAccessor.addNewTableGroup(tableGroupRecord);
        Long oldTableGroupId = null;
        assert newTableGroupID != null;
        for (Map.Entry<Long, PartitionGroupRecord> entry : partitionGroupRecordMap.entrySet()) {
            if (oldTableGroupId == null) {
                oldTableGroupId = entry.getValue().tg_id;
            }
            entry.getValue().tg_id = newTableGroupID;
            Long oldPgId = entry.getKey();
            Long pgId = partitionGroupAccessor.addNewPartitionGroup(entry.getValue(), false);
            tablePartitionAccessor.updateGroupId(oldPgId, pgId);
        }
        tablePartitionAccessor.updateGroupId(oldTableGroupId, newTableGroupID);

        String finalTgName = TableGroupNameUtil.autoBuildTableGroupName(newTableGroupID, tableGroupRecord.tg_type);
        List<TableGroupRecord> tableGroupRecords =
            tableGroupAccessor
                .getTableGroupsBySchemaAndName(tableGroupRecord.getSchema(),
                    finalTgName, false);
        if (GeneralUtil.isNotEmpty(tableGroupRecords)) {
            finalTgName = "tg" + tableGroupRecord.tg_name;
        }
        tableGroupAccessor.updateTableGroupName(newTableGroupID, finalTgName);
        tablePartitionAccessor.deletePartitionConfigs(tableSchema, tableName, partitionName);
        return oldTableGroupId;
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
        MetaDbConfigManager.getInstance()
            .notify(MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName), this.connection);
        return newVersion;
    }

    public void updateTablesExtVersion(String tableSchema, String tableName, long newVersion) {
        tablesExtAccessor.updateVersion(tableSchema, tableName, newVersion);
    }

    public void updateTablePartitionsVersion(String tableSchema, String tableName, long newVersion) {
        tablePartitionAccessor.updateVersion(tableSchema, tableName, newVersion);
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
                            SystemTableRecord sequenceRecord) {
        tablesAccessor.rename(tableSchema, tableName, newTableName);
        tablesExtAccessor.rename(tableSchema, tableName, newTableName, newTbNamePattern);
        columnsAccessor.rename(tableSchema, tableName, newTableName);
        indexesAccessor.rename(tableSchema, tableName, newTableName);
        filesAccessor.rename(tableSchema, tableName, newTableName);
        columnMetaAccessor.rename(tableSchema, tableName, newTableName);
        if (sequenceRecord != null) {
            sequencesAccessor.rename(sequenceRecord);
        }
    }

    public void renamePartitionTable(String tableSchema, String tableName, String newTableName,
                                     SystemTableRecord sequenceRecord) {
        tablesAccessor.rename(tableSchema, tableName, newTableName);
        tablePartitionAccessor.rename(tableSchema, tableName, newTableName);
        columnsAccessor.rename(tableSchema, tableName, newTableName);
        indexesAccessor.rename(tableSchema, tableName, newTableName);
        filesAccessor.rename(tableSchema, tableName, newTableName);
        columnMetaAccessor.rename(tableSchema, tableName, newTableName);
        if (sequenceRecord != null) {
            sequencesAccessor.rename(sequenceRecord);
        }
    }

    public void hideColumns(String tableSchema, String tableName, List<String> columnsNames) {
        columnsAccessor.updateStatus(tableSchema, tableName, columnsNames, TableStatus.ABSENT.getValue());
    }

    public void showColumns(String tableSchema, String tableName, List<String> columnsNames) {
        columnsAccessor.updateStatus(tableSchema, tableName, columnsNames, TableStatus.PUBLIC.getValue());
    }

    public void removeColumns(String tableSchema, String tableName, List<String> columnNames) {
        columnsAccessor.delete(tableSchema, tableName, columnNames);
        indexesAccessor.deleteColumns(tableSchema, tableName, columnNames);
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
    }

    public void changeColumns(PhyInfoSchemaContext context, Map<String, Map<String, Object>> columnsJdbcExtInfo,
                              Map<String, String> columnNamePairs) {
        List<String> newColumnNames = new ArrayList<>();
        newColumnNames.addAll(columnNamePairs.keySet());

        List<ColumnsInfoSchemaRecord> columnsInfoSchemaRecords =
            fetchColumnMetaFromInfoSchema(context.phyTableSchema, context.phyTableName, newColumnNames,
                context.dataSource);

        List<ColumnsRecord> columnsRecords =
            RecordConverter.convertColumn(columnsInfoSchemaRecords, columnsJdbcExtInfo, context.tableSchema,
                context.tableName);

        columnsAccessor.change(columnsRecords, columnNamePairs);

        for (String newColumnName : newColumnNames) {
            // Must change the corresponding column names in indexes.
            indexesAccessor.updateColumnName(context.tableSchema, context.tableName, newColumnName,
                columnNamePairs.get(newColumnName));
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

    public void updateBinaryColumnDefaults(String tableSchema, String tableName,
                                           Map<String, String> binaryColumnDefaults) {
        if (GeneralUtil.isEmpty(binaryColumnDefaults)) {
            return;
        }

        List<ColumnsRecord> columnsToUpdate = new ArrayList<>();
        List<ColumnsRecord> columnsRecords;
        columnsRecords = columnsAccessor.query(tableSchema, tableName);

        // binaryColumnDefaults may include columns that do not exist in table
        for (ColumnsRecord record : columnsRecords) {
            String convertedColumnDefault = binaryColumnDefaults.get(record.columnName);
            if (TStringUtil.isNotBlank(convertedColumnDefault)) {
                record.columnDefault = convertedColumnDefault;
                columnsToUpdate.add(record);
                setColumnBinaryDefaultFlag(tableSchema, tableName, record.columnName);
            }
        }

        if (GeneralUtil.isNotEmpty(columnsToUpdate)) {
            columnsAccessor.update(columnsToUpdate);
        }
    }

    public void updateAllIndexColumn(String tableSchema, String tableName, String indexName, Integer indexColumnType,
                                     Integer indexStatus) {
        indexesAccessor.updateAllColumnType(tableSchema, tableName, indexName, indexColumnType, indexStatus);
    }

    public void updateIndexColumn(String tableSchema, String tableName, String indexName, List<String> columnNameList,
                                  Integer indexColumnType, Integer indexStatus) {
        indexesAccessor
            .updateColumnType(tableSchema, tableName, indexName, columnNameList, indexColumnType, indexStatus);
    }

    public void updateColumnsStatus(String tableSchema, List<String> tableNames, List<String> columnNames,
                                    int oldStatus, int newStatus) {
        columnsAccessor.updateStatus(tableSchema, tableNames, columnNames, oldStatus, newStatus);
    }

    public void setColumnBinaryDefaultFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.setColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
    }

    public void resetColumnBinaryDefaultFlag(String tableSchema, String tableName, String columnName) {
        columnsAccessor.resetColumnFlag(tableSchema, tableName, columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
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

    public void renameIndexes(String tableSchema, String tableName, Map<String, String> indexNamePairs) {
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

    public void createSequence(SystemTableRecord sequenceRecord) {
        sequencesAccessor.insert(sequenceRecord);
    }

    public void alterSequence(SystemTableRecord sequenceRecord) {
        sequencesAccessor.update(sequenceRecord);
    }

    public SystemTableRecord fetchSequence(String schemaName, String seqName) {
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
        public SystemTableRecord sequenceRecord = null;

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
    }

    public void addTablePartitionInfos(TableGroupConfig tableGroupConfig, boolean isUpsert) {

        try {
            TablePartRecordInfoContext tablePartRecordInfoContext =
                tableGroupConfig.getTables().get(0);
            boolean mayEmptyTableGroup = false;
            boolean isEmptyTableGroup = false;
            if (tableGroupConfig.getTableGroupRecord() != null) {
                Long lastInsertId = tableGroupAccessor.addNewTableGroup(tableGroupConfig.getTableGroupRecord());
                int i = 0;

                if (lastInsertId != null) {
                    tablePartRecordInfoContext.getLogTbRec().groupId = lastInsertId;
                    for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
                        partitionGroupRecord.tg_id = lastInsertId;
                        Long pgId = partitionGroupAccessor.addNewPartitionGroup(partitionGroupRecord, false);
                        tablePartRecordInfoContext.getPartitionRecList().get(i).groupId = pgId;
                        i++;
                    }

                    String finalTgName = TableGroupNameUtil
                        .autoBuildTableGroupName(lastInsertId, tableGroupConfig.getTableGroupRecord().tg_type);
                    tableGroupAccessor.updateTableGroupName(lastInsertId, finalTgName);
                }
            } else {
                mayEmptyTableGroup = tableGroupConfig.getTables().size() == 1;
            }

            for (int i = 0; i < GeneralUtil.emptyIfNull(tablePartRecordInfoContext.getPartitionRecList()).size(); i++) {
                TablePartitionRecord partition = tablePartRecordInfoContext.getPartitionRecList().get(i);
                if (partition.groupId == null || partition.groupId == -1) {
                    PartitionGroupRecord pgRecord = tableGroupConfig.getPartitionGroupRecords().get(i);
                    pgRecord.tg_id = tablePartRecordInfoContext.getLogTbRec().groupId;
                    Long pgId = partitionGroupAccessor.addNewPartitionGroup(pgRecord, false);
                    tablePartRecordInfoContext.getPartitionRecList().get(i).groupId = pgId;
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

    public TableLocalPartitionRecord getLocalPartitionRecord(String schemaName, String tableName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(schemaName));
        TableLocalPartitionRecord record = localPartitionAccessor.queryByTableName(schemaName, tableName);
        return record;
    }

    public TableLocalPartitionRecord getLocalPartitionRecordByArchiveTable(String archiveSchemaName, String archiveTableName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(archiveSchemaName));
        TableLocalPartitionRecord record = localPartitionAccessor.queryByArchiveTableName(archiveSchemaName, archiveTableName);
        return record;
    }

    public int addLocalPartitionRecord(TableLocalPartitionRecord tableLocalPartitionRecord) {
        Preconditions.checkNotNull(tableLocalPartitionRecord);
        return localPartitionAccessor.insert(tableLocalPartitionRecord);
    }

    public void updateArchiveTable(String schemaName, String tableName, String archiveTableSchema, String archiveTableName) {
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

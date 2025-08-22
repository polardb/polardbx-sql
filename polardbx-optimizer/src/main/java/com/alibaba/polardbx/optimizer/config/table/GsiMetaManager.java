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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ColumnarConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigWithIndexNameRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition.SqlIndexResiding;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.MapUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Wrapper;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.config.table.GsiUtils.getConnectionForWrite;
import static com.alibaba.polardbx.optimizer.config.table.GsiUtils.wrapWithTransaction;

/**
 * @author chenmo.cm
 */
public class GsiMetaManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(GsiMetaManager.class);

    public static final String __DRDS__SYSTABLE__TABLES__TABLE_NAME = ConfigDataMode.isPolarDbX()
        ? GmsSystemTables.TABLES_EXT : SystemTables.DRDS_SYSTABLE_TABLES;
    public static final String __DRDS__SYSTABLE__INDEXES__TABLE_NAME = ConfigDataMode.isPolarDbX()
        ? GmsSystemTables.INDEXES : SystemTables.DRDS_SYSTABLE_INDEXES;

    private static final String __DRDS__SYSTABLE__INDEXES__UK_NAME = "uk_schema_table_index_column";
    private static final String __DRDS__SYSTABLE__TABLES__UK_NAME = "uk_schema_table";

    private static final String __DRDS__SYSTABLE__TABLES__ = "CREATE TABLE IF NOT EXISTS `{0}` ("
        + "  `ID` BIGINT(21) UNSIGNED NOT NULL AUTO_INCREMENT,"
        + "  `TABLE_CATALOG` VARCHAR(512) DEFAULT ''def'',"
        + "  `TABLE_SCHEMA` VARCHAR(64) NOT NULL DEFAULT '''',"
        + "  `TABLE_NAME` VARCHAR(64) NOT NULL DEFAULT '''',"
        + "  `TABLE_TYPE` BIGINT(10) NOT NULL DEFAULT 0 COMMENT ''0:SINGLE,1:SHARDING,2:BROADCAST,3:GSI'',"
        + "  `DB_PARTITION_KEY` VARCHAR(64) DEFAULT NULL,"
        + "  `DB_PARTITION_POLICY` VARCHAR(64) DEFAULT NULL,"
        + "  `DB_PARTITION_COUNT` BIGINT(4) NOT NULL DEFAULT 1,"
        + "  `TB_PARTITION_KEY` VARCHAR(64) DEFAULT NULL,"
        + "  `TB_PARTITION_POLICY` VARCHAR(64) DEFAULT NULL,"
        + "  `TB_PARTITION_COUNT` BIGINT(4) NOT NULL DEFAULT 1,"
        + "  `COMMENT` VARCHAR(2048) DEFAULT NULL,"
        + "  PRIMARY KEY(`ID`)"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

    private static final String __DRDS__SYSTABLE__INDEXES__ = "CREATE TABLE IF NOT EXISTS `{0}` ("
        + "  `ID` BIGINT(21) UNSIGNED NOT NULL AUTO_INCREMENT,"
        + "  `TABLE_CATALOG` VARCHAR(512) NOT NULL DEFAULT ''def'',"
        + "  `TABLE_SCHEMA` VARCHAR(64) NOT NULL DEFAULT '''',"
        + "  `TABLE_NAME` VARCHAR(64) NOT NULL DEFAULT '''',"
        + "  `NON_UNIQUE` BIGINT(1) NOT NULL DEFAULT 0,"
        + "  `INDEX_SCHEMA` VARCHAR(64) NOT NULL DEFAULT '''',"
        + "  `INDEX_NAME` VARCHAR(64) NOT NULL DEFAULT '''',"
        + "  `SEQ_IN_INDEX` BIGINT(10) NOT NULL DEFAULT 0,"
        + "  `COLUMN_NAME` VARCHAR(64) NOT NULL DEFAULT '''',"
        + "  `COLLATION` VARCHAR(3) DEFAULT NULL,"
        + "  `CARDINALITY` BIGINT(21) DEFAULT NULL,"
        + "  `SUB_PART` BIGINT(3) DEFAULT NULL,"
        + "  `PACKED` VARCHAR(10) DEFAULT NULL,"
        + "  `NULLABLE` VARCHAR(3) NOT NULL DEFAULT '''',"
        + "  `INDEX_TYPE` VARCHAR(16) NULL DEFAULT NULL COMMENT ''BTREE, FULLTEXT, HASH, RTREE, GLOBAL'',"
        + "  `COMMENT` VARCHAR(16) DEFAULT NULL COMMENT ''INDEX, COVERING'',"
        + "  `INDEX_COMMENT` VARCHAR(1024) NOT NULL DEFAULT '''',"
        + "  `INDEX_COLUMN_TYPE` BIGINT(10) DEFAULT 0 COMMENT ''0:INDEX,1:COVERING'',"
        + "  `INDEX_LOCATION` BIGINT(10) NOT NULL DEFAULT 1 COMMENT ''0:LOCAL,1:GLOBAL'',"
        + "  `INDEX_TABLE_NAME` VARCHAR(64) NOT NULL DEFAULT '''',"
        + "  `INDEX_STATUS` BIGINT(10) NOT NULL DEFAULT 0 COMMENT ''0:CREATING,1:DELETE_ONLY,2:WRITE_ONLY,3:WRITE_REORG,4:PUBLIC,5:DELETE_REORG,6:REMOVING,7:ABSENT'',"
        + "  `VERSION` BIGINT(21) NOT NULL COMMENT ''index meta version'',"
        + "  PRIMARY KEY(`ID`),"
        + "  KEY `i_index_name_version`(`INDEX_NAME`, `VERSION`)"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

    private static final String __DRSD__SYSTABLE__INDEXES__INDEX_STATUS__ =
        "ALTER TABLE `{0}` MODIFY COLUMN `INDEX_STATUS` BIGINT(10) NOT NULL DEFAULT 0 COMMENT ''0:CREATING,1:DELETE_ONLY,2:WRITE_ONLY,3:WRITE_REORG,4:PUBLIC,5:DELETE_REORG,6:REMOVING,7:ABSENT''";

    private static final String __DRSD__SYSTABLE__TABLES__ADD_UK__ =
        "ALTER TABLE `{0}` ADD UNIQUE KEY " + __DRDS__SYSTABLE__TABLES__UK_NAME
            + "(TABLE_CATALOG(64), TABLE_SCHEMA, TABLE_NAME)";

    private static final String __DRSD__SYSTABLE__TABLES__CHECK_UK__ =
        "SELECT COUNT(1) IndexIsThere FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema = DATABASE() AND table_name = ''{0}'' AND index_name=''"
            + __DRDS__SYSTABLE__TABLES__UK_NAME + "''";

    private static final String SELECT_PARTITIONED_TABLE_INFO =
        "select id, '''' as table_catalog, table_schema, table_name, (case tbl_type when 0 then 1 when 1 then 3 when 2 then 0 when 3 then 2 when 4 then 3 when 5 then 3 when 7 then 4 else -1 end ) as table_type, '' as db_partition_key, '' as db_partition_policy, "
            + "null as db_partition_count, '' as tb_partition_key, '' as tb_partition_policy, null as tb_partition_count, part_comment as comment from table_partitions";

    private static final String SELECT_PARTITIONED_TABLE_INFO_BY_SCHEMA_TABLE =
        SELECT_PARTITIONED_TABLE_INFO + " where table_schema=? and table_name= ? and part_level=0";

    private static final String SELECT_PARTITIONED_TABLE_INFO_BY_SCHEMA =
        SELECT_PARTITIONED_TABLE_INFO + " where table_schema=? and part_level=0";

    private static final String SELECT_ALL_PARTITIONED_TABLE_INFO =
        SELECT_PARTITIONED_TABLE_INFO + " where part_level=0";

    private static final String SELECT_DRDS_TABLE_INFO =
        "select id, table_catalog, table_schema, table_name, table_type, db_partition_key, db_partition_policy, db_partition_count, tb_partition_key, tb_partition_policy, tb_partition_count,"
            + " '''' as comment from " ;

    /**
     * check system table exists
     */
    private final static Cache<String, Boolean> APPNAME_GSI_ENABLED = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build();

    private final String schema;
    private DataSource dataSource;

    /**
     * you should ALWAYS create a new GsiMetaManager when you need it!
     */
    public GsiMetaManager(DataSource dataSource, String schema) {
        this.dataSource = dataSource;
        this.schema = schema;
    }

    public static void invalidateCache(String appname) {
        if (TStringUtil.isBlank(appname)) {
            return;
        }
        APPNAME_GSI_ENABLED.invalidate(appname);
    }

    public GsiMetaBean getAllGsiMetaBean(String schema) {
        return GsiMetaBean.initAllMeta(this, schema);
    }

    /**
     * this function only return GSI table meta beans (excluding main table meta beans)
     *
     * @param schemaNames filter condition of schema names, empty means no such condition
     * @param tableNames filter condition of table names, empty means no such condition
     */
    public GsiMetaBean getAllGsiMetaBean(Set<String> schemaNames, Set<String> tableNames) {
        return GsiMetaBean.initAllMeta(this, schemaNames, tableNames);
    }

    /**
     * Get table meta and all related gsi meta by primary table name
     *
     * @param tableName primary table name or index table name
     */
    public GsiTableMetaBean getTableMeta(String schema, String tableName, EnumSet<IndexStatus> statusSet) {
        final GsiMetaBean gsiMetaBean = GsiMetaBean.initTableMeta(this, schema, tableName, statusSet);
        return gsiMetaBean.getTableMeta().get(tableName);
    }

    public GsiTableMetaBean getTableMeta(String tableName, EnumSet<IndexStatus> statusSet) {
        return getTableMeta(this.schema, tableName, statusSet);
    }

    public GsiTableMetaBean initTableMeta(String tableName, List<IndexRecord> allIndexRecords,
                                          List<IndexRecord> indexRecordsByIndexName) {
        if (allIndexRecords == null) {
            allIndexRecords = new ArrayList<>();
        }

        if (indexRecordsByIndexName == null) {
            indexRecordsByIndexName = new ArrayList<>();
        }
        final GsiMetaBean gsiMetaBean =
            GsiMetaBean.initTableMeta(this, schema, tableName, allIndexRecords, indexRecordsByIndexName);
        return gsiMetaBean.getTableMeta().get(tableName);
    }

    /**
     * Get meta of gsi with specified status
     *
     * @param tableName primary table name or index tableName
     * @param statusSet gsi status
     */
    public GsiMetaBean getTableAndIndexMeta(String schema, String tableName, EnumSet<IndexStatus> statusSet) {
        final GsiMetaBean gsiMetaBean = GsiMetaBean.initTableMeta(this, schema, tableName, statusSet);
        return gsiMetaBean.getTableMeta().containsKey(tableName) ? gsiMetaBean : GsiMetaBean.empty();
    }

    public GsiMetaBean getTableAndIndexMeta(String tableName, EnumSet<IndexStatus> statusSet) {
        return getTableAndIndexMeta(this.schema, tableName, statusSet);
    }

    /**
     * Get meta of gsi with specified status
     *
     * @param mainTableName main table name
     * @param indexTableName index table name
     * @param statusSet gsi status
     */
    public GsiIndexMetaBean getIndexMeta(String schema, String mainTableName, String indexTableName,
                                         EnumSet<IndexStatus> statusSet) {
        final GsiMetaBean gsiMetaBean = GsiMetaBean.initTableMeta(this, schema, mainTableName, indexTableName,
            statusSet);
        final String resultMainTableName = gsiMetaBean.getIndexTableRelation().get(indexTableName);

        if (null == resultMainTableName) {
            return null;
        }

        final GsiTableMetaBean gsiTableMetaBean = gsiMetaBean.getTableMeta().get(resultMainTableName);
        return gsiTableMetaBean.indexMap.get(indexTableName);
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

        long newVersion;
        try (PreparedStatement pstmt = conn.prepareStatement("select last_insert_id()")) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            newVersion = rs.getLong(1);
        }

        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getTableDataId(schema, table), conn);
        return newVersion;
    }

    /**
     * for CREATE TABLE / CREATE INDEX / ALTER TABLE ADD INDEX
     */
    public void insertIndexMetaForPolarX(Connection connection, List<IndexRecord> indexRecords) {

        try {
//            Collections.sort(indexRecords, Comparator.comparing(IndexRecord::getColumnName));
//            Collections.reverse(indexRecords);
            doBatchInsert(getSqlAddIndexMeta(), ImmutableList.copyOf(indexRecords), connection);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "add Global Secondary Index meta failed!");
        }
    }

    /**
     * for CREATE TABLE / CREATE INDEX / ALTER TABLE ADD INDEX
     */
    public void changeTablesExtType(Connection connection,
                                    String schemaName,
                                    String targetTableName,
                                    int type) {
        try {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            tableInfoManager.alterTableType(schemaName, targetTableName, type);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "add Global Secondary Index meta failed!");
        }
    }

    /**
     * for ALTER TABLE ADD COLUMN
     */
    public void insertIndexMetaByAddColumn(Connection connection, String schemaName, String tableName,
                                           List<IndexRecord> indexRecords) {
        try {
            doBatchInsert(getSqlAddIndexMeta(), ImmutableList.copyOf(indexRecords), connection);

            TableInfoManager.updateTableVersion(schemaName, tableName, connection);
        } catch (SQLException e) {
            try {
                if (null != connection) {
                    connection.rollback();
                }
            } catch (SQLException ignored) {
            }
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "add Clustered Index columns failed!");
        }
    }

    /**
     * Alter all index in schema.table from before -> after.
     * Typical: Create table with GSI. creating -> public.
     * Alter table adding GSI or creating GSI. Status evolution.
     */
    public long updateIndexStatus(Connection connection, String schemaName, String tableName, String indexName,
                                  IndexStatus before,
                                  IndexStatus after) {
        long newVersion = 0;
        try {
            String sql;
            List params;
            if (before == null) {
                sql = getSqlUpdateIndexStatusAnyCurrentStatus();
                params = ImmutableList.of(stringParamRow(String
                    .valueOf(after.getValue()), schemaName, tableName, indexName));
            } else {
                sql = getSqlUpdateIndexStatus();
                params = ImmutableList.of(stringParamRow(String
                    .valueOf(after.getValue()), schemaName, tableName, indexName, String.valueOf(before.getValue())));
            }

            doExecuteUpdate(sql,
                params,
                connection);

            return newVersion;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "update Global Secondary Index status failed!");
        }
    }

    /**
     * Alter gsi visibility from before -> after.
     */
    public long updateIndexVisibility(Connection connection, String schemaName, String tableName, String indexName,
                                      IndexVisibility beforeVisibility, IndexVisibility afterVisibility) {
        try {
            String sql = getSqlUpdateIndexVisibility();
            List params = ImmutableList.of(
                stringParamRow(String
                        .valueOf(afterVisibility.getValue()), schemaName, tableName, indexName,
                    String.valueOf(beforeVisibility.getValue())
                ));
            doExecuteUpdate(sql,
                params,
                connection);

            return 0;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "update Global Secondary Index visibility failed!");
        }
    }

    /**
     * Alter gsi visibility from before -> after.
     */
    public long updateLackingLocalIndex(Connection connection, String schemaName, String tableName, String indexName,
                                        LackLocalIndexStatus lackingLocalIndex) {
        try {
            String sql = getSqlUpdateLocalIndexStatus();
            List params = ImmutableList.of(
                stringParamRow(String
                    .valueOf(lackingLocalIndex.getValue()), schemaName, tableName, indexName
                ));
            doExecuteUpdate(sql,
                params,
                connection);

            return 0;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "update Global Secondary Index local index status failed!");
        }
    }

    /**
     * Remove index meta of specified status, and remove table meta if no index after removing.
     * Typical: Removing index with creating status for rollback.
     */
    public void removeIndexMetaWithStatus(String schemaName, String tableName, IndexStatus status) {

        wrapWithTransaction(dataSource, connection -> {
            List<IndexRecord> indexRecords = doExecuteQuery(getSqlGetIndexInfoByPrimaryTableAndStatus(),
                stringParamRow(schemaName, tableName, String.valueOf(status.getValue())),
                connection,
                IndexRecord.ORM);
            if (indexRecords.size() > 0) {
                // Remove index meta.
                doExecuteUpdate(getSqlRemoveIndexMetaWithStatus(),
                    ImmutableList.of(stringParamRow(schemaName, tableName, String.valueOf(status.getValue()))),
                    connection);
                // Remove table meta.
                Set<String> indexNameSet = new HashSet<>();
                for (IndexRecord indexRecord : indexRecords) {
                    indexNameSet.add(indexRecord.getIndexName());
                }
                boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
                if (!isNewPartDb) {
                    for (String indexName : indexNameSet) {
                        doExecuteUpdate(getSqlRemoveTableMeta(),
                            ImmutableList.of(stringParamRow(schemaName, indexName)),
                            connection);
                    }
                } else {
                    for (String indexName : indexNameSet) {
                        removePartitionTableMeta(schemaName, indexName, connection);
                    }
                }
            }
        }, (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
            "update Global Secondary Index status failed!"));
    }

    /**
     * for ALTER DROP INDEX
     */
    public void removeIndexMeta(String schemaName, String tableName, String indexName) {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        wrapWithTransaction(dataSource, connection -> {
            doExecuteUpdate(getSqlRemoveIndexMeta(),
                ImmutableList.of(stringParamRow(schemaName, tableName, indexName)),
                connection);
            if (!isNewPartDb) {
                doExecuteUpdate(getSqlRemoveTableMeta(), ImmutableList.of(stringParamRow(schemaName, indexName)),
                    connection);
            } else {
                removePartitionTableMeta(schemaName, indexName, connection);
            }
        }, (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
            "remove Global Secondary Index meta failed!"));
    }

    /**
     * for ALTER DROP INDEX
     */
    public void removeIndexMeta(Connection connection, String schemaName, String tableName, String indexName) {
        try {
            doExecuteUpdate(getSqlRemoveIndexMeta(),
                ImmutableList.of(stringParamRow(schemaName, tableName, indexName)),
                connection);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "remove Global Secondary Index meta failed!");
        }
    }

    public void removePartitionTableMeta(String schemaName, String tableName, Connection connection) {
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(connection);
        tablePartitionAccessor.deleteTablePartitionConfigs(schemaName, tableName);
    }

    /**
     * for ALTER DROP COLUMN
     */
    public void removeColumnMeta(Connection connection, String schemaName, String tableName, String indexName,
                                 String columnName) {
        try {
            doExecuteUpdate(getSqlRemoveColumnMeta(),
                ImmutableList.of(stringParamRow(schemaName, tableName, indexName, columnName)),
                connection);
            TableInfoManager.updateTableVersion(schemaName, tableName, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "remove Global Secondary Index meta failed!");
        }
    }

    /**
     * for ALTER CHANGE COLUMN
     */
    public void changeColumnMeta(Connection connection, String schemaName, String tableName, String indexName,
                                 String oldColumnName, String newColumnName, String nullable) {
        try {
            doExecuteUpdate(getSqlChangeColumnMeta(),
                ImmutableList
                    .of(stringParamRow(newColumnName, nullable, schemaName, tableName, indexName, oldColumnName)),
                connection);
            TableInfoManager.updateTableVersion(schemaName, tableName, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "update Global Secondary Index meta failed!");
        }
    }

    /**
     * for ALTER CHANGE COLUMN
     */
    public void changeColumnMetaNotAddVersion(Connection connection, String schemaName, String tableName,
                                              String indexName, String oldColumnName, String newColumnName,
                                              String nullable) {

        try {
            doExecuteUpdate(getSqlChangeColumnMeta(),
                ImmutableList
                    .of(stringParamRow(newColumnName, nullable, schemaName, tableName, indexName, oldColumnName)),
                connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "update Global Secondary Index meta failed!");
        }
    }

    // ~ Native methods
    // -----------------------------------------------------------------------------------------------------

    @Override
    protected void doInit() {
        // PolarDB-X has its own schema change.
    }

    private boolean check() {
        return true;
    }

    private List<TableRecord> getAllTableRecords(String schema) {
        try {

            String queryAllTblInfoSql;
            if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                queryAllTblInfoSql = getSqlGetAllPartitionedTableInfo();
            } else {
                queryAllTblInfoSql = getSqlGetAllTableInfo();
            }
            return doExecuteQuery(queryAllTblInfoSql,
                stringParamRow(schema), dataSource, TableRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get table info from system table");
        }
    }

    /**
     * For information_schema, get all table records including GSI tables
     * both from new partition schemas and the old ones.
     *
     * @return all table records in schemas with {schemaNames} and have table names of {tableNames}
     */
    private List<TableRecord> getAllTableRecords(Set<String> schemaNames, Set<String> tableNames) {
        List<String> paramList = new ArrayList<>(schemaNames);
        paramList.addAll(tableNames);

        try {
            String queryAllTblInfoSql;
            if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                queryAllTblInfoSql = getSqlGetAllPartitionedTableInfo(schemaNames.size(), tableNames.size())
                    + " union all " + getSqlGetAllTableInfo(schemaNames.size(), tableNames.size());
                // Duplicate the parameters since we union two sql.
                paramList.addAll(paramList);
            } else {
                queryAllTblInfoSql = getSqlGetAllTableInfo(schemaNames.size(), tableNames.size());
            }
            return doExecuteQuery(queryAllTblInfoSql, stringParamRow(paramList), dataSource, TableRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get table info from system table");
        }
    }

    private List<IndexRecord> getAllIndexRecords(String schema) {
        try {
            return doExecuteQuery(getSqlGetAllIndexInfo(), stringParamRow(schema), dataSource, IndexRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get index info from system table");
        }
    }

    /**
     * get all GSI index records
     *
     * @return all GSI index records in schema with {schemaNames} and whose main table names are in {tableNames}
     */
    private List<IndexRecord> getAllIndexRecords(Set<String> schemaNames, Set<String> tableNames) {
        List<String> paramList = new ArrayList<>(schemaNames);
        paramList.addAll(tableNames);

        try {
            return doExecuteQuery(getSqlGetAllIndexInfoBySchemasTables(schemaNames.size(), tableNames.size()),
                stringParamRow(paramList),
                dataSource,
                IndexRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get index info from system table");
        }
    }

    private List<TableRecord> getAllTableAllSchemaRecords() {
        try {
            String getAllTblAllSchemaInfoSql;
            if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                getAllTblAllSchemaInfoSql =
                    getSqlGetAllTableAllSchemaInfo() + " union all " + getSqlGetAllPartitionedTableAllSchemaInfo();
            } else {
                getAllTblAllSchemaInfoSql = getSqlGetAllTableAllSchemaInfo();
            }
            return doExecuteQuery(
                getAllTblAllSchemaInfoSql,
                ImmutableMap.of(), dataSource, TableRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get table info from system table");
        }
    }

    private List<IndexRecord> getAllIndexAllSchemaRecords() {
        try {
            return doExecuteQuery(getSqlGetAllIndexAllSchemaInfo(), ImmutableMap.of(), dataSource, IndexRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get index info from system table");
        }
    }

    public List<TableRecord> getTableRecords(String schema, String tableName) {
        try {
            String queryTblInfoSql;
            if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                queryTblInfoSql = getSqlGetPartitionedTableInfo();
            } else {
                queryTblInfoSql = getSqlGetTableInfo();
            }
            return doExecuteQuery(queryTblInfoSql,
                stringParamRow(schema, tableName), dataSource,
                TableRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get table info for " + schema + "." + tableName + " from system table");
        }
    }

    private List<TableRecord> getTableRecords(String schema, List<String> tableNames) {
        if (GeneralUtil.isEmpty(tableNames)) {
            return ImmutableList.of();
        }
        try {
            String queryTblInfoSql;
            if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                queryTblInfoSql = getPartitionedTableInfoSql(tableNames);
            } else {
                queryTblInfoSql = getTableInfoSql(tableNames);
            }
            return doExecuteQuery(
                queryTblInfoSql,
                stringParamRow(ImmutableList.<String>builder().add(
                    schema).addAll(tableNames).build()), dataSource,
                TableRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get table info for " + TStringUtil.join(tableNames, ",") + " from system table");
        }
    }

    private List<IndexRecord> getIndexRecordsByIndexName(String schema, String indexName) {
        try {
            return doExecuteQuery(getSqlGetIndexInfoByIndex(), stringParamRow(schema, indexName), dataSource,
                IndexRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get index info for " + schema + "." + indexName + " from system table");
        }
    }

    public List<IndexRecord> getIndexRecords(String schema) {
        try {
            List<IndexRecord> indexRecords = doExecuteQuery(getSqlTplGetIndexInfoBySchema(),
                stringParamRow(ImmutableList.<String>builder().add(schema).build()),
                dataSource,
                IndexRecord.ORM);
            return indexRecords;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get index info for " + schema + " from system table");
        }
    }

    private List<IndexRecord> getIndexRecords(String schema, String tableName, List<String> statusSet) {
        try {
            return doExecuteQuery(expandInCondition(getSqlTplGetIndexInfoByPrimaryAndStatuses(), statusSet.size()),
                stringParamRow(ImmutableList.<String>builder().add(schema).add(tableName).addAll(statusSet).build()),
                dataSource,
                IndexRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get index info for " + schema + "." + tableName + " from system table");
        }
    }

    private List<IndexRecord> getIndexRecords(String schema, String tableName, String indexName,
                                              List<String> statusSet) {
        try {
            return doExecuteQuery(
                expandInCondition(getSqlTplGetIndexInfoByPrimaryAndIndexAndStatus(), statusSet.size()),
                stringParamRow(ImmutableList.<String>builder().add(schema).add(tableName).add(indexName)
                    .addAll(statusSet).build()),
                dataSource,
                IndexRecord.ORM);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                e,
                "cannot get index info for " + schema + "." + tableName + " from system table");
        }
    }

    // ~ Basic data access methods
    // ------------------------------------------------------------------------------------------

    private static final String SQL_ADD_INDEX_META =
        "insert into {0} (TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,NON_UNIQUE,INDEX_SCHEMA,INDEX_NAME,SEQ_IN_INDEX,"
            + "COLUMN_NAME,COLLATION,CARDINALITY,SUB_PART,PACKED,NULLABLE,INDEX_TYPE,COMMENT,INDEX_COMMENT,"
            + "INDEX_COLUMN_TYPE,INDEX_LOCATION,INDEX_TABLE_NAME,INDEX_STATUS,VERSION) values "
            + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SQL_ADD_INDEX_META_X =
        "insert into {0} (TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,NON_UNIQUE,INDEX_SCHEMA,INDEX_NAME,SEQ_IN_INDEX,"
            + "COLUMN_NAME,COLLATION,CARDINALITY,SUB_PART,PACKED,NULLABLE,INDEX_TYPE,COMMENT,INDEX_COMMENT,"
            + "INDEX_COLUMN_TYPE,INDEX_LOCATION,INDEX_TABLE_NAME,INDEX_STATUS,VERSION,FLAG) values "
            + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private String getSqlAddIndexMeta() {
        return MessageFormat.format(SQL_ADD_INDEX_META_X, GmsSystemTables.INDEXES);
    }

    private static final String SQL_ADD_TABLE_META =
        "INSERT INTO {0} (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, DB_PARTITION_KEY, DB_PARTITION_POLICY, "
            + "DB_PARTITION_COUNT, TB_PARTITION_KEY, TB_PARTITION_POLICY, TB_PARTITION_COUNT, COMMENT) values (?, "
            + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private String getSqlAddTableMeta() {
        return MessageFormat.format(SQL_ADD_TABLE_META, GmsSystemTables.TABLES_EXT);
    }

    private static final String SQL_GET_INDEX_INFO_BY_INDEX =
        "SELECT * FROM {0} WHERE TABLE_SCHEMA = ? AND INDEX_NAME = ? AND INDEX_LOCATION = 1 ORDER BY VERSION, SEQ_IN_INDEX";

    private String getSqlGetIndexInfoByIndex() {
        return MessageFormat.format(SQL_GET_INDEX_INFO_BY_INDEX, GmsSystemTables.INDEXES);
    }

    private static final String SQL_GET_ALL_INDEX_INFO =
        "SELECT * FROM {0} WHERE TABLE_SCHEMA = ? AND INDEX_LOCATION = 1 ORDER BY INDEX_NAME, VERSION, SEQ_IN_INDEX";

    private String getSqlGetAllIndexInfo() {
        return MessageFormat.format(SQL_GET_ALL_INDEX_INFO, GmsSystemTables.INDEXES);
    }

    private static final String SQL_GET_ALL_FROM_PARAM_TABLE = "SELECT * FROM {0} ";

    /**
     * get a SQL that queries the [indexes] system table for GSI
     * <p>
     * if schemaNum == 0, "AND TABLE_SCHEMA IN (?,...)" will not appear in the returned SQL
     * if tableNum == 0, "AND TABLE_NAME IN (?,...)" will not appear in the returned SQL
     *
     * @return SELECT * FROM {indexes} WHERE INDEX_LOCATION = 1
     * AND TABLE_SCHEMA IN (?,?...) AND TABLE_NAME IN (?,?,...)
     * ORDER BY INDEX_NAME, VERSION, SEQ_IN_INDEX
     */
    private String getSqlGetAllIndexInfoBySchemasTables(int schemaNum, int tableNum) {
        StringBuilder sb = new StringBuilder(SQL_GET_ALL_FROM_PARAM_TABLE);
        sb.append(" WHERE INDEX_LOCATION = 1 ");

        if (schemaNum > 0) {
            sb.append("AND TABLE_SCHEMA IN (");
            for (int i = 0; i < schemaNum - 1; i++) {
                sb.append("?,");
            }
            sb.append("?) ");
        }

        if (tableNum > 0) {
            sb.append("AND TABLE_NAME IN (");
            for (int i = 0; i < tableNum - 1; i++) {
                sb.append("?,");
            }
            sb.append("?) ");
        }

        sb.append(" ORDER BY INDEX_NAME, VERSION, SEQ_IN_INDEX");
        return MessageFormat.format(sb.toString(), GmsSystemTables.INDEXES);
    }

    private static final String SQL_GET_ALL_INDEX_ALL_SCHEMA_INFO =
        "SELECT * FROM {0} WHERE INDEX_LOCATION = 1 ORDER BY INDEX_NAME, VERSION, SEQ_IN_INDEX";

    private String getSqlGetAllIndexAllSchemaInfo() {
        return MessageFormat.format(SQL_GET_ALL_INDEX_ALL_SCHEMA_INFO, GmsSystemTables.INDEXES);
    }

    private static final String SQL_GET_TABLE_INFO =
        SELECT_DRDS_TABLE_INFO + " {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

    private String getSqlGetTableInfo() {
        return MessageFormat.format(SQL_GET_TABLE_INFO, GmsSystemTables.TABLES_EXT);
    }

    private String getSqlGetPartitionedTableInfo() {
        return SELECT_PARTITIONED_TABLE_INFO_BY_SCHEMA_TABLE;
    }

    private static final String SQL_GET_INDEX_INFO_BY_PRIMARY_TABLE_AND_STATUS =
        "SELECT * FROM {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_STATUS = ? AND INDEX_LOCATION = 1 ORDER BY VERSION, SEQ_IN_INDEX";

    private String getSqlGetIndexInfoByPrimaryTableAndStatus() {
        return MessageFormat.format(SQL_GET_INDEX_INFO_BY_PRIMARY_TABLE_AND_STATUS, GmsSystemTables.INDEXES);
    }

    private static final String SQL_TPL_GET_INDEX_INFO_BY_PRIMARY_AND_STATUSES =
        "SELECT * FROM {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_STATUS IN ({1}) AND INDEX_LOCATION = 1 ORDER BY VERSION, SEQ_IN_INDEX";

    private static final String SQL_TPL_GET_INDEX_INFO_BY_SCHEMA =
        "SELECT * FROM {0} WHERE TABLE_SCHEMA = ? AND INDEX_LOCATION = 1 ORDER BY VERSION, SEQ_IN_INDEX";

    private String getSqlTplGetIndexInfoByPrimaryAndStatuses() {
        return MessageFormat
            .format(SQL_TPL_GET_INDEX_INFO_BY_PRIMARY_AND_STATUSES, GmsSystemTables.INDEXES, "{0}");
    }

    private String getSqlTplGetIndexInfoBySchema() {
        return MessageFormat
            .format(SQL_TPL_GET_INDEX_INFO_BY_SCHEMA, GmsSystemTables.INDEXES, "{0}");
    }

    private static final String SQL_TPL_GET_INDEX_INFO_BY_PRIMARY_AND_INDEX_AND_STATUS =
        "SELECT * FROM {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND INDEX_STATUS IN ({1}) AND INDEX_LOCATION = 1 ORDER BY VERSION, SEQ_IN_INDEX";

    private String getSqlTplGetIndexInfoByPrimaryAndIndexAndStatus() {
        return MessageFormat
            .format(SQL_TPL_GET_INDEX_INFO_BY_PRIMARY_AND_INDEX_AND_STATUS, GmsSystemTables.INDEXES, "{0}");
    }

    private String expandInCondition(String tpl, int statusCount) {
        return MessageFormat.format(tpl, IntStream.range(0, statusCount).mapToObj(i -> "?")
            .collect(Collectors.joining(", ")));
    }

    private String getTableInfoSql(List<String> tableNames) {
        StringBuilder sql = new StringBuilder(
            SELECT_DRDS_TABLE_INFO
                + GmsSystemTables.TABLES_EXT
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN( ");
        boolean first = true;
        for (String tableName : tableNames) {
            if (!first) {
                sql.append(", ");
            } else {
                first = false;
            }

            sql.append("?");
        }
        sql.append(" )");

        return sql.toString();
    }

    private String getPartitionedTableInfoSql(List<String> tableNames) {
        StringBuilder sql = new StringBuilder(SELECT_PARTITIONED_TABLE_INFO
            + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN( ");
        boolean first = true;
        for (String tableName : tableNames) {
            if (!first) {
                sql.append(", ");
            } else {
                first = false;
            }

            sql.append("?");
        }
        sql.append(" ) and part_level = 0");

        return sql.toString();
    }

    private static final String SQL_GET_ALL_TABLE_INFO = SELECT_DRDS_TABLE_INFO + " {0} WHERE TABLE_SCHEMA = ?";

    private String getSqlGetAllTableInfo() {
        return MessageFormat.format(SQL_GET_ALL_TABLE_INFO, GmsSystemTables.TABLES_EXT);
    }

    /**
     * get a SQL that queries the [tables_ext] system table for old partition tables
     * <p>
     * if schemaNum == 0, "AND TABLE_SCHEMA IN (?,...)" will not appear in the returned SQL
     * if tableNum == 0, "AND TABLE_NAME IN (?,...)" will not appear in the returned SQL
     *
     * @return SELECT * FROM {tables_ext} WHERE 1
     * AND TABLE_SCHEMA IN (?,?...) AND TABLE_NAME IN (?,?,...)
     */
    private String getSqlGetAllTableInfo(int schemaNum, int tableNum) {
        StringBuilder sb = new StringBuilder(SELECT_DRDS_TABLE_INFO);
        sb.append(" {0} WHERE 1 ");
        if (schemaNum > 0) {
            sb.append("AND TABLE_SCHEMA IN (");
            for (int i = 0; i < schemaNum - 1; i++) {
                sb.append("?,");
            }
            sb.append("?) ");
        }

        if (tableNum > 0) {
            sb.append("AND TABLE_NAME IN (");
            for (int i = 0; i < tableNum - 1; i++) {
                sb.append("?,");
            }
            sb.append("?)");
        }

        return MessageFormat.format(sb.toString(), GmsSystemTables.TABLES_EXT);
    }

    private String getSqlGetAllPartitionedTableInfo() {
        return SELECT_PARTITIONED_TABLE_INFO_BY_SCHEMA;
    }

    /**
     * get a SQL that queries the [table_partitions] system table for new partition tables
     * <p>
     * if schemaNum == 0, "AND TABLE_SCHEMA IN (?,...)" will not appear in the returned SQL
     * if tableNum == 0, "AND TABLE_NAME IN (?,...)" will not appear in the returned SQL
     *
     * @return SELECT * FROM {table_partitions} WHERE PART_LEVEL = 0
     * AND TABLE_SCHEMA IN (?,?...) AND TABLE_NAME IN (?,?,...)
     */
    private String getSqlGetAllPartitionedTableInfo(int schemaNum, int tableNum) {
        StringBuilder sb = new StringBuilder(SELECT_ALL_PARTITIONED_TABLE_INFO);

        if (schemaNum > 0) {
            sb.append(" AND TABLE_SCHEMA IN (");
            for (int i = 0; i < schemaNum - 1; i++) {
                sb.append("?,");
            }
            sb.append("?) ");
        }

        if (tableNum > 0) {
            sb.append(" AND TABLE_NAME IN (");
            for (int i = 0; i < tableNum - 1; i++) {
                sb.append("?,");
            }
            sb.append("?)");
        }

        return sb.toString();
    }

    private static final String SQL_GET_ALL_TABLE_ALL_SCHEMA_INFO = SELECT_DRDS_TABLE_INFO + " {0}";

    private String getSqlGetAllTableAllSchemaInfo() {
        return MessageFormat.format(SQL_GET_ALL_TABLE_ALL_SCHEMA_INFO, GmsSystemTables.TABLES_EXT);
    }

    private String getSqlGetAllPartitionedTableAllSchemaInfo() {
        return SELECT_ALL_PARTITIONED_TABLE_INFO;
    }

    private static final String SQL_UPDATE_ALL_INDEX_STATUS =
        "UPDATE {0} SET INDEX_STATUS = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_STATUS = ? AND INDEX_LOCATION = 1";

    private String getSqlUpdateAllIndexStatus() {
        return MessageFormat.format(SQL_UPDATE_ALL_INDEX_STATUS, GmsSystemTables.INDEXES);
    }

    private static final String SQL_UPDATE_INDEX_STATUS =
        "UPDATE {0} SET INDEX_STATUS = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND INDEX_STATUS = ? AND INDEX_LOCATION = 1";

    private static final String SQL_UPDATE_INDEX_STATUS_ANY_CURRENT_STATUS =
        "UPDATE {0} SET INDEX_STATUS = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND INDEX_LOCATION = 1";

    private static final String SQL_UPDATE_INDEX_VISIBILITY =
        "UPDATE {0} SET VISIBLE = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND VISIBLE = ? AND INDEX_LOCATION = 1";

    private static final String SQL_UPDATE_LOCAL_INDEX_STATUS =
        "UPDATE {0} SET LACKING_LOCAL_INDEX = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND INDEX_LOCATION = 1";

    private String getSqlUpdateIndexStatus() {
        return MessageFormat.format(SQL_UPDATE_INDEX_STATUS, GmsSystemTables.INDEXES);
    }

    private String getSqlUpdateIndexStatusAnyCurrentStatus() {
        return MessageFormat.format(SQL_UPDATE_INDEX_STATUS_ANY_CURRENT_STATUS, GmsSystemTables.INDEXES);
    }

    private String getSqlUpdateIndexVisibility() {
        return MessageFormat.format(SQL_UPDATE_INDEX_VISIBILITY, GmsSystemTables.INDEXES);
    }

    private String getSqlUpdateLocalIndexStatus() {
        return MessageFormat.format(SQL_UPDATE_LOCAL_INDEX_STATUS, GmsSystemTables.INDEXES);
    }

    private static final String SQL_REMOVE_INDEX_META_WITH_STATUS =
        "DELETE FROM {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_STATUS = ? AND INDEX_LOCATION = 1";

    private String getSqlRemoveIndexMetaWithStatus() {
        return MessageFormat.format(SQL_REMOVE_INDEX_META_WITH_STATUS, GmsSystemTables.INDEXES);
    }

    private static final String SQL_CLEAR_INDEX_META =
        "DELETE FROM {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_LOCATION = 1";

    private String getSqlClearIndexMeta() {
        return MessageFormat.format(SQL_CLEAR_INDEX_META, GmsSystemTables.INDEXES);
    }

    private static final String SQL_REMOVE_INDEX_META =
        "DELETE FROM {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND INDEX_LOCATION = 1";

    private String getSqlRemoveIndexMeta() {
        return MessageFormat.format(SQL_REMOVE_INDEX_META, GmsSystemTables.INDEXES);
    }

    private static final String SQL_REMOVE_TABLE_META = "DELETE FROM {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    private static final String DELETE_TABLE_PARTITIONS_BY_TABLE_NAME =
        "delete from table_partitions where table_schema=? and table_name=?";

    private String getSqlRemoveTableMeta() {
        return MessageFormat.format(SQL_REMOVE_TABLE_META, GmsSystemTables.TABLES_EXT);
    }

    private static final String SQL_RENAME_INDEX_PRIMARY_RELATION_META =
        "UPDATE {0} SET INDEX_NAME = ? , INDEX_TABLE_NAME = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND INDEX_LOCATION = 1";

    private String getSqlRenameIndexPrimaryRelationMeta() {
        return MessageFormat.format(SQL_RENAME_INDEX_PRIMARY_RELATION_META, GmsSystemTables.INDEXES);
    }

    private static final String SQL_RENAME_INDEX_TABLE_META =
        "UPDATE {0} SET TABLE_NAME = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE=" + TableType.GSI
            .getValue();

    private String getSqlRenameIndexTableMeta() {
        return MessageFormat.format(SQL_RENAME_INDEX_TABLE_META, GmsSystemTables.TABLES_EXT);
    }

    private static final String SQL_RENAME_PRIMARY_INDEX_RELATION_META =
        "UPDATE {0} SET TABLE_NAME = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_LOCATION = 1";

    private String getSqlRenamePrimaryIndexRelationMeta() {
        return MessageFormat.format(SQL_RENAME_PRIMARY_INDEX_RELATION_META, GmsSystemTables.INDEXES);
    }

    private static final String SQL_RENAME_PRIMARY_TABLE_META =
        "UPDATE {0} SET TABLE_NAME = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

    private String getSqlRenamePrimaryTableMeta() {
        return MessageFormat.format(SQL_RENAME_PRIMARY_TABLE_META, GmsSystemTables.TABLES_EXT);
    }

    private static final String SQL_REMOVE_COLUMN_META =
        "DELETE FROM {0} WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND COLUMN_NAME = ? AND INDEX_LOCATION = 1";

    private String getSqlRemoveColumnMeta() {
        return MessageFormat.format(SQL_REMOVE_COLUMN_META, GmsSystemTables.INDEXES);
    }

    private static final String SQL_CHANGE_COLUMN_META =
        "UPDATE {0} SET COLUMN_NAME = ? , NULLABLE = ? WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND COLUMN_NAME = ? AND INDEX_LOCATION = 1";

    private String getSqlChangeColumnMeta() {
        return MessageFormat.format(SQL_CHANGE_COLUMN_META, GmsSystemTables.INDEXES);
    }

    private Map<Integer, ParameterContext> stringParamRow(String... values) {
        final Map<Integer, ParameterContext> result = new HashMap<>();
        Ord.zip(values)
            .forEach(ord -> result.put(ord.i + 1,
                new ParameterContext(ParameterMethod.setString, new Object[] {ord.i + 1, ord.e})));

        return result;
    }

    private Map<Integer, ParameterContext> stringParamRow(Collection<String> values) {
        final Map<Integer, ParameterContext> result = new HashMap<>();
        Ord.zip(values)
            .forEach(ord -> result.put(ord.i + 1,
                new ParameterContext(ParameterMethod.setString, new Object[] {ord.i + 1, ord.e})));

        return result;
    }

    private Map<Integer, ParameterContext> addLongParam(Map<Integer, ParameterContext> current, Long newParam) {
        final int index = current.size();
        current.put(index, new ParameterContext(ParameterMethod.setLong, new Object[] {index, newParam}));
        return current;
    }

    private <T> List<T> doExecuteQuery(String sql, DataSource dataSource, Orm<T> orm) throws SQLException {
        try (Connection connection = getConnectionForWrite(dataSource)) {
            return doExecuteQuery(sql, ImmutableMap.of(), connection, orm);
        }
    }

    private <T> List<T> doExecuteQuery(String sql, Map<Integer, ParameterContext> params, DataSource dataSource,
                                       Orm<T> orm) throws SQLException {
        try (Connection connection = getConnectionForWrite(dataSource)) {
            return doExecuteQuery(sql, params, connection, orm);
        }
    }

    private <T> List<T> doExecuteQuery(String sql, Map<Integer, ParameterContext> params, Connection connection,
                                       Orm<T> orm) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (ParameterContext param : params.values()) {
                param.getParameterMethod().setParameter(ps, param.getArgs());
            }

            final ResultSet resultSet = ps.executeQuery();
            final List<T> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(orm.convert(resultSet));
            }
            resultSet.close();
            return result;
        }
    }

    private void doExecuteUpdate(String sql, DataSource dataSource) throws SQLException {
        try (Connection connection = getConnectionForWrite(dataSource);
            Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private void doExecuteUpdate(String sql, List<Map<Integer, ParameterContext>> params,
                                 Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Map<Integer, ParameterContext> batch : params) {
                for (ParameterContext param : batch.values()) {
                    param.getParameterMethod().setParameter(ps, param.getArgs());
                }
                ps.addBatch();
            }

            ps.executeBatch();
        }
    }

    public static void doBatchInsert(String sql, List<Orm> params, Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Orm orm : params) {
                final Map<Integer, Object> row = orm.params();
                for (Entry<Integer, Object> value : row.entrySet()) {
                    ps.setObject(value.getKey(), value.getValue());
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // ~ Data model
    // ---------------------------------------------------------------------------------------------------------

    public enum IndexColumnType {
        INDEX(0), COVERING(1);

        private final int value;

        IndexColumnType(int value) {
            this.value = value;
        }

        public static IndexColumnType from(int value) {
            switch (value) {
            case 0:
                return INDEX;
            case 1:
                return COVERING;
            default:
                return null;
            }
        }

        public int getValue() {
            return value;
        }
    }

    public enum TableType {
        SINGLE(0), SHARDING(1), BROADCAST(2), GSI(3), COLUMNAR(4);

        private final int value;

        TableType(int value) {
            this.value = value;
        }

        public static TableType from(int value) {
            switch (value) {
            case 0:
                return SINGLE;
            case 1:
                return SHARDING;
            case 2:
                return BROADCAST;
            case 3:
                return GSI;
            case 4:
                return COLUMNAR;
            default:
                return null;
            }
        }

        public int getValue() {
            return value;
        }

        public boolean isPrimary() {
            return GSI != this;
        }
    }

    private interface Orm<T> {

        public T convert(ResultSet resultSet) throws SQLException;

        public Map<Integer, Object> params();
    }

    public static class GsiMetaBean extends GsiMeta {

        /**
         * map[table_name, table_meta]
         */
        private Map<String, GsiTableMetaBean> tableMeta;
        /**
         * map[index_table_name, table_name]
         */
        private Map<String, String> indexTableRelation;

        public boolean withGsi(String tableName) {
            return !isEmpty() && tableMeta.containsKey(tableName)
                && tableMeta.get(tableName).tableType != TableType.GSI
                && GeneralUtil.isNotEmpty(tableMeta.get(tableName).indexMap);
        }

        public boolean isGsi(String tableName) {
            return !isEmpty() && tableMeta.containsKey(tableName)
                && tableMeta.get(tableName).tableType == TableType.GSI;
        }

        public boolean isColumnar(String tableName) {
            return !isEmpty() && tableMeta.containsKey(tableName)
                && tableMeta.get(tableName).tableType == TableType.COLUMNAR;
        }

        public boolean withColumnar(String tableName) {
            return !isEmpty() && tableMeta.containsKey(tableName)
                && tableMeta.get(tableName).tableType != TableType.COLUMNAR
                && GeneralUtil.isNotEmpty(tableMeta.get(tableName).indexMap)
                && tableMeta.get(tableName).indexMap.values().stream().anyMatch(e -> e.columnarIndex);
        }

        public boolean isGsiOrCci(String tableName) {
            return !isEmpty()
                && tableMeta.containsKey(tableName)
                && (tableMeta.get(tableName).tableType == TableType.GSI
                || tableMeta.get(tableName).tableType == TableType.COLUMNAR);
        }

        public boolean isEmpty() {
            return GeneralUtil.isEmpty(tableMeta) && GeneralUtil.isEmpty(indexTableRelation);
        }

        public static GsiMetaBean empty() {
            final GsiMetaBean result = new GsiMetaBean();

            result.tableMeta = ImmutableMap.of();
            result.indexTableRelation = ImmutableMap.of();

            return result;
        }

        public static GsiMetaBean initAllMeta(GsiMetaManager gsiMetaManager, String schema) {
            final List<IndexRecord> allIndexRecords = null == schema ?
                gsiMetaManager.getAllIndexAllSchemaRecords() : gsiMetaManager.getAllIndexRecords(schema);
            final List<TableRecord> allTableRecords = null == schema ?
                gsiMetaManager.getAllTableAllSchemaRecords() : gsiMetaManager.getAllTableRecords(schema);

            return mergeIndexTableRecords(allIndexRecords, allTableRecords, null == schema);
        }

        /**
         * this function only return GSI meta beans related to {schemaNames} and {tableNames}
         * note that {schemaNames} and {tableNames} can be empty, but can not be null
         */
        public static GsiMetaBean initAllMeta(GsiMetaManager gsiMetaManager,
                                              Set<String> schemaNames,
                                              Set<String> tableNames) {
            if (schemaNames == null || tableNames == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    "init GSI meta failed, schemaNames/tableNames can not be null");
            }
            // expect to only get GSI index records related to {schemaNames} and {tableNames}
            final List<IndexRecord> allIndexRecords = gsiMetaManager.getAllIndexRecords(schemaNames, tableNames);
            // collect all GSI index names
            Set<String> indexNames = allIndexRecords.stream().map(e -> e.indexName).collect(Collectors.toSet());
            // expect to only get GSI table records related to {schemaNames} and {indexNames}
            final List<TableRecord> allTableRecords = gsiMetaManager.getAllTableRecords(schemaNames, indexNames);

            return mergeIndexTableRecords(allIndexRecords, allTableRecords, true);
        }

        private static GsiMetaBean mergeIndexTableRecords(List<IndexRecord> allIndexRecords,
                                                          List<TableRecord> allTableRecords,
                                                          boolean fullname) {
            final GsiMetaBean result = new GsiMetaBean();

            // table name -> {GSI index name -> index meta bean}
            final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap = TreeMaps.caseInsensitiveMap();
            // GSI index name -> table name
            final Builder<String, String> indexTableRelationBuilder = mergeIndexRecords(allIndexRecords,
                tmpTableIndexMap, fullname);
            // table name or GSI index name -> table meta bean
            final Builder<String, GsiTableMetaBean> tableMetaBuilder = mergeTableRecords(allTableRecords,
                tmpTableIndexMap, fullname);

            result.indexTableRelation = indexTableRelationBuilder.build();
            result.tableMeta = tableMetaBuilder.build();

            try {
                addColumnarOptions(result);
            } catch (Throwable t) {
                logger.error("Add columnar options failed.", t);
            }

            return result;
        }

        /**
         * @param tableName main table name or index table name
         */
        public static GsiMetaBean initTableMeta(GsiMetaManager gsiMetaManager, String schema, String tableName,
                                                List<IndexRecord> allIndexRecords,
                                                List<IndexRecord> indexRecordsByIndexName) {
            final GsiMetaBean result = new GsiMetaBean();

            String mainTableName = tableName;
            if (GeneralUtil.isEmpty(allIndexRecords) && !GeneralUtil.isEmpty(indexRecordsByIndexName)) {
                mainTableName = indexRecordsByIndexName.get(0).tableName;
            }
            allIndexRecords.addAll(indexRecordsByIndexName);

            final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap =
                Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            final Builder<String, String> indexTableRelationBuilder = mergeIndexRecords(allIndexRecords,
                tmpTableIndexMap);
            result.indexTableRelation = indexTableRelationBuilder.build();

            if (GeneralUtil.isNotEmpty(tmpTableIndexMap)) {
                final List<TableRecord> allTableRecords =
                    gsiMetaManager.getTableRecords(schema, ImmutableList.copyOf(tmpTableIndexMap.get(mainTableName)
                        .keySet()));
                final Builder<String, GsiTableMetaBean> tableMetaBuilder = mergeTableRecords(allTableRecords,
                    tmpTableIndexMap);
                result.tableMeta = tableMetaBuilder.build();
            } else {
                result.tableMeta = ImmutableMap.of();
            }

            try {
                addColumnarOptions(result);
            } catch (Throwable t) {
                logger.error("Add columnar options failed.", t);
            }

            return result;
        }

        /**
         * @param tableName main table name or index table name
         */
        public static GsiMetaBean initTableMeta(GsiMetaManager gsiMetaManager, String schema, String tableName,
                                                EnumSet<IndexStatus> statusSet) {
            final GsiMetaBean result = new GsiMetaBean();

            final List<String> statuses = statusSet.stream().map(s -> String.valueOf(s.getValue())).collect(
                Collectors.toList());
            final List<IndexRecord> allIndexRecords = gsiMetaManager.getIndexRecords(schema, tableName, statuses);
            final List<IndexRecord> indexRecordsByIndexName = gsiMetaManager.getIndexRecordsByIndexName(schema,
                tableName);
            String mainTableName = tableName;
            if (GeneralUtil.isEmpty(allIndexRecords) && !GeneralUtil.isEmpty(indexRecordsByIndexName)) {
                mainTableName = indexRecordsByIndexName.get(0).tableName;
            }
            allIndexRecords.addAll(indexRecordsByIndexName);

            final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap =
                Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            final Builder<String, String> indexTableRelationBuilder = mergeIndexRecords(allIndexRecords,
                tmpTableIndexMap);
            result.indexTableRelation = indexTableRelationBuilder.build();

            if (GeneralUtil.isNotEmpty(tmpTableIndexMap)) {
                final List<TableRecord> allTableRecords =
                    gsiMetaManager.getTableRecords(schema, ImmutableList.copyOf(tmpTableIndexMap.get(mainTableName)
                        .keySet()));
                final Builder<String, GsiTableMetaBean> tableMetaBuilder = mergeTableRecords(allTableRecords,
                    tmpTableIndexMap);
                result.tableMeta = tableMetaBuilder.build();
            } else {
                result.tableMeta = ImmutableMap.of();
            }

            try {
                addColumnarOptions(result);
            } catch (Throwable t) {
                logger.error("Add columnar options failed.", t);
            }

            return result;
        }

        public static void addColumnarOptions(GsiMetaBean gsiMetaBean) {
            String schemaName = null;
            String tableName = null;
            Map<String, GsiIndexMetaBean> columnarMetaBeans = new HashMap<>();
            for (GsiTableMetaBean gsiTableMetaBean : gsiMetaBean.tableMeta.values()) {
                GsiMetaManager.GsiIndexMetaBean indexMetaBean = gsiTableMetaBean.gsiMetaBean;
                if (null != indexMetaBean && indexMetaBean.indexStatus.isPublished() && indexMetaBean.columnarIndex) {
                    columnarMetaBeans.put(indexMetaBean.indexName, indexMetaBean);
                    if (null == schemaName) {
                        schemaName = indexMetaBean.tableSchema;
                        tableName = indexMetaBean.tableName;
                    }
                }
            }
            if (MapUtils.isEmpty(columnarMetaBeans)) {
                return;
            }

            // cci name -> config key -> record
            Map<String, Map<String, String>> records = new HashMap<>();
            Map<String, String> globalConfig = new HashMap<>();
            MetaDbUtil.generateColumnarConfig(schemaName, tableName, records, globalConfig);

            if (records.isEmpty()) {
                return;
            }

            for (Entry<String, GsiIndexMetaBean> gsiIndexMetaBean : columnarMetaBeans.entrySet()) {
                gsiIndexMetaBean.getValue().updateColumnarOptions(gsiIndexMetaBean.getKey(), records, globalConfig);
            }
        }

        /**
         * @param tableName main table name
         * @param indexName index table name
         */
        public static GsiMetaBean initTableMeta(GsiMetaManager gsiMetaManager, String schema, String tableName,
                                                String indexName, EnumSet<IndexStatus> statusSet) {
            final GsiMetaBean result = new GsiMetaBean();

            final List<String> statuses = statusSet.stream().map(s -> String.valueOf(s.getValue())).collect(
                Collectors.toList());
            final List<IndexRecord> allIndexRecords = gsiMetaManager.getIndexRecords(schema, tableName, indexName,
                statuses);
            final List<TableRecord> allTableRecords = gsiMetaManager.getTableRecords(schema, indexName);

            final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap = TreeMaps.caseInsensitiveMap();
            final Builder<String, String> indexTableRelationBuilder = mergeIndexRecords(allIndexRecords,
                tmpTableIndexMap);
            final Builder<String, GsiTableMetaBean> tableMetaBuilder = mergeTableRecords(allTableRecords,
                tmpTableIndexMap);

            result.indexTableRelation = indexTableRelationBuilder.build();
            result.tableMeta = tableMetaBuilder.build();

            try {
                addColumnarOptions(result);
            } catch (Throwable t) {
                logger.error("Add columnar options failed.", t);
            }

            return result;
        }

        public static Map<String, String> initIndexTableRelation(GsiMetaManager gsiMetaManager, String schema,
                                                                 String indexName) {
            final List<IndexRecord> allIndexRecords = gsiMetaManager.getIndexRecordsByIndexName(schema, indexName);

            final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap = TreeMaps.caseInsensitiveMap();
            final Builder<String, String> indexTableRelationBuilder = mergeIndexRecords(allIndexRecords,
                tmpTableIndexMap);
            return indexTableRelationBuilder.build();
        }

        public static Builder<String, GsiTableMetaBean> mergeTableRecords(List<TableRecord> allTableRecords,
                                                                          final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap) {
            return mergeTableRecords(allTableRecords, tmpTableIndexMap, false);
        }

        public static Builder<String, GsiTableMetaBean> mergeTableRecords(List<TableRecord> allTableRecords,
                                                                          final Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap,
                                                                          boolean fullName) {

            final Builder<String, GsiTableMetaBean> tableMetaBuilder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);

            // index tables in __DRDS__SYSTABLE__TABLES__
            final Set<String> tableNameSet = new HashSet<>();
            for (final TableRecord tableRecord : allTableRecords) {
                if (tableRecord.tableType != TableType.GSI.getValue()
                    && tableRecord.tableType != TableType.COLUMNAR.getValue()) {
                    continue;
                }

                final TableType tableType = TableType.from((int) tableRecord.tableType);
                GsiIndexMetaBean indexMetaBean = null;
                for (Entry<String, Map<String, GsiIndexMetaBean>> entry : tmpTableIndexMap.entrySet()) {
                    indexMetaBean = entry.getValue().get(fullName ?
                        SqlIdentifier.surroundWithBacktick(tableRecord.tableSchema) + "." + SqlIdentifier
                            .surroundWithBacktick(tableRecord.tableName) : tableRecord.tableName);
                    if (indexMetaBean != null) {
                        break;
                    }
                }
                final GsiTableMetaBean tableMeta = new GsiTableMetaBean(tableRecord.tableCatalog,
                    tableRecord.tableSchema,
                    tableRecord.tableName,
                    tableType,
                    tableRecord.dbPartitionKey,
                    tableRecord.dbPartitionPolicy,
                    tableRecord.dbPartitionCount,
                    tableRecord.tbPartitionKey,
                    tableRecord.tbPartitionPolicy,
                    tableRecord.tbPartitionCount,
                    (tableType == TableType.GSI || tableType == TableType.COLUMNAR) ?
                        TreeMaps.caseInsensitiveMap() :
                        tmpTableIndexMap.get(fullName ?
                            SqlIdentifier.surroundWithBacktick(tableRecord.tableSchema) + "." + SqlIdentifier
                                .surroundWithBacktick(tableRecord.tableName) : tableRecord.tableName),
                    tableRecord.comment,
                    indexMetaBean);

                if (fullName) {
                    tableMetaBuilder.put(
                        SqlIdentifier.surroundWithBacktick(tableRecord.tableSchema) + "." + SqlIdentifier
                            .surroundWithBacktick(tableRecord.tableName), tableMeta);
                } else {
                    tableMetaBuilder.put(tableRecord.tableName, tableMeta);
                }
                tableNameSet.add(fullName ?
                    SqlIdentifier.surroundWithBacktick(tableRecord.tableSchema) + "." + SqlIdentifier
                        .surroundWithBacktick(tableRecord.tableName) : tableRecord.tableName);
            }

            // main tables not in __DRDS__SYSTABLE__TABLES__
            for (Entry<String, Map<String, GsiIndexMetaBean>> entry : tmpTableIndexMap.entrySet()) {
                final String tableName = entry.getKey();
                final Map<String, GsiIndexMetaBean> indexMap = entry.getValue();

                if (tableNameSet.contains(tableName)) {
                    continue;
                }

                final GsiTableMetaBean tableMeta = new GsiTableMetaBean("def",
                    "",
                    tableName,
                    TableType.SHARDING,
                    "",
                    "",
                    1,
                    "",
                    "",
                    1,
                    indexMap,
                    "",
                    null);

                tableMetaBuilder.put(tableName, tableMeta);
            }

            return tableMetaBuilder;
        }

        public static Builder<String, String> mergeIndexRecords(List<IndexRecord> allIndexRecords,
                                                                Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap) {
            return mergeIndexRecords(allIndexRecords, tmpTableIndexMap, false);
        }

        public static Builder<String, String> mergeIndexRecords(List<IndexRecord> allIndexRecords,
                                                                Map<String, Map<String, GsiIndexMetaBean>> tmpTableIndexMap,
                                                                boolean fullName) {
            final Builder<String, String> indexTableRelationBuilder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);

            Map<String, GsiIndexMetaBean> tmpIndexColumnMap = TreeMaps.caseInsensitiveMap();
            for (final IndexRecord indexRecord : allIndexRecords) {
                if (indexRecord.indexLocation != SqlIndexResiding.GLOBAL.getValue()) {
                    continue;
                }

                final Map<String, GsiIndexMetaBean> currentIndexColumnMap = tmpTableIndexMap.computeIfAbsent(
                    fullName ? SqlIdentifier.surroundWithBacktick(indexRecord.tableSchema) + "." +
                        SqlIdentifier.surroundWithBacktick(indexRecord.tableName) : indexRecord.tableName,
                    s -> TreeMaps.caseInsensitiveMap());

                final GsiIndexMetaBean gsiIndexMetaBean = currentIndexColumnMap.computeIfAbsent(
                    fullName ? SqlIdentifier.surroundWithBacktick(indexRecord.indexSchema) + "."
                        + SqlIdentifier.surroundWithBacktick(indexRecord.indexName) : indexRecord.indexName,
                    s -> new GsiIndexMetaBean(indexRecord.tableCatalog,
                        indexRecord.tableSchema,
                        indexRecord.tableName,
                        indexRecord.nonUnique,
                        indexRecord.indexSchema,
                        indexRecord.indexName,
                        new ArrayList<GsiIndexColumnMetaBean>(),
                        new ArrayList<GsiIndexColumnMetaBean>(),
                        indexRecord.indexType,
                        indexRecord.comment,
                        indexRecord.indexComment,
                        SqlIndexResiding.from((int) indexRecord.indexLocation),
                        indexRecord.indexTableName,
                        IndexStatus.from((int) indexRecord.indexStatus),
                        indexRecord.version,
                        (indexRecord.flag & IndexesRecord.FLAG_CLUSTERED) != 0L,
                        (indexRecord.flag & IndexesRecord.FLAG_COLUMNAR) != 0L,
                        IndexVisibility.convert(indexRecord.visible),
                        LackLocalIndexStatus.convert(indexRecord.lackLocalIndex))
                );

                tmpIndexColumnMap.computeIfAbsent(
                    fullName ? SqlIdentifier.surroundWithBacktick(indexRecord.indexSchema) + "."
                        + SqlIdentifier.surroundWithBacktick(indexRecord.indexName) : indexRecord.indexName, s -> {
                        if (fullName) {
                            indexTableRelationBuilder.put(
                                SqlIdentifier.surroundWithBacktick(indexRecord.indexSchema) + "." +
                                    SqlIdentifier.surroundWithBacktick(indexRecord.indexName),
                                SqlIdentifier.surroundWithBacktick(indexRecord.tableSchema) + "." +
                                    SqlIdentifier.surroundWithBacktick(indexRecord.tableName));
                        } else {
                            indexTableRelationBuilder.put(indexRecord.indexName, indexRecord.tableName);
                        }
                        return gsiIndexMetaBean;
                    });

                switch (IndexColumnType.from((int) indexRecord.indexColumnType)) {
                case INDEX:
                    gsiIndexMetaBean.indexColumns.add(new GsiIndexColumnMetaBean(indexRecord.seqInIndex,
                        indexRecord.columnName,
                        indexRecord.collation,
                        indexRecord.cardinality,
                        indexRecord.subPart,
                        indexRecord.packed,
                        indexRecord.nullable,
                        indexRecord.nonUnique));
                    break;
                case COVERING:
                    gsiIndexMetaBean.coveringColumns.add(new GsiIndexColumnMetaBean(indexRecord.seqInIndex,
                        indexRecord.columnName,
                        indexRecord.collation,
                        indexRecord.cardinality,
                        indexRecord.subPart,
                        indexRecord.packed,
                        indexRecord.nullable,
                        indexRecord.nonUnique));
                    break;
                default:
                    break;
                } // end of switch
            } // end of for
            return indexTableRelationBuilder;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return GsiMetaBean.class.isAssignableFrom(iface);
        }

        public Map<String, GsiTableMetaBean> getTableMeta() {
            return tableMeta;
        }

        public Map<String, String> getIndexTableRelation() {
            return indexTableRelation;
        }

        public void setTableMeta(
            Map<String, GsiTableMetaBean> tableMeta) {
            this.tableMeta = tableMeta;
        }

        public void setIndexTableRelation(Map<String, String> indexTableRelation) {
            this.indexTableRelation = indexTableRelation;
        }
    }

    public static class GsiTableMetaBean extends GsiMeta {

        public final String tableCatalog;
        public final String tableSchema;
        public final String tableName;
        public final TableType tableType;
        public final String dbPartitionKey;
        public final String dbPartitionPolicy;
        public final Integer dbPartitionCount;
        public final String tbPartitionKey;
        public final String tbPartitionPolicy;
        public final Integer tbPartitionCount;
        public final Map<String, GsiIndexMetaBean> indexMap;
        public final String comment;
        public GsiIndexMetaBean gsiMetaBean; // For GSI table.

        public GsiTableMetaBean(String tableCatalog, String tableSchema, String tableName, TableType tableType,
                                String dbPartitionKey, String dbPartitionPolicy, Integer dbPartitionCount,
                                String tbPartitionKey, String tbPartitionPolicy, Integer tbPartitionCount,
                                Map<String, GsiIndexMetaBean> indexMap, String comment,
                                GsiIndexMetaBean gsiMetaBean) {
            this.tableCatalog = tableCatalog;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.tableType = tableType;
            this.dbPartitionKey = dbPartitionKey;
            this.dbPartitionPolicy = dbPartitionPolicy;
            this.dbPartitionCount = dbPartitionCount;
            this.tbPartitionKey = tbPartitionKey;
            this.tbPartitionPolicy = tbPartitionPolicy;
            this.tbPartitionCount = tbPartitionCount;
            this.indexMap = indexMap;
            this.comment = comment;
            this.gsiMetaBean = gsiMetaBean;
        }

        public GsiTableMetaBean copyWithOutIndexMap() {
            return new GsiTableMetaBean(
                this.tableCatalog,
                this.tableSchema,
                this.tableName,
                this.tableType,
                this.dbPartitionKey,
                this.dbPartitionPolicy,
                this.dbPartitionCount,
                this.tbPartitionKey,
                this.tbPartitionPolicy,
                this.tbPartitionCount,
                TreeMaps.caseInsensitiveMap(),
                this.comment,
                this.gsiMetaBean == null ? null : this.gsiMetaBean.clone());
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return GsiTableMetaBean.class.isAssignableFrom(iface);
        }

        @Override
        public int hashCode() {
            int hashcode = Objects
                .hash(tableCatalog, tableSchema, tableName, dbPartitionKey, dbPartitionPolicy, dbPartitionCount,
                    tbPartitionKey, tbPartitionPolicy, tbPartitionCount, comment);
            hashcode += tableType.getValue();
            for (GsiIndexMetaBean gsiIndexMetaBean : indexMap.values()) {
                /** NOTICE: make map element hashcode order independence */
                hashcode += gsiIndexMetaBean.hashCode();
            }
            return hashcode;
        }
    }

    public static class GsiIndexColumnMetaBean extends GsiMeta {

        public final long seqInIndex;
        public final String columnName;
        public final String collation;
        public final long cardinality;
        public final Long subPart;
        public final String packed;
        public final String nullable;
        public final boolean nonUnique;

        public GsiIndexColumnMetaBean(long seqInIndex, String columnName, String collation, long cardinality,
                                      Long subPart, String packed, String nullable, boolean nonUnique) {
            this.seqInIndex = seqInIndex;
            this.columnName = columnName;
            this.collation = collation;
            this.cardinality = cardinality;
            this.subPart = subPart;
            this.packed = packed;
            this.nullable = nullable;
            this.nonUnique = nonUnique;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return GsiIndexColumnMetaBean.class.isAssignableFrom(iface);
        }

        @Override
        public int hashCode() {

            return Objects.hash(seqInIndex, columnName, collation, cardinality, subPart, packed, nullable, nonUnique);
        }
    }

    public static class GsiIndexMetaBean extends GsiMeta {

        public final String tableCatalog;
        public final String tableSchema;
        public final String tableName;
        public final boolean nonUnique;
        public final String indexSchema;
        public final String indexName;

        public final List<GsiIndexColumnMetaBean> indexColumns;
        public final List<GsiIndexColumnMetaBean> coveringColumns;
        public final String indexType;
        public final String comment;
        public final String indexComment;
        public final SqlIndexResiding indexLocation;
        public final String indexTableName;
        public final IndexStatus indexStatus;
        public final long version;
        public final boolean clusteredIndex;
        public final boolean columnarIndex;
        public final IndexVisibility visibility;
        public final LackLocalIndexStatus lackLocalIndexStatus;
        public AtomicReference<Map<String, String>> columnarOptions = new AtomicReference<>(new HashMap<>());

        public GsiIndexMetaBean(String tableCatalog, String tableSchema, String tableName, boolean nonUnique,
                                String indexSchema, String indexName, List<GsiIndexColumnMetaBean> indexColumns,
                                List<GsiIndexColumnMetaBean> coveringColumns, String indexType, String comment,
                                String indexComment, SqlIndexResiding indexLocation, String indexTableName,
                                IndexStatus indexStatus, long version, boolean clusteredIndex, boolean columnarIndex,
                                IndexVisibility visibility, LackLocalIndexStatus lackLocalIndexStatus) {
            this.tableCatalog = tableCatalog;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.nonUnique = nonUnique;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.indexColumns = indexColumns;
            this.coveringColumns = coveringColumns;
            this.indexType = indexType;
            this.comment = comment;
            this.indexComment = indexComment;
            this.indexLocation = indexLocation;
            this.indexTableName = indexTableName;
            this.indexStatus = indexStatus;
            this.version = version;
            this.clusteredIndex = clusteredIndex;
            this.columnarIndex = columnarIndex;
            this.visibility = visibility;
            this.lackLocalIndexStatus = lackLocalIndexStatus;
        }

        public List<GsiIndexColumnMetaBean> getIndexColumns() {
            return indexColumns;
        }

        public List<GsiIndexColumnMetaBean> getCoveringColumns() {
            return coveringColumns;
        }

        @Override
        public GsiIndexMetaBean clone() {
            List<GsiIndexColumnMetaBean> newIndexColumns = new ArrayList<>();
            newIndexColumns.addAll(indexColumns);
            List<GsiIndexColumnMetaBean> newCoveringColumns = new ArrayList<>();
            newCoveringColumns.addAll(coveringColumns);
            return new GsiIndexMetaBean(tableCatalog, tableSchema, tableName, nonUnique,
                indexSchema, indexName, newIndexColumns,
                newCoveringColumns, indexType, comment,
                indexComment, indexLocation, indexTableName,
                indexStatus, version, clusteredIndex, columnarIndex, visibility, lackLocalIndexStatus);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return GsiTableMetaBean.class.isAssignableFrom(iface);
        }

        @Override
        public int hashCode() {
            int hashcode = Objects
                .hash(tableCatalog, tableSchema, tableName, nonUnique, indexSchema, indexName, indexColumns,
                    coveringColumns, indexType, comment, indexComment, indexTableName, version);
            hashcode += indexLocation.getValue();
            hashcode += indexStatus.getValue();
            if (clusteredIndex) {
                hashcode += 0xA55A; // Magic number.
            }
            if (columnarIndex) {
                hashcode += 0xB66B; // Magic number.
            }
            return hashcode;
        }

        public void updateColumnarOptions(String cciName, Map<String, Map<String, String>> records,
                                          Map<String, String> globalConfig) {
            Map<String, String> configMap;
            Map<String, String> options = new HashMap<>();
            if (null != (configMap = records.get(cciName))) {
                for (Entry<String, String> option : configMap.entrySet()) {
                    String optionName = option.getKey();
                    String optionValue = option.getValue();
                    if (ColumnarOptions.TYPE.equalsIgnoreCase(optionName)
                        && ColumnarConfig.SNAPSHOT.equalsIgnoreCase(optionValue)) {
                        // This cci is a snapshot cci, add columnar snapshot options.
                        options.put(ColumnarOptions.TYPE, configMap.get(ColumnarOptions.TYPE));
                        String attribute = ColumnarOptions.SNAPSHOT_RETENTION_DAYS;
                        options.put(attribute, ColumnarConfig.getValue(attribute, configMap, globalConfig));
                        attribute = ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL;
                        options.put(attribute, ColumnarConfig.getValue(attribute, configMap, globalConfig));
                    } else if ((ColumnarOptions.TYPE.equalsIgnoreCase(optionName)
                        && ColumnarConfig.DEFAULT.equalsIgnoreCase(optionValue))
                        || ColumnarOptions.SNAPSHOT_RETENTION_DAYS.equalsIgnoreCase(optionName)
                        || ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL.equalsIgnoreCase(optionName)) {
                        // ignore
                    } else if (null != ColumnarConfig.get(optionName)) {
                        options.put(optionName, optionValue);
                    }
                }
            }
            columnarOptions.set(options);
        }
    }

    private static abstract class GsiMeta implements Wrapper {

        @Override
        @SuppressWarnings("unchecked")
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (isWrapperFor(iface)) {
                return (T) this;
            } else {
                throw new SQLException("not a wrapper for " + iface);
            }
        }
    }

    @Getter
    public static class TableRecord extends GsiMeta implements Orm<TableRecord> {

        public static TableRecord ORM = new TableRecord();

        private final long id;
        private final String tableCatalog;
        private final String tableSchema;
        private final String tableName;
        private final long tableType;
        private final String dbPartitionKey;
        private final String dbPartitionPolicy;
        private final Integer dbPartitionCount;
        private final String tbPartitionKey;
        private final String tbPartitionPolicy;
        private final Integer tbPartitionCount;
        private final String comment;

        public TableRecord(long id, String tableCatalog, String tableSchema, String tableName, long tableType,
                           String dbPartitionKey, String dbPartitionPolicy, Integer dbPartitionCount,
                           String tbPartitionKey, String tbPartitionPolicy, Integer tbPartitionCount, String comment) {
            this.id = id;
            this.tableCatalog = tableCatalog;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.tableType = tableType;
            this.dbPartitionKey = dbPartitionKey;
            this.dbPartitionPolicy = dbPartitionPolicy;
            this.dbPartitionCount = dbPartitionCount;
            this.tbPartitionKey = tbPartitionKey;
            this.tbPartitionPolicy = tbPartitionPolicy;
            this.tbPartitionCount = tbPartitionCount;
            this.comment = comment;
        }

        private TableRecord() {
            this.id = -1;
            this.tableCatalog = null;
            this.tableSchema = null;
            this.tableName = null;
            this.tableType = -1;
            this.dbPartitionKey = null;
            this.dbPartitionPolicy = null;
            this.dbPartitionCount = -1;
            this.tbPartitionKey = null;
            this.tbPartitionPolicy = null;
            this.tbPartitionCount = -1;
            this.comment = null;
        }

        @Override
        public TableRecord convert(ResultSet resultSet) throws SQLException {
            return new TableRecord(-1,
                resultSet.getString("table_catalog"),
                resultSet.getString("table_schema"),
                resultSet.getString("table_name"),
                resultSet.getLong("table_type"),
                resultSet.getString("db_partition_key"),
                resultSet.getString("db_partition_policy"),
                resultSet.getInt("db_partition_count") <= 0 ? null : resultSet.getInt("db_partition_count"),
                resultSet.getString("tb_partition_key"),
                resultSet.getString("tb_partition_policy"),
                resultSet.getInt("tb_partition_count") <= 1 ? null : resultSet.getInt("tb_partition_count"),
                ""
            );
        }

        @Override
        public Map<Integer, Object> params() {
            final Map<Integer, Object> params = new LinkedHashMap<>();
            params.put(1, this.tableCatalog);
            params.put(2, this.tableSchema);
            params.put(3, this.tableName);
            params.put(4, this.tableType);
            params.put(5, this.dbPartitionKey);
            params.put(6, this.dbPartitionPolicy);
            params.put(7, this.dbPartitionCount == null ? 1 : this.dbPartitionCount);
            params.put(8, this.tbPartitionKey);
            params.put(9, this.tbPartitionPolicy);
            params.put(10, this.tbPartitionCount == null ? 1 : this.tbPartitionCount);
            return params;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return TableRecord.class.isAssignableFrom(iface);
        }
    }

    public static class IndexRecord extends GsiMeta implements Orm<IndexRecord> {

        public static IndexRecord ORM = new IndexRecord();

        private final long id;
        private final String tableCatalog;
        private final String tableSchema;
        private final String tableName;
        private final boolean nonUnique;
        private final String indexSchema;
        private final String indexName;
        private final long seqInIndex;
        private final String columnName;
        private final String collation;
        private final long cardinality;
        private final Long subPart;
        private final String packed;
        private final String nullable;
        private final String indexType;
        private final String comment;
        private final String indexComment;
        private final long indexColumnType;
        private final long indexLocation;
        private final String indexTableName;
        private final long indexStatus;
        private final long version;
        public final long flag;
        /**
         * visible字段的处理:
         * 1. 读取时使用orm读出
         * 2. 写入时不带visible字段，直接使用default值
         */
        public final long visible;
        public final long lackLocalIndex;

        public IndexRecord(long id, String tableCatalog, String tableSchema, String tableName, boolean nonUnique,
                           String indexSchema, String indexName, long seqInIndex, String columnName, String collation,
                           long cardinality, Long subPart, String packed, String nullable, String indexType,
                           String comment, String indexComment, long indexColumnType, long indexLocation,
                           String indexTableName, long indexStatus, long version, long flag, long visibility,
                           long lackLocalIndex) {
            this.id = id;
            this.tableCatalog = tableCatalog;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
            this.nonUnique = nonUnique;
            this.indexSchema = indexSchema;
            this.indexName = indexName;
            this.seqInIndex = seqInIndex;
            this.columnName = columnName;
            this.collation = collation;
            this.cardinality = cardinality;
            this.subPart = subPart;
            this.packed = packed;
            this.nullable = nullable;
            this.indexType = indexType;
            this.comment = comment;
            this.indexComment = indexComment;
            this.indexColumnType = indexColumnType;
            this.indexLocation = indexLocation;
            this.indexTableName = indexTableName;
            this.indexStatus = indexStatus;
            this.version = version;
            this.flag = flag;
            this.visible = visibility;
            this.lackLocalIndex = lackLocalIndex;
        }

        private IndexRecord() {
            this.id = -1;
            this.tableCatalog = null;
            this.tableSchema = null;
            this.tableName = null;
            this.nonUnique = true;
            this.indexSchema = null;
            this.indexName = null;
            this.seqInIndex = -1;
            this.columnName = null;
            this.collation = null;
            this.cardinality = 0;
            this.subPart = null;
            this.packed = null;
            this.nullable = null;
            this.indexType = null;
            this.comment = null;
            this.indexComment = null;
            this.indexColumnType = -1;
            this.indexLocation = -1;
            this.indexTableName = null;
            this.indexStatus = -1;
            this.version = -1;
            this.flag = 0;
            this.visible = IndexVisibility.VISIBLE.getValue();
            this.lackLocalIndex = LackLocalIndexStatus.NO_LACKIING.getValue();
        }

        @Override
        public IndexRecord convert(ResultSet resultSet) throws SQLException {
            if (ConfigDataMode.isPolarDbX()) {
                return new IndexRecord(resultSet.getLong("id"),
                    resultSet.getString("table_catalog"),
                    resultSet.getString("table_schema"),
                    resultSet.getString("table_name"),
                    resultSet.getInt("non_unique") != 0,
                    resultSet.getString("index_schema"),
                    resultSet.getString("index_name"),
                    resultSet.getLong("seq_in_index"),
                    resultSet.getString("column_name"),
                    resultSet.getString("collation"),
                    resultSet.getLong("cardinality"),
                    resultSet.getLong("sub_part") == 0 ? null : resultSet.getLong("sub_part"),
                    resultSet.getString("packed"),
                    resultSet.getString("nullable"),
                    resultSet.getString("index_type"),
                    resultSet.getString("comment"),
                    resultSet.getString("index_comment"),
                    resultSet.getLong("index_column_type"),
                    resultSet.getLong("index_location"),
                    resultSet.getString("index_table_name"),
                    resultSet.getLong("index_status"),
                    resultSet.getLong("version"),
                    resultSet.getLong("flag"),
                    resultSet.getLong("visible"),
                    resultSet.getLong("lacking_local_index"));
            } else {
                return new IndexRecord(resultSet.getLong("id"),
                    resultSet.getString("table_catalog"),
                    resultSet.getString("table_schema"),
                    resultSet.getString("table_name"),
                    resultSet.getInt("non_unique") != 0,
                    resultSet.getString("index_schema"),
                    resultSet.getString("index_name"),
                    resultSet.getLong("seq_in_index"),
                    resultSet.getString("column_name"),
                    resultSet.getString("collation"),
                    resultSet.getLong("cardinality"),
                    resultSet.getLong("sub_part") == 0 ? null : resultSet.getLong("sub_part"),
                    resultSet.getString("packed"),
                    resultSet.getString("nullable"),
                    resultSet.getString("index_type"),
                    resultSet.getString("comment"),
                    resultSet.getString("index_comment"),
                    resultSet.getLong("index_column_type"),
                    resultSet.getLong("index_location"),
                    resultSet.getString("index_table_name"),
                    resultSet.getLong("index_status"),
                    resultSet.getLong("version"),
                    0,
                    resultSet.getLong("visible"),
                    resultSet.getLong("lacking_local_index"));
            }
        }

        @Override
        public Map<Integer, Object> params() {
            final Map<Integer, Object> params = new LinkedHashMap<>();
            params.put(1, tableCatalog);
            params.put(2, tableSchema);
            params.put(3, tableName);
            params.put(4, nonUnique ? 1 : 0);
            params.put(5, indexSchema);
            params.put(6, indexName);
            params.put(7, seqInIndex);
            params.put(8, columnName);
            params.put(9, collation);
            params.put(10, cardinality);
            params.put(11, subPart);
            params.put(12, packed);
            params.put(13, nullable);
            params.put(14, indexType);
            params.put(15, comment);
            params.put(16, indexComment);
            params.put(17, indexColumnType);
            params.put(18, indexLocation);
            params.put(19, indexTableName);
            params.put(20, indexStatus);
            params.put(21, version);
            params.put(22, flag);
            return params;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return IndexRecord.class.isAssignableFrom(iface);
        }

        public long getId() {
            return id;
        }

        public String getTableCatalog() {
            return tableCatalog;
        }

        public String getTableSchema() {
            return tableSchema;
        }

        public String getTableName() {
            return tableName;
        }

        public boolean isNonUnique() {
            return nonUnique;
        }

        public String getIndexSchema() {
            return indexSchema;
        }

        public String getIndexName() {
            return indexName;
        }

        public long getSeqInIndex() {
            return seqInIndex;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getCollation() {
            return collation;
        }

        public long getCardinality() {
            return cardinality;
        }

        public Long getSubPart() {
            return subPart;
        }

        public String getPacked() {
            return packed;
        }

        public String getNullable() {
            return nullable;
        }

        public String getIndexType() {
            return indexType;
        }

        public String getComment() {
            return comment;
        }

        public String getIndexComment() {
            return indexComment;
        }

        public long getIndexColumnType() {
            return indexColumnType;
        }

        public long getIndexLocation() {
            return indexLocation;
        }

        public String getIndexTableName() {
            return indexTableName;
        }

        public long getIndexStatus() {
            return indexStatus;
        }

        public long getVersion() {
            return version;
        }

        public long getFlag() {
            return flag;
        }
    }
}

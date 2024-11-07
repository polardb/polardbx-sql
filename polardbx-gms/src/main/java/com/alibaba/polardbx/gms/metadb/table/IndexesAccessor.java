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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexesAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexesAccessor.class);

    private static final String INDEXES_INFO_SCHEMA = "information_schema.statistics";

    private static final String FROM_INDEXES_INFO_SCHEMA = " from " + INDEXES_INFO_SCHEMA;

    private static final String INDEXES_TABLE = wrap(GmsSystemTables.INDEXES);

    private static final String FROM_INDEXES_TABLE = " from " + INDEXES_TABLE;

    private static final String INSERT_INDEXES =
        "insert into " + INDEXES_TABLE
            + "(`table_schema`, `table_name`, `non_unique`, `index_schema`, `index_name`, `seq_in_index`, "
            + "`column_name`, `collation`, `cardinality`, `sub_part`, `packed`, `nullable`, `index_type`, "
            + "`comment`, `index_comment`, `index_column_type`, `index_location`, `index_table_name`,"
            + "`index_status`, `version`, `flag`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String WHERE_SCHEMA = " where `table_schema` = ?";

    private static final String WHERE_SCHEMA_TABLE = WHERE_SCHEMA + " and `table_name` = ?";

    private static final String ORDER_BY_SEQ = " order by `seq_in_index`";

    private static final String WHERE_SCHEMA_TABLE_COLUMN = WHERE_SCHEMA_TABLE + " and `column_name` = ?";

    private static final String WHERE_SCHEMA_TABLE_COLUMNS = WHERE_SCHEMA_TABLE + " and `column_name` in (%s)";

    private static final String WHERE_SCHEMA_TABLE_ONE_INDEX = WHERE_SCHEMA_TABLE + " and `index_name` = ?";

    private static final String WHERE_SCHEMA_TABLE_PRIMARY_KEY = WHERE_SCHEMA_TABLE + " and `index_name` = 'PRIMARY'";

    private static final String WHERE_SCHEMA_TABLE_INDEXES = WHERE_SCHEMA_TABLE + " and `index_name` in (%s)";

    private static final String WHERE_SCHEMA_TABLE_INDEXES_ID = WHERE_SCHEMA_TABLE + " and `id` in (%s)";

    private static final String WHERE_SCHEMA_TABLE_INDEXES_COLUMNS_STATUS_GSI =
        WHERE_SCHEMA_TABLE_INDEXES + " and `column_name` in (%s) and `index_status` = ? and `index_location` = 1";

    private static final String WHERE_COLUMNAR_INDEX =
        String.format("where (`flag` & %d) <> 0", IndexesRecord.FLAG_COLUMNAR);

    private static final String WHERE_SCHEMA_COLUMNAR_INDEX =
        String.format("where `table_schema` = ? and `table_name` = ? and (`flag` & %d) <> 0",
            IndexesRecord.FLAG_COLUMNAR);

    private static final String WHERE_COLUMNAR_INDEX_SCHEMA =
        String.format("where `table_schema` = ? and (`flag` & %d) <> 0", IndexesRecord.FLAG_COLUMNAR);

    private static final String WHERE_COLUMNAR_INDEX_SCHEMA_INDEX_NAME =
        String.format(
            "where `table_schema` = ?  and `index_name` = ? and (`flag` & %d) <> 0 order by `seq_in_index`",
            IndexesRecord.FLAG_COLUMNAR);

    private static final String WHERE_COLUMNAR_INDEX_SCHEMA_TABLE_INDEX_NAME =
        String.format(
            "where `table_schema` = ? and `table_name` = ? and `index_name` = ? and (`flag` & %d) <> 0 order by `seq_in_index`",
            IndexesRecord.FLAG_COLUMNAR);

    private static final String GROUP_BY_INDEX_NAME = " group by `index_name`";

    private static final String SELECT_CLAUSE =
        "select `table_schema`, `table_name`, `non_unique`, `index_schema`, `index_name`, `seq_in_index`, "
            + "`column_name`, `collation`, `cardinality`, `sub_part`, `packed`, `nullable`, `index_type`, "
            + "`comment`, `index_comment`";

    private static final String SELECT_CLAUSE_EXT =
        ", `index_column_type`, `index_location`, `index_table_name`, `index_status`, `version`, `flag`, `visible`, `visit_frequency`, `last_access_time`";

    private static final String SELECT_INFO_SCHEMA =
        SELECT_CLAUSE + FROM_INDEXES_INFO_SCHEMA + WHERE_SCHEMA_TABLE;

    private static final String SELECT_INFO_SCHEMA_SPECIFIED =
        SELECT_CLAUSE + FROM_INDEXES_INFO_SCHEMA + WHERE_SCHEMA_TABLE_INDEXES;

    private static final String SELECT_INFO_SCHEMA_BY_FIRST_COLUMN =
        SELECT_CLAUSE + FROM_INDEXES_INFO_SCHEMA + WHERE_SCHEMA_TABLE_COLUMN;

    private static final String SELECT_INFO_SCHEMA_PRIMARY_KEY =
        SELECT_CLAUSE + FROM_INDEXES_INFO_SCHEMA + WHERE_SCHEMA_TABLE_PRIMARY_KEY;

    private static final String SELECT_INDEXES_BY_FIRST_COLUMN =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_SCHEMA_TABLE_COLUMN;

    private static final String SELECT_INDEXES =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_SCHEMA_TABLE + ORDER_BY_SEQ;

    private static final String SELECT_ONE_INDEX =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_SCHEMA_TABLE_ONE_INDEX + ORDER_BY_SEQ;

    private static final String SELECT_ALL_INDEXES =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_SCHEMA + ORDER_BY_SEQ;

    private static final String SELECT_ALL_COLUMNAR_INDEXES =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_COLUMNAR_INDEX;

    private static final String SELECT_COLUMNAR_INDEXES_BY_TABLE =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_SCHEMA_COLUMNAR_INDEX;

    private static final String SELECT_ALL_COLUMNAR_INDEXES_BY_SCHEMA =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_COLUMNAR_INDEX_SCHEMA + GROUP_BY_INDEX_NAME;

    private static final String SELECT_PRIMARY_KEY_BY_SCHEMA_TABLE =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_SCHEMA_TABLE_PRIMARY_KEY + ORDER_BY_SEQ;

    private static final String SELECT_COLUMNAR_COLUMNS_BY_SCHEMA_TABLE_INDEX_NAME =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_COLUMNAR_INDEX_SCHEMA_TABLE_INDEX_NAME;

    private static final String SELECT_COLUMNAR_COLUMNS_BY_SCHEMA_INDEX_NAME =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_COLUMNAR_INDEX_SCHEMA_INDEX_NAME;

    private static final String SELECT_ALL_PUBLIC_GSI =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE
            + " where `index_status` = 4 and `index_location` = 1 and `seq_in_index` = 1";

    private static final String SELECT_FK_REF_INDEX =
        SELECT_CLAUSE + " from (" + SELECT_CLAUSE + ", GROUP_CONCAT(`column_name`" +
            ORDER_BY_SEQ + ") as `column_names`" +
            FROM_INDEXES_TABLE + WHERE_SCHEMA_TABLE +
            " group by `index_name`) as `t` where `t`.`column_names` like ";

    private static final String UPDATE_INDEXES = "update " + INDEXES_TABLE + " set ";

    protected static final String UPDATE_VISIT_FREQUENCY =
        UPDATE_INDEXES + "`visit_frequency`=`visit_frequency` + ? where `table_schema`=? and `index_name`=?";

    protected static final String UPDATE_LAST_ACCESS_TIME = UPDATE_INDEXES
        + "`last_access_time`=? where `table_schema`=? and `index_name`=? and (`last_access_time` is null or `last_access_time` < ?)";

    protected static final String RESET_STATISTICS = UPDATE_INDEXES + "`visit_frequency`= 0, `last_access_time`= null";

    private static final String UPDATE_INDEXES_NAME =
        UPDATE_INDEXES + "`index_name` = ?" + WHERE_SCHEMA_TABLE_ONE_INDEX;

    private static final String UPDATE_INDEXES_VERSION = UPDATE_INDEXES + "`version` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_INDEXES_VERSION_BY_INDEX_NAME =
        UPDATE_INDEXES + "`version` = ?" + WHERE_SCHEMA + " and `index_name` = ?";

    private static final String UPDATE_INDEXES_FLAG_BY_INDEX_NAME =
        UPDATE_INDEXES + "`flag` = ?" + WHERE_SCHEMA + " and `index_name` = ? and seq_in_index = 1";

    private static final String UPDATE_INDEXES_STATUS = UPDATE_INDEXES + "`index_status` = ?";

    private static final String UPDATE_INDEXES_STATUS_ALL = UPDATE_INDEXES_STATUS + WHERE_SCHEMA_TABLE;
    private static final String UPDATE_LOCAL_INDEXES_STATUS_ALL =
        UPDATE_INDEXES_STATUS + WHERE_SCHEMA_TABLE + " and `index_location` = 0";

    private static final String UPDATE_INDEXES_STATUS_SPECIFIED = UPDATE_INDEXES_STATUS + WHERE_SCHEMA_TABLE_INDEXES;

    private static final String UPDATE_INDEXES_STATUS_SPECIFIED_GSI_COLUMNS =
        UPDATE_INDEXES_STATUS + WHERE_SCHEMA_TABLE_INDEXES_COLUMNS_STATUS_GSI;

    private static final String UPDATE_INDEXES_STATUS_SPECIFIED_COLUMNS =
        UPDATE_INDEXES_STATUS + WHERE_SCHEMA_TABLE + "and `column_name` in (%s)";

    private static final String UPDATE_INDEXES_STATUS_BY_INDEX_NAME =
        UPDATE_INDEXES_STATUS + WHERE_SCHEMA + " and `index_name` = ?";

    private static final String UPDATE_INDEXES_RENAME = UPDATE_INDEXES + "`table_name` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_INDEXES_TABLE_NAME =
        UPDATE_INDEXES + "`index_name` = ?, `index_table_name` = `index_name`"
            + WHERE_SCHEMA + " and `index_name` = ?";

    private static final String UPDATE_CCI_TABLE_NAME =
        UPDATE_INDEXES + "`index_table_name` = ?"
            + WHERE_SCHEMA + " and `index_name` = ?";

    private static final String UPDATE_LOCAL_INDEXES_RENAME =
        UPDATE_INDEXES + "`table_name` = ?" + WHERE_SCHEMA_TABLE + " and `index_location` = 0";

    private static final String UPDATE_GLOBAL_INDEXES_CUT_OVER =
        UPDATE_INDEXES + "`index_name` = ?, `index_table_name` = `index_name`"
            + WHERE_SCHEMA + "and index_name = ? and `index_location` = 1";

    private static final String UPDATE_INDEXES_COLUMN_NAME =
        UPDATE_INDEXES + "`column_name` = ?" + WHERE_SCHEMA_TABLE_COLUMN;

    private static final String UPDATE_INDEXES_COLUMN_NAMES =
        UPDATE_INDEXES + "`column_name` = ?" + WHERE_SCHEMA_TABLE_COLUMN;
    private static final String DELETE_INDEXES = "delete" + FROM_INDEXES_TABLE;

    private static final String DELETE_INDEXES_ALL = DELETE_INDEXES + WHERE_SCHEMA;

    private static final String DELETE_INDEXES_ALL_TABLES = DELETE_INDEXES + WHERE_SCHEMA_TABLE;

    private static final String DELETE_INDEXES_SPECIFIED = DELETE_INDEXES + WHERE_SCHEMA_TABLE_INDEXES;

    private static final String DELETE_INDEXES_COLUMNS = DELETE_INDEXES + WHERE_SCHEMA_TABLE_COLUMNS;

    private static final String DELETE_PRIMARY_KEY = DELETE_INDEXES + WHERE_SCHEMA_TABLE_PRIMARY_KEY;

    public List<IndexesRecord> queryTableIndexes(String tableSchema, String tableName) {
        return query(tableSchema, tableName);
    }

    public int[] insert(List<IndexesRecord> records, String tableSchema, String tableName) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (IndexesRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_INDEXES, paramsBatch);
            return MetaDbUtil.insert(INSERT_INDEXES, paramsBatch, connection);
        } catch (SQLException e) {
            String extraMsg = "";
            if (checkIfDuplicate(e)) {
                List<IndexesRecord> existingRecords = query(tableSchema, tableName);
                if (compare(records, existingRecords)) {
                    // Ignore new and use existing records.
                    int[] affectRowsArray = new int[records.size()];
                    for (int i = 0; i < records.size(); i++) {
                        affectRowsArray[i] = 1;
                    }
                    return affectRowsArray;
                } else {
                    extraMsg = ". New and existing records don't match";
                }
            }
            LOGGER.error("Failed to insert a batch of new records into " + INDEXES_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                INDEXES_TABLE,
                e.getMessage());
        }
    }

    private boolean compare(List<IndexesRecord> newRecords, List<IndexesRecord> existingRecords) {
        if (newRecords.size() != existingRecords.size()) {
            return false;
        }

        Map<String, IndexesRecord> existingRecordsMap = new HashMap<>(existingRecords.size());
        for (IndexesRecord existingRecord : existingRecords) {
            existingRecordsMap.put(existingRecord.columnName, existingRecord);
        }

        for (IndexesRecord newRecord : newRecords) {
            if (!existingRecordsMap.containsKey(newRecord.columnName) ||
                !compare(newRecord, existingRecordsMap.get(newRecord.columnName))) {
                return false;
            }
        }

        return true;
    }

    private boolean compare(IndexesRecord newRecord, IndexesRecord existingRecord) {
        return TStringUtil.equalsIgnoreCase(newRecord.tableSchema, existingRecord.tableSchema) &&
            TStringUtil.equalsIgnoreCase(newRecord.tableName, existingRecord.tableName) &&
            TStringUtil.equalsIgnoreCase(newRecord.indexSchema, existingRecord.indexSchema) &&
            TStringUtil.equalsIgnoreCase(newRecord.indexName, existingRecord.indexName) &&
            newRecord.nonUnique == existingRecord.nonUnique &&
            TStringUtil.equalsIgnoreCase(newRecord.columnName, existingRecord.columnName) &&
            TStringUtil.equalsIgnoreCase(newRecord.collation, existingRecord.collation) &&
            TStringUtil.equalsIgnoreCase(newRecord.nullable, existingRecord.nullable) &&
            TStringUtil.equalsIgnoreCase(newRecord.indexType, existingRecord.indexType) &&
            newRecord.indexColumnType == existingRecord.indexColumnType &&
            newRecord.indexLocation == existingRecord.indexLocation &&
            TStringUtil.equalsIgnoreCase(newRecord.indexTableName, existingRecord.indexTableName);
    }

    public List<IndexesInfoSchemaRecord> queryInfoSchema(String phyTableSchema, String phyTableName,
                                                         DataSource dataSource) {
        return query(SELECT_INFO_SCHEMA, INDEXES_INFO_SCHEMA, IndexesInfoSchemaRecord.class, phyTableSchema,
            phyTableName, dataSource);
    }

    public List<IndexesInfoSchemaRecord> queryInfoSchema(String phyTableSchema, String phyTableName,
                                                         List<String> indexNames, DataSource dataSource) {
        Map<Integer, ParameterContext> params = buildParams(phyTableSchema, phyTableName, indexNames);
        return query(String.format(SELECT_INFO_SCHEMA_SPECIFIED, concatParams(indexNames)), INDEXES_INFO_SCHEMA,
            IndexesInfoSchemaRecord.class, params, dataSource);
    }

    public List<IndexesInfoSchemaRecord> queryInfoSchemaByFirstColumn(String phyTableSchema, String phyTableName,
                                                                      String firstColumnName, DataSource dataSource) {
        return query(SELECT_INFO_SCHEMA_BY_FIRST_COLUMN, INDEXES_INFO_SCHEMA,
            IndexesInfoSchemaRecord.class, phyTableSchema, phyTableName, firstColumnName, dataSource);
    }

    public List<IndexesInfoSchemaRecord> queryInfoSchemaForPrimaryKey(String phyTableSchema, String phyTableName,
                                                                      DataSource dataSource) {
        return query(SELECT_INFO_SCHEMA_PRIMARY_KEY, INDEXES_INFO_SCHEMA, IndexesInfoSchemaRecord.class, phyTableSchema,
            phyTableName, dataSource);
    }

    public List<IndexesInfoSchemaRecord> queryForeignKeyRefIndexes(String tableSchema, String tableName,
                                                                   List<String> columnNames) {
        String c = generateFkReferenceTableColumNames(columnNames);
        String sql = SELECT_FK_REF_INDEX + "'" + c + "%'";
        return query(sql, INDEXES_TABLE, IndexesInfoSchemaRecord.class, tableSchema, tableName);
    }

    public List<IndexesRecord> queryAllPublicGsi() {
        return query(SELECT_ALL_PUBLIC_GSI, INDEXES_TABLE, IndexesRecord.class, (String) null);
    }

    public List<IndexesRecord> queryGsiByCondition(Set<String> schemaNames, Set<String> tableNames, String tableLike,
                                                   Set<String> indexNames, String indexLike) {
        String sql = generateGsiStatisticsByConditionSql(schemaNames, tableNames, tableLike, indexNames, indexLike);
        if (sql == null) {
            return new ArrayList<>();
        }
        return query(sql, INDEXES_TABLE, IndexesRecord.class, (String) null);
    }

    public int[] updateVisitFrequency(List<IndexesRecord> records) {
        if (records.isEmpty()) {
            return new int[0];
        }
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        try {
            for (int i = 0; i < records.size(); i++) {
                String schemaName = records.get(i).indexSchema;
                String gsiName = records.get(i).indexName;
                Long freq = records.get(i).visitFrequency;
                Map<Integer, ParameterContext> params = new HashMap<>();
                int index = 0;
                MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, freq);
                MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, schemaName);
                MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, gsiName);
                paramsBatch.add(params);
            }
            return MetaDbUtil.update(UPDATE_VISIT_FREQUENCY, paramsBatch, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update records, " + UPDATE_VISIT_FREQUENCY, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update table ",
                INDEXES_TABLE,
                e.getMessage());
        }
    }

    public int[] updateLastAccessTime(List<IndexesRecord> records) {
        if (records.isEmpty()) {
            return new int[0];
        }
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        try {
            for (int i = 0; i < records.size(); i++) {
                String schemaName = records.get(i).indexSchema;
                String gsiName = records.get(i).indexName;
                Date lastAccessTime = records.get(i).lastAccessTime;
                Map<Integer, ParameterContext> params = new HashMap<>();
                int index = 0;
                MetaDbUtil.setParameter(++index, params, ParameterMethod.setTimestamp1, lastAccessTime);
                MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, schemaName);
                MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, gsiName);
                MetaDbUtil.setParameter(++index, params, ParameterMethod.setTimestamp1, lastAccessTime);
                paramsBatch.add(params);
            }
            return MetaDbUtil.update(UPDATE_LAST_ACCESS_TIME, paramsBatch, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to update records, " + UPDATE_LAST_ACCESS_TIME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update table ",
                INDEXES_TABLE,
                e.getMessage());
        }
    }

    public void resetAllStatistics() {
        try {
            MetaDbUtil.execute(RESET_STATISTICS, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to reset statistics, " + UPDATE_LAST_ACCESS_TIME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update table ",
                INDEXES_TABLE,
                e.getMessage());
        }
    }

    public List<IndexesRecord> query(String tableSchema) {
        return query(SELECT_ALL_INDEXES, INDEXES_TABLE, IndexesRecord.class, tableSchema);
    }

    public List<IndexesRecord> query(String tableSchema, String tableName) {
        return query(SELECT_INDEXES, INDEXES_TABLE, IndexesRecord.class, tableSchema, tableName);
    }

    public List<IndexesRecord> query(String tableSchema, String tableName, String indexName) {
        return query(SELECT_ONE_INDEX, INDEXES_TABLE, IndexesRecord.class, tableSchema, tableName, indexName);
    }

    public List<IndexesRecord> queryByFirstColumn(String tableSchema, String tableName, String firstColumnName) {
        return query(SELECT_INDEXES_BY_FIRST_COLUMN, INDEXES_TABLE, IndexesRecord.class, tableSchema, tableName,
            firstColumnName);
    }

    public List<IndexesRecord> queryColumnarIndex() {
        return query(SELECT_ALL_COLUMNAR_INDEXES, INDEXES_TABLE, IndexesRecord.class, (String) null);
    }

    public List<IndexesRecord> queryColumnarIndexByTable(String tableSchema, String tableName) {
        return query(SELECT_COLUMNAR_INDEXES_BY_TABLE, INDEXES_TABLE, IndexesRecord.class, tableSchema, tableName);
    }

    public List<IndexesRecord> queryColumnarIndexBySchema(String tableSchema) {
        return query(SELECT_ALL_COLUMNAR_INDEXES_BY_SCHEMA, INDEXES_TABLE, IndexesRecord.class, tableSchema);
    }

    public List<IndexesRecord> queryColumnarIndexColumnsByName(String tableSchema, String tableName, String indexName) {
        return query(SELECT_COLUMNAR_COLUMNS_BY_SCHEMA_TABLE_INDEX_NAME, INDEXES_TABLE, IndexesRecord.class,
            tableSchema, tableName, indexName);
    }

    public List<IndexesRecord> queryColumnarIndexColumnsByName(String tableSchema, String indexName) {
        return query(SELECT_COLUMNAR_COLUMNS_BY_SCHEMA_INDEX_NAME, INDEXES_TABLE, IndexesRecord.class,
            tableSchema, indexName);
    }

    public List<IndexesRecord> queryPrimaryKeyBySchemaAndTable(String tableSchema, String tableName) {
        return query(SELECT_PRIMARY_KEY_BY_SCHEMA_TABLE, INDEXES_TABLE, IndexesRecord.class,
            tableSchema, tableName);
    }

    public boolean checkIfExists(String tableSchema, String tableName) {
        List<IndexesRecord> records = query(tableSchema, tableName);
        return records != null && records.size() > 0;
    }

    public boolean checkIfColumnarExists(String tableSchema, String tableName) {
        List<IndexesRecord> records = query(tableSchema, tableName);
        if (records != null) {
            for (IndexesRecord record : records) {
                if (record.isColumnar()) {
                    return true;
                }
            }
        }
        return false;
    }

    public long getColumnarIndexNum(String tableSchema, String tableName) {
        List<IndexesRecord> records = query(tableSchema, tableName);

        if (records != null) {
            return records.stream().filter(IndexesRecord::isColumnar).count();
        }
        return 0L;
    }

    public boolean checkIfExists(String tableSchema, String tableName, String indexName) {
        List<IndexesRecord> records = query(tableSchema, tableName);
        for (IndexesRecord record : records) {
            if (TStringUtil.equalsIgnoreCase(indexName, record.indexName)) {
                return true;
            }
        }
        return false;
    }

    public int updateVersion(String tableSchema, String tableName, long newOpVersion) {
        return update(UPDATE_INDEXES_VERSION, INDEXES_TABLE, tableSchema, tableName, newOpVersion);
    }

    public int updateIndexVersion(String tableSchema, String indexName, long newOpVersion) {
        return update(UPDATE_INDEXES_VERSION_BY_INDEX_NAME, INDEXES_TABLE, tableSchema, indexName, newOpVersion);
    }

    public int updateStatus(String tableSchema, String tableName, int newStatus) {
        return update(UPDATE_INDEXES_STATUS_ALL, INDEXES_TABLE, tableSchema, tableName, newStatus);
    }

    public int updateStatusExceptGsi(String tableSchema, String tableName, int newStatus) {
        return update(UPDATE_LOCAL_INDEXES_STATUS_ALL, INDEXES_TABLE, tableSchema, tableName, newStatus);
    }

    public int updateStatus(String tableSchema, String tableName, List<String> indexNames, int newStatus) {
        Map<Integer, ParameterContext> params = buildParams(tableSchema, tableName, indexNames, newStatus);
        return update(String.format(UPDATE_INDEXES_STATUS_SPECIFIED, concatParams(indexNames)), INDEXES_TABLE, params);
    }

    public int updateStatusByIndexName(String tableSchema, String indexName, long newStatus) {
        return update(UPDATE_INDEXES_STATUS_BY_INDEX_NAME, INDEXES_TABLE, tableSchema, indexName, newStatus);
    }

    public int updateFlagByIndexName(String tableSchema, String indexName, long newStatus) {
        return update(UPDATE_INDEXES_FLAG_BY_INDEX_NAME, INDEXES_TABLE, tableSchema, indexName, newStatus);
    }

    public int updateStatus(String tableSchema, String tableName, List<String> indexNames, List<String> columns,
                            int oldStatus, int newStatus) {
        List<String> paramValues = new ArrayList<>();
        paramValues.add(String.valueOf(newStatus));
        paramValues.add(tableSchema);
        paramValues.add(tableName);
        if (GeneralUtil.isNotEmpty(indexNames)) {
            paramValues.addAll(indexNames);
        }
        if (GeneralUtil.isNotEmpty(columns)) {
            paramValues.addAll(columns);
        }
        paramValues.add(String.valueOf(oldStatus));

        Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(paramValues.toArray(new String[0]));

        return update(
            String.format(UPDATE_INDEXES_STATUS_SPECIFIED_GSI_COLUMNS, concatParams(indexNames), concatParams(columns)),
            INDEXES_TABLE, params);
    }

    public int updateColumnStatus(String tableSchema, String tableName, List<String> columns, int newStatus) {
        List<String> paramValues = new ArrayList<>();
        paramValues.add(String.valueOf(newStatus));
        paramValues.add(tableSchema);
        paramValues.add(tableName);
        if (GeneralUtil.isNotEmpty(columns)) {
            paramValues.addAll(columns);
        }

        Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(paramValues.toArray(new String[0]));

        return update(
            String.format(UPDATE_INDEXES_STATUS_SPECIFIED_COLUMNS, concatParams(columns)), INDEXES_TABLE, params);
    }

    public int updateColumnName(String tableSchema, String tableName, String newColumnName, String oldColumnName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {newColumnName, tableSchema, tableName, oldColumnName});
        return update(UPDATE_INDEXES_COLUMN_NAME, INDEXES_TABLE, params);
    }

    public int[] updateIndexesByRewrite(String tableSchema, String tableName, List<IndexesRecord> records,
                                        List<String> indexNames) {
        int[] affectedRows = new int[0];
        if (indexNames == null || indexNames.isEmpty()) {
            return affectedRows;
        }
        List<Map<Integer, ParameterContext>> paramsForInsert = new ArrayList<>(records.size());
        for (IndexesRecord record : records) {
            paramsForInsert.add(record.buildInsertParams());
        }

        Map<Integer, ParameterContext> paramsForDelete = buildParams(tableSchema, tableName, indexNames);
        try {
            delete(String.format(DELETE_INDEXES_SPECIFIED, concatParams(indexNames)), INDEXES_TABLE, paramsForDelete);
            DdlMetaLogUtil.logSql(INSERT_INDEXES, paramsForInsert);
            affectedRows = MetaDbUtil.insert(INSERT_INDEXES, paramsForInsert, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + INDEXES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                INDEXES_TABLE,
                e.getMessage());
        }
        return affectedRows;
    }

    public int[] rename(String tableSchema, String tableName, List<Pair<String, String>> indexNamePairs) {
        int[] affectedRows = new int[indexNamePairs.size()];
        for (int i = 0; i < indexNamePairs.size(); i++) {
            String newIndexName = indexNamePairs.get(i).getKey();
            String oldIndexName = indexNamePairs.get(i).getValue();
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildStringParameters(new String[] {newIndexName, tableSchema, tableName, oldIndexName});
            affectedRows[i] = update(UPDATE_INDEXES_NAME, INDEXES_TABLE, params);
        }
        return affectedRows;
    }

    public int rename(String tableSchema, String tableName, String newTableName) {
        return update(UPDATE_INDEXES_RENAME, INDEXES_TABLE, tableSchema, tableName, newTableName);
    }

    public void renameGsiIndexes(String tableSchema, String indexName, String newIndexName) {
        update(UPDATE_INDEXES_TABLE_NAME, INDEXES_TABLE, tableSchema, indexName, newIndexName);
    }

    public void renameCciIndex(String tableSchema, String newIndexName) {
        update(UPDATE_CCI_TABLE_NAME, INDEXES_TABLE, tableSchema, newIndexName, newIndexName);
    }

    public int renameLocalIndexes(String tableSchema, String tableName, String newTableName) {
        return update(UPDATE_LOCAL_INDEXES_RENAME, INDEXES_TABLE, tableSchema, tableName, newTableName);
    }

    public int cutOverGlobalIndex(String tableSchema, String indexName, String newIndexTableName) {
        return update(UPDATE_GLOBAL_INDEXES_CUT_OVER, INDEXES_TABLE, tableSchema, indexName, newIndexTableName);
    }

    public int delete(String tableSchema) {
        return delete(DELETE_INDEXES_ALL, INDEXES_TABLE, tableSchema);
    }

    public int delete(String tableSchema, String tableName) {
        return delete(DELETE_INDEXES_ALL_TABLES, INDEXES_TABLE, tableSchema, tableName);
    }

    public int delete(String tableSchema, String tableName, List<String> indexNames) {
        Map<Integer, ParameterContext> params = buildParams(tableSchema, tableName, indexNames);
        return delete(String.format(DELETE_INDEXES_SPECIFIED, concatParams(indexNames)), INDEXES_TABLE, params);
    }

    public int deleteColumns(String tableSchema, String tableName, List<String> columnNames) {
        Map<Integer, ParameterContext> params = buildParams(tableSchema, tableName, columnNames);
        return delete(String.format(DELETE_INDEXES_COLUMNS, concatParams(columnNames)), INDEXES_TABLE, params);
    }

    public int deletePrimaryKey(String tableSchema, String tableName) {
        return delete(DELETE_PRIMARY_KEY, INDEXES_TABLE, tableSchema, tableName);
    }

    private String generateGsiStatisticsByConditionSql(Set<String> schemaNames, Set<String> tableNames,
                                                       String tableLike, Set<String> indexNames, String indexLike) {
        StringBuilder sb = new StringBuilder();
        if (schemaNames == null || schemaNames.isEmpty()) {
            return null;
        }

        int schemaIndex = 0;
        for (String schemaName : schemaNames) {
            if (schemaIndex != 0) {
                sb.append(" union all ");
            }
            sb.append(
                "select * from " + INDEXES_TABLE +
                    " where `index_status` = 4 and `index_location` = 1 and `seq_in_index` = 1 and `index_schema` = "
            );
            sb.append("'" + schemaName + "' ");

            if (tableNames != null && !tableNames.isEmpty()) {
                sb.append(" and (");
                int tableIndex = 0;
                for (String tableName : tableNames) {
                    String filter = "table_name = '" + tableName + "'";
                    if (tableIndex != 0) {
                        sb.append(" or ");
                    }
                    sb.append(filter);
                    tableIndex++;
                }
                sb.append(")");
            }

            if (tableLike != null) {
                String filter = " and table_name like '" + tableLike + "%'";
                sb.append(filter);
            }

            if (indexNames != null && !indexNames.isEmpty()) {
                sb.append(" and (");
                int index = 0;
                for (String indexName : indexNames) {
                    String filter = "index_name = '" + indexName + "'";
                    if (index != 0) {
                        sb.append(" or ");
                    }
                    sb.append(filter);
                    index++;
                }
                sb.append(")");
            }

            if (indexLike != null) {
                String filter = " and index_name like '" + indexLike + "'%";
                sb.append(filter);
            }

            schemaIndex++;
        }

        return sb.toString();
    }

    private String generateFkReferenceTableColumNames(List<String> columnNames) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            if (i != 0) {
                sb.append(",").append(columnNames.get(i));
            } else {
                sb.append(columnNames.get(i));
            }
        }
        return sb.toString();
    }

}

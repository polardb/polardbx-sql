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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static final String WHERE_SCHEMA_TABLE_ONE_INDEX_COLUMNS =
        WHERE_SCHEMA_TABLE_ONE_INDEX + " and `column_name` in (%s)";

    private static final String WHERE_SCHEMA_TABLE_INDEXES_COLUMNS_STATUS_GSI =
        WHERE_SCHEMA_TABLE_INDEXES + " and `column_name` in (%s) and `index_status` = ? and `index_location` = 1";

    private static final String SELECT_CLAUSE =
        "select `table_schema`, `table_name`, `non_unique`, `index_schema`, `index_name`, `seq_in_index`, "
            + "`column_name`, `collation`, `cardinality`, `sub_part`, `packed`, `nullable`, `index_type`, "
            + "`comment`, `index_comment`";

    private static final String SELECT_CLAUSE_EXT =
        ", `index_column_type`, `index_location`, `index_table_name`, `index_status`, `version`, `flag`";

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

    private static final String SELECT_ALL_INDEXES =
        SELECT_CLAUSE + SELECT_CLAUSE_EXT + FROM_INDEXES_TABLE + WHERE_SCHEMA + ORDER_BY_SEQ;

    private static final String UPDATE_INDEXES = "update " + INDEXES_TABLE + " set ";

    private static final String UPDATE_INDEXES_NAME =
        UPDATE_INDEXES + "`index_name` = ?" + WHERE_SCHEMA_TABLE_ONE_INDEX;

    private static final String UPDATE_INDEXES_VERSION = UPDATE_INDEXES + "`version` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_INDEXES_STATUS = UPDATE_INDEXES + "`index_status` = ?";

    private static final String UPDATE_INDEXES_STATUS_ALL = UPDATE_INDEXES_STATUS + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_INDEXES_STATUS_SPECIFIED = UPDATE_INDEXES_STATUS + WHERE_SCHEMA_TABLE_INDEXES;

    private static final String UPDATE_INDEXES_STATUS_SPECIFIED_GSI_COLUMNS =
        UPDATE_INDEXES_STATUS + WHERE_SCHEMA_TABLE_INDEXES_COLUMNS_STATUS_GSI;

    private static final String UPDATE_INDEXES_RENAME = UPDATE_INDEXES + "`table_name` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_LOCAL_INDEXES_RENAME =
        UPDATE_INDEXES + "`table_name` = ?" + WHERE_SCHEMA_TABLE + " and `index_location` = 0";

    private static final String UPDATE_INDEXES_COLUMN_NAME =
        UPDATE_INDEXES + "`column_name` = ?" + WHERE_SCHEMA_TABLE_COLUMN;

    private static final String UPDATE_INDEXES_COLUMN_TYPE_AND_STATUS_ALL =
        UPDATE_INDEXES + "`comment` = ?, `index_column_type`=?, `index_status`=? " + WHERE_SCHEMA_TABLE_ONE_INDEX;

    private static final String UPDATE_INDEXES_COLUMN_TYPE_AND_STATUS_SPECIFIED =
        UPDATE_INDEXES + "`comment` = ?, `index_column_type`=?, `index_status`=? "
            + WHERE_SCHEMA_TABLE_ONE_INDEX_COLUMNS;

    private static final String DELETE_INDEXES = "delete" + FROM_INDEXES_TABLE;

    private static final String DELETE_INDEXES_ALL = DELETE_INDEXES + WHERE_SCHEMA;

    private static final String DELETE_INDEXES_ALL_TABLES = DELETE_INDEXES + WHERE_SCHEMA_TABLE;

    private static final String DELETE_INDEXES_SPECIFIED = DELETE_INDEXES + WHERE_SCHEMA_TABLE_INDEXES;

    private static final String DELETE_INDEXES_COLUMNS = DELETE_INDEXES + WHERE_SCHEMA_TABLE_COLUMNS;

    private static final String DELETE_PRIMARY_KEY = DELETE_INDEXES + WHERE_SCHEMA_TABLE_PRIMARY_KEY;

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
        return query(String.format(SELECT_INFO_SCHEMA_SPECIFIED, concat(indexNames)), INDEXES_INFO_SCHEMA,
            IndexesInfoSchemaRecord.class, phyTableSchema, phyTableName, dataSource);
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

    public List<IndexesRecord> query(String tableSchema) {
        return query(SELECT_ALL_INDEXES, INDEXES_TABLE, IndexesRecord.class, tableSchema);
    }

    public List<IndexesRecord> query(String tableSchema, String tableName) {
        return query(SELECT_INDEXES, INDEXES_TABLE, IndexesRecord.class, tableSchema, tableName);
    }

    public List<IndexesRecord> queryByFirstColumn(String tableSchema, String tableName, String firstColumnName) {
        return query(SELECT_INDEXES_BY_FIRST_COLUMN, INDEXES_TABLE, IndexesRecord.class, tableSchema, tableName,
            firstColumnName);
    }

    public boolean checkIfExists(String tableSchema, String tableName) {
        List<IndexesRecord> records = query(tableSchema, tableName);
        return records != null && records.size() > 0;
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

    public int updateStatus(String tableSchema, String tableName, int newStatus) {
        return update(UPDATE_INDEXES_STATUS_ALL, INDEXES_TABLE, tableSchema, tableName, newStatus);
    }

    public int updateStatus(String tableSchema, String tableName, List<String> indexNames, int newStatus) {
        return update(String.format(UPDATE_INDEXES_STATUS_SPECIFIED, concat(indexNames)), INDEXES_TABLE, tableSchema,
            tableName, newStatus);
    }

    public int updateStatus(String tableSchema, String tableName, List<String> indexNames, List<String> columns,
                            int oldStatus, int newStatus) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(new String[] {
            String.valueOf(newStatus),
            tableSchema, tableName, String.valueOf(oldStatus)});
        return update(String.format(UPDATE_INDEXES_STATUS_SPECIFIED_GSI_COLUMNS, concat(indexNames), concat(columns)),
            INDEXES_TABLE, params);
    }

    public int updateColumnName(String tableSchema, String tableName, String newColumnName, String oldColumnName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {newColumnName, tableSchema, tableName, oldColumnName});
        return update(UPDATE_INDEXES_COLUMN_NAME, INDEXES_TABLE, params);
    }

    public int updateAllColumnType(
        String tableSchema,
        String tableName,
        String indexName,
        Integer indexColumnType,
        Integer indexStatus) {

        /**
         * @see com.taobao.tddl.optimizer.config.table.GsiMetaManager.IndexColumnType
         */
        String comment = indexColumnType == 0 ? "INDEX" : "COVERING";

        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {
                comment,
                String.valueOf(indexColumnType),
                String.valueOf(indexStatus),
                tableSchema,
                tableName,
                indexName,
            });
        return update(UPDATE_INDEXES_COLUMN_TYPE_AND_STATUS_ALL, INDEXES_TABLE, params);
    }

    public int updateColumnType(
        String tableSchema,
        String tableName,
        String indexName,
        List<String> columnNameList,
        Integer indexColumnType,
        Integer indexStatus) {

        if (CollectionUtils.isEmpty(columnNameList)) {
            return 0;
        }

        /**
         * @see com.taobao.tddl.optimizer.config.table.GsiMetaManager.IndexColumnType
         */
        String comment = indexColumnType == 0 ? "INDEX" : "COVERING";

        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {
                comment,
                String.valueOf(indexColumnType),
                String.valueOf(indexStatus),
                tableSchema,
                tableName,
                indexName,
            });
        return update(String.format(UPDATE_INDEXES_COLUMN_TYPE_AND_STATUS_SPECIFIED, concat(columnNameList)),
            INDEXES_TABLE, params);
    }

    public int[] rename(String tableSchema, String tableName, Map<String, String> indexNamePairs) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(indexNamePairs.size());
        for (Map.Entry<String, String> indexName : indexNamePairs.entrySet()) {
            String newIndexName = indexName.getKey();
            String oldIndexName = indexName.getValue();
            Map<Integer, ParameterContext> params =
                MetaDbUtil.buildStringParameters(new String[] {newIndexName, tableSchema, tableName, oldIndexName});
            paramsBatch.add(params);
        }
        return update(UPDATE_INDEXES_NAME, INDEXES_TABLE, paramsBatch);
    }

    public int rename(String tableSchema, String tableName, String newTableName) {
        return update(UPDATE_INDEXES_RENAME, INDEXES_TABLE, tableSchema, tableName, newTableName);
    }

    public int renameLocalIndexes(String tableSchema, String tableName, String newTableName) {
        return update(UPDATE_LOCAL_INDEXES_RENAME, INDEXES_TABLE, tableSchema, tableName, newTableName);
    }

    public int delete(String tableSchema) {
        return delete(DELETE_INDEXES_ALL, INDEXES_TABLE, tableSchema);
    }

    public int delete(String tableSchema, String tableName) {
        return delete(DELETE_INDEXES_ALL_TABLES, INDEXES_TABLE, tableSchema, tableName);
    }

    public int delete(String tableSchema, String tableName, List<String> indexNames) {
        return delete(String.format(DELETE_INDEXES_SPECIFIED, concat(indexNames)), INDEXES_TABLE, tableSchema,
            tableName);
    }

    public int deleteColumns(String tableSchema, String tableName, List<String> columnNames) {
        return delete(String.format(DELETE_INDEXES_COLUMNS, concat(columnNames)), INDEXES_TABLE, tableSchema,
            tableName);
    }

    public int deletePrimaryKey(String tableSchema, String tableName) {
        return delete(DELETE_PRIMARY_KEY, INDEXES_TABLE, tableSchema, tableName);
    }

}

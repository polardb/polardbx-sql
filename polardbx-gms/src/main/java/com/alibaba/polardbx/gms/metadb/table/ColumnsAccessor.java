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

import com.mysql.jdbc.Field;
import com.alibaba.polardbx.rpc.compatible.XResultSetMetaData;
import com.alibaba.polardbx.rpc.result.XMetaUtil;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.record.CountRecord;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnsAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnsAccessor.class);

    public static final String JDBC_TYPE = "JDBC_TYPE";
    public static final String JDBC_TYPE_NAME = "JDBC_TYPE_NAME";
    public static final String FIELD_LENGTH = "FIELD_LENGTH";

    private static final String COLUMNS_INFO_SCHEMA = "information_schema.columns";

    private static final String COLUMNS_TABLE = wrap(GmsSystemTables.COLUMNS);

    private static final String INSERT_COLUMNS =
        "insert into " + COLUMNS_TABLE
            + "(`table_schema`, `table_name`, `column_name`, `ordinal_position`, `column_default`, "
            + "`is_nullable`, `data_type`, `character_maximum_length`, `character_octet_length`, "
            + "`numeric_precision`, `numeric_scale`, `datetime_precision`, `character_set_name`, "
            + "`collation_name`, `column_type`, `column_key`, `extra`, `privileges`, `column_comment`, "
            + "`generation_expression`, `jdbc_type`, `jdbc_type_name`, `field_length`, `version`, `status`, `flag`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String WHERE_SCHEMA = " where `table_schema` = ?";

    private static final String WHERE_SCHEMA_TABLE = WHERE_SCHEMA + " and `table_name` = ?";

    private static final String WHERE_SCHEMA_TABLES = WHERE_SCHEMA + " and `table_name` in (%s)";

    private static final String WHERE_SCHEMA_TABLE_ONE_COLUMN = WHERE_SCHEMA_TABLE + " and `column_name` = ?";

    private static final String WHERE_SCHEMA_TABLE_COLUMNS = WHERE_SCHEMA_TABLE + " and `column_name` in (%s)";

    private static final String WHERE_SCHEMA_TABLES_COLUMNS = WHERE_SCHEMA_TABLES + " and `column_name` in (%s)";

    private static final String WHERE_SCHEMA_TABLES_COLUMNS_STATUS = WHERE_SCHEMA_TABLES_COLUMNS + " and `status` = ?";

    private static final String ORDER_BY_ORDINAL_POSITION = " order by ordinal_position asc";

    private static final String SELECT_CLAUSE =
        "select `table_schema`, `table_name`, `column_name`, `ordinal_position`, `column_default`, "
            + "`is_nullable`, `data_type`, `character_maximum_length`, `character_octet_length`, "
            + "`numeric_precision`, `numeric_scale`, `datetime_precision`, `character_set_name`, "
            + "`collation_name`, `column_type`, `column_key`, `extra`, `privileges`, `column_comment`, "
            + "`generation_expression`";

    private static final String SELECT_INFO_SCHEMA =
        SELECT_CLAUSE + " from " + COLUMNS_INFO_SCHEMA + WHERE_SCHEMA_TABLE;

    private static final String SELECT_INFO_SCHEMA_SPECIFIED =
        SELECT_CLAUSE + " from " + COLUMNS_INFO_SCHEMA + WHERE_SCHEMA_TABLE_COLUMNS;

    private static final String SELECT_COLUMNS =
        SELECT_CLAUSE + ", `jdbc_type`, `jdbc_type_name`, `field_length`, `version`, `status`, `flag` from "
            + COLUMNS_TABLE + WHERE_SCHEMA_TABLE + ORDER_BY_ORDINAL_POSITION;

    private static final String SELECT_ALL_TABLE_COLUMNS =
        SELECT_CLAUSE + ", `jdbc_type`, `jdbc_type_name`, `field_length`, `version`, `status`, `flag` from "
            + COLUMNS_TABLE + WHERE_SCHEMA + ORDER_BY_ORDINAL_POSITION;

    private static final String SELECT_ONE_COLUMN =
        SELECT_CLAUSE + ", `jdbc_type`, `jdbc_type_name`, `field_length`, `version`, `status`, `flag` from "
            + COLUMNS_TABLE + WHERE_SCHEMA_TABLE_ONE_COLUMN;

    private static final String SELECT_JDBC_TYPES = "select * from %s.%s limit 1";

    private static final String SELECT_COLUNN_COUNT = "select count(*) from " + COLUMNS_TABLE + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_COLUMNS = "update " + COLUMNS_TABLE + " set ";

    private static final String UPDATE_COLUMNS_ALL = UPDATE_COLUMNS
        + "`column_name` = ?, `ordinal_position` = ?, `column_default` = ?, `is_nullable` = ?, `data_type` = ?, "
        + "`character_maximum_length` = ?, `character_octet_length` = ?, `numeric_precision` = ?, `numeric_scale` = ?, "
        + "`datetime_precision` = ?, `character_set_name` = ?, `collation_name` = ?, `column_type` = ?, "
        + "`column_key` = ?, `extra` = ?, `privileges` = ?, `column_comment` = ?, `generation_expression` = ?, "
        + "`jdbc_type` = ?, `jdbc_type_name` = ?, `field_length` = ?"
        + WHERE_SCHEMA_TABLE_ONE_COLUMN;

    private static final String UPDATE_COLUMNS_VERSION = UPDATE_COLUMNS + "`version` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_COLUMNS_STATUS = UPDATE_COLUMNS + "`status` = ?";

    private static final String UPDATE_COLUMNS_STATUS_ALL = UPDATE_COLUMNS_STATUS + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_COLUMNS_STATUS_SPECIFIED = UPDATE_COLUMNS_STATUS + WHERE_SCHEMA_TABLE_COLUMNS;

    private static final String UPDATE_COLUMNS_STATUS_SPECIFIED_STATUS =
        UPDATE_COLUMNS_STATUS + WHERE_SCHEMA_TABLES_COLUMNS_STATUS;

    private static final String UPDATE_COLUMN_FLAG =
        UPDATE_COLUMNS + "`flag` = ?" + WHERE_SCHEMA_TABLE_ONE_COLUMN + " and `flag` = ?";

    private static final String UPDATE_COLUMNS_RENAME = UPDATE_COLUMNS + "`table_name` = ?" + WHERE_SCHEMA_TABLE;

    private static final String DELETE_COLUMNS = "delete from " + COLUMNS_TABLE;

    private static final String DELETE_COLUMNS_ALL = DELETE_COLUMNS + WHERE_SCHEMA;

    private static final String DELETE_COLUMNS_ALL_TABLES = DELETE_COLUMNS + WHERE_SCHEMA_TABLE;

    private static final String DELETE_COLUMNS_SPECIFIED = DELETE_COLUMNS + WHERE_SCHEMA_TABLE_COLUMNS;

    public int[] insert(List<ColumnsRecord> records, String tableSchema, String tableName) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnsRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNS, paramsBatch);
            return MetaDbUtil.insert(INSERT_COLUMNS, paramsBatch, connection);
        } catch (SQLException e) {
            String extraMsg = "";
            if (checkIfDuplicate(e)) {
                List<ColumnsRecord> existingRecords = query(tableSchema, tableName);
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
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNS_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNS_TABLE,
                e.getMessage());
        }
    }

    private boolean compare(List<ColumnsRecord> newRecords, List<ColumnsRecord> existingRecords) {
        if (newRecords.size() != existingRecords.size()) {
            return false;
        }

        Map<String, ColumnsRecord> existingRecordsMap = new HashMap<>(existingRecords.size());
        for (ColumnsRecord existingRecord : existingRecords) {
            existingRecordsMap.put(existingRecord.columnName, existingRecord);
        }

        for (ColumnsRecord newRecord : newRecords) {
            if (!existingRecordsMap.containsKey(newRecord.columnName) ||
                !compare(newRecord, existingRecordsMap.get(newRecord.columnName))) {
                return false;
            }
        }

        return true;
    }

    private boolean compare(ColumnsRecord newRecord, ColumnsRecord existingRecord) {
        return TStringUtil.equalsIgnoreCase(newRecord.tableSchema, existingRecord.tableSchema) &&
            TStringUtil.equalsIgnoreCase(newRecord.tableName, existingRecord.tableName) &&
            TStringUtil.equalsIgnoreCase(newRecord.columnDefault, existingRecord.columnDefault) &&
            TStringUtil.equalsIgnoreCase(newRecord.isNullable, existingRecord.isNullable) &&
            TStringUtil.equalsIgnoreCase(newRecord.dataType, existingRecord.dataType) &&
            newRecord.characterMaximumLength == existingRecord.characterMaximumLength &&
            newRecord.numericPrecision == existingRecord.numericPrecision &&
            newRecord.numericScale == existingRecord.numericScale &&
            TStringUtil.equalsIgnoreCase(newRecord.characterSetName, existingRecord.characterSetName) &&
            TStringUtil.equalsIgnoreCase(newRecord.collationName, existingRecord.collationName) &&
            TStringUtil.equalsIgnoreCase(newRecord.columnType, existingRecord.columnType) &&
            newRecord.jdbcType == existingRecord.jdbcType &&
            TStringUtil.equalsIgnoreCase(newRecord.jdbcTypeName, existingRecord.jdbcTypeName) &&
            newRecord.fieldLength == existingRecord.fieldLength;
    }

    public List<ColumnsInfoSchemaRecord> queryInfoSchema(String phyTableSchema, String phyTableName,
                                                         DataSource dataSource) {
        return query(SELECT_INFO_SCHEMA, COLUMNS_INFO_SCHEMA, ColumnsInfoSchemaRecord.class, phyTableSchema,
            phyTableName, dataSource);
    }

    public List<ColumnsInfoSchemaRecord> queryInfoSchema(String phyTableSchema, String phyTableName,
                                                         List<String> columnNames, DataSource dataSource) {
        return query(String.format(SELECT_INFO_SCHEMA_SPECIFIED, concat(columnNames)), COLUMNS_INFO_SCHEMA,
            ColumnsInfoSchemaRecord.class, phyTableSchema, phyTableName, dataSource);
    }

    private static String surroundWithBacktick(String identifier) {
        if (identifier.contains("`")) {
            return "`" + identifier.replaceAll("`", "``") + "`";
        }
        return "`" + identifier + "`";
    }

    public Map<String, Map<String, Object>> queryColumnJdbcExtInfo(String phyTableSchema, String phyTableName,
                                                                   DataSource dataSource) {
        Map<String, Map<String, Object>> columnExtInfo = new HashMap<>();
        String sql =
            String.format(SELECT_JDBC_TYPES, surroundWithBacktick(phyTableSchema), surroundWithBacktick(phyTableName));
        try (Connection phyDbConn = dataSource.getConnection();
            PreparedStatement ps = phyDbConn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData rsmd = rs.getMetaData();
            if (rsmd != null) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String columnName = rsmd.getColumnName(i);
                    final int jdbcType;
                    final String jdbcTypeName;
                    final long fieldLength;
                    if (rsmd.isWrapperFor(XResultSetMetaData.class)) {
                        XMetaUtil util =
                            new XMetaUtil(rsmd.unwrap(XResultSetMetaData.class).getResult().getMetaData().get(i - 1));
                        jdbcType = util.getJdbcType();
                        jdbcTypeName = util.getJdbcTypeString();
                        fieldLength = util.getFieldLength();
                    } else {
                        Field[] fields = getFieldsFromMeta(rsmd);
                        jdbcType = rsmd.getColumnType(i);
                        jdbcTypeName = rsmd.getColumnTypeName(i);
                        fieldLength = fields[i - 1].getLength();
                    }

                    Map<String, Object> extInfo = new HashMap<>(3);
                    extInfo.put(JDBC_TYPE, jdbcType);
                    extInfo.put(JDBC_TYPE_NAME, jdbcTypeName);
                    extInfo.put(FIELD_LENGTH, fieldLength);

                    columnExtInfo.put(columnName, extInfo);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to query jdbc types for " + wrap(phyTableSchema) + "." + wrap(phyTableName), e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e, "query jdbc types");
        }
        return columnExtInfo;
    }

    private Field[] getFieldsFromMeta(ResultSetMetaData rsmd) {
        try {
            java.lang.reflect.Field field = com.mysql.jdbc.ResultSetMetaData.class.getDeclaredField("fields");
            field.setAccessible(true);
            if (rsmd instanceof com.mysql.jdbc.ResultSetMetaData) {
                return (Field[]) field.get(rsmd);
            }
            return null;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnsRecord> query(String tableSchema) {
        return query(SELECT_ALL_TABLE_COLUMNS, COLUMNS_TABLE, ColumnsRecord.class, tableSchema);
    }

    public List<ColumnsRecord> query(String tableSchema, String tableName) {
        return query(SELECT_COLUMNS, COLUMNS_TABLE, ColumnsRecord.class, tableSchema, tableName);
    }

    public List<ColumnsRecord> query(String tableSchema, String tableName, String columnName) {
        return query(SELECT_ONE_COLUMN, COLUMNS_TABLE, ColumnsRecord.class, tableSchema, tableName, columnName);
    }

    public int count(String tableSchema, String tableName) {
        List<CountRecord> records =
            query(SELECT_COLUNN_COUNT, COLUMNS_TABLE, CountRecord.class, tableSchema, tableName);
        if (records != null && records.size() > 0) {
            return records.get(0).count;
        }
        return 0;
    }

    public int updateVersion(String tableSchema, String tableName, long newOpVersion) {
        return update(UPDATE_COLUMNS_VERSION, COLUMNS_TABLE, tableSchema, tableName, newOpVersion);
    }

    public int updateStatus(String tableSchema, String tableName, int newStatus) {
        return update(UPDATE_COLUMNS_STATUS_ALL, COLUMNS_TABLE, tableSchema, tableName, newStatus);
    }

    public int updateStatus(String tableSchema, String tableName, List<String> columnNames, int newStatus) {
        return update(String.format(UPDATE_COLUMNS_STATUS_SPECIFIED, concat(columnNames)), COLUMNS_TABLE, tableSchema,
            tableName, newStatus);
    }

    public int updateStatus(String tableSchema, List<String> tableNames, List<String> columnNames, int oldStatus,
                            int newStatus) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(new String[] {
            String.valueOf(newStatus),
            tableSchema, String.valueOf(oldStatus)});

        return update(String.format(UPDATE_COLUMNS_STATUS_SPECIFIED_STATUS, concat(tableNames), concat(columnNames)),
            COLUMNS_TABLE, params);
    }

    public int setColumnFlag(String tableSchema, String tableName, String columnName, long flag) {
        final List<ColumnsRecord> columns = query(tableSchema, tableName, columnName);
        if (columns.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "column '" + columnName + "' not " + flag + " found or multiple");
        }
        final ColumnsRecord column = columns.get(0);
        // Do not check this because compound job may invoke this multiple times.
//        if ((column.flag & flag) != 0L) {
//            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
//                "column '" + columnName + "' flag changed and operation still in process");
//        }

        final long oldFlag = column.flag;
        column.flag |= flag;
        Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(new String[] {
            String.valueOf(column.flag),
            tableSchema, tableName, columnName,
            String.valueOf(oldFlag)});
        return update(UPDATE_COLUMN_FLAG, COLUMNS_TABLE, params);
    }

    public int clearColumnFlag(String tableSchema, String tableName, String columnName, long flag) {
        final List<ColumnsRecord> columns = query(tableSchema, tableName, columnName);
        if (columns.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "column '" + columnName + "' not found or multiple");
        }
        final ColumnsRecord column = columns.get(0);
        // Do not check this because compound job may invoke multiple times.
        if (0L == (column.flag & flag)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "column '" + columnName + "' flag " + flag + " not set");
        }

        final long oldFlag = column.flag;
        column.flag &= ~flag;
        Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(new String[] {
            String.valueOf(column.flag),
            tableSchema, tableName, columnName,
            String.valueOf(oldFlag)});
        return update(UPDATE_COLUMN_FLAG, COLUMNS_TABLE, params);
    }

    public int[] update(List<ColumnsRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnsRecord record : records) {
            Map<Integer, ParameterContext> params = record.buildUpdateParams();
            int index = params.size();
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.tableSchema);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.tableName);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.columnName);
            paramsBatch.add(params);
        }
        return update(UPDATE_COLUMNS_ALL, COLUMNS_TABLE, paramsBatch);
    }

    public int[] change(List<ColumnsRecord> records, Map<String, String> columnNamePairs) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnsRecord record : records) {
            Map<Integer, ParameterContext> params = record.buildUpdateParams();
            int index = params.size();
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.tableSchema);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.tableName);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, columnNamePairs.get(record.columnName));
            paramsBatch.add(params);
        }
        return update(UPDATE_COLUMNS_ALL, COLUMNS_TABLE, paramsBatch);
    }

    public int rename(String tableSchema, String tableName, String newTableName) {
        return update(UPDATE_COLUMNS_RENAME, COLUMNS_TABLE, tableSchema, tableName, newTableName);
    }

    public int delete(String tableSchema) {
        return delete(DELETE_COLUMNS_ALL, COLUMNS_TABLE, tableSchema);
    }

    public int delete(String tableSchema, String tableName) {
        return delete(DELETE_COLUMNS_ALL_TABLES, COLUMNS_TABLE, tableSchema, tableName);
    }

    public int delete(String tableSchema, String tableName, List<String> columnNames) {
        return delete(String.format(DELETE_COLUMNS_SPECIFIED, concat(columnNames)), COLUMNS_TABLE, tableSchema,
            tableName);
    }

}

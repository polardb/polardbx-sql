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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TablesAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TablesAccessor.class);

    private static final String TABLES_INFO_SCHEMA = "information_schema.tables";

    private static final String TABLES_TABLE = wrap(GmsSystemTables.TABLES);

    private static final String INSERT_TABLES =
        "insert into " + TABLES_TABLE
            + "(`table_schema`, `table_name`, `table_type`, `engine`, `version`, `row_format`, `table_rows`, "
            + "`avg_row_length`, `data_length`, `max_data_length`, `index_length`, `data_free`, `auto_increment`, "
            + "`table_collation`, `checksum`, `create_options`, `table_comment`, `new_table_name`, `status`, `flag`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String WHERE_SCHEMA = " where `table_schema` = ?";

    private static final String WHERE_ENGINE = " where `engine` = ?";

    private static final String WHERE_SCHEMA_TABLE = WHERE_SCHEMA + " and `table_name` = ?";

    private static final String WHERE_SCHEMA_NEW_TABLE = WHERE_SCHEMA + " and `new_table_name` = ?";

    private static final String NORMAL_COLUMNS =
        " `table_schema`, `table_name`, `table_type`, `engine`, `version`, `row_format`, "
            + "`table_rows`, `avg_row_length`, `data_length`, `max_data_length`, `index_length`, `data_free`, "
            + "`auto_increment`, `table_collation`, `checksum`, `create_options`, `table_comment`";

    private static final String ALL_COLUMNS = "`id`, " + NORMAL_COLUMNS;

    private static final String SELECT_CLAUSE = "select " + ALL_COLUMNS;

    private static final String SELECT_INFO_SCHEMA =
        "select " + NORMAL_COLUMNS + " from " + TABLES_INFO_SCHEMA + WHERE_SCHEMA_TABLE;

    private static final String SELECT_TABLES =
        SELECT_CLAUSE + ", `new_table_name`, `status`, `flag` from " + TABLES_TABLE;

    private static final String SELECT_TABLES_ALL = SELECT_TABLES + WHERE_SCHEMA;

    private static final String SELECT_TABLES_BY_ENGINE = SELECT_TABLES + WHERE_ENGINE;

    private static final String SELECT_TABLES_ONE = SELECT_TABLES + WHERE_SCHEMA_TABLE;

    private static final String SELECT_TABLES_NEW = SELECT_TABLES + WHERE_SCHEMA_NEW_TABLE;

    private static final String SELECT_BY_ID = SELECT_TABLES + " where id = ?";

    private static final String UPDATE_TABLES = "update " + TABLES_TABLE + " set ";

    private static final String UPDATE_TABLES_STATUS = UPDATE_TABLES + "`status` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_VERSION = UPDATE_TABLES + "`version` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_NEW_NAME = UPDATE_TABLES + "`new_table_name` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_ENGINE = UPDATE_TABLES + "`engine` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_COMMENT = UPDATE_TABLES + "`table_comment` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_ROW_FORMAT = UPDATE_TABLES + "`row_format` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_STATISTIC =
        UPDATE_TABLES
            + "`table_rows` = ?, `avg_row_length` = ?, `data_length` = ?, `max_data_length` = ?, `index_length` = ?, "
            + "`data_free` = ?"
            + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_FLAG = UPDATE_TABLES + "`flag` = ?" + WHERE_SCHEMA_TABLE;

    private static final String UPDATE_TABLES_RENAME =
        UPDATE_TABLES + "`table_name` = ?, `new_table_name` = ''" + WHERE_SCHEMA_TABLE;

    private static final String DELETE_TABLES = "delete from " + TABLES_TABLE + WHERE_SCHEMA_TABLE;

    private static final String DELETE_TABLES_ALL = "delete from " + TABLES_TABLE + WHERE_SCHEMA;

    public static final String SELECT_VERSION_FOR_UPDATE =
        "select version from " + TABLES_TABLE + " where table_schema=? and table_name=? for update";

    public int insert(TablesRecord record) {
        try {
            DdlMetaLogUtil.logSql(INSERT_TABLES, record.buildInsertParams());
            return MetaDbUtil.insert(INSERT_TABLES, record.buildInsertParams(), connection);
        } catch (SQLException e) {
            String extraMsg = "";
            if (checkIfDuplicate(e)) {
                TablesRecord existingRecord = query(record.tableSchema, record.tableName, false);
                if (compare(record, existingRecord)) {
                    // Ignore new and use existing record.
                    return 1;
                } else {
                    extraMsg = ". New and existing records don't match";
                }
            }
            LOGGER.error("Failed to insert a new record into " + TABLES_TABLE + extraMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert into",
                TABLES_TABLE,
                e.getMessage());
        }
    }

    private boolean compare(TablesRecord newRecord, TablesRecord existingRecord) {
        if (newRecord != null && existingRecord != null) {
            return TStringUtil.equalsIgnoreCase(newRecord.tableType, existingRecord.tableType) &&
                TStringUtil.equalsIgnoreCase(newRecord.engine, existingRecord.engine) &&
                TStringUtil.equalsIgnoreCase(newRecord.rowFormat, existingRecord.rowFormat) &&
                newRecord.autoIncrement == existingRecord.autoIncrement &&
                TStringUtil.equalsIgnoreCase(newRecord.tableCollation, existingRecord.tableCollation);
        }
        return false;
    }

    public TablesInfoSchemaRecord queryInfoSchema(String phyTableSchema, String phyTableName,
                                                  DataSource dataSource) {
        List<TablesInfoSchemaRecord> records =
            query(SELECT_INFO_SCHEMA, TABLES_INFO_SCHEMA, TablesInfoSchemaRecord.class, phyTableSchema, phyTableName,
                dataSource);
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public List<TablesRecord> query(String tableSchema) {
        return query(SELECT_TABLES_ALL, TABLES_TABLE, TablesRecord.class, tableSchema);
    }

    public List<TablesRecord> queryByEngine(String engine) {
        return query(SELECT_TABLES_BY_ENGINE, TABLES_TABLE, TablesRecord.class, engine);
    }

    public long getTableMetaVersionForUpdate(String tableSchema, String tableName) {

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            long version = -1;
            stmt =
                connection.prepareStatement(SELECT_VERSION_FOR_UPDATE);
            stmt.setString(1, tableSchema);
            stmt.setString(2, tableName);
            rs = stmt.executeQuery();
            if (rs.next()) {
                version = rs.getLong(1);
            }
            return version;
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, ex,
                ex.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Throwable ex) {
                // Ignore
            }

            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Throwable ex) {
                // Ignore
            }

        }

    }

    public TablesRecord query(String tableSchema, String tableName, boolean isNewTableForRename) {
        String selectSql = isNewTableForRename ? SELECT_TABLES_NEW : SELECT_TABLES_ONE;
        List<TablesRecord> records = query(selectSql, TABLES_TABLE, TablesRecord.class, tableSchema, tableName);
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public TablesRecord query(long tableId) {
        List<TablesRecord> records = query(SELECT_BY_ID, TABLES_TABLE, TablesRecord.class, tableId);
        return records.stream().findFirst().orElse(null);
    }

    public int updateEngine(String tableSchema, String tableName, String engine) {
        return update(UPDATE_TABLES_ENGINE, TABLES_TABLE, tableSchema, tableName, engine);
    }

    public int updateVersion(String tableSchema, String tableName, long newOpVersion) {
        return update(UPDATE_TABLES_VERSION, TABLES_TABLE, tableSchema, tableName, newOpVersion);
    }

    public int updateStatus(String tableSchema, String tableName, int newStatus) {
        return update(UPDATE_TABLES_STATUS, TABLES_TABLE, tableSchema, tableName, newStatus);
    }

    public int updateNewName(String tableSchema, String tableName, String newTableName) {
        return update(UPDATE_TABLES_NEW_NAME, TABLES_TABLE, tableSchema, tableName, newTableName);
    }

    public int updateComment(String tableSchema, String tableName, String comment) {
        return update(UPDATE_TABLES_COMMENT, TABLES_TABLE, tableSchema, tableName, comment);
    }

    public int updateRowFormat(String tableSchema, String tableName, String rowFormat) {
        return update(UPDATE_TABLES_ROW_FORMAT, TABLES_TABLE, tableSchema, tableName, rowFormat);
    }

    public int updateStatistic(String tableSchema, String tableName, Long tableRows, Long avgRowLength, Long dataLength,
                               Long maxDataLength, Long indexLength, Long dataFree) {
        try {
            int index = 0;
            Map<Integer, ParameterContext> params = new HashMap<>(8);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, tableRows);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, avgRowLength);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, dataLength);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, maxDataLength);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, indexLength);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, dataFree);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.update(UPDATE_TABLES_STATISTIC, params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to update " + TABLES_TABLE + " with statistic for table " + wrap(tableName),
                e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                TABLES_TABLE,
                e.getMessage());
        }
    }

    public int updateFlag(String tableSchema, String tableName, long flag) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, flag);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
            DdlMetaLogUtil.logSql(UPDATE_TABLES_FLAG, params);
            return MetaDbUtil.update(UPDATE_TABLES_FLAG, params, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update flag", TABLES_TABLE,
                e.getMessage());
        }
    }

    public int rename(String tableSchema, String tableName, String newTableName) {
        return update(UPDATE_TABLES_RENAME, TABLES_TABLE, tableSchema, tableName, newTableName);
    }

    public int delete(String tableSchema, String tableName) {
        return delete(DELETE_TABLES, TABLES_TABLE, tableSchema, tableName);
    }

    public int delete(String tableSchema) {
        return delete(DELETE_TABLES_ALL, TABLES_TABLE, tableSchema);
    }

}

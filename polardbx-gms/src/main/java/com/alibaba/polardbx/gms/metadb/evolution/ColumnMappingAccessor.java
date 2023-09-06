package com.alibaba.polardbx.gms.metadb.evolution;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Maps;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnMappingAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnMappingAccessor.class);

    private static final String MAPPING_TABLES = wrap(GmsSystemTables.COLUMN_MAPPING);

    private static final String SELECT_ALL =
        "select `field_id`, `table_schema`, `table_name`, `column_name`";

    private static final String FROM_TABLE = " from " + MAPPING_TABLES;

    private static final String AND_STATUS = " and `status` = " + TableStatus.PUBLIC.getValue();

    private static final String WHERE_SCHEMA = " where `table_schema` = ?";

    private static final String WHERE_SCHEMA_STATUS = WHERE_SCHEMA + AND_STATUS;

    private static final String WHERE_SCHEMA_TABLE = WHERE_SCHEMA + " and `table_name` = ?";

    private static final String WHERE_SCHEMA_TABLE_STATUS = WHERE_SCHEMA_TABLE + AND_STATUS;
    private static final String WHERE_SCHEMA_TABLE_COLUMN = WHERE_SCHEMA_TABLE + " and `column_name` = ?";

    private static final String WHERE_SCHEMA_TABLE_COLUMN_STATUS = WHERE_SCHEMA_TABLE_COLUMN + AND_STATUS;

    private static final String WHERE_SCHEMA_TABLE_IN_COLUMN = WHERE_SCHEMA_TABLE + " and `column_name` in (%s)";

    private static final String WHERE_SCHEMA_TABLE_IN_COLUMN_STATUS = WHERE_SCHEMA_TABLE_IN_COLUMN + AND_STATUS;
    private static final String INSERT_COLUMNS =
        "insert into " + MAPPING_TABLES
            + "(`table_schema`, `table_name`, `column_name`) values (?,?,?)";

    private static final String SELECT_SCHEMA_STATUS = SELECT_ALL + FROM_TABLE + WHERE_SCHEMA_STATUS;

    private static final String SELECT_SCHEMA_TABLE_STATUS = SELECT_ALL + FROM_TABLE + WHERE_SCHEMA_TABLE_STATUS;

    private static final String SELECT_SCHEMA_TABLE_IN_COLUMN_STATUS =
        SELECT_ALL + FROM_TABLE + WHERE_SCHEMA_TABLE_IN_COLUMN_STATUS;

    private static final String SELECT_SCHEMA_TABLE_ABSENT = SELECT_ALL + FROM_TABLE + WHERE_SCHEMA_TABLE
        + " and `status` = " + TableStatus.ABSENT.getValue();

    private static final String UPDATE_COLUMNS_COLUMN_NAME_STATUS =
        "update " + MAPPING_TABLES + " set `column_name` = ? " + WHERE_SCHEMA_TABLE_COLUMN_STATUS;

    private static final String DELETE_SCHEMA = "Delete" + FROM_TABLE + WHERE_SCHEMA;

    private static final String DELETE_SCHEMA_TABLE = "Delete" + FROM_TABLE + WHERE_SCHEMA_TABLE;

    private static final String HIDE_BY_SCHEMA_TABLE_COLUMN =
        "update " + MAPPING_TABLES + " set `status` = " + TableStatus.ABSENT.getValue() + WHERE_SCHEMA_TABLE_IN_COLUMN;

    private static final String DELETE_ABSENT_SCHEMA_TABLE = "Delete" + FROM_TABLE + WHERE_SCHEMA_TABLE
        + " and `status` = " + TableStatus.ABSENT.getValue();
    private static final String RENAME_MAPPING = "update " + MAPPING_TABLES
        + " set `table_name` = ? " + WHERE_SCHEMA_TABLE_STATUS;

    public int[] insert(List<ColumnMappingRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnMappingRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNS, paramsBatch);
            return MetaDbUtil.insert(INSERT_COLUMNS, paramsBatch, connection);
        } catch (SQLException e) {
            throw logAndThrow("Failed to insert a batch of new records into ", "insert into", e);
        }
    }

    public List<ColumnMappingRecord> querySchema(String schema) {
        return query(SELECT_SCHEMA_STATUS, MAPPING_TABLES, ColumnMappingRecord.class, schema);
    }

    public List<ColumnMappingRecord> querySchemaTable(String schema, String table) {
        return query(SELECT_SCHEMA_TABLE_STATUS, MAPPING_TABLES, ColumnMappingRecord.class, schema, table);
    }

    public List<ColumnMappingRecord> querySchemaTableAbsent(String schema, String table) {
        return query(SELECT_SCHEMA_TABLE_ABSENT, MAPPING_TABLES, ColumnMappingRecord.class, schema, table);
    }

    public List<ColumnMappingRecord> querySchemaTableColumns(String schema, String table, List<String> columns) {
        Map<Integer, ParameterContext> params = buildParams(schema, table, columns);
        return query(String.format(SELECT_SCHEMA_TABLE_IN_COLUMN_STATUS, concatParams(columns)),
            MAPPING_TABLES, ColumnMappingRecord.class, params);
    }

    public int[] change(List<ColumnsRecord> records, Map<String, String> columnNameMap) {
        int[] affectedRows = new int[records.size()];
        for (int i = 0; i < records.size(); i++) {
            ColumnsRecord record = records.get(i);
            Map<Integer, ParameterContext> params = Maps.newHashMapWithExpectedSize(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, record.columnName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, record.tableSchema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, record.tableName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, columnNameMap.get(record.columnName));
            affectedRows[i] = update(UPDATE_COLUMNS_COLUMN_NAME_STATUS, MAPPING_TABLES, params);
        }
        return affectedRows;
    }

    public void delete(String schema) {
        super.delete(DELETE_SCHEMA, MAPPING_TABLES, schema);
    }

    public void delete(String schema, String table) {
        super.delete(DELETE_SCHEMA_TABLE, MAPPING_TABLES, schema, table);
    }

    public void deleteAbsent(String schema, String table) {
        super.delete(DELETE_ABSENT_SCHEMA_TABLE, MAPPING_TABLES, schema, table);
    }

    public void hide(String schemaName, String tableName, List<String> columnNames) {
        Map<Integer, ParameterContext> params = buildParams(schemaName, tableName, columnNames);
        update(String.format(HIDE_BY_SCHEMA_TABLE_COLUMN, concatParams(columnNames)), MAPPING_TABLES, params);
    }

    public void rename(String schemaName, String tableName, String newTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newTableName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
        update(RENAME_MAPPING, MAPPING_TABLES, params);
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            MAPPING_TABLES, e.getMessage());
    }
}

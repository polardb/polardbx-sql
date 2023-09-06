package com.alibaba.polardbx.gms.metadb.evolution;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Maps;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnEvolutionAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnEvolutionAccessor.class);

    private static final String MAPPING_TABLES = wrap(GmsSystemTables.COLUMN_MAPPING);
    private static final String EVOLUTION_TABLES = wrap(GmsSystemTables.COLUMN_EVOLUTION);

    private static final String SELECT_ALL =
        "select `field_id`, `ts`, `column_record`, `gmt_created`";

    private static final String FROM_TABLE = " from " + EVOLUTION_TABLES;

    private static final String WHERE_FIELD = " where `field_id` = ?";

    private static final String ORDER = " order by `ts` asc";

    private static final String WHERE_FIELD_TS_ORDER = WHERE_FIELD + " and `ts` > ?" + ORDER;

    private static final String INSERT_COLUMNS =
        "insert into " + EVOLUTION_TABLES
            + "(`field_id`, `ts`, `column_record`) values (?,?,?)";

    private static final String SELECT_FIELD_TS_ORDER = SELECT_ALL + FROM_TABLE + WHERE_FIELD_TS_ORDER;

    private static final String SELECT_MAX_TS = String.format(
        "select b.`field_id` as `field_id`, max(b.`ts`) as `ts`, null as `column_record`, null as `gmt_created`"
            + " from %s a join %s b on a.`field_id`=b.`field_id` "
            + "where a.`table_schema` = ? and a.`table_name` = ? and a.status = %s group by b.`field_id`",
        MAPPING_TABLES, EVOLUTION_TABLES, TableStatus.PUBLIC.getValue());

    private static final String DELETE_FILED = "Delete" + FROM_TABLE + WHERE_FIELD;

    private static final String DELETE_BY_SCHEMA_TABLE_COLUMN =
        "Delete from " + EVOLUTION_TABLES + " where `field_id` "
            + "in (select `field_id` from " + MAPPING_TABLES
            + " where `table_schema` = ? and `table_name` = ? and `column_name` in (%s))";

    private static final String DELETE_BY_SCHEMA_TABLE = String.format(
        "Delete from %s where `field_id` "
            + "in (select `field_id` from %s where `table_schema` = ? and `table_name` = ?)"
        , EVOLUTION_TABLES, MAPPING_TABLES);

    private static final String DELETE_BY_SCHEMA = String.format(
        "Delete from %s where `field_id` "
            + "in (select `field_id` from %s where `table_schema` = ?)"
        , EVOLUTION_TABLES, MAPPING_TABLES);

    public int[] insert(List<ColumnEvolutionRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnEvolutionRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNS, paramsBatch);
            return MetaDbUtil.insert(INSERT_COLUMNS, paramsBatch, connection);
        } catch (SQLException e) {
            throw logAndThrow("Failed to insert a batch of new records into ", "insert into", e);
        }
    }

    public List<ColumnEvolutionRecord> querySchemaTable(String schema, String table) {
        return query(SELECT_MAX_TS, EVOLUTION_TABLES, ColumnEvolutionRecord.class, schema, table);
    }

    public List<ColumnEvolutionRecord> queryFieldTs(Long fieldId, Long ts) {
        Map<Integer, ParameterContext> params = Maps.newHashMapWithExpectedSize(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, fieldId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, ts);
        return query(SELECT_FIELD_TS_ORDER, EVOLUTION_TABLES, ColumnEvolutionRecord.class, params, connection);
    }

    public void delete(List<ColumnMappingRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();
        for (ColumnMappingRecord record : records) {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, record.getFieldId());
            paramsBatch.add(params);
        }
        if (GeneralUtil.isNotEmpty(paramsBatch)) {
            try {
                DdlMetaLogUtil.logSql(DELETE_FILED, paramsBatch);
                MetaDbUtil.delete(DELETE_FILED, paramsBatch, connection);
            } catch (SQLException e) {
                throw logAndThrow("Failed to delete a batch of records", "delete from", e);
            }
        }
    }

    public void delete(Long fieldId) {
        Map<Integer, ParameterContext> params = Maps.newHashMapWithExpectedSize(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, fieldId);
        delete(DELETE_FILED, EVOLUTION_TABLES, params);
    }

    public void delete(String schemaName, String tableName, List<String> columnNames) {
        Map<Integer, ParameterContext> params = buildParams(schemaName, tableName, columnNames);
        delete(String.format(DELETE_BY_SCHEMA_TABLE_COLUMN, concatParams(columnNames)), EVOLUTION_TABLES, params);
    }

    public void delete(String schemaName, String tableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        delete(DELETE_BY_SCHEMA_TABLE, EVOLUTION_TABLES, params);
    }

    public void delete(String schemaName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        delete(DELETE_BY_SCHEMA, EVOLUTION_TABLES, params);
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            EVOLUTION_TABLES, e.getMessage());
    }
}

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnarTableMappingAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String COLUMNAR_TABLE_MAPPING_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_MAPPING);

    private static final String INSERT_COLUMNAR_TABLE_MAPPING_RECORDS = "insert into " + COLUMNAR_TABLE_MAPPING_TABLE +
        "(`table_schema`, `table_name`, `index_name`, `latest_version_id`, `status`, `extra`) values (?, ?, ?, ?, ?, ?)";

    private static final String FROM_TABLE = " from " + COLUMNAR_TABLE_MAPPING_TABLE;

    private static final String AND_VERSION_ID = " and `latest_version_id` = ?";

    private static final String WHERE_BY_TABLE_ID = " where `table_id`=?";

    private static final String WHERE_SCHEMA_AND_STATUS = " where `table_schema`=? and `status`=? ";
    private static final String WHERE_STATUS = " where `status`=? ";

    private static final String WHERE_BY_TABLE_ID_AND_STATUS = " where `table_id`=? and `status`=? ";

    private static final String WHERE_BY_MULTI_SCHEMA_TABLE_INDEX =
        " where (`table_schema`, `table_name`, `index_name`) in (%s)";

    private static final String WHERE_BY_SCHEMA_TABLE_INDEX =
        " where `table_schema` = ? and `table_name` = ? and `index_name` = ? order by gmt_created desc";

    private static final String WHERE_BY_SCHEMA_TABLE =
        " where `table_schema` = ? and `table_name` = ?";

    private static final String WHERE_BY_SCHEMA_INDEX =
        " where `table_schema` = ? and `index_name` = ?";

    private static final String WHERE_BY_SCHEMA = " where `table_schema` = ?";

    private static final String DELETE_SCHEMA_TABLE_INDEX = "delete " + FROM_TABLE + WHERE_BY_SCHEMA_TABLE_INDEX;

    private static final String DELETE_SCHEMA = "delete " + FROM_TABLE + WHERE_BY_SCHEMA;

    private static final String SELECT_ALL_COLUMNS =
        "select `table_id`, `table_schema`, `table_name`, `index_name`, `latest_version_id`, `status`, `extra`";

    private static final String SELECT_SCHEMA_TABLE_INDEX =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_SCHEMA_TABLE_INDEX;

    private static final String SELECT_MULTI_SCHEMA_TABLE_INDEX =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_MULTI_SCHEMA_TABLE_INDEX;

    private static final String SELECT_SCHEMA = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_SCHEMA;

    private static final String SELECT_SCHEMA_AND_VERSION_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_SCHEMA + AND_VERSION_ID;

    private static final String SELECT_SCHEMA_TABLE = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_SCHEMA_TABLE;

    private static final String SELECT_SCHEMA_TABLE_AND_VERSION_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_SCHEMA_TABLE + AND_VERSION_ID;

    private static final String SELECT_SCHEMA_INDEX = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_SCHEMA_INDEX;

    private static final String SELECT_TABLE_ID = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID;

    private static final String SELECT_ALL = SELECT_ALL_COLUMNS + FROM_TABLE;

    private static final String SELECT_TABLE_BY_STATUS = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_STATUS;

    private static final String SELECT_TABLE_BY_SCHEMA_AND_STATUS =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_SCHEMA_AND_STATUS;

    private static final String DELETE_TABLE_ID = "delete " + FROM_TABLE + WHERE_BY_TABLE_ID;

    private static final String UPDATE_LATEST_VERSION_ID_BY_TABLE_ID =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set `latest_version_id` = ?" + WHERE_BY_TABLE_ID;

    private static final String UPDATE_TABLE_NAME_LATEST_VERSION_ID_BY_TABLE_ID =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set table_name = ?, `latest_version_id` = ?" + WHERE_BY_TABLE_ID;

    private static final String UPDATE_TABLE_STATUS_BY_TABLE_ID =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set status = ? " + WHERE_BY_TABLE_ID;

    private static final String UPDATE_TABLE_STATUS_BY_SCHEMA =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set status = ? " + WHERE_BY_SCHEMA;

    private static final String UPDATE_TABLE_STATUS_AND_VERSION_ID_BY_TABLE_ID =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set `status` = ?, `latest_version_id` = ? " + WHERE_BY_TABLE_ID;

    private static final String UPDATE_TABLE_STATUS_AND_VERSION_ID_BY_NAME =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set `status` = ?, `latest_version_id` = ? "
            + WHERE_BY_SCHEMA_TABLE_INDEX;

    private static final String UPDATE_TABLE_STATUS_BY_TABLE_ID_AND_STATUS =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set status = ? " + WHERE_BY_TABLE_ID_AND_STATUS;

    private static final String UPDATE_TABLE_STATUS_AND_EXTRA_BY_TABLE_ID =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set `status` = ?, `extra` = ? " + WHERE_BY_TABLE_ID;

    private static final String UPDATE_EXTRA_BY_TABLE_ID =
        "update " + COLUMNAR_TABLE_MAPPING_TABLE + " set `extra` = ? " + WHERE_BY_TABLE_ID;

    public int[] insert(List<ColumnarTableMappingRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarTableMappingRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNAR_TABLE_MAPPING_RECORDS, paramsBatch);
            return MetaDbUtil.insert(INSERT_COLUMNAR_TABLE_MAPPING_RECORDS, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_TABLE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_TABLE_MAPPING_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarTableMappingRecord> querySchemaTableIndex(String schemaName, String tableName,
                                                                  String indexName) {
        return query(SELECT_SCHEMA_TABLE_INDEX, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class,
            schemaName, tableName, indexName);
    }

    public List<ColumnarTableMappingRecord> querySchemaTableIndexes(
        List<Pair<String, Pair<String, String>>> indexesList) {
        if (CollectionUtils.isEmpty(indexesList)) {
            return new ArrayList<>();
        }
        try {
            String sql = String.format(SELECT_MULTI_SCHEMA_TABLE_INDEX,
                StringUtils.join(
                    indexesList.stream().map(sti ->
                        String.format("('%s','%s','%s')",
                            sti.getKey(), sti.getValue().getKey(), sti.getValue().getValue())
                    ).iterator(),
                    ","
                )
            );
            return MetaDbUtil.query(sql, ColumnarTableMappingRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query " + COLUMNAR_TABLE_MAPPING_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch IN query",
                COLUMNAR_TABLE_MAPPING_TABLE, e.getMessage());
        }
    }

    public List<ColumnarTableMappingRecord> querySchema(String schemaName) {
        return query(SELECT_SCHEMA, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class, schemaName);
    }

    public List<ColumnarTableMappingRecord> querySchemaByVersionId(String schemaName, Long versionId) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, versionId);
        return query(SELECT_SCHEMA_AND_VERSION_ID, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class,
            params);
    }

    public List<ColumnarTableMappingRecord> querySchemaTable(String schemaName, String tableName) {
        return query(SELECT_SCHEMA_TABLE, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class,
            schemaName, tableName);
    }

    public List<ColumnarTableMappingRecord> querySchemaTableByVersionId(String schemaName, String tableName,
                                                                        Long versionId) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, versionId);
        return query(SELECT_SCHEMA_TABLE_AND_VERSION_ID, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class,
            params);
    }

    public List<ColumnarTableMappingRecord> querySchemaIndex(String schemaName, String indexName) {
        return query(SELECT_SCHEMA_INDEX, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class, schemaName,
            indexName);
    }

    public List<ColumnarTableMappingRecord> queryTableId(long tableId) {
        return query(SELECT_TABLE_ID, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class, tableId);
    }

    public List<ColumnarTableMappingRecord> queryAll() {
        return query(SELECT_ALL, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class,
            Collections.emptyMap());
    }

    public List<ColumnarTableMappingRecord> queryByStatus(String status) {
        return query(SELECT_TABLE_BY_STATUS, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class, status);
    }

    public List<ColumnarTableMappingRecord> queryBySchemaAndStatus(String schemaName, String status) {
        return query(SELECT_TABLE_BY_SCHEMA_AND_STATUS, COLUMNAR_TABLE_MAPPING_TABLE, ColumnarTableMappingRecord.class,
            schemaName, status);
    }

    public void deleteTableSchemaIndex(String schemaName, String tableName, String indexName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, indexName);
        delete(DELETE_SCHEMA_TABLE_INDEX, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public void deleteSchema(String schemaName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        try {
            DdlMetaLogUtil.logSql(DELETE_SCHEMA, params);
            MetaDbUtil.delete(DELETE_SCHEMA, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void deleteId(long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, tableId);
        delete(DELETE_TABLE_ID, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public void updateVersionId(long versionId, long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, versionId);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableId);
        update(UPDATE_LATEST_VERSION_ID_BY_TABLE_ID, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public void updateTableNameId(String tableName, long versionId, long tableId) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, versionId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tableId);
        update(UPDATE_TABLE_NAME_LATEST_VERSION_ID_BY_TABLE_ID, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public int updateStatusAndVersionIdByTableId(long tableId,
                                                 long newVersionId,
                                                 String status) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, newVersionId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tableId);

        return update(UPDATE_TABLE_STATUS_AND_VERSION_ID_BY_TABLE_ID, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public void updateStatusByTableIdAndStatus(long tableId, String oldStatus, String newStatus) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newStatus);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, oldStatus);

        update(UPDATE_TABLE_STATUS_BY_TABLE_ID_AND_STATUS, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public void updateStatusBySchema(String schemaName, String status) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);

        update(UPDATE_TABLE_STATUS_BY_SCHEMA, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public void updateStatusByName(String schemaName, String tableName, String indexName, String status,
                                   Long versionId) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, versionId);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(5, params, ParameterMethod.setString, indexName);

        update(UPDATE_TABLE_STATUS_AND_VERSION_ID_BY_NAME, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public void updateStatusAndExtraByTableId(long tableId, String status, String extra) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, status);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, extra);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, tableId);

        update(UPDATE_TABLE_STATUS_AND_EXTRA_BY_TABLE_ID, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

    public void UpdateExtraByTableId(long tableId, String extra) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, extra);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableId);

        update(UPDATE_EXTRA_BY_TABLE_ID, COLUMNAR_TABLE_MAPPING_TABLE, params);
    }

}

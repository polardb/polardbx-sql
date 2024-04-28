package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnarTableEvolutionAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String COLUMNAR_TABLE_MAPPING_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_MAPPING);

    private static final String COLUMNAR_TABLE_EVOLUTION_TABLE = wrap(GmsSystemTables.COLUMNAR_TABLE_EVOLUTION);

    private static final String INSERT_COLUMNAR_TABLE_EVOLUTION_RECORDS =
        "insert into " + COLUMNAR_TABLE_EVOLUTION_TABLE +
            "(`version_id`, `table_id`, `table_schema`, `table_name`, `index_name`, `ddl_job_id`, `ddl_type`, `commit_ts`, `columns`, `options`) values (?, ?, ?, ?, ?, ?, ? ,? ,?, ?)";

    private static final String SELECT_ALL_COLUMNS =
        "select `version_id`, `table_id`, `table_schema`, `table_name`, `index_name`, `ddl_job_id`, `ddl_type`, `commit_ts`, `columns`, `options`";

    private static final String FROM_TABLE = " from " + COLUMNAR_TABLE_EVOLUTION_TABLE;

    private static final String WHERE_BY_TABLE_ID = " where `table_id` = ? ";

    private static final String WHERE_BY_TABLE_ID_ORDER_BY_COMMIT_TS = " where `table_id` = ? order by `commit_ts`";

    // For the case that commit_ts = Long.MAX_VALUE, the order should be determined by gmt_created
    private static final String WHERE_BY_TABLE_ID_AND_GT_COMMIT_TS_ORDER_BY_COMMIT_TS =
        " where `table_id` = ? and `commit_ts` > ? order by `commit_ts`, `gmt_created`";
    private static final String WHERE_BY_TABLE_ID_VERSION_ID =
        " where `table_id` = ? and `version_id` = ?";

    private static final String WHERE_BY_TABLE_ID_LATEST =
        " where `table_id` = ? order by `version_id` desc limit 1";

    private static final String WHERE_BY_SCHEMA_INDEX_LATEST =
        " where `table_schema` = ? and `index_name` = ? order by `version_id` desc limit 1";

    private static final String WHERE_BY_DDL_JOB_ID_LATEST =
        " where `ddl_job_id` = ? order by `version_id` desc limit 1";

    private static final String WHERE_BY_DDL_JOB_ID =
        " where `ddl_job_id` = ? order by `version_id` desc";

    private static final String ORDER_BY_VERSION_ID_LATEST = " order by `version_id` desc limit 1";

    private static final String WHERE_BY_VERSION_ID = " where `version_id` = ?";

    private static final String WHERE_BY_COMMIT_TS = " where `commit_ts` = ?";

    private static final String WHERE_BY_SCHEMA_TABLE_INDEX =
        " where `table_id` in (select `table_id` from " + COLUMNAR_TABLE_MAPPING_TABLE
            + " where `table_schema` = ? and `table_name` = ? and `index_name` = ?) order by `commit_ts`";

    private static final String WHERE_BY_SCHEMA =
        " where `table_id` in (select `table_id` from " + COLUMNAR_TABLE_MAPPING_TABLE
            + " where `table_schema`=?) order by `commit_ts`";

    private static final String SELECT_BY_SCHEMA = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_SCHEMA;

    private static final String DELETE_SCHEMA_TABLE_INDEX = "delete " + FROM_TABLE + WHERE_BY_SCHEMA_TABLE_INDEX;

    private static final String DELETE_SCHEMA = "delete " + FROM_TABLE + WHERE_BY_SCHEMA;

    private static final String UPDATE_COMMIT_TS_VERSION_ID =
        "update " + COLUMNAR_TABLE_EVOLUTION_TABLE + " set commit_ts=? " + WHERE_BY_VERSION_ID;

    private static final String UPDATE_DDL_TYPE_BY_VERSION_ID =
        "update " + COLUMNAR_TABLE_EVOLUTION_TABLE + " set ddl_type=? " + WHERE_BY_VERSION_ID;

    private static final String WITH_READ_LOCK = " LOCK IN SHARE MODE";

    private static final String SELECT_TABLE_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_ORDER_BY_COMMIT_TS;

    private static final String SELECT_TABLE_ID_AND_GT_COMMIT_TS =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_AND_GT_COMMIT_TS_ORDER_BY_COMMIT_TS;

    private static final String SELECT_BY_VERSION_ID = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_VERSION_ID;
    private static final String SELECT_BY_VERSION_ID_WITH_READ_LOCK =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_VERSION_ID + WITH_READ_LOCK;
    private static final String SELECT_BY_TABLE_ID_VERSION_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_VERSION_ID;

    private static final String SELECT_COMMIT_TS = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_COMMIT_TS;

    private static final String SELECT_TABLE_ID_LATEST = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_TABLE_ID_LATEST;
    private static final String SELECT_SCHEMA_INDEX_LATEST =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_SCHEMA_INDEX_LATEST;
    private static final String SELECT_DDL_JOB_ID_LATEST = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_DDL_JOB_ID_LATEST;
    private static final String SELECT_DDL_JOB_ID = SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_DDL_JOB_ID;

    private static final String SELECT_LATEST_BY_VERSION_ID =
        SELECT_ALL_COLUMNS + FROM_TABLE + WHERE_BY_VERSION_ID;

    private static final String DELETE_TABLE_ID = "delete " + FROM_TABLE + WHERE_BY_TABLE_ID;

    public int[] insert(List<ColumnarTableEvolutionRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ColumnarTableEvolutionRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_COLUMNAR_TABLE_EVOLUTION_RECORDS, paramsBatch);
            return MetaDbUtil.insert(INSERT_COLUMNAR_TABLE_EVOLUTION_RECORDS, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + COLUMNAR_TABLE_EVOLUTION_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                COLUMNAR_TABLE_EVOLUTION_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarTableEvolutionRecord> queryTableId(Long tableId) {
        return query(SELECT_TABLE_ID, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class, tableId);
    }

    public List<ColumnarTableEvolutionRecord> queryTableIdAndGreaterThanTso(Long tableId, Long commitTs) {
        return query(SELECT_TABLE_ID_AND_GT_COMMIT_TS, COLUMNAR_TABLE_EVOLUTION_TABLE,
            ColumnarTableEvolutionRecord.class, tableId, commitTs);
    }

    public List<ColumnarTableEvolutionRecord> queryTableIdVersionId(Long tableId, Long versionId) {
        return query(SELECT_BY_TABLE_ID_VERSION_ID,
            COLUMNAR_TABLE_EVOLUTION_TABLE,
            ColumnarTableEvolutionRecord.class,
            tableId,
            versionId);
    }

    public List<ColumnarTableEvolutionRecord> queryTableIdLatest(Long tableId) {
        return query(SELECT_TABLE_ID_LATEST, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class,
            tableId);
    }

    public List<ColumnarTableEvolutionRecord> querySchemaIndexLatest(String schemaName, String indexName) {
        return query(SELECT_SCHEMA_INDEX_LATEST, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class,
            schemaName, indexName);
    }

    public List<ColumnarTableEvolutionRecord> queryDdlJobIdLatest(Long ddlJobId) {
        return query(SELECT_DDL_JOB_ID_LATEST, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class,
            ddlJobId);
    }

    public List<ColumnarTableEvolutionRecord> queryDdlJobId(Long ddlJobId) {
        return query(SELECT_DDL_JOB_ID, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class,
            ddlJobId);
    }

    public List<ColumnarTableEvolutionRecord> queryByVersionIdLatest(Long versionId) {
        return query(SELECT_LATEST_BY_VERSION_ID, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class,
            versionId);
    }

    public List<ColumnarTableEvolutionRecord> queryCommitTs(long commitTs) {
        return query(SELECT_COMMIT_TS, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class,
            commitTs);
    }

    public void deleteSchemaTableIndex(String schemaName, String tableName, String indexName) {
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, indexName);
        delete(DELETE_SCHEMA_TABLE_INDEX, COLUMNAR_TABLE_EVOLUTION_TABLE, params);
    }

    public void updateCommitTs(long commitTs, long versionId) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, commitTs);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, versionId);
        update(UPDATE_COMMIT_TS_VERSION_ID, COLUMNAR_TABLE_EVOLUTION_TABLE, params);
    }

    public void updateDdlType(String ddlType, long ddlId) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, ddlType);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, ddlId);
        update(UPDATE_DDL_TYPE_BY_VERSION_ID, COLUMNAR_TABLE_EVOLUTION_TABLE, params);
    }

    public List<ColumnarTableEvolutionRecord> querySchema(String schemaName) {
        return query(SELECT_BY_SCHEMA, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class, schemaName);
    }

    public List<ColumnarTableEvolutionRecord> queryVersionId(long versionId) {
        return query(SELECT_BY_VERSION_ID, COLUMNAR_TABLE_EVOLUTION_TABLE, ColumnarTableEvolutionRecord.class,
            versionId);
    }

    public List<ColumnarTableEvolutionRecord> queryVersionIdWithReadLock(long versionId) {
        return query(SELECT_BY_VERSION_ID_WITH_READ_LOCK, COLUMNAR_TABLE_EVOLUTION_TABLE,
            ColumnarTableEvolutionRecord.class, versionId);
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
        delete(DELETE_TABLE_ID, COLUMNAR_TABLE_EVOLUTION_TABLE, params);
    }
}

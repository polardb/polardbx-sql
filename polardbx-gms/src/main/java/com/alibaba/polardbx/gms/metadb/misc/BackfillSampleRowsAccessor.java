package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.Map;


public class BackfillSampleRowsAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackfillSampleRowsAccessor.class);

    public static final String BACKFILL_SAMPLE_ROWS_TABLE = wrap(GmsSystemTables.BACKFILL_SAMPLE_ROWS);

    public static final String BACKFILL_SAMPLE_ROWS_ARCHIVE_TABLE = wrap(GmsSystemTables.BACKFILL_SAMPLE_ROWS_ARCHIVE);

    private static final String FROM_TABLE = " from " + BACKFILL_SAMPLE_ROWS_TABLE;

    private static final String WHERE_JOB_ID = " where `job_id` = ?";

    private static final String WHERE_SCHEMA_NAME = " where `schema_name` = ?";

    private static final String ARCHIVE_BASE =
        "insert into " + BACKFILL_SAMPLE_ROWS_ARCHIVE_TABLE + " select * from " + BACKFILL_SAMPLE_ROWS_TABLE;

    private static final String ARCHIVE_SPECIFIC = ARCHIVE_BASE + WHERE_JOB_ID;

    private static final String DELETE_BASE = "delete" + FROM_TABLE;

    private static final String DELETE_ARCHIVE_BASE = "delete from " + BACKFILL_SAMPLE_ROWS_ARCHIVE_TABLE;

    private static final String DELETE_BY_JOB_ID = DELETE_BASE + WHERE_JOB_ID;

    private static final String DELETE_ARCHIVE_BY_JOB_ID = DELETE_ARCHIVE_BASE + WHERE_JOB_ID;

    private static final String DELETE_ARCHIVE_BY_SCHEMA_NAME = DELETE_ARCHIVE_BASE + WHERE_SCHEMA_NAME;

    private static final String DELETE_BY_SCHEMA_NAME = DELETE_BASE + WHERE_SCHEMA_NAME;

    public int deleteByJobId(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
            deleteArchiveByJobId(jobId);
            archive(jobId);
            return MetaDbUtil.delete(DELETE_BY_JOB_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + BACKFILL_SAMPLE_ROWS_TABLE + " for job " + jobId,
                "delete from", e);
        }
    }

    public int deleteArchiveByJobId(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
            return MetaDbUtil.delete(DELETE_ARCHIVE_BY_JOB_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + BACKFILL_SAMPLE_ROWS_TABLE + " for job " + jobId,
                "delete from", e);
        }
    }

    public int archive(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
            DdlMetaLogUtil.logSql(ARCHIVE_SPECIFIC, params);
            return MetaDbUtil.delete(ARCHIVE_SPECIFIC, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to copy record from " + BACKFILL_SAMPLE_ROWS_TABLE + " for job " + jobId,
                "archive",
                e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            BACKFILL_SAMPLE_ROWS_TABLE, e.getMessage());
    }

    public int deleteAll(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            MetaDbUtil.delete(DELETE_ARCHIVE_BY_SCHEMA_NAME, params, connection);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + BACKFILL_SAMPLE_ROWS_TABLE + " for schemaName: " + schemaName,
                "delete from",
                e);
        }
    }

}

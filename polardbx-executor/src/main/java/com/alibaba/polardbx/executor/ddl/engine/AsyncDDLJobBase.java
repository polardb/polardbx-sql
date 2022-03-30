package com.alibaba.polardbx.executor.ddl.engine;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.Job.JobPhase;
import com.alibaba.polardbx.common.ddl.Job.JobState;
import com.alibaba.polardbx.common.ddl.Job.JobType;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.optimizer.context.AsyncDDLContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class AsyncDDLJobBase extends AsyncDDLCommon {

    public static final String JOB_SCHEDULER_PREFIX = "DDL-Job-Scheduler";

    public static final String DDL_JOBS_TABLE = GmsSystemTables.DDL_JOBS;

    protected static final boolean IS_POLARDB_X = true;

    static final String CREATE_DDL_JOBS_TABLE = "CREATE TABLE IF NOT EXISTS `" + DDL_JOBS_TABLE + "` ("
        + "  `ID` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
        + "  `JOB_ID` BIGINT UNSIGNED NOT NULL,"
        + "  `PARENT_JOB_ID` BIGINT UNSIGNED NOT NULL DEFAULT 0,"
        + "  `JOB_NO` SMALLINT UNSIGNED NOT NULL DEFAULT 0,"
        + "  `SERVER` VARCHAR(64) NOT NULL,"
        + "  `OBJECT_SCHEMA` VARCHAR(256) NOT NULL,"
        + "  `OBJECT_NAME` VARCHAR(256) NOT NULL,"
        + "  `NEW_OBJECT_NAME` VARCHAR(256) NOT NULL,"
        + "  `JOB_TYPE` VARCHAR(64) NOT NULL,"
        + "  `PHASE` VARCHAR(64) NOT NULL,"
        + "  `STATE` VARCHAR(64) NOT NULL,"
        + "  `PHYSICAL_OBJECT_DONE` LONGTEXT DEFAULT NULL,"
        + "  `PROGRESS` SMALLINT DEFAULT 0,"
        + "  `DDL_STMT` LONGTEXT DEFAULT NULL,"
        + "  `OLD_RULE_TEXT` LONGTEXT DEFAULT NULL,"
        + "  `NEW_RULE_TEXT` LONGTEXT DEFAULT NULL,"
        + "  `GMT_CREATED` BIGINT UNSIGNED NOT NULL,"
        + "  `GMT_MODIFIED` BIGINT UNSIGNED NOT NULL,"
        + "  `REMARK` LONGTEXT DEFAULT NULL,"
        + "  `RESERVED_GSI_INT` INT DEFAULT NULL,"
        + "  `RESERVED_GSI_TXT` LONGTEXT DEFAULT NULL,"
        + "  `RESERVED_DDL_INT` INT DEFAULT NULL,"
        + "  `RESERVED_DDL_TXT` LONGTEXT DEFAULT NULL,"
        + "  `RESERVED_CMN_INT` INT DEFAULT NULL,"
        + "  `RESERVED_CMN_TXT` LONGTEXT DEFAULT NULL,"
        + "  PRIMARY KEY (`ID`),"
        + "  UNIQUE KEY `UNI_JOB_ID`(`JOB_ID`)"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    private static final int PERCENTAGE_DONE = 100;

    static final String JOB_INSERT = "INSERT INTO "
        + DDL_JOBS_TABLE
        + "(JOB_ID, PARENT_JOB_ID, JOB_NO, SERVER, OBJECT_SCHEMA, OBJECT_NAME, "
        + "NEW_OBJECT_NAME, JOB_TYPE, PHASE, STATE, REMARK, DDL_STMT, "
        + "GMT_CREATED, GMT_MODIFIED, RESERVED_DDL_INT" + (IS_POLARDB_X ? ", SCHEMA_NAME" : "") + ")"
        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" + (IS_POLARDB_X ? ", ?" : "") + ")";

    private static final String JOB_SELECT =
        "SELECT JOB_ID, PARENT_JOB_ID, JOB_NO, SERVER, OBJECT_SCHEMA, OBJECT_NAME, "
            + "NEW_OBJECT_NAME, JOB_TYPE, PHASE, STATE, PHYSICAL_OBJECT_DONE, PROGRESS, "
            + "DDL_STMT, GMT_CREATED, GMT_MODIFIED, REMARK, "
            + "RESERVED_GSI_INT, RESERVED_DDL_INT FROM "
            + DDL_JOBS_TABLE;

    private static final String GSI_JOB_SELECT = "SELECT RESERVED_GSI_TXT, RESERVED_GSI_INT FROM " + DDL_JOBS_TABLE;

    private static final String DDL_JOB_SELECT = "SELECT RESERVED_DDL_TXT, RESERVED_DDL_INT FROM "
        + DDL_JOBS_TABLE;

    private static final String JOB_UPDATE = "UPDATE " + DDL_JOBS_TABLE + " SET GMT_MODIFIED = ?, ";

    private static final String JOB_DELETE = "DELETE FROM " + DDL_JOBS_TABLE;

    private static final String WHERE_JOB_SPECIFIC = " WHERE JOB_ID = ? ";

    private static final String SCHEMA_NAME_FILTER = (IS_POLARDB_X ? "SCHEMA_NAME = ? AND " : "");

    private static final String ORDER_BY_ID = " ORDER BY ID ";

    private static final String ORDER_BY_ID_DESC = ORDER_BY_ID + "DESC";

    static final String JOB_COUNT_ALL = "SELECT COUNT(1) FROM " + DDL_JOBS_TABLE;

    static final String JOB_FETCH_ALL_IN_SCHEMA = JOB_SELECT
        + (IS_POLARDB_X ? " WHERE SCHEMA_NAME = ?" : "")
        + ORDER_BY_ID_DESC;

    static final String JOB_FETCH_ALL_DB_JOBS = JOB_SELECT + ORDER_BY_ID_DESC;

    static final String JOB_FETCH_SHOW_IN_SCHEMA = JOB_SELECT + " WHERE "
        + SCHEMA_NAME_FILTER +
        "(JOB_ID IN (%s) OR PARENT_JOB_ID IN (%s))"
        + ORDER_BY_ID_DESC;

    static final String JOB_FETCH_SHOW = JOB_SELECT + " WHERE "
        + "(JOB_ID IN (%s) OR PARENT_JOB_ID IN (%s))"
        + ORDER_BY_ID_DESC;

    static final String JOB_FETCH_SPECIFIC = JOB_SELECT
        + WHERE_JOB_SPECIFIC;

    static final String JOB_FETCH_ACTIVE = JOB_SELECT + " WHERE "
        + SCHEMA_NAME_FILTER
        + "STATE NOT IN (?, ?, ?) AND PARENT_JOB_ID IN (?, ?)"
        + ORDER_BY_ID + " LIMIT ?";

    static final String JOB_FETCH_BATCH = JOB_SELECT
        + " WHERE JOB_ID IN (%s)"
        + ORDER_BY_ID;

    static final String JOB_FETCH_STATED = JOB_SELECT + " WHERE "
        + SCHEMA_NAME_FILTER
        + "STATE = ? AND PARENT_JOB_ID IN (?, ?)"
        + ORDER_BY_ID;

    static final String JOB_FETCH_INCOMPLETE = JOB_SELECT + " WHERE "
        + SCHEMA_NAME_FILTER
        + "STATE NOT IN (?, ?) AND PARENT_JOB_ID IN (?, ?)"
        + ORDER_BY_ID;

    static final String JOB_FETCH_SUB_JOBS = JOB_SELECT + " WHERE "
        + SCHEMA_NAME_FILTER
        + "PARENT_JOB_ID = ?"
        + ORDER_BY_ID;

    private static final String JOB_STATE_UPDATE = JOB_UPDATE + "STATE = ?, REMARK = ?";

    private static final String JOB_STATE_PHASE_UPDATE = JOB_STATE_UPDATE + ", PHASE = ?";

    static final String JOB_STATE_CHANGE = JOB_STATE_PHASE_UPDATE
        + WHERE_JOB_SPECIFIC;

    static final String JOB_STATE_DONE = JOB_STATE_PHASE_UPDATE
        + ", PROGRESS = " + PERCENTAGE_DONE
        + WHERE_JOB_SPECIFIC;

    static final String JOB_STATE_ONLY = JOB_STATE_UPDATE
        + WHERE_JOB_SPECIFIC;

    private static final String JOB_RECORD_DONE = JOB_UPDATE
        + "PHYSICAL_OBJECT_DONE = CONCAT_WS('" + SEPARATOR_COMMON + "', PHYSICAL_OBJECT_DONE, ?)";

    static final String JOB_RECORD_DONE_ONLY = JOB_RECORD_DONE
        + WHERE_JOB_SPECIFIC;

    static final String JOB_RECORD_DONE_FULL = JOB_RECORD_DONE
        + ", PROGRESS = ?"
        + WHERE_JOB_SPECIFIC;

    static final String JOB_RESET_DONE = JOB_UPDATE
        + "PHYSICAL_OBJECT_DONE = ?, PROGRESS = ?"
        + WHERE_JOB_SPECIFIC;

    static final String JOB_ROLLBACK = JOB_STATE_CHANGE;

    static final String JOB_FLAG_UPDATE = JOB_UPDATE
        + "RESERVED_DDL_INT = RESERVED_DDL_INT | ?"
        + WHERE_JOB_SPECIFIC;

    static final String JOB_SERVER_UPDATE = JOB_UPDATE
        + "SERVER = ?"
        + WHERE_JOB_SPECIFIC;

    static final String JOB_FLAG_SERVER_UPDATE = JOB_UPDATE
        + "SERVER = ?, RESERVED_DDL_INT = RESERVED_DDL_INT | ?"
        + WHERE_JOB_SPECIFIC;

    static final String JOB_REMOVE_DONE = JOB_DELETE + " WHERE "
        + SCHEMA_NAME_FILTER
        + "(JOB_ID = ? OR PARENT_JOB_ID = ?) AND PHASE = ? AND STATE in (?, ?)";

    static final String SCALEOUT_JOB_REMOVE = JOB_DELETE + " WHERE "
        + SCHEMA_NAME_FILTER
        + "(JOB_ID = ? OR PARENT_JOB_ID = ?)";

    static final String JOB_REMOVE_FORCE = JOB_DELETE + " WHERE (JOB_ID = ? OR PARENT_JOB_ID = ?)";

    static final String JOB_REMOVE_BATCH = JOB_DELETE
        + " WHERE JOB_ID IN (%s) AND STATE IN (?, ?)";

    static final String JOB_REMARK_UPDATE = JOB_UPDATE
        + "REMARK = ?"
        + WHERE_JOB_SPECIFIC;

    static final String GSI_JOB_STATE_UPDATE = JOB_UPDATE
        + "RESERVED_GSI_TXT = ?"
        + WHERE_JOB_SPECIFIC;

    static final String GSI_JOB_PROGRESS_UPDATE = JOB_UPDATE
        + "RESERVED_GSI_INT = ?"
        + WHERE_JOB_SPECIFIC;

    static final String GSI_JOB_FETCH_SPECIFIC = GSI_JOB_SELECT
        + WHERE_JOB_SPECIFIC;

    static final String DDL_JOB_STATE_UPDATE = JOB_UPDATE + "RESERVED_DDL_TXT = ?" + WHERE_JOB_SPECIFIC;

    static final String DDL_JOB_PROGRESS_UPDATE = JOB_UPDATE + "RESERVED_DDL_INT = ?"
        + WHERE_JOB_SPECIFIC;

    static final String SCALEOUT_JOB_PROGRESS_UPDATE = JOB_UPDATE + "PROGRESS = ?"
        + WHERE_JOB_SPECIFIC;

    static final String DDL_JOB_FETCH_SPECIFIC = DDL_JOB_SELECT + WHERE_JOB_SPECIFIC;

    static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    String getCommonInfo(AsyncDDLContext asyncDDLContext) {
        Job job = asyncDDLContext.getJob();
        return " for job '" + job.getId() + "' with type '" + job.getType() + "' and object '" + job.getObjectSchema()
            + "." + job.getObjectName() + "' in " + asyncDDLContext.getSchemaName();
    }

    Job fillInJob(ResultSet rs) throws SQLException {
        return fillInJob(rs, false);
    }

    Job fillInJob(ResultSet rs, boolean withDate) throws SQLException {
        Job job = new Job();

        job.setId(rs.getLong("JOB_ID"));
        job.setParentId(rs.getLong("PARENT_JOB_ID"));
        job.setSeq(rs.getInt("JOB_NO"));

        job.setServer(rs.getString("SERVER"));

        job.setObjectSchema(rs.getString("OBJECT_SCHEMA"));
        job.setObjectName(rs.getString("OBJECT_NAME"));
        job.setNewObjectName(rs.getString("NEW_OBJECT_NAME"));
        job.setType(JobType.valueOf(rs.getString("JOB_TYPE")));

        job.setPhase(JobPhase.valueOf(rs.getString("PHASE")));
        job.setState(JobState.valueOf(rs.getString("STATE")));

        job.setPhysicalObjectDone(rs.getString("PHYSICAL_OBJECT_DONE"));

        job.setProgress(rs.getInt("PROGRESS"));

        job.setDdlStmt(rs.getString("DDL_STMT"));

        if (withDate) {
            long gmtCreated = rs.getLong("GMT_CREATED");
            long gmtModified = rs.getLong("GMT_MODIFIED");
            long gmtCurrent = System.currentTimeMillis();

            job.setGmtCreated(DATE_FORMATTER.format(new Date(gmtCreated)));
            job.setGmtModified(DATE_FORMATTER.format(new Date(gmtModified)));

            switch (job.getState()) {
            case PENDING:
            case STAGED:
            case COMPLETED:
                job.setElapsedTime(gmtModified - gmtCreated);
                break;
            default:
                job.setElapsedTime(gmtCurrent - gmtCreated);
                break;
            }
        }

        job.setRemark(rs.getString("REMARK"));
        job.setBackfillProgress(rs.getString("RESERVED_GSI_INT"));
        job.setFlag(rs.getInt("RESERVED_DDL_INT"));

        return job;
    }

}

package com.alibaba.polardbx.qatest.ddl.auto.mpp.base;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableGsiPkRangeBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ImportTableSpaceDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.SperateCheckGsiTask;
import com.alibaba.polardbx.qatest.ddl.auto.mpp.pkrange.PkTest;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;

@Data
public class PkRangeTestParam {
    final static Log log = LogFactory.getLog(PkTest.class);
    public static final String inspectDdlEngineStatusSql =
        "select task_name, task_state, execution_time, node_ip from information_schema.ddl_scheduler where task_name != \"SubJobTask\" order by task_state;";

    public static final String inspectFullDdlEngineStatusSql =
        "show ddl engine status;";

    public PkRangeTestParam(String schemaName, String originalTableName, String gsiTableName,
                            String createTableStmt,
                            String addGsiStmt, int partNum, int gsiPartNum, int eachPartRows, Boolean enablePkRange,
                            Boolean enableLocalIndexLater) {
        this.schemaName = schemaName;
        this.originalTableName = originalTableName;
        this.gsiTableName = gsiTableName;
        this.createTableStmt = createTableStmt;
        this.addGsiStmt = addGsiStmt;
        this.partNum = partNum;
        this.eachPartRows = eachPartRows;
        this.enablePkRange = enablePkRange;
        this.gsiPartNum = gsiPartNum;
        this.enableLocalIndexLater = enableLocalIndexLater;
        this.expectedPkRangeNum = enablePkRange ? 1 : 0;
        this.pkRangeSize = "2kB";
        this.taskRangeSize = "16kB";
        this.expectedLocalIndexConcurrency = -1;
        this.expectedPkRange = enablePkRange;
        this.expectedLocalIndexLater = enableLocalIndexLater;
        this.mppTaskAllocationCheck = false;
    }

    public String schemaName;
    public String originalTableName;
    public String gsiTableName;
    public String createTableStmt;
    public String addGsiStmt;
    public int partNum;
    public int gsiPartNum;
    public int eachPartRows;
    public Boolean enablePkRange;
    public Boolean enableLocalIndexLater;
    public Boolean expectedPkRange;
    public Boolean expectedLocalIndexLater;
    public long expectedPkRangeNum;
    public String pkRangeSize;
    public String taskRangeSize;
    public int expectedLocalIndexConcurrency;
    // for pause and continue test
    public long sleepInterval;
    public long checkInterval;
    public Boolean mppTaskAllocationCheck;

    public static void baseTest(PkRangeTestParam pkRangeTestParam, Connection connection)
        throws Exception {
        String schemaName = pkRangeTestParam.schemaName;
        String originalTableName = pkRangeTestParam.originalTableName;
        String gsiName = pkRangeTestParam.gsiTableName;
        String createTableStmt = pkRangeTestParam.createTableStmt;
        int partNum = pkRangeTestParam.partNum;
        int gsiPartNum = pkRangeTestParam.gsiPartNum;
        int eachPartRows = pkRangeTestParam.eachPartRows;
        Boolean enablePkRange = pkRangeTestParam.enablePkRange;
        Boolean buildLocalIndexLater = pkRangeTestParam.enableLocalIndexLater;
        String addGsiStmt = pkRangeTestParam.addGsiStmt;
        String taskRangeSize = pkRangeTestParam.taskRangeSize;
        String pkRangeSize = pkRangeTestParam.pkRangeSize;
        long expectedPkRangeNum = pkRangeTestParam.expectedPkRangeNum;
        int expectedLocalIndexConcurrency = pkRangeTestParam.expectedLocalIndexConcurrency;
        Boolean expectedBuildLocalIndexLater = pkRangeTestParam.expectedLocalIndexLater;
        Boolean expectedPkRange = pkRangeTestParam.expectedPkRange;
        Boolean mppTaskAllocationCheck = pkRangeTestParam.mppTaskAllocationCheck;
        String addGsiDdl = String.format(addGsiStmt, originalTableName, gsiName, gsiPartNum);
        String addGsiHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(PURE_ASYNC_DDL_MODE=true,GSI_BACKFILL_BY_PK_RANGE=%s,GSI_BUILD_LOCAL_INDEX_LATER=%s,BACKFILL_MAX_TASK_PK_RANGE_SIZE=%s,BACKFILL_MAX_PK_RANGE_SIZE=%s,"
                    + "GSI_JOB_MAX_PARALLELISM=16,GSI_PK_RANGE_CPU_ACQUIRE=6,FORBID_REMOTE_DDL_TASK=%s)*/",
                enablePkRange, buildLocalIndexLater, taskRangeSize, pkRangeSize, !mppTaskAllocationCheck);

        final Logger logger = LoggerFactory.getLogger(DdlStateCheckUtil.class);
        if (!StringUtils.isEmpty(createTableStmt)) {
            prepareData(connection, schemaName, originalTableName, eachPartRows, createTableStmt,
                partNum, DataManipulateUtil.TABLE_TYPE.PARTITION_TABLE);
            String analyzeTableSql = String.format("analyze table %s", originalTableName);
            DdlStateCheckUtil.alterTableViaJdbc(connection, schemaName, originalTableName, analyzeTableSql);
        }
        DdlStateCheckUtil.alterTableViaJdbc(connection, schemaName, originalTableName, addGsiHint + addGsiDdl);
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(connection, addGsiDdl);
        String msg =
            String.format("jobId: %d, table: %s, addGsiDdl: %s", jobId, originalTableName, addGsiHint + addGsiDdl);
        logger.info(String.format("wait for ddl %d...", jobId));
        if (!DdlStateCheckUtil.waitTillDdlDone(connection, jobId, originalTableName, expectedLocalIndexConcurrency,
            mppTaskAllocationCheck)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait addGsiDdl done timeout for %s", msg));
        }
        logger.info(String.format("ddl %d finished...", jobId));
        if (!DdlStateCheckUtil.checkIfCompleteSuccessful(connection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job failed for some unknown reason!!! %s", msg));
        }
        List<String> errMsg = new ArrayList<>();
        if (!DdlStateCheckUtil.checkIfExecuteByPkRangeAndBuildLocalIndexLater(connection, jobId, expectedPkRange,
            expectedBuildLocalIndexLater, expectedPkRangeNum, errMsg)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format(
                    "this job success, but we don't expect that pkRange and localIndexLater behave in this way !!! %s, %s",
                    msg, errMsg.get(0)));
        }
        if (!DdlStateCheckUtil.checkIfBackfillSampleRowsArchived(connection, jobId, !expectedPkRange)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format(
                    "this job success, but we don't expect the sample rows archived failed! for %d", jobId));
        }
        if (!StringUtils.isEmpty(createTableStmt) && !DdlStateCheckUtil.compareForLocalIndex(connection, schemaName,
            jobId, originalTableName, createTableStmt,
            addGsiStmt, gsiName, partNum, errMsg)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job success but local key is not right!! %s, %s", msg, errMsg.get(0)));
        }
    }

    public static void showDdlTest(PkRangeTestParam pkRangeTestParam, Connection connection, Boolean withSubjob)
        throws Exception {
        showDdlTest(pkRangeTestParam, connection, withSubjob, false, false);
    }

    public static void showDdlTest(PkRangeTestParam pkRangeTestParam, Connection connection, Boolean withSubjob,
                                   Boolean showDdlEngineTest, Boolean physicalBackfill)
        throws Exception {
        String schemaName = pkRangeTestParam.schemaName;
        String originalTableName = pkRangeTestParam.originalTableName;
        String gsiName = pkRangeTestParam.gsiTableName;
        String createTableStmt = pkRangeTestParam.createTableStmt;
        int partNum = pkRangeTestParam.partNum;
        int gsiPartNum = pkRangeTestParam.gsiPartNum;
        int eachPartRows = pkRangeTestParam.eachPartRows;
        Boolean enablePkRange = pkRangeTestParam.enablePkRange;
        Boolean buildLocalIndexLater = pkRangeTestParam.enableLocalIndexLater;
        String taskRangeSize = pkRangeTestParam.taskRangeSize;
        String pkRangeSize = pkRangeTestParam.pkRangeSize;
        String addGsiStmt = pkRangeTestParam.addGsiStmt;
        String addGsiDdl = String.format(addGsiStmt, originalTableName, gsiName, gsiPartNum);
        String addGsiHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(PURE_ASYNC_DDL_MODE=true,GSI_BACKFILL_BY_PK_RANGE=%s,GSI_BUILD_LOCAL_INDEX_LATER=%s,BACKFILL_MAX_TASK_PK_RANGE_SIZE=%s,PHYSICAL_BACKFILL_ENABLE=true,PHYSICAL_BACKFILL_MAX_SLAVE_LATENCY=4000000000,FORBID_REMOTE_DDL_TASK=%s)*/",
                enablePkRange, buildLocalIndexLater, taskRangeSize, !physicalBackfill);
        final Logger logger = LoggerFactory.getLogger(DdlStateCheckUtil.class);
        prepareData(connection, schemaName, originalTableName, eachPartRows, createTableStmt,
            partNum, DataManipulateUtil.TABLE_TYPE.PARTITION_TABLE);
        DdlStateCheckUtil.alterTableViaJdbc(connection, schemaName, originalTableName, addGsiHint + addGsiDdl);
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(connection, addGsiDdl);
        String msg =
            String.format("jobId: %d, table: %s, addGsiDdl: %s", jobId, originalTableName, addGsiHint + addGsiDdl);
        List<List<Object>> showDdlResult = null;
        List<List<Object>> showFullDdlResult = null;
        List<List<Object>> showDdlEngineStatusResult = null;
        int maxRecordNum = 0;
        int maxTaskRecordNum = 0;
        if (!showDdlEngineTest) {
            for (int i = 0; i < 3; i++) {
                showDdlResult = JdbcUtil.getAllResult(JdbcUtil.executeQuery("show ddl", connection));
                maxRecordNum = Math.max(maxRecordNum, showDdlResult.size());
                if(!physicalBackfill) {
                    if (showFullDdlResult != null) {
                        DdlStateCheckUtil.compareShowDdlResult(showFullDdlResult, showDdlResult);
                    }
                    showFullDdlResult =
                            JdbcUtil.getAllResult(JdbcUtil.executeQuery("show full ddl", connection));
                    DdlStateCheckUtil.compareShowDdlResult(showDdlResult, showFullDdlResult);
                }
                showDdlEngineStatusResult =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery(inspectDdlEngineStatusSql, connection));
                maxTaskRecordNum = Math.max(maxTaskRecordNum, showDdlEngineStatusResult.size());
                Thread.sleep(1000L);
            }
            logger.info(String.format("wait for ddl %d ...", jobId));
            if (!DdlStateCheckUtil.waitTillDdlDone(connection, jobId, originalTableName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("wait addGsiDdl done timeout for %s", msg));
            }
            logger.info(String.format("ddl %d finished...", jobId));
//            if (maxTaskRecordNum < 1) {
//                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                    String.format("expected record occur in show ddl engine status", msg));
//            }
        } else {
            int maxPkRangeBackfillTaskRecordNum = 0;
            int maxCheckerTaskRecordNum = 0;
            int maxPhysicalBackfillTaskRecordNum = 0;
            int maxImportTableSpaceTaskRecordNum = 0;
            while (!DdlStateCheckUtil.checkIfTerminate(connection, jobId)) {
                logger.info(String.format("show ddl check..."));
                showDdlResult = JdbcUtil.getAllResult(JdbcUtil.executeQuery("show ddl", connection));
                maxRecordNum = Math.max(maxRecordNum, showDdlResult.size());
                if(!physicalBackfill) {
                    if (showFullDdlResult != null) {
                        DdlStateCheckUtil.compareShowDdlResult(showFullDdlResult, showDdlResult);
                    }
                    showFullDdlResult =
                        JdbcUtil.getAllResult(JdbcUtil.executeQuery("show full ddl", connection));
                    DdlStateCheckUtil.compareShowDdlResult(showDdlResult, showFullDdlResult);
                }
                showDdlEngineStatusResult =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery(inspectFullDdlEngineStatusSql, connection));
                logger.info(String.format("get ddl engine status..."));
                maxTaskRecordNum = Math.max(maxTaskRecordNum, showDdlEngineStatusResult.size());
                maxPhysicalBackfillTaskRecordNum =
                    getMaxTaskRecordNum(showDdlEngineStatusResult, maxPhysicalBackfillTaskRecordNum,
                        PhysicalBackfillTask.class.getName());
                maxCheckerTaskRecordNum = getMaxTaskRecordNum(showDdlEngineStatusResult, maxCheckerTaskRecordNum,
                    SperateCheckGsiTask.class.getName());
                maxImportTableSpaceTaskRecordNum =
                    getMaxTaskRecordNum(showDdlEngineStatusResult, maxImportTableSpaceTaskRecordNum,
                        ImportTableSpaceDdlTask.class.getName());
                maxPkRangeBackfillTaskRecordNum =
                    getMaxTaskRecordNum(showDdlEngineStatusResult, maxPkRangeBackfillTaskRecordNum,
                        LogicalTableGsiPkRangeBackfillTask.class.getName());
                Thread.sleep(50L);
            }
            logger.info(String.format("ddl %d finished...", jobId));
            if (!DdlStateCheckUtil.waitTillDdlDone(connection, jobId, originalTableName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("wait addGsiDdl done timeout for %s", msg));
            }
            if (maxTaskRecordNum < 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("expected record occur in show ddl engine status", msg));
            }
            if (physicalBackfill) {
                if (maxImportTableSpaceTaskRecordNum < 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        String.format("expected import table space record occur in show ddl engine status", msg));
                }
                if (maxPhysicalBackfillTaskRecordNum < 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        String.format("expected physical backfill record occur in show ddl engine status, but now only %d", maxPhysicalBackfillTaskRecordNum));
                }
            } else {
                if (maxPkRangeBackfillTaskRecordNum < 2) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        String.format("expected pk range record occur in show ddl engine status, but now only %d", maxPkRangeBackfillTaskRecordNum));
                }
                if (maxCheckerTaskRecordNum < 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        String.format("expected checker record occur in show ddl engine status", msg));
                }
            }
        }
        logger.info(String.format("check %d archived...", jobId));
        if (!DdlStateCheckUtil.checkIfBackfillSampleRowsArchived(connection, jobId, physicalBackfill || withSubjob)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format(
                    "this job success, but we don't expect the sample rows archived failed! for %d", jobId));
        }
    }

    public static int getMaxTaskRecordNum(List<List<Object>> record, int n, String taskName) {
        // task_name was 4th column
//        result.addColumn("JOB_ID", DataTypes.LongType);
//        result.addColumn("TASK_ID", DataTypes.LongType);
//        result.addColumn("TASK_STATE", DataTypes.StringType);
//        result.addColumn("TASK_NAME", DataTypes.StringType);
//        result.addColumn("TASK_INFO", DataTypes.StringType);
//        result.addColumn("EXECUTION_TIME", DataTypes.StringType);
//        result.addColumn("NODE_IP", DataTypes.StringType);
//        result.addColumn("RESOURCES", DataTypes.StringType);
//        result.addColumn("EXTRA", DataTypes.StringType);
//        result.addColumn("DDL_STMT", DataTypes.StringType);
        long result = record.stream()
            .filter(o -> taskName.endsWith(o.get(3).toString()) && o.get(2).toString().equalsIgnoreCase("ACTIVE"))
            .count();
        return Math.max((int) result, n);
    }

    public static void controlTest(final Log logger, PkRangeTestParam pkRangeTestParam, Connection connection)
        throws Exception {
        String schemaName = pkRangeTestParam.schemaName;
        String originalTableName = pkRangeTestParam.originalTableName;
        String gsiName = pkRangeTestParam.gsiTableName;
        String createTableStmt = pkRangeTestParam.createTableStmt;
        int partNum = pkRangeTestParam.partNum;
        int gsiPartNum = pkRangeTestParam.gsiPartNum;
        int eachPartRows = pkRangeTestParam.eachPartRows;
        Boolean enablePkRange = pkRangeTestParam.enablePkRange;
        Boolean buildLocalIndexLater = pkRangeTestParam.enableLocalIndexLater;
        String addGsiStmt = pkRangeTestParam.addGsiStmt;
        String taskRangeSize = pkRangeTestParam.taskRangeSize;
        String pkRangeSize = pkRangeTestParam.pkRangeSize;
        long expectedPkRangeNum = pkRangeTestParam.expectedPkRangeNum;
        int expectedLocalIndexConcurrency = pkRangeTestParam.expectedLocalIndexConcurrency;
        Boolean expectedBuildLocalIndexLater = pkRangeTestParam.expectedLocalIndexLater;
        Boolean expectedPkRange = pkRangeTestParam.expectedPkRange;
        long sleepInterval = pkRangeTestParam.sleepInterval;
        long checkInterval = pkRangeTestParam.checkInterval;
        String addGsiDdl = String.format(addGsiStmt, originalTableName, gsiName, gsiPartNum);
        String addGsiHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(PURE_ASYNC_DDL_MODE=true,GSI_BACKFILL_BY_PK_RANGE=%s,GSI_BUILD_LOCAL_INDEX_LATER=%s,BACKFILL_MAX_TASK_PK_RANGE_SIZE=%s,BACKFILL_MAX_PK_RANGE_SIZE=%s,"
                    + "GSI_JOB_MAX_PARALLELISM=16,GSI_PK_RANGE_CPU_ACQUIRE=6)*/",
                enablePkRange, buildLocalIndexLater, taskRangeSize, pkRangeSize);
        if (!StringUtils.isEmpty(createTableStmt)) {
            prepareData(connection, schemaName, originalTableName, eachPartRows, createTableStmt,
                partNum, DataManipulateUtil.TABLE_TYPE.PARTITION_TABLE);
            String analyzeTableSql = String.format("analyze table %s", originalTableName);
            DdlStateCheckUtil.alterTableViaJdbc(connection, schemaName, originalTableName, analyzeTableSql);
        }
        DdlStateCheckUtil.alterTableViaJdbc(connection, schemaName, originalTableName, addGsiHint + addGsiDdl);
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(connection, addGsiDdl);
        String msg =
            String.format("jobId: %d, table: %s, addGsiDdl: %s", jobId, originalTableName, addGsiHint + addGsiDdl);

        List<String> errMsg = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Thread.sleep(sleepInterval);
            DdlStateCheckUtil.pauseDdl(connection, jobId);
            logger.info(String.format("pause ddl for %d time", i));
            Thread.sleep(sleepInterval);
            try {
                DdlStateCheckUtil.continueDdlAsync(connection, jobId);
            } catch (Exception e) {
                throw e;
            }
            logger.info(String.format("continue ddl for %d time", i));
            Thread.sleep(checkInterval);
            if (!DdlStateCheckUtil.checkIfContinueValid(connection, jobId, errMsg)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("continue failed  for %s, %s", msg, errMsg));
            }
        }
        logger.info(String.format("wait for ddl %d finish...", jobId));
        if (!DdlStateCheckUtil.waitTillDdlDone(logger, connection, jobId, originalTableName, expectedLocalIndexConcurrency,
            false)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait addGsiDdl done timeout for %s", msg));
        }
//        if (!DdlStateCheckUtil.checkBackfillObjectsValid(connection, jobId, errMsg)){
//            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                String.format("check backfill objects failed %s", msg, errMsg.get(0)));
//        }
        logger.info(String.format("ddl %d finished...", jobId));
        if (!DdlStateCheckUtil.checkIfCompleteSuccessful(connection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job failed for some unknown reason!!! %s", msg));
        }
        if (!DdlStateCheckUtil.checkIfExecuteByPkRangeAndBuildLocalIndexLater(connection, jobId, expectedPkRange,
            expectedBuildLocalIndexLater, expectedPkRangeNum, errMsg)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format(
                    "this job success, but we don't expect that pkRange and localIndexLater behave in this way !!! %s, %s",
                    msg, errMsg.get(0)));
        }
        if (!StringUtils.isEmpty(createTableStmt) && !DdlStateCheckUtil.compareForLocalIndex(connection, schemaName,
            jobId, originalTableName, createTableStmt,
            addGsiStmt, gsiName, partNum, errMsg)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job success but local key is not right!! %s, %s", msg, errMsg.get(0)));
        }
    }
}

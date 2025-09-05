package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.trx.CleanLogTableScheduledJob;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.CHECK_CCI;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class CheckCciScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(CleanLogTableScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public CheckCciScheduledJob(ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();

        try {
            // Mark as RUNNING.
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {CHECK_CCI + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }

            final Map savedMdcContext = MDC.getCopyOfContextMap();
            String remark;
            try {
                MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                // check cci
                remark = doCheckIfNecessary();
            } finally {
                MDC.setContextMap(savedMdcContext);
            }

            long finishTime = System.currentTimeMillis() / 1000;
            return ScheduledJobsManager
                .casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
        } catch (Throwable t) {
            logger.error(t);
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.TRX,
                    UNEXPECTED,
                    new String[] {
                        CHECK_CCI + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            String remark = "Check cci task error: " + t.getMessage();
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, remark, t.getMessage());
            return false;
        }
    }

    private String doCheckIfNecessary() throws SQLException {
        if (DynamicConfig.getInstance().isSkipCheckCciScheduleJob()) {
            return null;
        }
        ExecutorContext executorContext = ExecutorContext.getContext(DEFAULT_DB_NAME);
        // Get last task status.
        String lastStatusRemark = ScheduledJobsManager.queryLatestRemark(executableScheduledJob.getScheduleId());
        Status lastStatus = null;
        try {
            lastStatus = JSON.parseObject(lastStatusRemark, Status.class);
        } catch (Throwable t) {
            logger.error("Parse last status failed: " + lastStatusRemark, t);
        }
        long lastCheckTso = lastStatus == null ? 0 : lastStatus.lastCheckTso;
        long lastFullyCheckTso = lastStatus == null ? 0 : lastStatus.lastFullyCheckTso;

        // Get latest forced checkpoints.
        List<Pair<Long, Long>> columnarAndInnodbTso = new ArrayList<>();
        long checkpointLimit = DynamicConfig.getInstance().getCheckCciCheckpointLimit();
        checkpointLimit = Math.max(1, checkpointLimit);
        checkpointLimit = Math.min(100, checkpointLimit);
        List<Pair<String, String>> cciList = new ArrayList<>();
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarCheckpointsAccessor accessor1 = new ColumnarCheckpointsAccessor();
            accessor1.setConnection(connection);
            accessor1.queryLatestForcedCheckpoints(lastCheckTso, checkpointLimit).forEach(
                record -> columnarAndInnodbTso.add(new Pair<>(record.checkpointTso, record.binlogTso))
            );

            if (columnarAndInnodbTso.isEmpty()) {
                return emptyStatus(executorContext, lastFullyCheckTso, "No checkpoints found. Mark it success.");
            }

            // Get all cci.
            ColumnarTableMappingAccessor accessor2 = new ColumnarTableMappingAccessor();
            accessor2.setConnection(connection);
            accessor2.queryAllSnapshotCci()
                .forEach(record -> cciList.add(new Pair<>(record.tableSchema.toLowerCase(), record.indexName)));

            if (cciList.isEmpty()) {
                return emptyStatus(executorContext, lastFullyCheckTso, "No cci found. Mark it success.");
            }
        }

        // Check these cci.
        StringBuilder statusStr = new StringBuilder();
        long maxTso = lastFullyCheckTso;
        for (Pair<Long, Long> tsoPair : columnarAndInnodbTso) {
            long columnarTso = tsoPair.getKey();
            long innodbTso = tsoPair.getValue();
            long fullyDiffMs = (columnarTso - lastFullyCheckTso) >> 22;
            long diffMs = (columnarTso - lastCheckTso) >> 22;
            Collections.shuffle(cciList);
            boolean success = true;
            boolean skip = false;
            for (Pair<String, String> schemaAndTableIndex : cciList) {
                try (IInnerConnection connection = executorContext.getInnerConnectionManager()
                    .getConnection(schemaAndTableIndex.getKey());
                    Statement stmt = connection.createStatement()) {
                    if ((fullyDiffMs > 24 * 60 * 60 * 1000 || diffMs > 30 * 60 * 1000)
                        && InstConfUtil.isInMaintenanceTimeWindow()) {
                        // fully snapshot check
                        ResultSet rs = stmt.executeQuery("CHECK COLUMNAR INDEX " + schemaAndTableIndex.getValue()
                            + " SNAPSHOT " + innodbTso + " " + columnarTso);
                        if (!processDetails(rs, schemaAndTableIndex.getKey(), schemaAndTableIndex.getValue())) {
                            success = false;
                        } else {
                            maxTso = Math.max(maxTso, columnarTso);
                        }
                    } else if (diffMs < 30 * 60 * 1000) {
                        // incremental check
                        ResultSet rs = stmt.executeQuery("CHECK COLUMNAR INDEX " + schemaAndTableIndex.getValue()
                            + " INCREMENT " + lastCheckTso + " " + columnarTso + " " + innodbTso);
                        if (!processDetails(rs, schemaAndTableIndex.getKey(), schemaAndTableIndex.getValue())) {
                            success = false;
                        }
                    } else {
                        // just skip this check
                        logger.warn("Skip check cci " + schemaAndTableIndex.getKey() + "."
                            + schemaAndTableIndex.getValue() + " due to diffMs: " + diffMs);
                        skip = true;
                    }
                }
            }
            // Update columnar_checkpoints.
            try (Connection connection = MetaDbUtil.getConnection()) {
                ColumnarCheckpointsAccessor accessor = new ColumnarCheckpointsAccessor();
                accessor.setConnection(connection);
                accessor.updateExtraByTso((success ? "success" : "fail") + (skip ? "(some skipped)" : ""), columnarTso);
            }
            statusStr.append("Check cci ").append(success ? "success" : "fail").append(skip ? "(some skipped);" : ";");
        }
        Status newStatus = new Status();
        newStatus.status = statusStr.toString();
        newStatus.lastCheckTso = columnarAndInnodbTso.get(0).getKey();
        newStatus.lastFullyCheckTso = maxTso;
        return JSON.toJSONString(newStatus);
    }

    private static String emptyStatus(ExecutorContext executorContext, long lastFullyCheckTso, String status) {
        Status newStatus = new Status();
        newStatus.status = status;
        newStatus.lastCheckTso = executorContext.getTransactionManager().getTimestampOracle().nextTimestamp();
        newStatus.lastFullyCheckTso = 0 == lastFullyCheckTso ? newStatus.lastCheckTso : lastFullyCheckTso;
        return JSON.toJSONString(newStatus);
    }

    private boolean processDetails(ResultSet rs, String schema, String index) throws SQLException {
        StringBuilder errMsg = null;
        if (rs.next()) {
            String detail = rs.getString("DETAILS");
            if (null == detail || !detail.startsWith("OK")) {
                // fail
                errMsg = new StringBuilder("Check cci " + schema + "." + index + " failed. Details: " + detail + ";");
                while (rs.next()) {
                    errMsg.append(rs.getString("DETAILS")).append(";");
                }
            }
        } else {
            // fail
            errMsg = new StringBuilder("Check cci " + schema + "." + index + " failed. No results found.");
        }
        if (null != errMsg) {
            logger.error(errMsg.toString());
            EventLogger.log(EventType.COLUMNAR_ERR, errMsg.toString());
            return false;
        }
        return true;
    }

    @Data
    private static class Status {
        public String status;
        public long lastCheckTso;
        public long lastFullyCheckTso;
    }
}

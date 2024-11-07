package com.alibaba.polardbx.executor.scheduler.executor.trx;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingExtra;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.common.columnar.ColumnarUtils.AddCDCMarkEvent;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.GENERATE_COLUMNAR_SNAPSHOT;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhili
 */
public class GenerateColumnarSnapshotScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(GenerateColumnarSnapshotScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public GenerateColumnarSnapshotScheduledJob(ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();

        StringBuffer remark = new StringBuffer("Begin generate columnar snapshots.");
        try {
            // Mark as RUNNING.
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {GENERATE_COLUMNAR_SNAPSHOT + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }

            final Map savedMdcContext = MDC.getCopyOfContextMap();
            try {
                MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                generateColumnarSnapshots(remark, false);
            } finally {
                MDC.setContextMap(savedMdcContext);
            }

            long finishTime = System.currentTimeMillis() / 1000;
            remark.append("End generate columnar snapshots. Cost ").append(finishTime - startTime).append("s.");
            return ScheduledJobsManager
                .casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark.toString());
        } catch (Throwable t) {
            logger.error(t);
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.TRX,
                    UNEXPECTED,
                    new String[] {
                        GENERATE_COLUMNAR_SNAPSHOT + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            remark.append("Generate columnar snapshots error: ").append(t.getMessage());
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, remark.toString(), t.getMessage());
            return false;
        }
    }

    public static void generateColumnarSnapshots(StringBuffer remark, boolean force) throws SQLException {
        if (!force && !DynamicConfig.getInstance().isEnableAutoGenColumnarSnapshot()) {
            remark.append("Not enabled, skip.");
            return;
        }
        List<ColumnarTableMappingRecord> columnarTableMappingRecords;
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            columnarTableMappingAccessor.setConnection(connection);
            columnarTableMappingRecords = columnarTableMappingAccessor.queryAllSnapshotCci();
        }

        if (columnarTableMappingRecords.isEmpty()) {
            remark.append("No snapshot cci, skip.");
            return;
        }

        EventLogger.logCciSnapshot(columnarTableMappingRecords.size());

        long currentTso =
            ExecutorContext.getContext(DEFAULT_DB_NAME).getTransactionManager().getTimestampOracle().nextTimestamp();
        Collections.shuffle(columnarTableMappingRecords);

        AtomicBoolean stop = new AtomicBoolean(false);
        ConcurrentLinkedQueue<ColumnarTableMappingRecord> workQueue = new ConcurrentLinkedQueue<>();
        int parallelism = DynamicConfig.getInstance().getAutoGenColumnarSnapshotParallelism();
        List<Future> futures = new ArrayList<>();
        try {
            // Consumers.
            for (int i = 0; i < parallelism; i++) {
                futures.add(
                    ExecutorContext.getContext(DEFAULT_DB_NAME).getTopologyExecutor().getExecutorService()
                        .submit(null, null, () -> {
                            final Map savedMdcContext = MDC.getCopyOfContextMap();
                            try {
                                MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                                ColumnarTableMappingRecord r;
                                while (null != (r = workQueue.poll()) || !stop.get()) {
                                    if (null == r) {
                                        Thread.sleep(100);
                                        continue;
                                    }
                                    String sql = String.format("call polardbx.columnar_flush(%s)", r.tableId);
                                    Long tso = AddCDCMarkEvent(sql, SqlKind.PROCEDURE_CALL.name());
                                    ColumnarTableMappingExtra extra = getColumnarTableMappingExtra(r);
                                    assert extra != null;
                                    extra.setTso(tso);
                                    try {
                                        saveColumnarTableMappingExtra(r, extra);
                                        remark.append("Add ")
                                            .append(r.indexName)
                                            .append(", tso: ")
                                            .append(tso)
                                            .append(". ");
                                    } catch (SQLException e) {
                                        logger.error("Call columnar flush failed.", e);
                                    }
                                }
                            } catch (InterruptedException e) {
                                logger.error(e);
                            } finally {
                                MDC.setContextMap(savedMdcContext);
                            }
                        }));
            }
            // Producer.
            for (ColumnarTableMappingRecord record : columnarTableMappingRecords) {
                long intervalMinute = -1;
                try {
                    GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = ExecutorContext.getContext(record.tableSchema)
                        .getGsiManager()
                        .getGsiIndexMeta(record.tableSchema, record.tableName, record.indexName, IndexStatus.ALL);
                    String interval = gsiIndexMetaBean.columnarOptions.get()
                        .get(ColumnarOptions.AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL);
                    if (null != interval) {
                        intervalMinute = Long.parseLong(interval);
                    }
                } catch (Exception e) {
                    logger.error("Fetch table meta error", e);
                    continue;
                }

                if (intervalMinute < 0) {
                    continue;
                }

                ColumnarTableMappingExtra extra = getColumnarTableMappingExtra(record);
                if (extra == null) {
                    continue;
                }

                if (force || (((currentTso - extra.getTso()) >> 22) > intervalMinute * 55 * 1000)) {
                    // Should generate.
                    workQueue.add(record);
                }
            }
        } finally {
            stop.set(true);
        }

        try {
            for (Future future : futures) {
                future.get();
            }
        } catch (Throwable t) {
            for (Future future : futures) {
                future.cancel(true);
            }
        }
    }

    private static ColumnarTableMappingExtra getColumnarTableMappingExtra(ColumnarTableMappingRecord record) {
        ColumnarTableMappingExtra extra;
        try {
            if (null == record.extra) {
                extra = new ColumnarTableMappingExtra();
                extra.setTso(0L);
            } else {
                extra = JSON.parseObject(record.extra, ColumnarTableMappingExtra.class);
            }
        } catch (Throwable t) {
            logger.error("Parse extra failed: " + record.extra, t);
            return null;
        }
        return extra;
    }

    private static void saveColumnarTableMappingExtra(ColumnarTableMappingRecord record,
                                                      ColumnarTableMappingExtra extra) throws SQLException {
        record.extra = JSON.toJSONString(extra);
        // Update meta db.
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);
            columnarTableMappingAccessor.UpdateExtraByTableId(record.tableId, record.extra);
        }
    }
}

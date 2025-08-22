package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.sync.MetricSyncAction;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarDuplicatesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.google.common.collect.Lists;

import java.sql.Connection;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.executor.statistic.RealStatsLog.statLog;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.NOT_ENABLED;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.LOG_SYSTEM_METRICS;

/**
 * @author fangwu
 */
public class LogSystemMetricsScheduledJob extends SchedulerExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogSystemMetricsScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public LogSystemMetricsScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();
        String remark = "";
        try {
            // check conf
            boolean enableStatisticBackground = InstConfUtil.getBool(ConnectionParams.ENABLE_LOG_SYSTEM_METRICS);
            if (!enableStatisticBackground) {
                remark = " task not enabled";
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        NOT_ENABLED,
                        new String[] {
                            ConnectionProperties.ENABLE_LOG_SYSTEM_METRICS,
                            LOG_SYSTEM_METRICS + "," + fireTime + " exit"
                        },
                        NORMAL);
                return succeedExit(scheduleId, fireTime, remark);
            }

            //mark as RUNNING
            boolean casSuccess = markRunning(scheduleId, fireTime, startTime);
            if (!casSuccess) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {LOG_SYSTEM_METRICS + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }

            List<List<Map<String, Object>>> metrics =
                GmsSyncManagerHelper.sync(new MetricSyncAction(), "polardbx", SyncScope.CURRENT_ONLY);
            FeatureStats fs = FeatureStats.build();
            List<FeatureStats> mergeList = Lists.newArrayList();
            for (List<Map<String, Object>> metric : metrics) {
                for (Map<String, Object> map : metric) {
                    String json = (String) map.get(MetricSyncAction.JSON_KEY);
                    mergeList.add(FeatureStats.deserialize(json));
                }
            }
            FeatureStats.merge(fs, mergeList);
            String incrementLog = fs.log();

            // real-time information + Incremental information
            String log = statLog() + incrementLog;
            EventLogger.log(EventType.METRICS, log);
            ModuleLogInfo.getInstance().logRecord(Module.METRIC, LogPattern.PROCESS_END,
                new String[] {"feature metric log", log}, NORMAL);
            // columnar status
            for (final String status : columnarStatus()) {
                EventLogger.log(EventType.COLUMNAR_STATUS, status);
            }
            //mark as SUCCESS
            succeedExit(scheduleId, fireTime, remark);
        } catch (Throwable t) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    UNEXPECTED,
                    new String[] {
                        LOG_SYSTEM_METRICS + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t);
            errorExit(scheduleId, fireTime, t.getMessage());
            return false;
        }
        return true;
    }

    protected boolean markRunning(long scheduleId, long fireTime, long startTime) {
        return ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
    }

    protected void errorExit(long scheduleId, long fireTime, String error) {
        //mark as fail
        ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, error);
        // alert
        OptimizerAlertUtil.statisticErrorAlert();
    }

    protected boolean succeedExit(long scheduleId, long fireTime, String remark) {
        long finishTime = System.currentTimeMillis() / 1000;
        //mark as SUCCESS
        return ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
    }

    static List<String> columnarStatus() {
        final List<ColumnarTableMappingRecord> columnarRecords = new ArrayList<>();
        final Map<Long, Long> duplicates = new HashMap<>();
        try (final Connection metaDbConn = MetaDbUtil.getConnection()) {
            final ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
            tableMappingAccessor.setConnection(metaDbConn);

            columnarRecords.addAll(tableMappingAccessor.queryByStatus(ColumnarTableStatus.PUBLIC.name()));
            columnarRecords.addAll(tableMappingAccessor.queryByStatus(ColumnarTableStatus.CREATING.name()));

            final ColumnarDuplicatesAccessor duplicatesAccessor = new ColumnarDuplicatesAccessor();
            for (final ColumnarTableMappingRecord record : columnarRecords) {
                final long count = duplicatesAccessor.countDuplicates(record.tableId);
                duplicates.put(record.tableId, count);
            }
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            return Collections.emptyList();
        }

        final List<ColumnarTransactionUtils.ColumnarIndexStatusRow> rows;
        try {
            final long tso = ColumnarTransactionUtils.getLatestShowColumnarStatusTsoFromGms();
            rows = ColumnarTransactionUtils.queryColumnarIndexStatus(tso, columnarRecords);
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            return Collections.emptyList();
        }

        // generate status string
        final List<String> results = new ArrayList<>();
        for (final ColumnarTransactionUtils.ColumnarIndexStatusRow row : rows) {
            final String str =
                String.valueOf(row.tso) + '|' + row.tableSchema + '|' + row.tableName + '|' + row.indexName + '|'
                    + row.indexId + '|' + row.csvRows + '|' + row.orcRows + '|' + row.delRows + '|' + row.csvFileNum
                    + '|' + row.orcFileNum + '|' + row.delFileNum + '|' + row.csvFileSize + '|' + row.orcFileSize + '|'
                    + row.delFileSize + '|' + row.status + '|' + duplicates.get(row.indexId);
            results.add(str);
        }
        return results;
    }
}

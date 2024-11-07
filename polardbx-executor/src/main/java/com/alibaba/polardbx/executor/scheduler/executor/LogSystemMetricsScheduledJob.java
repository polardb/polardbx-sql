package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.sync.MetricSyncAction;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.google.common.collect.Lists;

import java.time.ZonedDateTime;
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
}

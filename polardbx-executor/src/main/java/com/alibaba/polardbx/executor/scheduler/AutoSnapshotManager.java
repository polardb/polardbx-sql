package com.alibaba.polardbx.executor.scheduler;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.util.SyncUtil;
import org.apache.calcite.sql.SqlKind;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.TddlConstants.CRON_EXPR;
import static com.alibaba.polardbx.common.TddlConstants.ZONE_ID;
import static com.alibaba.polardbx.common.columnar.ColumnarUtils.AddCDCMarkEventForColumnar;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class AutoSnapshotManager {
    private final static Logger logger = LoggerFactory.getLogger(AutoSnapshotManager.class);
    private final static SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    private final static Scheduler scheduler;
    private final static JobDetail columnarFlushJob;
    private final static ScheduledThreadPoolExecutor scanner = ExecutorUtil.createScheduler(1,
        new NamedThreadFactory("Auto-Snapshot-Scanner-Thread", true),
        new ThreadPoolExecutor.DiscardPolicy());
    private final static String JOB_ID = "AutoSnapshotJob";
    private final static String TRIGGER_ID = "AutoSnapshotTrigger";
    private static String cronExpr;
    private static String zoneId;

    static {
        Scheduler tmp = null;
        try {
            tmp = schedulerFactory.getScheduler();
            tmp.start();
        } catch (SchedulerException e) {
            logger.error("Setup schedule manager failed.", e);
        }
        scheduler = tmp;

        columnarFlushJob = newJob(ColumnarFlushJob.class)
            .withIdentity(JOB_ID)
            .build();

        scanner.scheduleWithFixedDelay(AsyncTask.build(AutoSnapshotManager::scan), 0L, 5, TimeUnit.SECONDS);
    }

    public static class ColumnarFlushJob implements Job {
        public void execute(JobExecutionContext context) {
            if (DynamicConfig.getInstance().isEnableColumnarReadInstanceAutoGenerateSnapshot()) {
                // Only run on columnar slave instance.
                // Remember to set this option on both master and columnar slave instances.
                if (ConfigDataMode.isColumnarMode() && SyncUtil.isNodeWithSmallestId()) {
                    AddCDCMarkEventForColumnar("call polardbx.columnar_flush()", SqlKind.PROCEDURE_CALL.name());
                }
            } else {
                // Run on leader node of master instance.
                if (ExecUtils.hasLeadership(null)) {
                    AddCDCMarkEventForColumnar("call polardbx.columnar_flush()", SqlKind.PROCEDURE_CALL.name());
                }
            }
        }
    }

    private AutoSnapshotManager() {

    }

    public static void init() {

    }

    private static void scan() {
        Map<String, String> config = ExecUtils.getColumnarAutoSnapshotConfig();
        if (null != config) {
            String newCronExpr = config.get(CRON_EXPR);
            String newZoneId = config.get(ZONE_ID);
            if (!newCronExpr.equalsIgnoreCase(cronExpr) || !newZoneId.equalsIgnoreCase(zoneId)) {
                cronExpr = newCronExpr;
                zoneId = newZoneId;
                try {
                    trigger();
                } catch (SchedulerException e) {
                    logger.error(e);
                }
            }
        }
    }

    private static void trigger() throws SchedulerException {
        Trigger old = scheduler.getTrigger(new TriggerKey(TRIGGER_ID));

        Trigger trigger = newTrigger()
            .withIdentity(TRIGGER_ID)
            .withSchedule(CronScheduleBuilder
                .cronSchedule(cronExpr)
                .inTimeZone(TimeZone.getTimeZone(zoneId)))
            .build();

        if (null != old) {
            scheduler.rescheduleJob(old.getKey(), trigger);
        } else {
            scheduler.scheduleJob(columnarFlushJob, trigger);
        }
    }
}

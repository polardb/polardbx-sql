package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.scheduler.SchedulerJobStatus;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scheduler.DefaultQuartzCronTrigger;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import lombok.Getter;

import java.sql.Connection;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.cronutils.model.CronType.QUARTZ;

@Getter
@TaskName(name = "AlterTtlInfoTask")
public class AlterTtlInfoTask extends BaseGmsTask {

    private TtlDefinitionInfo oldTtlInfo;
    private TtlDefinitionInfo newTtlInfo;

    public AlterTtlInfoTask(TtlDefinitionInfo oldTtlInfo,
                            TtlDefinitionInfo newTtlInfo) {
        super(newTtlInfo.getTtlInfoRecord().getTableSchema(), newTtlInfo.getTtlInfoRecord().getTableName());
        this.oldTtlInfo = oldTtlInfo;
        this.newTtlInfo = newTtlInfo;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateTtlInfoAndSchedule(metaDbConnection, executionContext, newTtlInfo);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateTtlInfoAndSchedule(metaDbConnection, executionContext, oldTtlInfo);
    }

    protected void updateTtlInfoAndSchedule(Connection metaDbConnection,
                                            ExecutionContext executionContext,
                                            TtlDefinitionInfo newTtlInfo) {

        try {
            String oldTtlTblSchema = oldTtlInfo.getTtlInfoRecord().getTableSchema();
            String oldTtlTblName = oldTtlInfo.getTtlInfoRecord().getTableName();
            TableMetaChanger.alterTtlInfoMeta(metaDbConnection,
                newTtlInfo.getTtlInfoRecord(),
                oldTtlTblSchema,
                oldTtlTblName);
            boolean enableTtlScheduleOnOldTtlInfo = oldTtlInfo.isEnableTtlSchedule();
            boolean enableTtlScheduleOnNewTtlInfo = newTtlInfo.isEnableTtlSchedule();
            String cronExpr = newTtlInfo.getTtlInfoRecord().getTtlCron();
            if (cronExpr == null) {
                cronExpr =
                    executionContext.getParamManager().getString(ConnectionParams.DEFAULT_TTL_SCHEDULE_CRON_EXPR);
            }

            List<ScheduledJobsRecord> scheduledJobsRecordList =
                ScheduledJobsManager.getScheduledJobByTableNameAndExecutorType(schemaName, logicalTableName,
                    ScheduledJobExecutorType.TTL_JOB.name());
            boolean existScheduledOldJobs = !scheduledJobsRecordList.isEmpty();
            if (existScheduledOldJobs) {
                /**
                 * A ttlInfo must be a scheduled job definition
                 */
                ScheduledJobsRecord scheduledJobsRecord = scheduledJobsRecordList.get(0);
                String stateVal =
                    !enableTtlScheduleOnNewTtlInfo ? SchedulerJobStatus.DISABLED.name() :
                        SchedulerJobStatus.ENABLED.name();
                CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
                Cron cron = quartzCronParser.parse(cronExpr);
                cron.validate();

                String oldCronExpr = scheduledJobsRecord.getScheduleExpr();
                boolean modifiedCronExpr = !(cronExpr.equalsIgnoreCase(oldCronExpr));
                boolean openTtlJobSchedule = !enableTtlScheduleOnOldTtlInfo && enableTtlScheduleOnNewTtlInfo;

                String cronTimeZoneStr = TtlInfoRecord.TTL_JOB_CRON_DEFAULT_TIME_ZONE;
                CronDescriptor descriptor = CronDescriptor.instance(Locale.US);
                scheduledJobsRecord.setScheduleComment(descriptor.describe(cron));
                scheduledJobsRecord.setStatus(stateVal);
                scheduledJobsRecord.setScheduleExpr(cronExpr);
                scheduledJobsRecord.setTimeZone(cronTimeZoneStr);

                if (modifiedCronExpr || openTtlJobSchedule) {
                    Optional<ZonedDateTime> newNextFiredTime =
                        DefaultQuartzCronTrigger.calcNextFireTimeByNow(scheduledJobsRecord);
                    long nextFiredTimeSec = newNextFiredTime.get().toEpochSecond();
                    scheduledJobsRecord.setNextFireTime(nextFiredTimeSec);
                }

                TableMetaChanger.updateScheduledJob(metaDbConnection, scheduledJobsRecord);
                if (!enableTtlScheduleOnNewTtlInfo) {
                    TableMetaChanger.removeTtlFiredScheduledJobs(metaDbConnection, schemaName, logicalTableName);
                }
                TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
            }
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
            throw new TtlJobRuntimeException(ex, ex.getMessage());
        }
    }
}
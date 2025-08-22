package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.common.scheduler.SchedulerJobStatus;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AddTtlInfoTask")
public class AddTtlInfoTask extends BaseGmsTask {

    private TtlDefinitionInfo ttlDefinitionInfo;

    public AddTtlInfoTask(TtlDefinitionInfo ttlDefinitionInfo) {
        super(ttlDefinitionInfo.getTtlInfoRecord().getTableSchema(),
            ttlDefinitionInfo.getTtlInfoRecord().getTableName());
        this.ttlDefinitionInfo = ttlDefinitionInfo;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.addTtlInfoMeta(metaDbConnection, ttlDefinitionInfo.getTtlInfoRecord());

        boolean disableSchedule = !ttlDefinitionInfo.isEnableTtlSchedule();

        String cronExpr = ttlDefinitionInfo.getTtlInfoRecord().getTtlCron();
        if (cronExpr == null) {
            cronExpr = executionContext.getParamManager().getString(ConnectionParams.DEFAULT_TTL_SCHEDULE_CRON_EXPR);
        }

        String ttlCronJobTz = TtlInfoRecord.TTL_JOB_CRON_DEFAULT_TIME_ZONE;
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            schemaName,
            null,
            logicalTableName,
            ScheduledJobExecutorType.TTL_JOB,
            cronExpr,
            ttlCronJobTz,
            SchedulePolicy.WAIT
        );
        String stateVal = disableSchedule ? SchedulerJobStatus.DISABLED.name() : SchedulerJobStatus.ENABLED.name();
        scheduledJobsRecord.setStatus(stateVal);

        try {
            TableMetaChanger.addScheduledJob(metaDbConnection, scheduledJobsRecord);
            TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }

    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.removeTtlInfoMeta(metaDbConnection, schemaName, logicalTableName);
        TableMetaChanger.removeScheduledJobs(metaDbConnection, schemaName, logicalTableName);
    }
}
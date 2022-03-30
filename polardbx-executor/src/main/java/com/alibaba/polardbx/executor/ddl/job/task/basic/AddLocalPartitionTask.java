package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.scheduler.SchedulePolicy;
import com.alibaba.polardbx.common.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AddLocalPartitionTask")
public class AddLocalPartitionTask extends BaseGmsTask {

    private LocalPartitionDefinitionInfo localPartitionDefinitionInfo;

    public AddLocalPartitionTask(LocalPartitionDefinitionInfo localPartitionDefinitionInfo) {
        super(localPartitionDefinitionInfo.getTableSchema(), localPartitionDefinitionInfo.getTableName());
        this.localPartitionDefinitionInfo = localPartitionDefinitionInfo;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.addLocalPartitionMeta(metaDbConnection, localPartitionDefinitionInfo.convertToRecord());

        boolean disableSchedule = localPartitionDefinitionInfo.isDisableSchedule();
        if(disableSchedule){
            return;
        }

        String defaultCronExpr =
            executionContext.getParamManager().getString(ConnectionParams.DEFAULT_LOCAL_PARTITION_SCHEDULE_CRON_EXPR);
        ScheduledJobsRecord scheduledJobsRecord = ScheduledJobsManager.createQuartzCronJob(
            schemaName,
            logicalTableName,
            ScheduledJobExecutorType.LOCAL_PARTITION,
            defaultCronExpr,
            executionContext.getTimeZone().getMySqlTimeZoneName(),
            SchedulePolicy.WAIT
        );
        TableMetaChanger.addScheduledJob(metaDbConnection, scheduledJobsRecord);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.removeLocalPartitionMeta(metaDbConnection, schemaName, logicalTableName);
        TableMetaChanger.removeScheduledJobs(metaDbConnection, schemaName, logicalTableName);
    }
}
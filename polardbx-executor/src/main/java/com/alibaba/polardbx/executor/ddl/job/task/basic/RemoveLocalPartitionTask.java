package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "RemoveLocalPartitionTask")
public class RemoveLocalPartitionTask extends BaseGmsTask {

    public RemoveLocalPartitionTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.removeLocalPartitionMeta(metaDbConnection, schemaName, logicalTableName);
        TableMetaChanger.removeScheduledJobs(metaDbConnection, schemaName, logicalTableName);
        updateSupportedCommands(true, false, metaDbConnection);
    }
}
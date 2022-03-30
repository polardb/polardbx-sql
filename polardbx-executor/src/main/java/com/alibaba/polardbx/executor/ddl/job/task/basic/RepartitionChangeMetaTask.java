package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.executor.ddl.job.meta.misc.RepartitionMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.List;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RepartitionChangeMetaTask")
public class RepartitionChangeMetaTask extends BaseGmsTask {
    private List<String> changeShardColumns;

    public RepartitionChangeMetaTask(String schemaName, String logicalTableName, List<String> changeShardColumns) {
        super(schemaName, logicalTableName);
        this.changeShardColumns = changeShardColumns;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        RepartitionMetaChanger.changeTableMeta4RepartitionKey(
            metaDbConnection,
            schemaName,
            logicalTableName,
            changeShardColumns
        );
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        if (CollectionUtils.isNotEmpty(this.changeShardColumns)) {
            sb.append("add shard columns ").append(this.changeShardColumns);
        }
        sb.append(" on table ").append(this.getLogicalTableName());
        return "|" + sb;
    }
}

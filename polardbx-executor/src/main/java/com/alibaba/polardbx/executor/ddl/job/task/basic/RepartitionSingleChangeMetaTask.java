package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.executor.ddl.job.meta.misc.RepartitionMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RepartitionSingleChangeMetaTask")
public class RepartitionSingleChangeMetaTask extends BaseGmsTask {

    private final TableGroupDetailConfig tableGroupConfig;
    private final Long oldGroupId;

    public RepartitionSingleChangeMetaTask(String schemaName, String logicalTableName, Long oldGroupId,
                                           TableGroupDetailConfig tableGroupConfig) {
        super(schemaName, logicalTableName);
        this.oldGroupId = oldGroupId;
        this.tableGroupConfig = tableGroupConfig;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        RepartitionMetaChanger.changeTableMeta4RepartitionKeySingleOptimize(
            metaDbConnection,
            schemaName,
            logicalTableName,
            oldGroupId,
            tableGroupConfig
        );
    }
}

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableGroupSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "ReloadTableMetaAfterChangeTableGroupTask")
public class ReloadTableMetaAfterChangeTableGroupTask extends BaseGmsTask {

    protected String targetTableGroup;

    @JSONCreator
    public ReloadTableMetaAfterChangeTableGroupTask(final String schemaName,
                                                    final String targetTableGroup) {
        super(schemaName, null);
        this.targetTableGroup = targetTableGroup;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        reloadTableGroup();
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        reloadTableGroup();
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    protected void reloadTableGroup() {

        DdlJobManager jobManager = new DdlJobManager();
        List<DdlTask> prevTasks = jobManager.getTasksFromMetaDB(getJobId(),
            (new AlterTableSetTableGroupChangeMetaOnlyTask(null, null, null, null, false, false)).getName());
        assert prevTasks.size() == 0;
        AlterTableSetTableGroupChangeMetaOnlyTask setTableGroupChangeMetaOnlyTask =
            (AlterTableSetTableGroupChangeMetaOnlyTask) prevTasks.get(0);
        //get the targetTableGroup from AlterTableSetTableGroupChangeMetaOnlyTask in the some job
        targetTableGroup = setTableGroupChangeMetaOnlyTask.getTargetTableGroup();

        syncTableGroup();
    }

    private void syncTableGroup() {
        try {
            SyncManagerHelper
                .sync(new TableGroupSyncAction(schemaName, targetTableGroup));
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table group, schemaName:%s, tableGroupName:%s", schemaName, targetTableGroup));
            throw GeneralUtil.nestedException(t);
        }
    }

}
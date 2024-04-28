package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.CreateViewSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@TaskName(name = "CreateViewSyncTask")
@Getter
public class CreateViewSyncTask extends BaseDdlTask {
    final private String schemaName;
    final private String viewName;

    @JSONCreator
    public CreateViewSyncTask(String schemaName, String viewName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.viewName = viewName;
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.sync(new CreateViewSyncAction(schemaName, viewName), schemaName, SyncScope.ALL);
        } catch (Throwable ignore) {
            LOGGER.error(
                "error occurs while execute GsiStatisticsSyncAction"
            );
        }
    }
}

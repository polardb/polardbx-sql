package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.DropViewSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Collections;

@TaskName(name = "DropViewSyncTask")
@Getter
public class DropViewSyncTask extends BaseDdlTask {
    final private String schemaName;
    final private String viewName;

    @JSONCreator
    public DropViewSyncTask(String schemaName, String viewName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.viewName = viewName;
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        try {
            SyncManagerHelper.sync(new DropViewSyncAction(schemaName, Collections.singletonList(viewName)), schemaName, SyncScope.ALL);
        } catch (Throwable ignore) {
            LOGGER.error(
                "error occurs while execute DropViewSyncTask"
            );
        }
    }
}

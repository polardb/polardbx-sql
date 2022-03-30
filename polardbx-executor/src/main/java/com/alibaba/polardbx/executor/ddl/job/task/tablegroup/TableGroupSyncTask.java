package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableGroupSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "TableGroupSyncTask")
public class TableGroupSyncTask extends BaseSyncTask {

    String tableGroupName;

    public TableGroupSyncTask(String schemaName,
                              String tableGroupName) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        syncTableGroup();
    }

    private void syncTableGroup() {
        try {
            SyncManagerHelper
                .sync(new TableGroupSyncAction(schemaName, tableGroupName));
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table group, schemaName:%s, tableGroupName:%s", schemaName, tableGroupName));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    protected String remark() {
        return "|sync tableGroup, tableGroupName: " + tableGroupName;
    }
}
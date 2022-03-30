package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableGroupSyncAction;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "TopologySyncTask")
public class TopologySyncTask extends BaseSyncTask {

    public TopologySyncTask(String schemaName) {
        super(schemaName);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        syncTopology();
    }

    private void syncTopology() {
        try {
            String topologyDataId = MetaDbDataIdBuilder.getDbTopologyDataId(schemaName);
            MetaDbConfigManager.getInstance().sync(topologyDataId);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync topology for schemaName:%s", schemaName));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    protected String remark() {
        return "|sync tableGroup, schemaName: " + schemaName;
    }
}
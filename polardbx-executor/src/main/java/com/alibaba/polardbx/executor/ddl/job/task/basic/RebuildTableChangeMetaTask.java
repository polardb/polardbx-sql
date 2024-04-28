package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RebuildTableChangeMetaTask")
public class RebuildTableChangeMetaTask extends BaseGmsTask {

    public RebuildTableChangeMetaTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        EventLogger.log(EventType.DDL_INFO, "Online modify column start");

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        tableInfoManager.updateRebuildingTableFlag(schemaName, logicalTableName, false);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        EventLogger.log(EventType.DDL_WARN, "Online modify column rollback");

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        tableInfoManager.updateRebuildingTableFlag(schemaName, logicalTableName, true);
    }

    @Override
    public String remark() {
        String sb = "set rebuilding table flag on table " + this.getLogicalTableName();
        return "|" + sb;
    }
}

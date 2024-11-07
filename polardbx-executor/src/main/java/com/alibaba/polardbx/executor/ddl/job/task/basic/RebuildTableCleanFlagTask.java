package com.alibaba.polardbx.executor.ddl.job.task.basic;

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
@TaskName(name = "RebuildTableCleanFlagTask")
public class RebuildTableCleanFlagTask extends BaseGmsTask {
    public RebuildTableCleanFlagTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.updateRebuildingTableFlag(schemaName, logicalTableName, true);
        LOGGER.info(
            String.format(
                "[rebuild table] clean rebuild table flag for primary table: %s.%s",
                schemaName, logicalTableName)
        );
    }

    @Override
    public String remark() {
        String sb = "clean rebuilding table flag on table " + this.getLogicalTableName();
        return "|" + sb;
    }
}
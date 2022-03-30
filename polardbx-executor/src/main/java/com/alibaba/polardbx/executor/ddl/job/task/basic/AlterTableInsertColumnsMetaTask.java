package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
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
@TaskName(name = "AlterTableInsertColumnsMetaTask")
public class AlterTableInsertColumnsMetaTask extends BaseGmsTask {
    private String dbIndex;
    private String phyTableName;
    private List<String> addedColumns;

    public AlterTableInsertColumnsMetaTask(String schemaName,
                                           String logicalTableName,
                                           String dbIndex,
                                           String phyTableName,
                                           List<String> addedColumns) {
        super(schemaName, logicalTableName);
        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;
        this.addedColumns = addedColumns;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, true, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.changeTableMeta(metaDbConnection, schemaName, logicalTableName, dbIndex, phyTableName,
            addedColumns, null);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.changeTableMeta(metaDbConnection, schemaName, logicalTableName, dbIndex, phyTableName,
            null, addedColumns);

        //sync have to be successful to continue
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName));
        executionContext.refreshTableMeta();

        LOGGER.info(String.format("Rollback Insert GSI columns meta. schema:%s, table:%s, index:%s",
            schemaName,
            logicalTableName,
            dbIndex
        ));
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        if (CollectionUtils.isNotEmpty(this.addedColumns)) {
            sb.append("add columns ").append(this.addedColumns);
        }
        sb.append(" on table ").append(this.getLogicalTableName());
        return "|" + sb;
    }

}

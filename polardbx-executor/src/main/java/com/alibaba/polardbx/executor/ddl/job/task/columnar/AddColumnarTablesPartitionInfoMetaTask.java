package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AddColumnarTablesPartitionInfoMetaTask")
public class AddColumnarTablesPartitionInfoMetaTask extends BaseGmsTask {

    private final TableGroupDetailConfig tableGroupConfig;
    private final String primaryTable;

    @JSONCreator
    public AddColumnarTablesPartitionInfoMetaTask(String schemaName,
                                                  String logicalTableName,
                                                  TableGroupDetailConfig tableGroupConfig,
                                                  String primaryTable) {
        super(schemaName, logicalTableName);
        this.tableGroupConfig = tableGroupConfig;
        this.primaryTable = primaryTable;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (!isCreateTableSupported(executionContext)) {
            return;
        }
        TableMetaChanger.addPartitionInfoMeta(metaDbConnection, tableGroupConfig, executionContext, false);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (!isCreateTableSupported(executionContext)) {
            return;
        }

        TableMetaChanger.removePartitionInfoMeta(metaDbConnection, schemaName, logicalTableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    private boolean isCreateTableSupported(ExecutionContext executionContext) {
        return !(executionContext.isUseHint());
    }

    private TablePartitionConfig getTablePartitionConfig(String primaryTable, Connection metaDbConnection) {
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        TablePartitionConfig
            tablePartitionConfig = tablePartitionAccessor.getTablePartitionConfig(schemaName, primaryTable, false);
        return tablePartitionConfig;
    }
}

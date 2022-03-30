package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Joiner;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "UpdateTablesVersionTask")
public class UpdateTablesVersionTask extends BaseGmsTask {

    final List<String> tableNames;

    public UpdateTablesVersionTask(String schemaName,
                                   List<String> tableNames) {
        super(schemaName, null);
        this.tableNames = tableNames;
    }

    @Override
    protected String remark() {
        return "|updateTablesVersion: " + Joiner.on(", ").join(tableNames);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateTablesVersion(metaDbConnection);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateTablesVersion(metaDbConnection);
    }

    private void updateTablesVersion(Connection metaDbConnection) {
        if (GeneralUtil.isNotEmpty(tableNames)) {
            int i = 0;
            try {
                for (String tableName : tableNames) {
                    TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConnection);
                    i++;
                }
            } catch (Throwable t) {
                LOGGER.error(String.format(
                    "error occurs while update table version, schemaName:%s, tableName:%s", schemaName,
                    tableNames.get(i)));
                throw GeneralUtil.nestedException(t);
            }
        }
    }
}
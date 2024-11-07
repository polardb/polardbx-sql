package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "UpdateTtlTmpTablePartStateTask")
public class UpdateTtlTmpTablePartStateTask extends BaseGmsTask {

    protected List<String> partNames;
    protected Integer beforeTtlState;
    protected Integer newTtlState;

    public UpdateTtlTmpTablePartStateTask(String schemaName, String logicalTableName, List<String> partNames,
                                          Integer beforeTtlState, Integer newTtlState) {
        super(schemaName, logicalTableName);
        this.beforeTtlState = beforeTtlState;
        this.newTtlState = newTtlState;
        this.partNames = partNames;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        try {
            if (executionContext.getParamManager().getBoolean(ConnectionParams.CHECK_ARCHIVE_PARTITION_READY)) {
                tableInfoManager.updateArcStateByFirstLevelPartitionList(schemaName, logicalTableName, partNames,
                    beforeTtlState,
                    newTtlState);
            } else {
                tableInfoManager.updateArcStateByFirstLevelPartitionList(schemaName, logicalTableName, partNames,
                    newTtlState);
            }
        } catch (Exception e) {
            throw new TtlJobRuntimeException(e);
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);

        try {
            tableInfoManager.updateArcStateByFirstLevelPartitionList(schemaName, logicalTableName, partNames,
                beforeTtlState);
        } catch (Exception e) {
            throw new TtlJobRuntimeException(e);
        }
    }

}

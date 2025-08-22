package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "AlterTableGroupAddPartitionRemoveMetaTask")
public class AlterTableGroupAddPartitionRemoveMetaTask extends BaseDdlTask {

    protected String tableGroup;

    @JSONCreator
    public AlterTableGroupAddPartitionRemoveMetaTask(String schemaName,
                                                     String tableGroup) {
        super(schemaName);
        this.tableGroup = tableGroup;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TablePartitionAccessor tpAccess = new TablePartitionAccessor();
        tpAccess.setConnection(metaDbConnection);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroup);
        for (String tableName : tableGroupConfig.getTables()) {
            tpAccess.deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        //do not support rollback
        updateSupportedCommands(true, false, metaDbConnection);
        executeImpl(metaDbConnection, executionContext);
    }

}

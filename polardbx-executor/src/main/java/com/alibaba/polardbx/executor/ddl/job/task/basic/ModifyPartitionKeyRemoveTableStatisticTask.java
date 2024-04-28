package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "ModifyPartitionKeyRemoveTableStatisticTask")
public class ModifyPartitionKeyRemoveTableStatisticTask extends BaseSyncTask {
    private final String tableName;
    private final List<String> columnList;

    public ModifyPartitionKeyRemoveTableStatisticTask(String schemaName, String tableName, List<String> columnList) {
        super(schemaName);
        this.tableName = tableName;
        this.columnList = columnList;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        if (GeneralUtil.isNotEmpty(columnList)) {
            CommonMetaChanger.alterTableColumnFinalOperationsOnSuccess(schemaName, tableName, columnList);
        }
    }

    @Override
    protected String remark() {
        return "|tableName: " + tableName;
    }
}

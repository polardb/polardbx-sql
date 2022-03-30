package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupRenamePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupSplitPartitionByHotValueJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupSplitPartitionByHotValue;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import org.apache.calcite.sql.SqlAlterTableGroup;

public class LogicalAlterTableGroupSplitPartitionByHotValueHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupSplitPartitionByHotValueHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupSplitPartitionByHotValue alterTableGroupSplitPartitionByHotValue =
            (LogicalAlterTableGroupSplitPartitionByHotValue) logicalDdlPlan;
        alterTableGroupSplitPartitionByHotValue.preparedData(executionContext);
        AlterTableGroupSplitPartitionByHotValuePreparedData preparedData =
            alterTableGroupSplitPartitionByHotValue.getPreparedData();

        ExecutableDdlJob executableDdlJob = AlterTableGroupSplitPartitionByHotValueJobFactory
            .create(alterTableGroupSplitPartitionByHotValue.relDdl,
                preparedData,
                executionContext);
        if (executableDdlJob instanceof TransientDdlJob) {
            if (preparedData.hotPartitionNameNeedChange()) {
                AlterTableGroupRenamePartitionPreparedData renamePartitionPreparedData =
                    new AlterTableGroupRenamePartitionPreparedData();
                renamePartitionPreparedData.setSchemaName(preparedData.getSchemaName());
                renamePartitionPreparedData.setTableGroupName(preparedData.getTableGroupName());
                renamePartitionPreparedData.setChangePartitionsPair(preparedData.getChangeHotPartitionNames());
                executableDdlJob = AlterTableGroupRenamePartitionJobFactory
                    .create(alterTableGroupSplitPartitionByHotValue.relDdl, renamePartitionPreparedData,
                        executionContext);
            }
        }
        return executableDdlJob;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) (logicalDdlPlan.relDdl
                .getSqlNode()),
            executionContext);
        return false;
    }

}

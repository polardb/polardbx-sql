package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupRenamePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableSplitPartitionByHotValueJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableSplitPartitionByHotValue;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;

public class LogicalAlterTableSplitPartitionByHotValueHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableSplitPartitionByHotValueHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableSplitPartitionByHotValue logicalAlterTableSplitPartitionByHotValue =
            (LogicalAlterTableSplitPartitionByHotValue) logicalDdlPlan;
        logicalAlterTableSplitPartitionByHotValue.preparedData(executionContext);
        AlterTableGroupSplitPartitionByHotValuePreparedData preparedData =
            logicalAlterTableSplitPartitionByHotValue.getPreparedData();
        ExecutableDdlJob executableDdlJob = AlterTableSplitPartitionByHotValueJobFactory
            .create(logicalAlterTableSplitPartitionByHotValue.relDdl,
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
                    .create(logicalAlterTableSplitPartitionByHotValue.relDdl, renamePartitionPreparedData,
                        executionContext);
            }
        }
        return executableDdlJob;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableSplitPartitionByHotValue logicalAlterTableSplitPartitionByHotValue =
            (LogicalAlterTableSplitPartitionByHotValue) logicalDdlPlan;
        AlterTable alterTable = (AlterTable) logicalAlterTableSplitPartitionByHotValue.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue =
            (SqlAlterTableSplitPartitionByHotValue) sqlAlterTable.getAlters().get(0);
        SqlNode partitions =
            sqlAlterTableSplitPartitionByHotValue.getPartitions();
        int splitIntoParts = ((SqlNumericLiteral) (partitions)).intValue(true);
        if (splitIntoParts <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "partitions must greater than 0");
        }
        return false;
    }

}

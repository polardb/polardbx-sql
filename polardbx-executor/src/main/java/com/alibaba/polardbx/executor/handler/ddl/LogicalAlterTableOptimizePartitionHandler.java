package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableOptimizePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableTruncatePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableOptimizePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableTruncatePartition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableOptimizePartition;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

public class LogicalAlterTableOptimizePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableOptimizePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableOptimizePartition logicalAlterTableOptimizePartition =
            (LogicalAlterTableOptimizePartition) logicalDdlPlan;
        logicalAlterTableOptimizePartition.prepareData();
        AlterTableOptimizePartitionJobFactory jobFactory =
            new AlterTableOptimizePartitionJobFactory(logicalAlterTableOptimizePartition.relDdl,
                logicalAlterTableOptimizePartition.getPreparedData(),
                executionContext);
        DdlJob ddlJob = jobFactory.create();
        return ddlJob;
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTable alterTable = (AlterTable) logicalDdlPlan.relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        assert sqlAlterTable.getAlters().size() == 1;
        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableTruncatePartition;

        SqlAlterTableOptimizePartition sqlAlterTableOptimizePartition =
            (SqlAlterTableOptimizePartition) sqlAlterTable.getAlters().get(0);

        assert sqlAlterTableOptimizePartition.getPartitions().size() >= 1;

        String schemaName = logicalDdlPlan.getSchemaName();
        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            throw new TddlNestableRuntimeException(
                "can't execute the truncate partition command in a non-auto mode database");
        }

        TableValidator.validateOptimizePartition(schemaName, logicalTableName, sqlAlterTable);

        TableValidator.validateTableNotReferenceFk(schemaName, logicalTableName, executionContext);

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(
                partitionInfo.getTableGroupId());

        AlterTableGroupUtils.alterTableGroupOptimizePartitionCheck(sqlAlterTableOptimizePartition, tableGroupConfig,
            executionContext);

        return super.validatePlan(logicalDdlPlan, executionContext);
    }

}

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.executor.ddl.job.factory.ttl.ArchivePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableArchivePartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableArchivePartitionPreparedData;
import com.taobao.tddl.common.privilege.PrivilegePoint;

import java.util.List;

/**
 * @author wumu
 */
public class LogicalAlterTableArchivePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableArchivePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableArchivePartition logicalAlterTableArchivePartition =
            (LogicalAlterTableArchivePartition) logicalDdlPlan;

        logicalAlterTableArchivePartition.prepare();

        AlterTableArchivePartitionPreparedData preparedData = logicalAlterTableArchivePartition.getPreparedData();

        final String schemaName = logicalDdlPlan.getSchemaName();
        final String primaryTableName = logicalDdlPlan.getTableName();
        final String tmpTableSchema = preparedData.getTmpTableSchema();
        final String tmpTableName = preparedData.getTmpTableName();
        final List<String> phyPartitions = preparedData.getPhyPartitionNames();
        final List<String> firstLevelPartNames = preparedData.getFirstLevelPartitionNames();

        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(tmpTableSchema, tmpTableName, PrivilegePoint.DROP, executionContext);

        executionContext.getDdlContext().setPausedPolicy(DdlState.PAUSED);

        return new ArchivePartitionJobFactory(
            schemaName,
            primaryTableName,
            tmpTableSchema,
            tmpTableName,
            phyPartitions,
            firstLevelPartNames,
            executionContext
        ).create();
    }
}

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.localpartition.RemoveLocalPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.localpartition.RepartitionLocalPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableRemoveLocalPartition;
import org.apache.calcite.sql.SqlAlterTableRepartitionLocalPartition;

public class LogicalAlterTableRemoveLocalPartitionHandler extends LogicalCommonDdlHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogicalAlterTableRemoveLocalPartitionHandler.class);

    public LogicalAlterTableRemoveLocalPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String primaryTableName = logicalDdlPlan.getTableName();

        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.DROP, executionContext);

        //3. 执行
        return new RemoveLocalPartitionJobFactory(
                schemaName,
                primaryTableName,
                logicalDdlPlan.relDdl,
                executionContext
            ).create();
    }

}
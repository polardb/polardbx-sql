package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.localpartition.ReorganizeLocalPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAllocateLocalPartition;

public class LogicalAlterTableAllocateLocalPartitionHandler extends LogicalCommonDdlHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogicalAlterTableAllocateLocalPartitionHandler.class);

    public LogicalAlterTableAllocateLocalPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();
        SqlAlterTableAllocateLocalPartition sqlAllocateLocalPartition =
            (SqlAlterTableAllocateLocalPartition) sqlAlterTable.getAlters().get(0);
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String primaryTableName = logicalDdlPlan.getTableName();

        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.ALTER, executionContext);

        //3. 执行
        ReorganizeLocalPartitionJobFactory jobFactory =
            new ReorganizeLocalPartitionJobFactory(schemaName, primaryTableName, logicalDdlPlan.relDdl, executionContext);
        return jobFactory.create();
    }

}
package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.localpartition.ExpireLocalPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExpireLocalPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableExpireLocalPartitionHandler extends LogicalCommonDdlHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogicalAlterTableExpireLocalPartitionHandler.class);

    public LogicalAlterTableExpireLocalPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();
        SqlAlterTableExpireLocalPartition expireLocalPartition =
            (SqlAlterTableExpireLocalPartition) sqlAlterTable.getAlters().get(0);
        List<SqlIdentifier> partitions = expireLocalPartition.getPartitions();
        List<String> partitionNameList = CollectionUtils.isEmpty(partitions)?
            new ArrayList<>() : partitions.stream().map(e->e.getLastName()).collect(Collectors.toList());
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String primaryTableName = logicalDdlPlan.getTableName();

        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.DROP, executionContext);

        //3. 执行
        ExpireLocalPartitionJobFactory jobFactory =
            new ExpireLocalPartitionJobFactory(schemaName, primaryTableName, partitionNameList, logicalDdlPlan.relDdl, executionContext);
        return jobFactory.create();
    }

}
package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.stream.Collectors;

public class AlterPartitionTableOptimizePartitionBuilder extends AlterTableBuilder {

    public AlterPartitionTableOptimizePartitionBuilder(DDL ddl, AlterTablePreparedData preparedData,
                                                       ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public void buildTableRuleAndTopology() {
        partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
            .getPartitionInfo(preparedData.getTableName());
        PartitionInfo tempPartitionInfo = partitionInfo.copy();
        List<PartitionSpec> targetTruncatePartitionSpecs = tempPartitionInfo.getPartitionBy().getPartitions().stream()
            .filter(o -> preparedData.getTruncatePartitionNames().contains(o.getName())).collect(
                Collectors.toList());
        tempPartitionInfo.getPartitionBy().setPartitions(targetTruncatePartitionSpecs);
        tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(tempPartitionInfo);
    }

    @Override
    protected void buildSqlTemplate() {
        final SqlNode primaryTableNode = new FastsqlParser()
            .parse("truncate table " + preparedData.getTableName(), executionContext)
            .get(0);
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(ddlPreparedData.getSchemaName(), executionContext);
        this.sqlTemplate = primaryTableNode.accept(visitor);
        this.originSqlTemplate = this.sqlTemplate;
    }

}

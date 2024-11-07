package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupOptimizePartitionItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionItemPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterTableGroupOptimizePartitionItemBuilder extends AlterTableGroupItemBuilder {

    protected AlterTableGroupOptimizePartitionItemPreparedData preparedData;

    public AlterTableGroupOptimizePartitionItemBuilder(DDL ddl,
                                                       AlterTableGroupOptimizePartitionItemPreparedData preparedData,
                                                       ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    @Override
    public void buildNewTableTopology(String schemaName, String tableName) {
        partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
            .getPartitionInfo(preparedData.getTableName());

        PartitionInfo tempPartitionInfo = partitionInfo.copy();

        PartitionByDefinition partByDef = tempPartitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        Set<String> optimizePartitionNames = preparedData.getOptimizePartitionNames();
        List<PartitionSpec> targetOptimizePartitionSpecs = new ArrayList<>();

        partByDef.getPartitions().forEach(p -> {
            boolean hasSubParts = false;
            if (subPartByDef != null && GeneralUtil.isNotEmpty(p.getSubPartitions())) {
                List<PartitionSpec> targetTruncateSubPartSpecs =
                    p.getSubPartitions().stream().filter(sp -> optimizePartitionNames.contains(sp.getName()))
                        .collect(Collectors.toList());
                p.setSubPartitions(targetTruncateSubPartSpecs);
                hasSubParts = true;
            }
            if (optimizePartitionNames.contains(p.getName()) || hasSubParts) {
                targetOptimizePartitionSpecs.add(p);
            }
        });

        partByDef.setPartitions(targetOptimizePartitionSpecs);

        partByDef.getPhysicalPartitions().clear();

        tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(tempPartitionInfo);
    }

    @Override
    protected void buildSqlTemplate() {
        final SqlNode optimizeTableNode =
            new FastsqlParser().parse("optimize table " + preparedData.getTableName(), executionContext).get(0);
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(ddlPreparedData.getSchemaName(), executionContext);
        this.originSqlTemplate = this.sqlTemplate;
        SqlNode tmpSqlTemp = optimizeTableNode.accept(visitor);
        this.sqlTemplate = tmpSqlTemp;
    }

}

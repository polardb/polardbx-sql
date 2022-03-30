package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ReorganizeLocalPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import org.apache.calcite.rel.core.DDL;

/**
 * @author guxu
 */
public class LocalPartitionPhysicalSqlBuilder extends DdlPhyPlanBuilder {


    public LocalPartitionPhysicalSqlBuilder(DDL ddl,
                                            ReorganizeLocalPartitionPreparedData preparedData,
                                            ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public void buildTableRuleAndTopology() {
        partitionInfo = OptimizerContext.getContext(ddlPreparedData.getSchemaName())
            .getPartitionInfoManager().getPartitionInfo(ddlPreparedData.getTableName());
        this.tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(partitionInfo);
    }

    @Override
    protected void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(ddlPreparedData.getTableName());
    }

}

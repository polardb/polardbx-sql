package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupOptimizePartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableOptimizePartitionBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupOptimizePartitionPreparedData;
import org.apache.calcite.rel.core.DDL;

public class AlterTableOptimizePartitionJobFactory extends AlterTableGroupOptimizePartitionJobFactory {

    public AlterTableOptimizePartitionJobFactory(DDL ddl,
                                                 AlterTableGroupOptimizePartitionPreparedData preparedData,
                                                 ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    protected AlterTableGroupOptimizePartitionBuilder getDdlPhyPlanBuilder() {
        return (AlterTableGroupOptimizePartitionBuilder) new AlterTableOptimizePartitionBuilder(ddl, preparedData,
            executionContext).build();
    }

}

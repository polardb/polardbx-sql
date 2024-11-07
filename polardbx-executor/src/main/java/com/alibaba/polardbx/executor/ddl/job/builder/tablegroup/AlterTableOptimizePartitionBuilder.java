package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupOptimizePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;

import java.util.List;

public class AlterTableOptimizePartitionBuilder extends AlterTableGroupOptimizePartitionBuilder {

    public AlterTableOptimizePartitionBuilder(DDL ddl, AlterTableGroupOptimizePartitionPreparedData preparedData,
                                              ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public List<String> getAllTableNames() {
        return ImmutableList.of(preparedData.getTableName());
    }
}

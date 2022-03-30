package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import org.apache.calcite.rel.core.DDL;

public class AlterTableGroupSplitPartitionByHotValueBuilder extends AlterTableGroupBaseBuilder {

    public AlterTableGroupSplitPartitionByHotValueBuilder(DDL ddl,
                                                          AlterTableGroupSplitPartitionByHotValuePreparedData preparedData,
                                                          ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }
}

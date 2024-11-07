package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import java.util.Set;

public class AlterTableGroupOptimizePartitionItemPreparedData extends AlterTableGroupItemPreparedData {

    private Set<String> optimizePartitionNames;

    public AlterTableGroupOptimizePartitionItemPreparedData(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    public Set<String> getOptimizePartitionNames() {
        return optimizePartitionNames;
    }

    public void setOptimizePartitionNames(Set<String> optimizePartitionNames) {
        this.optimizePartitionNames = optimizePartitionNames;
    }
}

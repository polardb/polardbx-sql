package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import java.util.Set;

/**
 * @author chenghui.lch
 */
public class AlterTableGroupOptimizePartitionPreparedData extends AlterTableGroupBasePreparedData {

    private Set<String> optimizePartitionNames;

    protected boolean isSubPartition = false;

    public AlterTableGroupOptimizePartitionPreparedData() {
    }

    public Set<String> getOptimizePartitionNames() {
        return optimizePartitionNames;
    }

    public void setOptimizePartitionNames(Set<String> optimizePartitionNames) {
        this.optimizePartitionNames = optimizePartitionNames;
    }

    public boolean isSubPartition() {
        return isSubPartition;
    }

    public void setSubPartition(boolean subPartition) {
        isSubPartition = subPartition;
    }

}

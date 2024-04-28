package org.apache.calcite.rel;

import org.apache.calcite.plan.RelMultipleTrait;

public interface RelPartitionWise extends RelMultipleTrait {

    //~ Methods ----------------------------------------------------------------
    boolean isLocalPartition();

    boolean isRemotePartition();

    int getCode();
}
package com.alibaba.polardbx.executor.mpp.operator;

public interface RemotePartitionFunction extends PartitionFunction {
    int getPartitionCount();
}

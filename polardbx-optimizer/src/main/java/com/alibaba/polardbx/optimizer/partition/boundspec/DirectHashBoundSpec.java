package com.alibaba.polardbx.optimizer.partition.boundspec;

public class DirectHashBoundSpec extends SingleValuePartitionBoundSpec {

    public DirectHashBoundSpec() {
    }

    public DirectHashBoundSpec(DirectHashBoundSpec hashBoundSpec) {
        super(hashBoundSpec);
    }

    @Override
    public PartitionBoundSpec copy() {
        return new DirectHashBoundSpec(this);
    }
}

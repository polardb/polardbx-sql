package com.alibaba.polardbx.optimizer.partition.boundspec;

/**
 * @author chenghui.lch
 */
public class CoHashBoundSpec extends SingleValuePartitionBoundSpec {

    public CoHashBoundSpec() {
    }

    public CoHashBoundSpec(CoHashBoundSpec hashBoundSpec) {
        super(hashBoundSpec);
    }

    @Override
    public PartitionBoundSpec copy() {
        return new CoHashBoundSpec(this);
    }
}

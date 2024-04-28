package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

public class PartialHyperLogLogAccumulator extends HyperLogLogAccumulator {

    public PartialHyperLogLogAccumulator(Aggregator aggregator, DataType[] rowInputType, int capacity) {
        super(aggregator, rowInputType, capacity);
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            bb.writeByteArray(groupState.getHll(groupId));
        }
    }
}

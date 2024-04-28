package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

import java.util.ArrayList;
import java.util.List;

public class WrapAggregatorAccumulator implements Accumulator {

    private Aggregator aggregator;

    private DataType[] inputTypes;

    private DataType aggValueType;

    private List<Aggregator> aggregatorList;

    public WrapAggregatorAccumulator(Aggregator aggregator, DataType[] rowInputType, DataType aggValueType,
                                     int capacity) {
        this.aggregator = aggregator;
        int[] inputColumnIndexes = aggregator.getInputColumnIndexes();
        this.inputTypes = new DataType[inputColumnIndexes.length];
        this.aggValueType = aggValueType;
        for (int i = 0; i < inputTypes.length; i++) {
            inputTypes[i] = rowInputType[inputColumnIndexes[i]];
        }
        this.aggregatorList = new ArrayList<>(capacity);
    }

    public DataType[] getInputTypes() {
        return inputTypes;
    }

    /**
     * Append a new group with initial value
     */
    public void appendInitValue() {
        aggregatorList.add(aggregator.getNewForAccumulator());
    }

    /**
     * Accumulate a value into group
     */
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        aggregatorList.get(groupId).aggregate(inputChunk.rowAt(position));
    }

    /**
     * Get the aggregated result
     */
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeObject(aggValueType.convertFrom(aggregatorList.get(groupId).value()));
    }

    /**
     * Estimate the memory consumption
     */
    public long estimateSize() {
        return aggregatorList.size() * 64;
    }

}

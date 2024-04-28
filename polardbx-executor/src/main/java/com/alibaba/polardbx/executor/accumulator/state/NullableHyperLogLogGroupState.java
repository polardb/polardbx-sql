package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.executor.accumulator.datastruct.BooleanSegmentArrayList;
import com.alibaba.polardbx.executor.accumulator.datastruct.ObjectSegmentArrayList;
import com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil;
import org.openjdk.jol.info.ClassLayout;

public class NullableHyperLogLogGroupState implements GroupState {
    private static final long INSTANCE_SIZE =
        ClassLayout.parseClass(NullableHyperLogLogGroupState.class).instanceSize();

    /**
     * The null value bitmap.
     */
    private final BooleanSegmentArrayList valueIsNull;

    /**
     * Disaggregated stored decimal objects.
     */
    private final ObjectSegmentArrayList<byte[]> hllList;

    private final int capacity;

    public NullableHyperLogLogGroupState(int capacity) {
        this.capacity = capacity;
        this.valueIsNull = new BooleanSegmentArrayList(capacity);
        this.hllList = new ObjectSegmentArrayList(capacity, byte[].class);
    }

    public void set(int groupId, byte[] value) {
        valueIsNull.set(groupId, false);
        hllList.set(groupId, value);
    }

    public void append() {
        hllList.add(null);
        valueIsNull.add(true);
    }

    public boolean isNull(int groupId) {
        return valueIsNull.get(groupId);
    }

    public Long get(int groupId) {
        return HyperLogLogUtil.getCardinality(hllList.get(groupId));
    }

    public byte[] getHll(int groupId) {
        return hllList.get(groupId);
    }

    @Override
    public long estimateSize() {
        long size = INSTANCE_SIZE + hllList.estimateSize() + valueIsNull.estimateSize();
        return size;
    }
}

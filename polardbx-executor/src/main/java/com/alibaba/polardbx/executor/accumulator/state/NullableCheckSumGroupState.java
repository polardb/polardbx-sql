package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.common.IOrderInvariantHash;
import com.alibaba.polardbx.executor.accumulator.datastruct.BooleanSegmentArrayList;
import com.alibaba.polardbx.executor.accumulator.datastruct.ObjectSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

public class NullableCheckSumGroupState implements GroupState {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(NullableCheckSumGroupState.class).instanceSize();

    /**
     * The null value bitmap.
     */
    private final BooleanSegmentArrayList valueIsNull;

    /**
     * Disaggregated stored decimal objects.
     */
    private final ObjectSegmentArrayList<IOrderInvariantHash> hasherList;

    private final int capacity;

    public NullableCheckSumGroupState(int capacity, Class clazz) {
        this.capacity = capacity;
        this.valueIsNull = new BooleanSegmentArrayList(capacity);
        this.hasherList = new ObjectSegmentArrayList(capacity, clazz);
    }

    public void set(int groupId, IOrderInvariantHash value) {
        valueIsNull.set(groupId, false);
        hasherList.set(groupId, value);
    }

    public void appendNull() {
        hasherList.add(null);
        valueIsNull.add(true);
    }

    public boolean isNull(int groupId) {
        return valueIsNull.get(groupId);
    }

    public Long get(int groupId) {
        return hasherList.get(groupId).getResult();
    }

    public IOrderInvariantHash getHasher(int groupId) {
        return hasherList.get(groupId);
    }

    @Override
    public long estimateSize() {
        long size = INSTANCE_SIZE + hasherList.estimateSize() + valueIsNull.estimateSize();
        return size;
    }
}

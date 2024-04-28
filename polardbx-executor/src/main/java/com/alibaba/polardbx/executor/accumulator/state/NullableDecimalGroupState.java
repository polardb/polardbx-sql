package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.accumulator.datastruct.BooleanSegmentArrayList;
import com.alibaba.polardbx.executor.accumulator.datastruct.ObjectSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

/**
 * Nullable Decimal Group State
 *
 * @author Eric Fu
 */
public class NullableDecimalGroupState implements GroupState {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(NullableDecimalGroupState.class).instanceSize();

    /**
     * The null value bitmap.
     */
    private final BooleanSegmentArrayList valueIsNull;

    /**
     * Disaggregated stored decimal objects.
     */
    private final ObjectSegmentArrayList<Decimal> decimals;

    public NullableDecimalGroupState(int capacity) {
        this.valueIsNull = new BooleanSegmentArrayList(capacity);
        this.decimals = new ObjectSegmentArrayList(capacity, Decimal.class);
    }

    public NullableDecimalGroupState(
        BooleanSegmentArrayList valueIsNull,
        ObjectSegmentArrayList<Decimal> decimals) {
        this.valueIsNull = valueIsNull;
        this.decimals = decimals;
    }

    public void set(int groupId, Decimal value) {
        valueIsNull.set(groupId, false);
        decimals.set(groupId, value);
    }

    public void appendNull() {
        decimals.add(null);
        valueIsNull.add(true);
    }

    public boolean isNull(int groupId) {
        return valueIsNull.get(groupId);
    }

    public Decimal get(int groupId) {
        return decimals.get(groupId);
    }

    @Override
    public long estimateSize() {
        long size = INSTANCE_SIZE + decimals.estimateSize() + valueIsNull.estimateSize();
        return size;
    }
}

package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.accumulator.datastruct.LongSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

/**
 * Nullable BigDecimal & Long Group State
 *
 * @author Eric Fu
 */
public class NullableDecimalLongGroupState extends NullableDecimalGroupState {

    private static final long INSTANCE_SIZE =
        ClassLayout.parseClass(NullableDecimalLongGroupState.class).instanceSize();

    private final LongSegmentArrayList longValues;

    public NullableDecimalLongGroupState(int capacity) {
        super(capacity);
        this.longValues = new LongSegmentArrayList(capacity);
    }

    public void set(int groupId, Decimal decimalVal, long longValue) {
        super.set(groupId, decimalVal);
        longValues.set(groupId, longValue);
    }

    @Override
    public void appendNull() {
        super.appendNull();
        longValues.add(0);
    }

    @Override
    public boolean isNull(int groupId) {
        return super.isNull(groupId);
    }

    public Decimal getDecimal(int groupId) {
        return super.get(groupId);
    }

    public long getLong(int groupId) {
        return longValues.get(groupId);
    }

    @Override
    public long estimateSize() {
        return super.estimateSize() + longValues.estimateSize();
    }
}

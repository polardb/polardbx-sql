package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.executor.accumulator.datastruct.BooleanSegmentArrayList;
import com.alibaba.polardbx.executor.accumulator.datastruct.DoubleSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

/**
 * Nullable Double Group State
 *
 * @author Eric Fu
 */
public class NullableDoubleGroupState implements GroupState {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(NullableDoubleGroupState.class).instanceSize();

    private final DoubleSegmentArrayList values;
    private final BooleanSegmentArrayList valueIsNull;

    public NullableDoubleGroupState(int capacity) {
        this.values = new DoubleSegmentArrayList(capacity);
        this.valueIsNull = new BooleanSegmentArrayList(capacity);
    }

    public void set(int groupId, double value) {
        values.set(groupId, value);
        valueIsNull.set(groupId, false);
    }

    public void append(double value) {
        values.add(value);
        valueIsNull.add(false);
    }

    public void appendNull() {
        values.add(0L);
        valueIsNull.add(true);
    }

    public boolean isNull(int groupId) {
        return valueIsNull.get(groupId);
    }

    public double get(int groupId) {
        return values.get(groupId);
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + values.estimateSize() + valueIsNull.estimateSize();
    }
}

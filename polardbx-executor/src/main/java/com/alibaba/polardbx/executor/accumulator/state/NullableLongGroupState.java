package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.executor.accumulator.datastruct.BooleanSegmentArrayList;
import com.alibaba.polardbx.executor.accumulator.datastruct.LongSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

/**
 * Nullable Long Group State
 *
 * @author Eric Fu
 */
public class NullableLongGroupState implements GroupState {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(NullableLongGroupState.class).instanceSize();

    private final LongSegmentArrayList values;
    private final BooleanSegmentArrayList valueIsNull;

    public NullableLongGroupState(int capacity) {
        this.values = new LongSegmentArrayList(capacity);
        this.valueIsNull = new BooleanSegmentArrayList(capacity);
    }

    public void set(int groupId, long value) {
        values.set(groupId, value);
        valueIsNull.set(groupId, false);
    }

    public void append(long value) {
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

    public long get(int groupId) {
        return values.get(groupId);
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + values.estimateSize() + valueIsNull.estimateSize();
    }
}

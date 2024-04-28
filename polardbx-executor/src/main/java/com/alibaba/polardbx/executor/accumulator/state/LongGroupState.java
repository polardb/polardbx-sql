package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.executor.accumulator.datastruct.LongSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

/**
 * Non-Nullable Long Group State
 *
 * @author Eric Fu
 */
public class LongGroupState implements GroupState {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(LongGroupState.class).instanceSize();

    private final LongSegmentArrayList values;

    public LongGroupState(int capacity) {
        this.values = new LongSegmentArrayList(capacity);
    }

    public void set(int groupId, long value) {
        values.set(groupId, value);
    }

    public void append(long value) {
        values.add(value);
    }

    public long get(int groupId) {
        return values.get(groupId);
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + values.estimateSize();
    }
}

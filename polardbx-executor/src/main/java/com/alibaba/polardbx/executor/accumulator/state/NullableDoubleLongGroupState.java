/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.executor.accumulator.datastruct.BooleanSegmentArrayList;
import com.alibaba.polardbx.executor.accumulator.datastruct.DoubleSegmentArrayList;
import com.alibaba.polardbx.executor.accumulator.datastruct.LongSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

/**
 * Nullable Double & Long Group State
 *
 * @author Eric Fu
 */
public class NullableDoubleLongGroupState implements GroupState {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(NullableDoubleLongGroupState.class).instanceSize();

    private final DoubleSegmentArrayList doubleValues;
    private final LongSegmentArrayList longValues;
    private final BooleanSegmentArrayList valueIsNull;

    public NullableDoubleLongGroupState(int capacity) {
        this.doubleValues = new DoubleSegmentArrayList(capacity);
        this.longValues = new LongSegmentArrayList(capacity);
        this.valueIsNull = new BooleanSegmentArrayList(capacity);
    }

    public void set(int groupId, double doubleValue, long longValue) {
        doubleValues.set(groupId, doubleValue);
        longValues.set(groupId, longValue);
        valueIsNull.set(groupId, false);
    }

    public void append(double doubleValue, long longValue) {
        doubleValues.add(doubleValue);
        longValues.add(longValue);
        valueIsNull.add(false);
    }

    public void appendNull() {
        doubleValues.add(0);
        longValues.add(0);
        valueIsNull.add(true);
    }

    public boolean isNull(int groupId) {
        return valueIsNull.get(groupId);
    }

    public double getDouble(int groupId) {
        return doubleValues.get(groupId);
    }

    public long getLong(int groupId) {
        return longValues.get(groupId);
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + doubleValues.estimateSize() + longValues.estimateSize() + valueIsNull.estimateSize();
    }
}

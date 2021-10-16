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

package com.alibaba.polardbx.optimizer.state;

import com.alibaba.polardbx.optimizer.datastruct.BooleanSegmentArrayList;
import com.alibaba.polardbx.optimizer.datastruct.ShortSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

public class NullableShortGroupState implements GroupState {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(NullableShortGroupState.class).instanceSize();

    private final ShortSegmentArrayList values;
    private final BooleanSegmentArrayList valueIsNull;

    public NullableShortGroupState(int capacity) {
        this.values = new ShortSegmentArrayList(capacity);
        this.valueIsNull = new BooleanSegmentArrayList(capacity);
    }

    public void set(int groupId, short value) {
        values.set(groupId, value);
        valueIsNull.set(groupId, false);
    }

    public void setNull(int groupId) {
        valueIsNull.set(groupId, true);
    }

    public void append(short value) {
        values.add(value);
        valueIsNull.add(false);
    }

    public void appendNull() {
        values.add((short) 0);
        valueIsNull.add(true);
    }

    public boolean isNull(int groupId) {
        return valueIsNull.get(groupId);
    }

    public short get(int groupId) {
        return values.get(groupId);
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + values.estimateSize() + valueIsNull.estimateSize();
    }

}

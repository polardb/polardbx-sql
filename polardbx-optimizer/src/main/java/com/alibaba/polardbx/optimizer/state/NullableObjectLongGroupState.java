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

import com.alibaba.polardbx.optimizer.datastruct.LongSegmentArrayList;
import com.alibaba.polardbx.optimizer.datastruct.ObjectSegmentArrayList;
import org.openjdk.jol.info.ClassLayout;

public class NullableObjectLongGroupState implements GroupState {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(NullableObjectLongGroupState.class).instanceSize();

    private final ObjectSegmentArrayList objectValues;
    private final LongSegmentArrayList longValues;

    public NullableObjectLongGroupState(int capacity) {
        this.objectValues = new ObjectSegmentArrayList(capacity);
        this.longValues = new LongSegmentArrayList(capacity);
    }

    public void set(int groupId, Object value, long longValue) {
        objectValues.set(groupId, value);
        longValues.set(groupId, longValue);
    }

    public void append(Object value, long longValue) {
        objectValues.add(value);
        longValues.add(longValue);
    }

    public void appendNull() {
        objectValues.add(null);
        longValues.add(0);
    }

    public void setNull(int groupId) {
        objectValues.set(groupId, null);
    }

    public boolean isNull(int groupId) {
        return objectValues.get(groupId) == null;
    }

    public Object getObject(int groupId) {
        return objectValues.get(groupId);
    }

    public long getLong(int groupId) {
        return longValues.get(groupId);
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + objectValues.estimateSize() + longValues.estimateSize();
    }
}

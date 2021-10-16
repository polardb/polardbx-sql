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

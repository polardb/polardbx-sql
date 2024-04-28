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

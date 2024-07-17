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
import com.alibaba.polardbx.executor.accumulator.datastruct.ObjectSegmentArrayList;
import com.alibaba.polardbx.executor.statistic.ndv.HyperLogLogUtil;
import org.openjdk.jol.info.ClassLayout;

public class NullableHyperLogLogGroupState implements GroupState {
    private static final long INSTANCE_SIZE =
        ClassLayout.parseClass(NullableHyperLogLogGroupState.class).instanceSize();

    /**
     * The null value bitmap.
     */
    private final BooleanSegmentArrayList valueIsNull;

    /**
     * Disaggregated stored decimal objects.
     */
    private final ObjectSegmentArrayList<byte[]> hllList;

    private final int capacity;

    public NullableHyperLogLogGroupState(int capacity) {
        this.capacity = capacity;
        this.valueIsNull = new BooleanSegmentArrayList(capacity);
        this.hllList = new ObjectSegmentArrayList(capacity, byte[].class);
    }

    public void set(int groupId, byte[] value) {
        valueIsNull.set(groupId, false);
        hllList.set(groupId, value);
    }

    public void append() {
        hllList.add(null);
        valueIsNull.add(true);
    }

    public boolean isNull(int groupId) {
        return valueIsNull.get(groupId);
    }

    public Long get(int groupId) {
        return HyperLogLogUtil.getCardinality(hllList.get(groupId));
    }

    public byte[] getHll(int groupId) {
        return hllList.get(groupId);
    }

    @Override
    public long estimateSize() {
        long size = INSTANCE_SIZE + hllList.estimateSize() + valueIsNull.estimateSize();
        return size;
    }
}

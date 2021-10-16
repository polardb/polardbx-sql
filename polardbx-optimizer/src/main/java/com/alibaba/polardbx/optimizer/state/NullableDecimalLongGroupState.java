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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.datastruct.LongSegmentArrayList;
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
    public void setNull(int groupId) {
        super.setNull(groupId);
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

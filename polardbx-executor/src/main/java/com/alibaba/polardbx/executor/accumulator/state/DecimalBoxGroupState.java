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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBox;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.accumulator.datastruct.LongSegmentArrayList;
import com.alibaba.polardbx.executor.accumulator.datastruct.ObjectSegmentArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class DecimalBoxGroupState implements GroupState {

    private static final byte IS_NULL = 0;
    private static final byte IS_NORMAL_DECIMAL = 1;
    private static final byte IS_DECIMAL_BOX = 2;
    private static final byte IS_DECIMAL_64 = 3;
    private static final byte IS_DECIMAL_128 = 4;

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DecimalBoxGroupState.class).instanceSize();

    private final LongSegmentArrayList decimal64List;
    private final LongSegmentArrayList decimal128HighList;
    /**
     * Disaggregated stored decimal objects.
     */
    private final ObjectSegmentArrayList<DecimalBox> decimalBoxes;
    private final ObjectSegmentArrayList<Decimal> decimals;

    private final ByteArrayList flags;

    private final int capacity;
    private int scale;

    protected boolean isNormalDecimal = false;

    public DecimalBoxGroupState(int capacity, int scale) {
        this.capacity = capacity;
        this.scale = scale;
        this.decimal64List = new LongSegmentArrayList(capacity);
        this.decimal128HighList = new LongSegmentArrayList(capacity);
        this.decimalBoxes = new ObjectSegmentArrayList<>(capacity, DecimalBox.class);
        this.decimals = new ObjectSegmentArrayList<>(capacity, Decimal.class);

        this.flags = new ByteArrayList(capacity);
    }

    public void set(int groupId, DecimalBox value) {
        decimalBoxes.set(groupId, value);
        flags.set(groupId, IS_DECIMAL_BOX);
    }

    public void set(int groupId, long value) {
        decimal64List.set(groupId, value);
        flags.set(groupId, IS_DECIMAL_64);
    }

    public void set(int groupId, long decimal128Low, long decimal128High) {
        decimal64List.set(groupId, decimal128Low);
        decimal128HighList.set(groupId, decimal128High);
        flags.set(groupId, IS_DECIMAL_128);
    }

    public void set(int groupId, Decimal value) {
        decimals.set(groupId, value);
        flags.set(groupId, IS_NORMAL_DECIMAL);
    }

    public void appendNull() {
        decimal64List.add(0L);
        decimal128HighList.add(0L);
        decimalBoxes.add(null);
        decimals.add(null);
        flags.add(IS_NULL);
    }

    public boolean isNull(int groupId) {
        return flags.getByte(groupId) == IS_NULL;
    }

    public boolean isDecimalBox(int groupId) {
        return flags.getByte(groupId) == IS_DECIMAL_BOX;
    }

    public boolean isDecimal64(int groupId) {
        return flags.getByte(groupId) == IS_DECIMAL_64;
    }

    public boolean isDecimal128(int groupId) {
        return flags.getByte(groupId) == IS_DECIMAL_128;
    }

    public boolean isNormalDecimal(int groupId) {
        return flags.getByte(groupId) == IS_NORMAL_DECIMAL;
    }

    public byte getFlag(int groupId) {
        return flags.getByte(groupId);
    }

    public long getLong(int groupId) {
        return decimal64List.get(groupId);
    }

    public long getDecimal128Low(int groupId) {
        return decimal64List.get(groupId);
    }

    public long getDecimal128High(int groupId) {
        return decimal128HighList.get(groupId);
    }

    public Decimal getDecimal(int groupId) {
        switch (flags.getByte(groupId)) {
        case IS_DECIMAL_128:
            DecimalStructure buffer = new DecimalStructure();
            DecimalStructure result = new DecimalStructure();
            FastDecimalUtils.setDecimal128WithScale(buffer, result,
                getDecimal128Low(groupId), getDecimal128High(groupId), scale);
            return new Decimal(result);
        case IS_DECIMAL_64:
            return new Decimal(getLong(groupId), scale);
        case IS_NORMAL_DECIMAL:
            return decimals.get(groupId);
        case IS_DECIMAL_BOX:
            return decimalBoxes.get(groupId).getDecimalSum();
        default:
            throw new IllegalStateException("Current flag: " + flags.getByte(groupId));
        }
    }

    public DecimalBox getBox(int groupId) {
        return decimalBoxes.get(groupId);
    }

    public boolean isNormalDecimal() {
        return isNormalDecimal;
    }

    @Override
    public long estimateSize() {
        long size = INSTANCE_SIZE + decimalBoxes.estimateSize() + decimal64List.estimateSize() +
            decimal128HighList.estimateSize() + flags.size();
        return size;
    }

    public void toNormalDecimalGroupState() {
        if (isNormalDecimal) {
            return;
        }
        this.isNormalDecimal = true;
        for (int i = 0; i < flags.size(); i++) {
            if (isDecimal64(i)) {
                // long -> decimal
                Decimal decimal = new Decimal(getLong(i), scale);
                set(i, decimal);
            } else if (isDecimal128(i)) {
                DecimalStructure buffer = new DecimalStructure();
                DecimalStructure result = new DecimalStructure();
                FastDecimalUtils.setDecimal128WithScale(buffer, result,
                    getDecimal128Low(i), getDecimal128High(i), scale);
                set(i, new Decimal(result));
            } else if (isDecimalBox(i)) {
                // decimalBox -> decimal
                Decimal decimal = getBox(i).getDecimalSum();
                set(i, decimal);
            }
        }
    }

    public void rescale(int newScale) {
        if (this.scale == newScale) {
            return;
        }

        // do nothing just update scale
        this.scale = newScale;
        for (int i = 0; i < flags.size(); i++) {
            if (isDecimalBox(i)) {
                getBox(i).setScale(newScale);
            }
        }
    }
}

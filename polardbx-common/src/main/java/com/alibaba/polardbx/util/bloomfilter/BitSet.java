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

package com.alibaba.polardbx.util.bloomfilter;

import com.alibaba.polardbx.common.utils.UnsafeUtil;

import java.util.Arrays;


class BitSet {

    private static final int BASE = UnsafeUtil.UNSAFE.arrayBaseOffset(long[].class);

    private final long[] data;

    private static final int SHIFT;

    static {
        int scale = UnsafeUtil.UNSAFE.arrayIndexScale(long[].class);
        if ((scale & (scale - 1)) != 0) {
            throw new Error("data type scale not a power of two");
        }
        SHIFT = 31 - Integer.numberOfLeadingZeros(scale);
    }

    BitSet(long bits) {
        this(new long[(int) Math.ceil((double) bits / (double) Long.SIZE)]);
    }

    BitSet(long[] data) {
        assert data.length > 0 : "data length is zero!";
        this.data = data;
    }

    public boolean set(int index) {
        if (get(index)) {
            return false;
        }
        int longIndex = (int) (index >>> 6);
        long mask = 1L << index;

        long oldValue;
        long newValue;
        do {
            oldValue = data[longIndex];
            newValue = oldValue | mask;
            if (oldValue == newValue) {
                return false;
            }
        } while (!compareAndSet(longIndex, oldValue, newValue));
        return true;
    }

    private boolean compareAndSet(int i, long oldValue, long newValue) {
        return UnsafeUtil.UNSAFE.compareAndSwapLong(data, byteOffset(i), oldValue, newValue);
    }

    private static long byteOffset(int i) {
        return ((long) i << SHIFT) + BASE;
    }

    public boolean get(int index) {
        return (data[index >>> 6] & (1L << index)) != 0;
    }

    public long[] getData() {
        return data;
    }

    public void putAll(BitSet array) {
        assert data.length == array.data.length :
            "BitArrays must be of equal length (" + data.length + "!= " + array.data.length + ")";
        for (int i = 0; i < data.length; i++) {
            data[i] |= array.data[i];
        }
    }

    public void clear() {
        Arrays.fill(data, 0);
    }
}

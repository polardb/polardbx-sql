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

package com.alibaba.polardbx.sequence;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 序列区间
 *
 * @author nianbing
 */
public class SequenceRange {

    private final long min;
    private final long max;

    private final AtomicLong value;

    private volatile boolean over = false;

    public SequenceRange(long min, long max) {
        this.min = min;
        this.max = max;
        this.value = new AtomicLong(min);
    }

    public long getBatch(int size) {
        if (over) {
            return -1;
        }

        long currentValue = value.getAndAdd(size) + size - 1;
        if (currentValue > max) {
            over = true;
            return -1;
        }

        return currentValue;
    }

    public long getAndIncrement() {
        if (over) {
            return -1;
        }

        long currentValue = value.getAndIncrement();
        if (currentValue > max) {
            over = true;
            return -1;
        }

        return currentValue;
    }

    public long[] getCurrentAndMax() {
        if (over) {
            return null;
        }

        long[] currentAndMax = new long[2];

        long currentValue = value.get();
        if (currentValue > max) {
            over = true;
            return null;
        }

        currentAndMax[0] = currentValue;
        currentAndMax[1] = max;

        return currentAndMax;
    }

    public boolean updateValue(long expect, long update) {
        if (over) {
            return true;
        }
        return value.compareAndSet(expect, update);
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public boolean isOver() {
        return over;
    }

    public void setOver(boolean over) {
        this.over = over;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("max: ").append(max).append(", min: ").append(min).append(", value: ").append(value);
        return sb.toString();
    }

}

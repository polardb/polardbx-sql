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

package com.alibaba.polardbx.optimizer.datastruct;

import com.alibaba.polardbx.common.utils.MathUtils;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

/**
 * Boolean Segmented Array List
 *
 * @author Eric Fu
 */
public class BooleanSegmentArrayList implements SegmentArrayList {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(BooleanSegmentArrayList.class).instanceSize();

    private static final int SEGMENT_SIZE = 1024;

    private List<boolean[]> arrays;

    private int size;
    private int capacity;

    public BooleanSegmentArrayList(int capacity) {
        this.arrays = new ArrayList<>(MathUtils.ceilDiv(capacity, SEGMENT_SIZE));
        this.size = 0;
        this.capacity = arrays.size() * SEGMENT_SIZE;
    }

    public void add(boolean value) {
        if (size == capacity) {
            grow();
        }
        arrays.get(arrays.size() - 1)[size++ % SEGMENT_SIZE] = value;
    }

    public void set(int index, boolean value) {
        assert index < size;
        arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE] = value;
    }

    public boolean get(int index) {
        return arrays.get(index / SEGMENT_SIZE)[index % SEGMENT_SIZE];
    }

    private void grow() {
        arrays.add(new boolean[SEGMENT_SIZE]);
        capacity += SEGMENT_SIZE;
    }

    public int size() {
        return size;
    }

    @Override
    public long estimateSize() {
        return INSTANCE_SIZE + (long) arrays.size() * SEGMENT_SIZE;
    }
}

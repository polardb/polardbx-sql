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

package com.alibaba.polardbx.executor.operator.util;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.Hash;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * An concurrent hash table implementation for marking position
 */
public class ConcurrentRawDirectHashTable implements Hash {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ConcurrentRawDirectHashTable.class).instanceSize();

    public static final int NOT_EXISTS = -1;

    public static final int EXISTS = 1;

    /**
     * The array of keys (buckets)
     */
    private final AtomicIntegerArray keys;
    /**
     * The current table size.
     */
    private final int n;

    public ConcurrentRawDirectHashTable(int size) {
        Preconditions.checkArgument(size >= 0, "The number of elements must be non-negative");

        this.n = size;

        int[] keys = new int[n];
        Arrays.fill(keys, NOT_EXISTS);
        this.keys = new AtomicIntegerArray(keys);
    }

    /**
     * Mark assigned position
     *
     * @return true if this slot was empty, false otherwise
     */
    public boolean markAndGet(int pos) {
        if (!keys.compareAndSet(pos, NOT_EXISTS, EXISTS)) {
            return false;
        } else {
            return true;
        }
    }

    public void rawMark(int pos) {
        keys.set(pos, EXISTS);
    }

    /**
     * Whether this position has been set yet
     *
     * @return return true if this position has been set
     */
    public boolean hasSet(int pos) {
        return keys.get(pos) == EXISTS;
    }

    public long estimateSize() {
        return INSTANCE_SIZE + keys.length() * Integer.BYTES;
    }

    public int size() {
        return keys.length();
    }

    public List<Integer> getNotMarkedPosition() {
        List<Integer> notMarkedPosition = new ArrayList<>();
        int length = keys.length();
        for (int pos = 0; pos < length; ++pos) {
            if (keys.get(pos) == NOT_EXISTS) {
                notMarkedPosition.add(pos);
            }
        }
        return notMarkedPosition;
    }

    public static long estimatedSizeInBytes(int size) {
        return INSTANCE_SIZE + size * Integer.BYTES;
    }
}

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
import it.unimi.dsi.fastutil.HashCommon;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * An concurrent hash table implementation mapping a hash code to an integer value
 *
 */
public class ConcurrentRawHashTable implements Hash {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ConcurrentRawHashTable.class).instanceSize();

    public static final int NOT_EXISTS = -1;

    /**
     * The array of keys (buckets)
     */
    private final AtomicIntegerArray keys;
    /**
     * The mask for wrapping a position counter
     */
    private final int mask;
    /**
     * The current table size.
     */
    private final int n;

    public ConcurrentRawHashTable(int size, float loadFactor) {
        Preconditions.checkArgument(loadFactor > 0 && loadFactor <= 1,
            "Load factor must be greater than 0 and smaller than or equal to 1");
        Preconditions.checkArgument(size >= 0, "The number of elements must be non-negative");

        this.n = HashCommon.arraySize(size, loadFactor);
        this.mask = n - 1;

        int[] keys = new int[n];
        Arrays.fill(keys, NOT_EXISTS);
        this.keys = new AtomicIntegerArray(keys);
    }

    public ConcurrentRawHashTable(int size) {
        this(size, selectLoadFactor(size));
    }

    private static float selectLoadFactor(int size) {
        if (size >= 100_000_000) { // more than 100M records
            return DEFAULT_LOAD_FACTOR;
        } else if (size >= 10_000_000) { // more than 10M records
            return FAST_LOAD_FACTOR;
        } else { // otherwise
            return VERY_FAST_LOAD_FACTOR;
        }
    }

    /**
     * Put a hash code to value
     *
     * @return the replaced value, or NOT_EXISTS if this slot was empty
     */
    public int put(int value, int hash) {
        final int h = insert(value, hash);
        if (h < 0) {
            return NOT_EXISTS;
        }
        return keys.getAndSet(h, value);
    }

    private int insert(int value, int hash) {
        final AtomicIntegerArray keys = this.keys;
        while (true) {
            int h = HashCommon.mix(hash) & mask;
            int k = keys.get(h);

            if (k != NOT_EXISTS) {
                return h; // found a used slot
            }
            // otherwise, insert this value
            if (!keys.compareAndSet(h, NOT_EXISTS, value)) {
                continue; // retry
            }
            return -1; // found an empty slot
        }
    }

    /**
     * Get a value by hash code
     *
     * @return the mapped value of given hash code, or NOT_EXISTS if not found
     */
    public int get(int hash) {
        final AtomicIntegerArray keys = this.keys;
        int h = HashCommon.mix(hash) & mask;
        return keys.get(h);
    }

    public long estimateSize() {
        return INSTANCE_SIZE + keys.length() * Integer.BYTES;
    }
}

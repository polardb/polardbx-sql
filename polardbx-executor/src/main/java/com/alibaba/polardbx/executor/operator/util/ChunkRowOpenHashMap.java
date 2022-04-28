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
import com.alibaba.polardbx.executor.chunk.Chunk;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Chunk Row to Position (int) open-addressing hash map
 *
 */
public class ChunkRowOpenHashMap implements Hash {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ChunkRowOpenHashMap.class).instanceSize();

    public static final int NOT_EXISTS = -1;

    /**
     * The referenced chunks index of the keys
     */
    private final ChunksIndex chunks;

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
    /**
     * Number of entries in the set (including the key zero, if present).
     */
    private int size;
    /**
     * The acceptable load factor.
     */
    private final float f;
    /**
     * Threshold after which we rehash. It must be the table size times {@link #f}.
     */
    private final int maxFill;

    public ChunkRowOpenHashMap(ChunksIndex chunks, int expectedSize, float loadFactor) {
        Preconditions.checkArgument(loadFactor > 0 && loadFactor <= 1,
            "Load factor must be greater than 0 and smaller than or equal to 1");
        Preconditions.checkArgument(expectedSize >= 0, "The expected number of elements must be non-negative");

        this.chunks = chunks;

        this.f = loadFactor;
        this.n = HashCommon.arraySize(expectedSize, loadFactor);
        this.mask = n - 1;
        this.maxFill = HashCommon.maxFill(n, loadFactor);

        int[] keys = new int[n];
        Arrays.fill(keys, NOT_EXISTS);
        this.keys = new AtomicIntegerArray(keys);
    }

    public ChunkRowOpenHashMap(ChunksIndex chunks, int expectedSize) {
        this(chunks, expectedSize, selectLoadFactor(expectedSize));
    }

    private static float selectLoadFactor(int expectedSize) {
        if (expectedSize >= 100_000_000) { // more than 100M records
            return DEFAULT_LOAD_FACTOR;
        } else if (expectedSize >= 10_000_000) { // more than 10M records
            return FAST_LOAD_FACTOR;
        } else { // otherwise
            return VERY_FAST_LOAD_FACTOR;
        }
    }

    public ChunkRowOpenHashMap(ChunksIndex chunks) {
        this(chunks, chunks.getPositionCount());
    }

    /**
     * Put a row from the referenced ChunksIndex into hash map
     */
    public int put(int position, int hashCode) {
        final int h = insert(position, hashCode);
        if (h < 0) {
            return NOT_EXISTS;
        }
        return keys.getAndSet(h, position);
    }

    private int insert(int position, int hashCode) {
        final AtomicIntegerArray keys = this.keys;
        while (true) {
            int h = HashCommon.mix(hashCode) & mask;
            int k = keys.get(h);

            if (k != NOT_EXISTS) {
                if (chunks.equals(k, position)) {
                    return h;
                }
                // Open-address probing
                while ((k = keys.get(h = (h + 1) & mask)) != NOT_EXISTS) {
                    if (chunks.equals(k, position)) {
                        return h;
                    }
                }
            }
            // otherwise, insert this position
            if (!keys.compareAndSet(h, NOT_EXISTS, position)) {
                continue; // retry
            }

            if (size++ >= maxFill) {
                throw new AssertionError("hash table size exceeds limit");
            }
            return -1;
        }
    }

    /**
     * Get a matched row position from hash map
     */
    public int get(Chunk chunk, int position) {
        final AtomicIntegerArray keys = this.keys;
        int h = HashCommon.mix(chunk.hashCode(position)) & mask;
        int k = keys.get(h);

        if (k == NOT_EXISTS) {
            return NOT_EXISTS;
        } else if (chunks.equals(k, chunk, position)) {
            return k;
        }

        // Open-address probing. Note that there is always an unused entry
        while (true) {
            if ((k = keys.get(h = (h + 1) & mask)) == NOT_EXISTS) {
                return NOT_EXISTS;
            } else if (chunks.equals(k, chunk, position)) {
                return k;
            }
        }
    }

    public long estimateSize() {
        return INSTANCE_SIZE + keys.length() * Integer.BYTES;
    }
}

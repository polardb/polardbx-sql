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

import com.alibaba.polardbx.common.utils.memory.ObjectSizeUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;

import java.util.Arrays;
import java.util.List;

class GroupOpenHashMap implements GroupHashMap, Hash {

    protected static final int NOT_EXISTS = -1;

    protected final int expectedSize;

    protected final int chunkSize;

    protected final DataType[] groupKeyType;

    protected TypedBuffer groupKeyBuffer;

    protected int groupCount;

    /**
     * The array of keys (buckets)
     */
    private int[] keys;
    /**
     * The mask for wrapping a position counter
     */
    private int mask;
    /**
     * The current table size.
     */
    private int n;
    /**
     * Number of entries in the set (including the key zero, if present).
     */
    private int size;
    /**
     * The acceptable load factor.
     */
    private float f;
    /**
     * Threshold after which we rehash. It must be the table size times {@link #f}.
     */
    private int maxFill;

    protected float loadFactor;

    protected ExecutionContext context;

    public GroupOpenHashMap(DataType[] groupKeyType, int expectedSize, int chunkSize, ExecutionContext context) {
        this(groupKeyType, expectedSize, DEFAULT_LOAD_FACTOR, chunkSize, context);
    }

    public GroupOpenHashMap(DataType[] groupKeyType, int expectedSize, float loadFactor, int chunkSize,
                            ExecutionContext context) {
        Preconditions.checkArgument(loadFactor > 0 && loadFactor <= 1,
            "Load factor must be greater than 0 and smaller than or equal to 1");
        Preconditions.checkArgument(expectedSize >= 0, "The expected number of elements must be non-negative");

        this.loadFactor = loadFactor;
        this.f = loadFactor;
        this.n = HashCommon.arraySize(expectedSize, loadFactor);
        this.mask = n - 1;
        this.maxFill = HashCommon.maxFill(n, loadFactor);
        this.size = 0;

        int[] keys = new int[n];
        Arrays.fill(keys, NOT_EXISTS);
        this.keys = keys;

        this.groupKeyType = groupKeyType;
        this.groupKeyBuffer = TypedBuffer.create(groupKeyType, chunkSize, context);
        this.chunkSize = chunkSize;
        this.expectedSize = expectedSize;
        this.context = context;
    }

    /**
     * @param groupId if groupId == -1 means need to generate a new groupid
     */
    int innerPut(Chunk chunk, int position, int groupId) {
        int h = HashCommon.mix(chunk.hashCode(position)) & mask;
        int k = keys[h];

        if (k != NOT_EXISTS) {
            if (groupKeyBuffer.equals(k, chunk, position)) {
                return k;
            }
            // Open-address probing
            while ((k = keys[h = (h + 1) & mask]) != NOT_EXISTS) {
                if (groupKeyBuffer.equals(k, chunk, position)) {
                    return k;
                }
            }
        }

        if (groupId == -1) {
            groupId = appendGroup(chunk, position);
        }

        // otherwise, insert this position
        keys[h] = groupId;

        if (size++ >= maxFill) {
            rehash();
        }
        return groupId;
    }

    protected void rehash() {
        this.n *= 2;
        this.mask = n - 1;
        this.maxFill = HashCommon.maxFill(n, this.f);
        this.size = 0;
        int[] keys = new int[n];
        Arrays.fill(keys, NOT_EXISTS);
        this.keys = keys;

        List<Chunk> groupChunks = groupKeyBuffer.buildChunks();
        int groupId = 0;
        for (Chunk chunk : groupChunks) {
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                innerPut(chunk, i, groupId++);
            }
        }
    }

    int appendGroup(Chunk chunk, int position) {
        groupKeyBuffer.appendRow(chunk, position);
        return groupCount++;
    }

    List<Chunk> buildGroupChunks() {
        List<Chunk> chunks = groupKeyBuffer.buildChunks();

        // set null to deallocate memory
        this.keys = null;
        this.groupKeyBuffer = null;

        return chunks;
    }

    boolean noGroupBy() {
        return groupKeyType.length == 0;
    }

    @Override
    public long estimateSize() {
        long size = 0L;
        if (keys != null) {
            size += keys.length * ObjectSizeUtils.SIZE_INTEGER;
        }
        if (groupKeyBuffer != null) {
            size += groupKeyBuffer.estimateSize();
        }
        return size;
    }

    int getGroupCount() {
        return groupCount;
    }
}

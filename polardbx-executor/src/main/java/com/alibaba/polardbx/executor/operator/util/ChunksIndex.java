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

import com.google.common.collect.AbstractIterator;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Index of several chunks
 *
 */
public final class ChunksIndex {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ChunksIndex.class).instanceSize();

    private final List<Chunk> chunks;
    private final IntArrayList offsets; // offsets.size() always equals to chunks.size() + 1

    public ChunksIndex() {
        chunks = new ArrayList<>();
        offsets = new IntArrayList();
        offsets.add(0);
    }

    public void addChunk(Chunk chunk) {
        chunks.add(chunk);
        offsets.add(getPositionCount() + chunk.getPositionCount());
    }

    public final long getAddress(int position) {
        int chunkId = upperBound(offsets, position) - 1;
        int offset = offsets.getInt(chunkId);
        return SyntheticAddress.encode(chunkId, position - offset);
    }

    public Object getObject(int columnIndex, int position) {
        long address = getAddress(position);
        Block block = getChunk(SyntheticAddress.decodeIndex(address)).getBlock(columnIndex);

        return block.getObject(SyntheticAddress.decodeOffset(address));
    }

    public Object getObjectForCmp(int columnIndex, int position) {
        long address = getAddress(position);
        Block block = getChunk(SyntheticAddress.decodeIndex(address)).getBlock(columnIndex);

        return block.getObjectForCmp(SyntheticAddress.decodeOffset(address));
    }

    public void writePositionTo(int columnIndex, int position, BlockBuilder blockBuilder) {
        long address = getAddress(position);
        Block block = getChunk(SyntheticAddress.decodeIndex(address)).getBlock(columnIndex);

        block.writePositionTo(SyntheticAddress.decodeOffset(address), blockBuilder);
    }

    public Chunk.ChunkRow rowAt(int position) {
        long address = getAddress(position);
        Chunk chunk = getChunk(SyntheticAddress.decodeIndex(address));
        return chunk.rowAt(SyntheticAddress.decodeOffset(address));
    }

    public int getPositionCount() {
        return offsets.getInt(offsets.size() - 1);
    }

    public int getChunkCount() {
        return chunks.size();
    }

    public boolean isEmpty() {
        return chunks.isEmpty();
    }

    public Chunk getChunk(int index) {
        return chunks.get(index);
    }

    public int getChunkOffset(int index) {
        return offsets.getInt(index);
    }

    /**
     * Estimate the memory size except the contained chunks
     */
    public long estimateSelfSize() {
        return INSTANCE_SIZE + SizeOf.sizeOf(offsets.elements());
    }

    public int hashCode(int position) {
        long address = getAddress(position);
        Chunk chunk = getChunk(SyntheticAddress.decodeIndex(address));
        return chunk.hashCode(SyntheticAddress.decodeOffset(address));
    }

    public boolean equals(int position, Chunk otherChunk, int otherPosition) {
        long address = getAddress(position);
        Chunk chunk = getChunk(SyntheticAddress.decodeIndex(address));
        return chunk.equals(SyntheticAddress.decodeOffset(address), otherChunk, otherPosition);
    }

    public boolean equals(int position1, int position2) {
        long address1 = getAddress(position1);
        Chunk chunk1 = getChunk(SyntheticAddress.decodeIndex(address1));
        long address2 = getAddress(position2);
        Chunk chunk2 = getChunk(SyntheticAddress.decodeIndex(address2));
        return chunk1.equals(SyntheticAddress.decodeOffset(address1), chunk2, SyntheticAddress.decodeOffset(address2));
    }

    /**
     * @return the index of first element greater than target
     */
    private int upperBound(IntArrayList values, int target) {
        int first = 0;
        int last = values.size() - 1;
        final int[] arr = values.elements();
        while (first < last) {
            final int mid = (first + last) >>> 1;
            if (arr[mid] <= target) {
                first = mid + 1;
            } else {
                last = mid;
            }
        }
        return first;
    }

    public Iterator<Chunk> getChunksAndDeleteAfterRead() {
        return new AbstractIterator<Chunk>() {
            private int chunkCounter;

            @Override
            protected Chunk computeNext() {
                if (chunkCounter == getChunkCount()) {
                    return endOfData();
                }
                return getChunk(chunkCounter++);
            }
        };
    }
}

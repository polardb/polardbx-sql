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
 * @author xiaoyinng
 */
public final class ElementaryChunksIndex {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ElementaryChunksIndex.class).instanceSize();

    private final List<Chunk> chunks;
    private int chunkLimit;
    private IntArrayList offsetList = new IntArrayList();
    private final IntArrayList positionIndexList;
    private int[] offsets;
    private int[] positionIndex = new int[0];
    private Chunk.ChunkRow[] chunkRows;

    public ElementaryChunksIndex(int initSize, int chunkLimit) {
        chunks = new ArrayList<>(initSize);
        this.chunkLimit = chunkLimit;
        positionIndexList = new IntArrayList(initSize * this.chunkLimit);
    }

    public ElementaryChunksIndex(int chunkLimit) {
        chunks = new ArrayList<>();
        positionIndexList = new IntArrayList(chunkLimit);
        this.chunkLimit = chunkLimit;
    }

    public void addChunk(Chunk chunk) {
        final int k = chunks.size();
        offsetList.add(getPositionCount() + chunk.getPositionCount());
        for (int i = 0; i < chunk.getPositionCount(); i++) {
            positionIndexList.add(k);
        }
        chunks.add(chunk);
        build();
    }

    public void build() {
        offsets = offsetList.elements();
        positionIndex = positionIndexList.elements();
    }

    public void buildRow() {
        if (chunkRows == null) {
            chunkRows = new Chunk.ChunkRow[positionIndexList.size()];
        }
    }

    public Object getObject(int columnIndex, int position) {
        final int chunkAt = positionIndex[position];
        final Chunk chunk = chunks.get(chunkAt);
        Block block = chunk.getBlock(columnIndex);
        if (chunkAt == 0) {
            return block.getObject(position);
        }
        final int preChunkAt = chunkAt - 1;
        int offset = offsets[preChunkAt];
        return block.getObject(position - offset);
    }

    public int getChunkOffset(int index) {
        if (index == 0) {
            return 0;
        }
        return offsets[index - 1];
    }

    public void writePositionTo(int columnIndex, int position, BlockBuilder blockBuilder) {
        final int chunkAt = positionIndex[position];
        final Chunk chunk = chunks.get(chunkAt);
        Block block = chunk.getBlock(columnIndex);
        final int preChunkAt = chunkAt - 1;
        if (chunkAt == 0) {
            block.writePositionTo(position, blockBuilder);
            return;
        }
        int offset = offsets[preChunkAt];
        block.writePositionTo(position - offset, blockBuilder);
    }

    public Chunk.ChunkRow rowAt(int position) {

        Chunk.ChunkRow chunkRow = chunkRows[position];
        if (chunkRow != null) {
            return chunkRow;
        }
        final int chunkAt = positionIndex[position];
        final Chunk chunk = chunks.get(chunkAt);
        if (chunkAt == 0) {
            chunkRow = chunk.rowAt(position);
            chunkRows[position] = chunkRow;
            return chunkRow;
        }
        final int preChunkAt = chunkAt - 1;
        int offset = offsets[preChunkAt];
        chunkRow = chunk.rowAt(position - offset);
        chunkRows[position] = chunkRow;
        return chunkRow;
    }

    public int getPositionCount() {
        return positionIndexList.size();
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

    /**
     * Estimate the memory size except the contained chunks
     */
    public long estimateSelfSize() {
        return INSTANCE_SIZE + positionIndex.length;
    }

    public int hashCode(int position) {
        final int chunkAt = positionIndex[position];
        Chunk chunk = chunks.get(chunkAt);
        if (chunkAt == 0) {
            return chunk.hashCode(position);
        }
        final int preChunkAt = chunkAt - 1;
        int offset = offsets[preChunkAt];
        return chunk.hashCode(position - offset);
    }

    public boolean equals(int position, Chunk otherChunk, int otherPosition) {
        final int chunkAt = positionIndex[position];
        Chunk chunk = chunks.get(chunkAt);
        if (chunkAt == 0) {
            return chunk.equals(position, otherChunk, otherPosition);
        }
        final int preChunkAt = chunkAt - 1;
        int offset = offsets[preChunkAt];
        return chunk.equals(position - offset, otherChunk, otherPosition);
    }

    public boolean equals(int position1, int position2) {
        final int chunkAt1 = positionIndex[position1];
        final int preChunkAt1 = chunkAt1 - 1;
        int offset1 = 0;
        if (chunkAt1 != 0) {
            offset1 = offsets[preChunkAt1];
        }
        final int chunkAt2 = positionIndex[position2];
        final int preChunkAt2 = chunkAt2 - 1;
        int offset2 = 0;
        if (chunkAt2 != 0) {
            offset2 = offsets[preChunkAt2];
        }
        Chunk chunk1 = chunks.get(chunkAt1);
        Chunk chunk2 = chunks.get(chunkAt2);

        return chunk1.equals(position1 - offset1, chunk2, position2 - offset2);
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

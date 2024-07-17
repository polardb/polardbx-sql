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

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class TypedBuffers {
    public static TypedBuffer createLong(int chunkSize, ExecutionContext context) {
        return new LongTypedBuffer(chunkSize, context);
    }

    public static TypedBuffer createInt(int chunkSize, ExecutionContext context) {
        return new IntegerTypedBuffer(chunkSize, context);
    }

    private static abstract class AbstractTypeSpecificBuffer<T> implements TypedBuffer<T> {
        protected final int chunkSize;
        protected final ExecutionContext context;
        protected int currentSize;
        protected final List<Chunk> chunks = new ArrayList<>();
        protected long estimateSize = 0;
        protected Block randomAccessBlock;

        AbstractTypeSpecificBuffer(int chunkSize, ExecutionContext context) {
            this.chunkSize = chunkSize;
            this.context = context;
        }

        protected abstract void doAppendRow(T array, int nullPosition, int sourceIndex, int positionCount);

        @Override
        public void appendInteger(int col, int value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendLong(int col, long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendNull(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendRow(T array, int nullPosition, int positionCount) {
            if (currentSize + positionCount < chunkSize) {
                doAppendRow(array, nullPosition, 0, positionCount);
            } else {
                int firstCopySize = chunkSize - currentSize;

                doAppendRow(array, nullPosition, 0, firstCopySize);
                doAppendRow(array, nullPosition, firstCopySize, positionCount - firstCopySize);
            }
        }

        @Override
        public void appendRow(Chunk chunk, int position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Chunk> buildChunks() {
            ArrayList<Chunk> allChunks = new ArrayList<>(this.chunks);
            if (currentSize > 0) {
                allChunks.add(getBuildingChunk());
            }
            return allChunks;
        }

        @Override
        public boolean equals(int position, Chunk otherChunk, int otherPosition) {
            final int chunkId = chunkIndexOf(position);
            final int offset = offsetOf(position);

            if (chunkId < chunks.size()) {
                // Just compare both chunks
                Chunk chunk = chunks.get(chunkId);
                return chunk.equals(offset, otherChunk, otherPosition);
            } else {
                // Compare the block builders with given chunk (block by block)
                assert chunkId == chunks.size();
                return otherChunk.getBlock(0).equals(otherPosition, randomAccessBlock, offset);
            }
        }

        @Override
        public void appendValuesTo(int position, ChunkBuilder chunkBuilder) {
            final int chunkId = chunkIndexOf(position);
            final int offset = offsetOf(position);
            if (chunkId < chunks.size()) {
                // Just compare both chunks
                Chunk chunk = chunks.get(chunkId);
                for (int i = 0; i < chunk.getBlockCount(); ++i) {
                    final Block block = chunk.getBlock(i);
                    chunkBuilder.appendTo(block, i, offset);
                }
            } else {
                // Compare the block builders with given chunk (block by block)
                assert chunkId == chunks.size();

                ((RandomAccessBlock) randomAccessBlock).resize(currentSize);
                chunkBuilder.appendTo(randomAccessBlock, 0, offset);
            }
        }

        @Override
        public long estimateSize() {
            return estimateSize;
        }

        protected Chunk getBuildingChunk() {
            Block[] blocks = new Block[] {randomAccessBlock};
            ((RandomAccessBlock) randomAccessBlock).resize(currentSize);
            return new Chunk(currentSize, blocks);
        }

        protected int chunkIndexOf(int position) {
            return position / chunkSize;
        }

        protected int offsetOf(int position) {
            return position % chunkSize;
        }
    }

    private static class LongTypedBuffer extends AbstractTypeSpecificBuffer<long[]> {
        LongTypedBuffer(int chunkSize, ExecutionContext context) {
            super(chunkSize, context);
            this.randomAccessBlock = new LongBlock(DataTypes.LongType, chunkSize);
        }

        @Override
        protected void doAppendRow(long[] array, int nullPosition, int sourceIndex, int positionCount) {
            // Check fulfilled before appending
            if (currentSize == chunkSize) {
                Chunk buildingChunk = getBuildingChunk();
                chunks.add(buildingChunk);
                estimateSize += buildingChunk.estimateSize();

                // reset long block
                randomAccessBlock = new LongBlock(DataTypes.LongType, chunkSize);
                currentSize = 0;
            }

            // copy value array
            long[] targetValueArray = ((LongBlock) randomAccessBlock).longArray();
            for (int position = 0; position < positionCount; position++) {
                targetValueArray[currentSize + position] = array[sourceIndex + position];
            }

            // copy null array
            if (nullPosition >= 0) {
                boolean[] targetNullArray = ((LongBlock) randomAccessBlock).nulls();
                targetNullArray[currentSize + nullPosition] = true;
            }
            currentSize += positionCount;
        }

    }

    private static class IntegerTypedBuffer extends AbstractTypeSpecificBuffer<int[]> {
        IntegerTypedBuffer(int chunkSize, ExecutionContext context) {
            super(chunkSize, context);
            this.randomAccessBlock = new IntegerBlock(DataTypes.IntegerType, chunkSize);
        }

        @Override
        protected void doAppendRow(int[] array, int nullPosition, int sourceIndex, int positionCount) {
            // Check fulfilled before appending
            if (currentSize == chunkSize) {
                Chunk buildingChunk = getBuildingChunk();
                chunks.add(buildingChunk);
                estimateSize += buildingChunk.estimateSize();

                // reset long block
                randomAccessBlock = new IntegerBlock(DataTypes.IntegerType, chunkSize);
                currentSize = 0;
            }

            // copy value array
            int[] targetValueArray = ((IntegerBlock) randomAccessBlock).intArray();
            for (int position = 0; position < positionCount; position++) {
                targetValueArray[currentSize + position] = array[sourceIndex + position];
            }

            // copy null array
            if (nullPosition >= 0) {
                boolean[] targetNullArray = ((IntegerBlock) randomAccessBlock).nulls();
                targetNullArray[currentSize + nullPosition] = true;
            }
            currentSize += positionCount;
        }
    }
}

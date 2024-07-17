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

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

public class MutableChunk extends Chunk {
    private int chunkLimit;
    private int[] outputIndexes;
    private boolean isFirstAllocation;

    // for literal
    BitSet literalBitmap;

    // for test
    public MutableChunk(Block... blocks) {
        super(blocks);
    }

    public MutableChunk(int[] selection, Block[] slots, int chunkLimit, int[] outputIndexes, BitSet literalBitmap) {
        super(selection, slots);
        this.chunkLimit = chunkLimit;
        this.outputIndexes = outputIndexes;
        this.isFirstAllocation = true;
        this.literalBitmap = literalBitmap;
    }

    public RandomAccessBlock slotIn(int index, DataType<?> dataType) {
        return slotIn(index);
    }

    /**
     * unsafe method to get vector
     */
    public RandomAccessBlock slotIn(int index) {
        return (RandomAccessBlock) blocks[index];
    }

    public void setSlotAt(RandomAccessBlock block, int index) {
        this.blocks[index] = (Block) block;
    }

    public Object elementAt(final int columnIndex, final int rowIndex) {
        return ((RandomAccessBlock) blocks[columnIndex]).elementAt(rowIndex);
    }

    public int width() {
        return blocks.length;
    }

    public int batchSize() {
        return positionCount;
    }

    public void setBatchSize(int positionCount) {
        this.positionCount = positionCount;
    }

    /**
     * If the runtime size of Chunk exceeds the batch size, reallocate the vector.
     */
    public void reallocate(int newBatchSize, int blockCount) {
        reallocate(newBatchSize, blockCount, false);
    }

    public void allocateWithObjectPool(final int newBatchSize, final int inputBlockCount, ObjectPools objectPools) {
        // Get the destination physical element count.
        // It's greater than or equal to batch size (position count).
        int distSize = newBatchSize;
        if (isSelectionInUse() && selection != null && selection.length > 0) {
            int selectionBound = selection[selection.length - 1] + 1;
            distSize = Math.max(newBatchSize, selectionBound);
        }

        int j = 0;
        for (int i = inputBlockCount; i < blocks.length; i++) {
            Block vector = blocks[i];
            RandomAccessBlock newVector;

            if (j < outputIndexes.length && outputIndexes[j] == i) {
                j++;
                // allocate memory for output vector in chunk limit size.
                newVector = BlockUtils.createBlock(((RandomAccessBlock) vector).getType(),
                    distSize, objectPools, chunkLimit);
            } else {
                newVector = BlockUtils.createBlock(((RandomAccessBlock) vector).getType(), distSize);
            }
            // set position count = max{selection count, batch size}
            newVector.resize(newBatchSize);
            blocks[i] = (Block) newVector;
        }

        this.positionCount = newBatchSize;
    }

    public void allocateWithReuse(final int newBatchSize, final int inputBlockCount) {
        if (isFirstAllocation) {
            // In first allocation, allocate memory for all output blocks.
            for (int i = inputBlockCount; i < blocks.length; i++) {
                Block vector = blocks[i];
                RandomAccessBlock newVector = (RandomAccessBlock) vector;

                if (literalBitmap == null || !literalBitmap.get(i)) {
                    newVector = BlockUtils.createBlock(((RandomAccessBlock) vector).getType(), chunkLimit);
                    // set position count = max{selection count, batch size}
                    newVector.resize(newBatchSize);
                } else {
                    // lazy allocation
                    newVector.resize(0);
                }

                blocks[i] = (Block) newVector;
            }
            isFirstAllocation = false;
        } else {
            for (int i = inputBlockCount; i < blocks.length; i++) {
                RandomAccessBlock vector = (RandomAccessBlock) blocks[i];
                if (literalBitmap == null || !literalBitmap.get(i)) {
                    // set position count = max{selection count, batch size}
                    vector.resize(newBatchSize);
                } else {
                    vector.resize(0);
                }
            }
        }
        this.positionCount = newBatchSize;
    }

    /**
     * If the runtime size of Chunk exceeds the batch size, reallocate the vector.
     */
    public void reallocate(final int newBatchSize, int blockCount, boolean reuse) {
        if (reuse && chunkLimit > 0 && outputIndexes != null) {

            if (isFirstAllocation) {
                // In first allocation, allocate memory for all output blocks.
                for (int i = blockCount; i < blocks.length; i++) {
                    Block vector = blocks[i];

                    // exclude the literal expression
                    RandomAccessBlock newVector = (RandomAccessBlock) vector;
                    if (literalBitmap == null || !literalBitmap.get(i)) {
                        newVector = BlockUtils.createBlock(((RandomAccessBlock) vector).getType(), chunkLimit);
                        // set position count = max{selection count, batch size}
                        newVector.resize(newBatchSize);
                    } else {
                        newVector.resize(0);
                    }

                    blocks[i] = (Block) newVector;

                }
                isFirstAllocation = false;
            } else {
                // Just allocate memory for output vector in chunk limit size.
                for (int i = 0; i < outputIndexes.length; i++) {
                    Block vector = blocks[i];
                    RandomAccessBlock newVector =
                        BlockUtils.createBlock(((RandomAccessBlock) vector).getType(), chunkLimit);
                    blocks[i] = (Block) newVector;
                }

                for (int i = blockCount; i < blocks.length; i++) {
                    // set position count = max{selection count, batch size}
                    RandomAccessBlock vector = (RandomAccessBlock) blocks[i];
                    if (literalBitmap == null || !literalBitmap.get(i)) {
                        vector.resize(newBatchSize);
                    } else {
                        vector.resize(0);
                    }

                }

            }
        } else {
            // allocate memory for request.

            // Get the destination physical element count.
            // It's greater than or equal to batch size (position count).
            int distSize = newBatchSize;
            if (isSelectionInUse() && selection != null && selection.length > 0) {
                int selectionBound = selection[selection.length - 1] + 1;
                distSize = Math.max(newBatchSize, selectionBound);
            }

            for (int i = blockCount; i < blocks.length; i++) {
                Block vector = blocks[i];
                RandomAccessBlock newVector;

                // don't merge branch
                if (literalBitmap != null && literalBitmap.get(i)) {
                    // exclude the literal expression
                    newVector = (RandomAccessBlock) vector;
                    newVector.resize(0);
                } else if (reuse
                    && vector.getPositionCount() == distSize
                    && vector instanceof RandomAccessBlock
                ) {
                    // Just reuse vector if dist size is equal.
                    newVector = (RandomAccessBlock) vector;
                    // set position count = max{selection count, batch size}
                    newVector.resize(newBatchSize);
                } else {
                    newVector = BlockUtils.createBlock(((RandomAccessBlock) vector).getType(), distSize);
                    // set position count = max{selection count, batch size}
                    newVector.resize(newBatchSize);
                }

                blocks[i] = (Block) newVector;
            }
        }

        this.positionCount = newBatchSize;
    }

    public static Builder newBuilder(int slotLen) {
        return new Builder(slotLen);
    }

    public static class Builder {
        final int positionCount;
        int[] selection;
        List<RandomAccessBlock> blocks = new ArrayList<>(64);
        int[] outputIndexes;
        int chunkLimit;

        // for literal
        BitSet literalBitmap;

        public Builder(int positionCount) {
            Preconditions.checkArgument(
                positionCount > 0, "Slot length expected to be greater than 0 but is " + positionCount);
            this.positionCount = positionCount;
            this.chunkLimit = 0;
            this.outputIndexes = null;
        }

        public Builder withSelection(int[] selection) {
            this.selection = selection;
            return this;
        }

        public Builder addSlot(RandomAccessBlock slot) {
            Preconditions.checkNotNull(slot, "Slot can't be null!");
            blocks.add(slot);
            return this;
        }

        public Builder addEmptySlots(Collection<DataType<?>> dataTypes) {
            dataTypes.forEach(d -> addSlot(BlockUtils.createBlock(d, 0)));
            return this;
        }

        public Builder addSlotsByTypes(Collection<DataType<?>> dataTypes) {
            dataTypes.forEach(d -> addSlot(BlockUtils.createBlock(d, positionCount)));
            return this;
        }

        public Builder addOutputIndexes(int[] outputIndexes) {
            this.outputIndexes = outputIndexes;
            return this;
        }

        public Builder addChunkLimit(int chunkLimit) {
            this.chunkLimit = chunkLimit;
            return this;
        }

        public Builder addLiteralBitmap(BitSet literalBitmap) {
            this.literalBitmap = literalBitmap;
            // exclude the root output index.
            this.literalBitmap.clear(outputIndexes[0]);
            return this;
        }

        public MutableChunk build() {
            if (blocks.isEmpty()) {
                throw new IllegalArgumentException("Can't create vector batch without any slots!");
            }
            return new MutableChunk(selection, blocks.toArray(new Block[0]), chunkLimit, outputIndexes, literalBitmap);
        }
    }
}


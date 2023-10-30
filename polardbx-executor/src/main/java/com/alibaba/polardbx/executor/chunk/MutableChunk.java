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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MutableChunk extends Chunk {
    public MutableChunk(int positionCount, Block... blocks) {
        super(positionCount, blocks);
    }

    public MutableChunk(Block... blocks) {
        super(blocks);
    }

    public MutableChunk(int[] selection, Block[] slots) {
        super(selection, slots);
    }

    public RandomAccessBlock slotIn(int index, DataType<?> dataType) {
        RandomAccessBlock block = slotIn(index);
        DataType slotDataType = block.getType();
        if (slotDataType != null && !slotDataType.getDataClass().equals(dataType.getDataClass())) {
            GeneralUtil.nestedException("block type " + slotDataType + " is not consistent with " + dataType);
        }

        return block;
    }

    /**
     * unsafe method to get vector
     */
    public RandomAccessBlock slotIn(int index) {
        RandomAccessBlock block = null;
        if (index < blocks.length) {
            block = (RandomAccessBlock) blocks[index];
            if (block == null) {
                GeneralUtil.nestedException("block in " + index + " does not exist");
            }
        } else {
            GeneralUtil.nestedException("block in " + index + " does not exist");
        }
        return block;
    }

    public void setSlotAt(RandomAccessBlock block, int index) {
        Preconditions.checkNotNull(block, "block can't be null");
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

    /**
     * If the runtime size of Chunk exceeds the batch size, reallocate the vector.
     */
    public void reallocate(final int newBatchSize, int blockCount, boolean reuse) {
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
            if (reuse
                && vector.getPositionCount() == distSize
                && vector instanceof RandomAccessBlock
            ) {
                newVector = (RandomAccessBlock) vector;
            } else {
                newVector = BlockUtils.createBlock(((RandomAccessBlock) vector).getType(), distSize);
            }
            // set position count = max{selection count, batch size}
            newVector.resize(newBatchSize);
            blocks[i] = (Block) newVector;
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

        public Builder(int positionCount) {
            Preconditions.checkArgument(
                positionCount > 0, "Slot length expected to be greater than 0 but is " + positionCount);
            this.positionCount = positionCount;
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

        public MutableChunk build() {
            if (blocks.isEmpty()) {
                throw new IllegalArgumentException("Can't create vector batch without any slots!");
            }
            return new MutableChunk(selection, blocks.toArray(new Block[0]));
        }
    }
}


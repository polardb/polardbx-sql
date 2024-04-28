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

import com.alibaba.polardbx.common.utils.hash.HashResult128;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.chunk.columnar.CommonLazyBlock;
import com.alibaba.polardbx.executor.operator.util.VectorUtils;
import com.alibaba.polardbx.optimizer.core.row.AbstractRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.annotations.VisibleForTesting;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * Chunk, a mutable vector batch used by vectorized expression evaluation.
 * For performance reason, we don't do a lot of check when manipulating states of vector batch,
 * the user should take care of that.
 */
public class Chunk implements Iterable<Row> {

    protected static final long INSTANCE_SIZE = ClassLayout.parseClass(Chunk.class).instanceSize();

    protected final static AtomicLongFieldUpdater<Chunk>
        sizeInBytesUpdater = AtomicLongFieldUpdater.newUpdater(Chunk.class, "sizeInBytesLong");

    protected volatile long sizeInBytesLong = -1;
    protected int positionCount;

    /**
     * all vectors held by this chunk(batch)
     */
    protected final Block[] blocks;

    /**
     * selection array which is used to filter invalid elements
     */
    protected int[] selection;

    /**
     * whether selection array is used or not
     */
    protected boolean selectionInUse;

    /**
     * partition index in storage layer, used by local partition wise join
     * default -1, means no partition info
     */
    private int partIndex = -1;

    /**
     * partitions scheduled to this computer node, used by local partition wise join
     * default -1, means no partition info
     */
    private int partCount = -1;

    public Chunk(int positionCount, Block... blocks) {
        this.positionCount = positionCount;
        this.blocks = blocks;
    }

    public Chunk() {
        this(0);
    }

    public Chunk(Block... blocks) {
        this(determinePositionCount(blocks), blocks);
    }

    public Chunk(int[] selection, Block[] slots) {
        this.positionCount = slots[0].getPositionCount();
        this.blocks = slots;

        this.selection = selection;
        if (selection != null) {
            this.selectionInUse = true;
            this.positionCount = selection.length;
        } else {
            this.selectionInUse = false;
        }
    }

    public Block[] getBlocks() {
        return blocks;
    }

    public Block getBlock(int i) {
        return blocks[i];
    }

    public int getBlockCount() {
        return blocks.length;
    }

    private static int determinePositionCount(Block... blocks) {
        requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            return 0;
        }
        return blocks[0].getPositionCount();
    }

    public int getPositionCount() {
        return positionCount;
    }

    public ChunkRow rowAt(int position) {
        return new ChunkRow(position);
    }

    public int[] hashCodeVector() {
        int[] h = new int[positionCount];
        for (int c = 0; c < getBlockCount(); c++) {
            h = VectorUtils.addInt(VectorUtils.multiplyInt(h, 31), blocks[c].hashCodeVector());
        }
        return h;
    }

    public void hashCodeVector(int[] hashCodeResults, int[] intermediates, int[] blockHashCodes, int positionCount) {
        if (blocks.length == 1) {
            // short circuit for single block
            blocks[0].hashCodeVector(hashCodeResults, positionCount);
            return;
        }

        Arrays.fill(hashCodeResults, 0);
        Arrays.fill(intermediates, 0);
        Arrays.fill(blockHashCodes, 0);
        for (int c = 0; c < getBlockCount(); c++) {
            // overwrite intermediates array.
            VectorUtils.multiplyInt(intermediates, hashCodeResults, 31, positionCount);

            // overwrite blockHashCodes array.
            blocks[c].hashCodeVector(blockHashCodes, positionCount);

            // overwrite hashCodeResults array.
            VectorUtils.addInt(hashCodeResults, intermediates, blockHashCodes, positionCount);
        }
    }

    public int hashCode(int position) {
        int h = 0;
        for (int c = 0; c < getBlockCount(); c++) {
            h = h * 31 + blocks[c].hashCode(position);
        }
        return h;
    }

    @VisibleForTesting
    public long hashCodeUseXxhash(int position) {
        long h = 0;
        for (int c = 0; c < getBlockCount(); c++) {
            h = h * 31 + blocks[c].hashCodeUseXxhash(position);
        }
        return h;
    }

    public boolean equals(int position, Chunk otherChunk, int otherPosition) {
        assert getBlockCount() == otherChunk.getBlockCount();
        for (int i = 0; i < getBlockCount(); ++i) {
            final Block block = blocks[i];
            Block otherBlock = otherChunk.blocks[i];
            if (otherBlock instanceof CommonLazyBlock) {
                ((CommonLazyBlock) otherBlock).load();
                otherBlock = ((CommonLazyBlock) otherBlock).getLoaded();
            }
            if (!block.equals(position, otherBlock, otherPosition)) {
                return false;
            }
        }
        return true;
    }

    /**
     * ChunkRow is a reference to specified position in this Chunk.
     * This class is designed to be compatible with legacy code.
     */
    public class ChunkRow extends AbstractRow {

        private final int position;

        private ChunkRow(int position) {
            super(null);
            this.position = position;
            this.colNum = blocks.length;
        }

        @Override
        public Object getObject(int index) {
            return getBlock(index).getObject(position);
        }

        @Override
        public Object getObjectForCmp(int index) {
            return getBlock(index).getObjectForCmp(position);
        }

        public int hashCode(int index) {
            return getBlock(index).hashCode(position);
        }

        @Override
        public void setObject(int index, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Object> getValues() {
            ArrayList<Object> values = new ArrayList<>(blocks.length);
            for (int i = 0; i < blocks.length; i++) {
                values.add(getObject(i));
            }
            return values;
        }

        public Chunk getChunk() {
            return Chunk.this;
        }

        public int getPosition() {
            return position;
        }

        public HashResult128 hashCode(IStreamingHasher hasher, Iterable<Integer> columns) {
            for (int columnIndex : columns) {
                getBlock(columnIndex).addToHasher(hasher, position);
            }

            return hasher.hash();
        }

        @Override
        public int hashCode() {
            return getChunk().hashCode(position);
        }

        @Override
        public boolean equals(Object obj) {
            ChunkRow chunkRow = (ChunkRow) obj;
            return Chunk.this.equals(position, chunkRow.getChunk(), chunkRow.position);
        }

        @Override
        public long estimateSize() {
            if (positionCount > 0) {
                return getElementUsedBytes() / positionCount;
            } else {
                return 0;
            }
        }
    }

    /**
     * Estimate the memory usage in bytes of this block
     */
    public long estimateSize() {
        long size = INSTANCE_SIZE + sizeOf(blocks);
        for (Block block : blocks) {
            size += block.estimateSize();
        }
        return size;
    }

    /**
     * Returns the logical size of this block in memory.
     */
    public long getElementUsedBytes() {
        long sizeInBytes = sizeInBytesUpdater.get(this);
        if (sizeInBytes < 0) {
            sizeInBytes = 0;
            for (Block block : blocks) {
                sizeInBytes += block.getElementUsedBytes();
            }
            sizeInBytesUpdater.set(this, sizeInBytes);
        }
        return sizeInBytes;
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {

            private int position = 0;

            @Override
            public boolean hasNext() {
                return position < getPositionCount();
            }

            @Override
            public ChunkRow next() {
                return rowAt(position++);
            }
        };
    }

    public boolean isSelectionInUse() {
        return this.selectionInUse;
    }

    public void setSelectionInUse(boolean selectionInUse) {
        this.selectionInUse = selectionInUse;
    }

    public int[] selection() {
        return this.selection;
    }

    public void setSelection(int[] newSel) {
        this.selection = newSel;
    }

    public int getPartIndex() {
        return partIndex;
    }

    public void setPartIndex(int partIndex) {
        this.partIndex = partIndex;
    }

    public int getPartCount() {
        return partCount;
    }

    public void setPartCount(int partCount) {
        this.partCount = partCount;
    }

    public void recycle() {
        for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
            Block block = blocks[blockIndex];
            if (block.isRecyclable()) {
                blocks[blockIndex].recycle();
            }
        }
    }
}

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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;

import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.lang.ref.WeakReference;

import static com.alibaba.polardbx.common.CrcAccumulator.NULL_TAG;
import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class SliceBlock extends AbstractCommonBlock {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SliceBlock.class).instanceSize();

    private static final byte[] EMPTY_BYTES = new byte[] {};
    private SliceType dataType;
    private Slice data;
    private int[] offsets;
    private WeakReference<SortKey>[] sortKeys;
    private int[] selection;
    private final boolean compatible;
    public SliceBlock(SliceType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets,
                      Slice data, boolean compatible) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        Preconditions.checkNotNull(dataType);
        this.dataType = dataType;
        this.offsets = offsets;
        this.data = data;
        this.sortKeys = new WeakReference[positionCount];
        this.selection = null;
        this.compatible = compatible;
        sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount + data.length();
        estimatedSize = INSTANCE_SIZE + sizeOf(offsets) + sizeOf(valueIsNull) + data.length();
    }

    public SliceBlock(SliceType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets,
                      Slice data, int[] selection, boolean compatible) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        Preconditions.checkNotNull(dataType);
        this.dataType = dataType;
        this.offsets = offsets;
        this.data = data;
        this.sortKeys = new WeakReference[positionCount];
        this.selection = selection;
        this.compatible = false;
        // Slice.length is the memory size in bytes.
        sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount + data.length();
        estimatedSize = INSTANCE_SIZE + sizeOf(offsets) + sizeOf(valueIsNull) + data.length();

        this.sortKeys = new WeakReference[positionCount];
    }

    public int realPositionOf(int position) {
        if (selection == null) {
            return position;
        }
        return selection[position];
    }
    public boolean[] nulls() {
        return isNull;
    }
    public int[] offsets() {
        return offsets;
    }
    public Slice data() {
        return data;
    }
    @Override
    public boolean isNull(int position) {
        position = realPositionOf(position);
        return isNull != null && isNull[position + arrayOffset];
    }

    @Override
    public Object getObjectForCmp(int position) {
        return getSortKey(position);
    }

    public Comparable getSortKey(int position) {
        if (isNull(position)) {
            return null;
        } else if (compatible) {
            WeakReference<SortKey> ref = sortKeys[realPositionOf(position)];
            SortKey sortKey;
            if (ref == null || ref.get() == null) {
                sortKey = dataType.getSortKey(getRegion(position));
                sortKeys[position] = new WeakReference<>(sortKey);
            } else {
                sortKey = ref.get();
            }
            return sortKey;
        } else {
            return getRegion(position);
        }
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : copySlice(position);
    }

    public Slice getRegion(int position) {
        position = realPositionOf(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return data.slice(beginOffset, endOffset - beginOffset);
    }

    public Slice copySlice(int position) {
        position = realPositionOf(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return Slices.copyOf(data, beginOffset, endOffset - beginOffset);
    }

    public byte[] copyBytes(int position) {
        position = realPositionOf(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return data.getBytes(beginOffset, endOffset - beginOffset);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof SliceBlockBuilder) {
            writePositionTo(position, (SliceBlockBuilder) blockBuilder);
        } else {
            throw new AssertionError();
        }
    }

    private void writePositionTo(int position, SliceBlockBuilder b) {
        if (isNull(position)) {
            b.appendNull();
        } else {
            position = realPositionOf(position);
            int beginOffset = beginOffset(position);
            int endOffset = endOffset(position);

            b.valueIsNull.add(false);
            b.sliceOutput.writeBytes(data, beginOffset, endOffset - beginOffset);
            b.offsets.add(b.sliceOutput.size());
        }
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putBytes(EMPTY_BYTES);
        } else {
            Slice encodedSlice = dataType.getCharsetHandler().encodeFromUtf8(getRegion(position));
            sink.putBytes(encodedSlice.getBytes());
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        } else if (compatible) {
            Slice subRegion = getRegion(position);
            return dataType.hashcode(subRegion);
        } else {
            position = realPositionOf(position);
            int beginOffset = beginOffset(position);
            int endOffset = endOffset(position);
            return data.hashCode(beginOffset, endOffset - beginOffset);
        }
    }

    @Override
    public int checksum(int position) {
        if (isNull(position)) {
            return NULL_TAG;
        }
        position = realPositionOf(position);
        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return ChunkUtil.hashCode(data, beginOffset, endOffset);
    }

    public int equals(int position, Slice that) {
        position = realPositionOf(position);
        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return this.data.compareTo(beginOffset, endOffset - beginOffset, that, 0, that.length()) == 0 ? 1 : 0;
    }

    public int anyMatch(int position, Slice that1, Slice that2) {
        position = realPositionOf(position);
        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return this.data.compareTo(beginOffset, endOffset - beginOffset, that1, 0, that1.length()) == 0
            ||  this.data.compareTo(beginOffset, endOffset - beginOffset, that2, 0, that2.length()) == 0
            ? 1 : 0;
    }

    public int anyMatch(int position, Slice that1, Slice that2, Slice that3) {
        position = realPositionOf(position);
        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return this.data.compareTo(beginOffset, endOffset - beginOffset, that1, 0, that1.length()) == 0
            || this.data.compareTo(beginOffset, endOffset - beginOffset, that2, 0, that2.length()) == 0
            || this.data.compareTo(beginOffset, endOffset - beginOffset, that3, 0, that3.length()) == 0
            ? 1 : 0;
    }

    public int anyMatch(int position, Comparable[] those) {
        position = realPositionOf(position);
        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        for (int i = 0; i < those.length; i++) {
            if (this.data.compareTo(
                beginOffset, endOffset - beginOffset, (Slice) those[i], 0, ((Slice) those[i]).length()) == 0) {
                return 1;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        if (other instanceof SliceBlock) {
            return equals(position, (SliceBlock) other, otherPosition);
        } else if (other instanceof SliceBlockBuilder) {
            return equals(position, (SliceBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    boolean equals(int position, SliceBlock other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        // by collation
        Slice region1 = getRegion(position);
        Slice region2 = other.getRegion(otherPosition);

        if (compatible) {
            return dataType.compare(region1, region2) == 0;
        } else {
            return region1.equals(region2);
        }
    }

    boolean equals(int position, SliceBlockBuilder other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        // by collation
        Slice region1 = getRegion(position);
        Slice region2 = other.getRegion(otherPosition);

        if (compatible) {
            return dataType.compare(region1, region2) == 0;
        } else {
            return region1.equals(region2);
        }
    }

    @Override
    public DataType getType() {
        return dataType;
    }

    /**
     * Reset the collation of this block.
     * This operation is risky cause collations from different charset will lead to error character parsing.
     */
    public void resetCollation(CollationName collationName) {
        // for mix of collation
        Preconditions.checkNotNull(collationName);
        SliceType newDataType = new VarcharType(CollationName.getCharsetOf(collationName), collationName);
        this.dataType = newDataType;
    }

    public void encoding(SliceOutput sliceOutput) {
        if (selection != null) {
            int currentSize = 0;
            int[] realOffsets = new int[positionCount];
            for (int i = 0; i < positionCount; i++) {
                int j = selection[i];
                if (isNull == null || !this.isNull[j]) {
                    int beginOffset = beginOffset(j);
                    int endOffset = endOffset(j);
                    int len = endOffset - beginOffset;
                    currentSize += len;
                }
                realOffsets[i] = currentSize;
            }
            for (int position = 0; position < positionCount; position++) {
                sliceOutput.writeInt(realOffsets[position]);
            }
            int maxOffset = realOffsets[positionCount - 1];
            if (maxOffset > 0) {
                sliceOutput.writeInt(maxOffset);
                for (int position = 0; position < positionCount; position++) {
                    int j = selection[position];
                    if (isNull == null || !this.isNull[j]) {
                        int beginOffset = beginOffset(j);
                        int endOffset = endOffset(j);
                        int len = endOffset - beginOffset;
                        Slice slice = data.slice(beginOffset, len);
                        sliceOutput.writeBytes(slice);
                    }
                }
            }
        } else {
            int[] offset = this.offsets;
            for (int position = 0; position < positionCount; position++) {
                sliceOutput.writeInt(offset[position]);
            }
            int maxOffset = offset[positionCount - 1];
            if (maxOffset > 0) {
                Slice data = this.data;
                sliceOutput.writeInt(maxOffset);
                sliceOutput.writeBytes(data, 0, maxOffset);
            }
        }
    }

    public int[] getOffsets() {
        return offsets;
    }

    private int beginOffset(int position) {
        return position + arrayOffset > 0 ? offsets[position + arrayOffset - 1] : 0;
    }

    private int endOffset(int position) {
        return offsets[position + arrayOffset];
    }

    public int[] getSelection() {
        return selection;
    }

    public boolean isCompatible() {
        return compatible;
    }
}
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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.util.bloomfilter.TddlHasher;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.lang.ref.WeakReference;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class SliceBlock extends AbstractCommonBlock {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SliceBlock.class).instanceSize();

    private static final byte[] EMPTY_BYTES = new byte[] {};
    private SliceType dataType;
    private Slice data;
    private int[] offsets;
    private WeakReference<SortKey>[] sortKeys;

    SliceBlock(SliceType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets,
               Slice data) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        Preconditions.checkNotNull(dataType);
        this.dataType = dataType;
        this.offsets = offsets;
        this.data = data;
        // Slice.length is the memory size in bytes.
        sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount + data.length();
        estimatedSize = INSTANCE_SIZE + sizeOf(offsets) + sizeOf(valueIsNull) + data.length();

        this.sortKeys = new WeakReference[positionCount];
    }

    @Override
    public Object getObjectForCmp(int position) {
        return getSortKey(position);
    }

    public SortKey getSortKey(int position) {
        if (isNull(position)) {
            return null;
        } else {
            WeakReference<SortKey> ref = sortKeys[position];
            SortKey sortKey;
            if (ref == null || ref.get() == null) {
                sortKey = dataType.getSortKey(getRegion(position));
                sortKeys[position] = new WeakReference<>(sortKey);
            } else {
                sortKey = ref.get();
            }
            return sortKey;
        }
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : copySlice(position);
    }

    public Slice getRegion(int position) {
        checkReadablePosition(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return data.slice(beginOffset, endOffset - beginOffset);
    }

    public Slice copySlice(int position) {
        checkReadablePosition(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return Slices.copyOf(data, beginOffset, endOffset - beginOffset);
    }

    public byte[] copyBytes(int position) {
        checkReadablePosition(position);

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
            int beginOffset = beginOffset(position);
            int endOffset = endOffset(position);

            b.valueIsNull.add(false);
            b.sliceOutput.writeBytes(data, beginOffset, endOffset - beginOffset);
            b.offsets.add(b.sliceOutput.size());
        }
    }

    @Override
    public void addToBloomFilter(TddlHasher sink, int position) {
        if (isNull(position)) {
            sink.putBytes(EMPTY_BYTES);
        } else {
            sink.putBytes(copyBytes(position));
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }

        Slice subRegion = getRegion(position);
        return dataType.hashcode(subRegion);
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

        return dataType.compare(region1, region2) == 0;
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

        return dataType.compare(region1, region2) == 0;
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

    public Slice getData() {
        return data;
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
}
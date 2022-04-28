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

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Byte Array (<pre>byte[]</pre>) Block
 *
 */
public class ByteArrayBlock extends AbstractCommonBlock {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ByteArrayBlock.class).instanceSize();

    private final int[] offsets;
    private final byte[] data;

    ByteArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets, byte[] data) {
        super(DataTypes.BytesType, positionCount, valueIsNull, valueIsNull != null);
        this.offsets = offsets;
        this.data = data;
        estimatedSize = INSTANCE_SIZE + sizeOf(offsets) + sizeOf(data) + sizeOf(valueIsNull);
        sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount + sizeOf(data);
    }

    @Override
    public byte[] getByteArray(int position) {
        checkReadablePosition(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return Arrays.copyOfRange(data, beginOffset, endOffset);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getByteArray(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof ByteArrayBlockBuilder) {
            writePositionTo(position, (ByteArrayBlockBuilder) blockBuilder);
        } else {
            throw new AssertionError();
        }
    }

    private void writePositionTo(int position, ByteArrayBlockBuilder b) {
        if (isNull(position)) {
            b.appendNull();
        } else {
            int beginOffset = beginOffset(position);
            int endOffset = endOffset(position);

            b.valueIsNull.add(false);
            b.data.addElements(b.data.size(), data, beginOffset, endOffset - beginOffset);
            b.offsets.add(b.data.size());
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }

        return ChunkUtil.hashCode(data, beginOffset(position), endOffset(position));
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        if (other instanceof ByteArrayBlock) {
            return equals(position, (ByteArrayBlock) other, otherPosition);
        } else if (other instanceof ByteArrayBlockBuilder) {
            return equals(position, (ByteArrayBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    boolean equals(int position, ByteArrayBlock other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        int pos1 = beginOffset(position);
        int len1 = endOffset(position) - pos1;
        int pos2 = other.beginOffset(otherPosition);
        int len2 = other.endOffset(otherPosition) - pos2;
        return ExecUtils.arrayEquals(data, pos1, len1, other.data, pos2, len2);
    }

    boolean equals(int position, ByteArrayBlockBuilder other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        int pos1 = beginOffset(position);
        int len1 = endOffset(position) - pos1;
        int pos2 = other.beginOffset(otherPosition);
        int len2 = other.endOffset(otherPosition) - pos2;
        return ExecUtils.arrayEquals(data, pos1, len1, other.data.elements(), pos2, len2);
    }

    private int beginOffset(int position) {
        return position + arrayOffset > 0 ? offsets[position + arrayOffset - 1] : 0;
    }

    private int endOffset(int position) {
        return offsets[position + arrayOffset];
    }

    public int[] getOffsets() {
        return offsets;
    }

    public byte[] getData() {
        return data;
    }
}


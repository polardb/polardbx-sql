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

import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Objects;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * String Block
 */
public class StringBlock extends AbstractCommonBlock {
    private static final String EMPTY_STRING = "";

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(StringBlock.class).instanceSize();

    private final int[] offsets;
    private final char[] data;

    StringBlock(DataType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets,
                char[] data) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.offsets = offsets;
        this.data = data;
        updateSizeInfo();
    }

    @Override
    public String getString(int position) {
        checkReadablePosition(position);

        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return new String(data, beginOffset, endOffset - beginOffset);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getString(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof StringBlockBuilder) {
            writePositionTo(position, (StringBlockBuilder) blockBuilder);
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putString(EMPTY_STRING);
        } else {
            sink.putString(getString(position));
        }
    }

    private void writePositionTo(int position, StringBlockBuilder b) {
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

        return ChunkUtil.hashCode(data, beginOffset(position), endOffset(position), true);
    }

    @Override
    public int checksum(int position) {
        if (isNull(position)) {
            return 0;
        }
        String str = getString(position);
        byte[] rawBytes = str.getBytes();
        return ChunkUtil.hashCode(rawBytes, 0, rawBytes.length);
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        if (other instanceof StringBlock) {
            return equals(position, (StringBlock) other, otherPosition);
        } else if (other instanceof StringBlockBuilder) {
            return equals(position, (StringBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    boolean equals(int position, StringBlock other, int otherPosition) {
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
        return ExecUtils.arrayEquals(data, pos1, len1, other.data, pos2, len2, true);
    }

    boolean equals(int position, StringBlockBuilder other, int otherPosition) {
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
        return ExecUtils.arrayEquals(data, pos1, len1, other.data.elements(), pos2, len2, true);
    }

    /**
     * Designed for test purpose
     */
    public static StringBlock of(String... values) {
        int totalLength = Arrays.stream(values).filter(Objects::nonNull).mapToInt(String::length).sum();
        StringBlockBuilder builder = new StringBlockBuilder(values.length, totalLength);
        for (String value : values) {
            if (value != null) {
                builder.writeString(value);
            } else {
                builder.appendNull();
            }
        }
        return (StringBlock) builder.build();
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

    public char[] getData() {
        return data;
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(data) + sizeOf(offsets);
        elementUsedBytes = Byte.BYTES * positionCount + sizeOf(data) + Integer.BYTES * positionCount;
    }
}
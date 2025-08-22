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
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import io.airlift.slice.XxHash64;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * String Block
 */
public class StringBlock extends AbstractCommonBlock {
    private static final String EMPTY_STRING = "";

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(StringBlock.class).instanceSize();

    private int[] offsets;
    private char[] data;

    public StringBlock(DataType dataType, int positionCount) {
        this(dataType, 0, positionCount, new boolean[positionCount], new int[positionCount], null);
    }

    StringBlock(DataType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets,
                char[] data) {
        super(dataType, positionCount, valueIsNull, valueIsNull != null);
        this.offsets = offsets;
        this.data = data;
        updateSizeInfo();
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
        return builder.build().cast(StringBlock.class);
    }

    public static StringBlock from(StringBlock other, int selSize, int[] selection) {
        if (other.data == null) {
            int[] newOffsets = new int[selSize];
            return new StringBlock(other.dataType, 0, selSize,
                BlockUtils.copyNullArray(other.isNull, selection, selSize),
                newOffsets, null);
        }
        if (selection == null) {
            return new StringBlock(other.dataType, 0, selSize,
                Arrays.copyOf(other.isNull, selSize), Arrays.copyOf(other.offsets, selSize),
                Arrays.copyOf(other.data, other.data.length));
        } else {
            StringBlockBuilder stringBlockBuilder = new StringBlockBuilder(other.dataType, selSize,
                other.data.length / (other.positionCount + 1) * selSize);
            for (int i = 0; i < selSize; i++) {
                if (other.isNull(selection[i])) {
                    stringBlockBuilder.appendNull();
                } else {
                    stringBlockBuilder.writeString(other.getString(selection[i]));
                }
            }
            return (StringBlock) stringBlockBuilder.build();
        }
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

    @Override
    public long hashCodeUseXxhash(int pos) {
        if (isNull(pos)) {
            return NULL_HASH_CODE;
        } else {
            byte[] rawBytes = getString(pos).getBytes(StandardCharsets.UTF_8);
            return XxHash64.hash(rawBytes, 0, rawBytes.length);
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

        return ChunkUtil.hashCodeIgnoreCase(data, beginOffset(position), endOffset(position));
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
            return equals(position, other.cast(StringBlock.class), otherPosition);
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

    private int beginOffset(int position) {
        return position + arrayOffset > 0 ? offsets[position + arrayOffset - 1] : 0;
    }

    private int endOffset(int position) {
        return offsets[position + arrayOffset];
    }

    public int[] getOffsets() {
        return offsets;
    }

    public void setOffsets(int[] offsets) {
        this.offsets = offsets;
    }

    public char[] getData() {
        return data;
    }

    public void setData(char[] data) {
        Preconditions.checkArgument(this.data == null);
        this.data = data;
    }

    @Override
    public void updateSizeInfo() {
        elementUsedBytes = INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(isNull))
            + VMSupport.align((int) sizeOf(data))
            + VMSupport.align((int) sizeOf(offsets));
        estimatedSize = elementUsedBytes;
    }
}
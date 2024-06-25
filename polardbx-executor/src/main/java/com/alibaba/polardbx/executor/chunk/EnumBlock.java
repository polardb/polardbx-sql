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

import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Map;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Enumblock
 *
 * @author xiaoying
 */
public class EnumBlock extends AbstractCommonBlock {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(EnumBlock.class).instanceSize();

    private final int[] offsets;
    private final Map<String, Integer> enumValues;
    private char[] data;

    public EnumBlock(int positionCount, final Map<String, Integer> enumValues) {
        this(0, positionCount, new boolean[positionCount], new int[positionCount], null, enumValues);
    }

    public EnumBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] offsets, char[] data,
                     final Map<String, Integer> enumValues) {
        super(DataTypes.StringType, positionCount, valueIsNull, valueIsNull != null);
        this.offsets = offsets;
        this.data = data;
        this.enumValues = enumValues;
        updateSizeInfo();
    }

    EnumBlock(int positionCount, boolean[] valueIsNull, int[] offsets, char[] data,
              final Map<String, Integer> enumValues, boolean hasNull) {
        super(DataTypes.StringType, positionCount, valueIsNull, hasNull);
        this.offsets = offsets;
        this.data = data;
        this.enumValues = enumValues;
        updateSizeInfo();
    }

    public static EnumBlock from(EnumBlock other, int selSize, int[] selection) {
        int[] newOffsets = new int[selSize];

        if (other.data == null) {
            return new EnumBlock(0, selSize,
                BlockUtils.copyNullArray(other.isNull, selection, selSize),
                newOffsets, null, other.enumValues);
        }
        if (selection == null) {
            return new EnumBlock(0, selSize,
                BlockUtils.copyNullArray(other.isNull, selection, selSize),
                newOffsets, Arrays.copyOf(other.data, other.data.length), other.enumValues);
        } else {
            EnumBlockBuilder enumBlockBuilder =
                new EnumBlockBuilder(selSize, other.data.length / (other.positionCount + 1) * selSize,
                    other.enumValues);
            for (int i = 0; i < selSize; i++) {
                if (other.isNull(selection[i])) {
                    enumBlockBuilder.appendNull();
                } else {
                    enumBlockBuilder.writeString(other.getString(selection[i]));
                }
            }
            return (EnumBlock) enumBlockBuilder.build();
        }
    }

    @Override
    public String getString(int position) {
        try {
            int beginOffset = beginOffset(position);
            int endOffset = endOffset(position);
            return new String(data, beginOffset, endOffset - beginOffset);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("position is not valid:" + position + "," + positionCount);
        }
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getString(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof EnumBlockBuilder) {
            writePositionTo(position, (EnumBlockBuilder) blockBuilder);
        } else {
            throw new AssertionError();
        }
    }

    private void writePositionTo(int position, EnumBlockBuilder b) {
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
    public long hashCodeUseXxhash(int pos) {
        if (isNull(pos)) {
            return NULL_HASH_CODE;
        } else {
            String val = getString(pos);
            Integer index = enumValues.get(val);
            if (index == null) {
                return XxhashUtils.finalShuffle(0);
            } else {
                return XxhashUtils.finalShuffle(index);
            }
        }
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
        if (other instanceof EnumBlock) {
            return equals(position, other.cast(EnumBlock.class), otherPosition);
        } else if (other instanceof EnumBlockBuilder) {
            return equals(position, (EnumBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    boolean equals(int position, EnumBlock other, int otherPosition) {
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

    boolean equals(int position, EnumBlockBuilder other, int otherPosition) {
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

    public char[] getData() {
        return data;
    }

    public void setData(char[] data) {
        Preconditions.checkArgument(this.data == null);
        this.data = data;
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(data) + sizeOf(offsets);
        elementUsedBytes = Byte.BYTES * positionCount + sizeOf(data) + Integer.BYTES * positionCount;
    }
}


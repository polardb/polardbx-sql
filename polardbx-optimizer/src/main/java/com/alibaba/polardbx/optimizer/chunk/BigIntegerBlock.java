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

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.util.bloomfilter.TddlHasher;
import org.openjdk.jol.info.ClassLayout;

import java.math.BigInteger;
import java.util.Arrays;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * A fixed-length implement of BigInteger block
 *
 */
public class BigIntegerBlock extends AbstractCommonBlock {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(BigIntegerBlock.class).instanceSize();

    /**
     * 30 byte represent a big decimal
     * the first 28 bytes for BigInteger data
     * 29th byte for actual BigInteger byte array length
     */
    public static final int LENGTH = 29;
    public static final int UNSCALED_LENGTH = 28;

    private final byte[] data;

    public BigIntegerBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, byte[] data) {
        super(DataTypes.ULongType, positionCount, valueIsNull, valueIsNull != null);
        this.data = data;

        estimatedSize = INSTANCE_SIZE + sizeOf(data);
        sizeInBytes = (LENGTH + Byte.BYTES) * positionCount;
    }

    public BigIntegerBlock(int positionCount, boolean[] valueIsNull, byte[] data, boolean hasNull) {
        super(DataTypes.ULongType, positionCount, valueIsNull, hasNull);
        this.data = data;

        estimatedSize = INSTANCE_SIZE + sizeOf(data);
        sizeInBytes = (LENGTH + Byte.BYTES) * positionCount;
    }

    @Override
    public BigInteger getBigInteger(int position) {
        checkReadablePosition(position);
        int beginOffset = beginOffset(position);
        int endOffset = beginOffset + LENGTH;
        final int length = data[endOffset - 1];
        final byte[] bytes = Arrays.copyOfRange(data, beginOffset, beginOffset + length);
        return new BigInteger(bytes);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getBigInteger(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof BigIntegerBlockBuilder) {
            writePositionTo(position, (BigIntegerBlockBuilder) blockBuilder);
        } else {
            throw new AssertionError();
        }
    }

    private void writePositionTo(int position, BigIntegerBlockBuilder b) {
        if (isNull(position)) {
            b.appendNull();
        } else {
            b.data.addElements(b.data.size(), data, beginOffset(position), LENGTH);
            b.valueIsNull.add(false);
        }
    }

    @Override
    public void addToBloomFilter(TddlHasher sink, int position) {
        if (isNull(position)) {
            sink.putLong(0);
        } else {
            sink.putLong(getBigInteger(position).longValue());
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }

        return getBigInteger(position).hashCode();
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        if (other instanceof BigIntegerBlock) {
            return equals(position, (BigIntegerBlock) other, otherPosition);
        } else if (other instanceof BigIntegerBlockBuilder) {
            return equals(position, (BigIntegerBlockBuilder) other, otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    private boolean equals(int position, BigIntegerBlock other, int otherPosition) {
        other.checkReadablePosition(otherPosition);
        return ExecUtils
            .arrayEquals(data, beginOffset(position), LENGTH, other.data, other.beginOffset(otherPosition), LENGTH);
    }

    private boolean equals(int position, BigIntegerBlockBuilder other, int otherPosition) {
        other.checkReadablePosition(otherPosition);
        return ExecUtils
            .arrayEquals(data, beginOffset(position), LENGTH, other.data.elements(), otherPosition * LENGTH, LENGTH);
    }

    private int beginOffset(int position) {
        return arrayOffset + position * LENGTH;
    }

    public byte[] getData() {
        return data;
    }
}

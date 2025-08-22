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

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.optimizer.core.datatype.BlobType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.XxHash64;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

import static com.alibaba.polardbx.common.CrcAccumulator.NULL_TAG;
import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Blob Block
 *
 */
public class BlobBlock extends ReferenceBlock<Blob> {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(BlobBlock.class).instanceSize();

    private static final int BLOB_INSTANCE_SIZE = ClassLayout.parseClass(
        com.alibaba.polardbx.optimizer.core.datatype.Blob.class).instanceSize();

    public BlobBlock(int positionCount) {
        super(0, positionCount, new boolean[positionCount],
            new Blob[positionCount], DataTypes.BlobType);
    }

    public BlobBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, Object[] values) {
        super(arrayOffset, positionCount, valueIsNull, values, DataTypes.BlobType);
    }

    public BlobBlock(DataType dataType, int arrayOffset, int positionCount, boolean[] valueIsNull, Object[] values) {
        super(arrayOffset, positionCount, valueIsNull, values, dataType);
    }

    public static BlobBlock from(BlobBlock other, int selSize, int[] selection) {
        if (selection == null) {
            // case 1: direct copy
            return new BlobBlock(other.dataType, other.arrayOffset, selSize,
                BlockUtils.copyNullArray(other.isNull, null, selSize),
                Arrays.copyOf(other.values, other.values.length));
        } else {
            // case 2: copy selected
            boolean[] targetNulls = BlockUtils.copyNullArray(other.isNull, selection, selSize);
            Object[] targetValues = new Object[selSize];

            for (int position = 0; position < selSize; position++) {
                targetValues[position] = other.values[selection[position]];
            }

            return new BlobBlock(other.dataType, other.arrayOffset, selSize,
                targetNulls, targetValues);
        }
    }

    @Override
    public Blob getBlob(int position) {
        return getReference(position);
    }

    @Override
    public Blob getObject(int position) {
        return isNull(position) ? null : getBlob(position);
    }

    /**
     * TODO dict
     */
    @Override
    public boolean equals(int position, Block otherBlock, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = otherBlock.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        if (otherBlock instanceof BlobBlock || otherBlock instanceof BlobBlockBuilder) {
            return DataTypes.BlobType.compare(getBlob(position), otherBlock.getBlob(otherPosition)) == 0;
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return ((BlobType) DataTypes.BlobType).hashcode(getBlob(position));
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        if (isNull(pos)) {
            return NULL_HASH_CODE;
        }
        Blob blob = getBlob(pos);

        com.alibaba.polardbx.optimizer.core.datatype.Blob value =
            (com.alibaba.polardbx.optimizer.core.datatype.Blob) (DataTypes.BlobType).convertFrom(blob);

        if (value == null) {
            return NULL_HASH_CODE;
        }

        return XxHash64.hash(value.getSlice());
    }

    @Override
    public int checksum(int position) {
        if (isNull(position)) {
            return NULL_TAG;
        }

        Blob blob = getBlob(position);
        int size;
        try {
            size = (int) blob.length();
            byte[] rawBytes = blob.getBytes(1, size);
            return ChunkUtil.hashCode(rawBytes, 0, rawBytes.length);
        } catch (SQLException e) {
            return NULL_TAG;
        }
    }

    @Override
    public void updateSizeInfo() {
        long valueMemoryUsage = 0L;
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof com.alibaba.polardbx.optimizer.core.datatype.Blob) {
                valueMemoryUsage += FastMemoryCounter.sizeOf((Blob) values[i], BLOB_INSTANCE_SIZE);
            }
        }

        elementUsedBytes = INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(isNull))
            + VMSupport.align((int) sizeOf(values))
            + valueMemoryUsage;
        estimatedSize = elementUsedBytes;
    }

    public Blob[] blobArray() {
        return (Blob[]) values;
    }
}

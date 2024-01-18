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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Block stores data in columnar format
 */
public interface Block {
    /**
     * Is the specified position null?
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    boolean isNull(int position);

    default byte getByte(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default short getShort(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default int getInt(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default long getLong(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default long getPackedLong(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default double getDouble(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default float getFloat(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default Timestamp getTimestamp(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default Date getDate(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default Time getTime(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default String getString(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default Decimal getDecimal(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default BigInteger getBigInteger(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default boolean getBoolean(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default byte[] getByteArray(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default Blob getBlob(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default Clob getClob(int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Similar with <pre>Object.hashCode</pre>. Feel free to override it
     * if there is more efficient implementation.
     */
    default int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return getObject(position).hashCode();
    }

    default int checksum(int position) {
        return hashCode(position);
    }

    /**
     * Vectorized version of hashCode
     */
    default int[] hashCodeVector() {
        final int n = getPositionCount();
        int[] results = new int[n];
        for (int position = 0; position < n; position++) {
            results[position] = hashCode(position);
        }
        return results;
    }

    /**
     * Similar with <pre>Object.equals</pre>. Feel free to override it
     * if there is more efficient implementation.
     */
    default boolean equals(int position, Block other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        return getObject(position).equals(other.getObject(otherPosition));
    }

    /**
     * Is it possible the block may have a null value?  If false, the block can not contain
     * a null, but if true, the block may or may not have a null.
     */
    boolean mayHaveNull();

    /**
     * Gets an object at {@code position}.
     * <p>
     * Just to keep compatible with legacy code. Do NOT use this method as far as possible.
     */
    Object getObject(int position);

    /**
     * Estimate the memory usage in bytes of this block
     */
    long estimateSize();

    /**
     * Returns the number of positions in this block.
     */
    int getPositionCount();

    /**
     * Appends the value at {@code position} to {@code blockBuilder} and close the entry.
     */
    void writePositionTo(int position, BlockBuilder blockBuilder);

    /**
     * Returns the logical size of this block in memory.
     */
    default long getElementUsedBytes() {
        throw new UnsupportedOperationException();
    }

    /**
     * Add value at the position to the hasher of a bloom filter.
     *
     * @param sink To support multi column bloom filter, must be a streaming hasher
     */
    default void addToHasher(IStreamingHasher sink, int position) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Get the Object for comparison.
     * Suitable for sorting.
     */
    default Object getObjectForCmp(int position) {
        return getObject(position);
    }
}

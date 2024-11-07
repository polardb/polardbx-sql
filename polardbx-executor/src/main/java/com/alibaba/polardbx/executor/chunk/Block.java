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
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.accumulator.state.NullableLongGroupState;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMapping;
import com.alibaba.polardbx.executor.operator.util.DriverObjectPool;
import com.alibaba.polardbx.executor.operator.util.TypedList;
import com.google.common.base.Preconditions;

import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.BitSet;

/**
 * Block stores data in columnar format
 */
public interface Block extends CastableBlock {
    public static final int NULL_HASH_CODE = 0;

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

    long hashCodeUseXxhash(int pos);

    /**
     * calculate of hash code when exchange is under partition wise
     * for most type, it's equals to com.alibaba.polardbx.executor.chunk.Block#hashCodeUseXxhash(int)
     * but for slice block, it's differ
     */
    default long hashCodeUnderPairWise(int pos, boolean enableCompatible) {
        return hashCodeUseXxhash(pos);
    }

    default int checksum(int position) {
        return hashCode(position);
    }

    default int checksumV2(int position) {
        return checksum(position);
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

    default void hashCodeVector(int[] results, int positionCount) {
        Preconditions.checkArgument(positionCount <= getPositionCount());
        for (int position = 0; position < positionCount; position++) {
            results[position] = hashCode(position);
        }
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

    default void writePositionTo(int[] selection, final int offsetInSelection, final int positionCount,
                                 BlockBuilder blockBuilder) {
        for (int i = 0; i < positionCount; i++) {
            writePositionTo(selection[i + offsetInSelection], blockBuilder);
        }
    }

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

    default void collectNulls(int positionOffset, int positionCount, BitSet nullBitmap, int targetOffset) {
        throw new UnsupportedOperationException();
    }

    /**
     * Copy memory from block into given target array.
     *
     * @param positionOffset position offset in block.
     * @param positionCount position count to copy in block.
     * @param targetArray the target array to store the copied value.
     * @param targetOffset the offset in target array.
     * @param dictionaryMapping To maintain the dictionary mapping relation if this block has dictionary.
     */
    default void copyToIntArray(int positionOffset, int positionCount, int[] targetArray, int targetOffset,
                                DictionaryMapping dictionaryMapping) {
        throw new UnsupportedOperationException();
    }

    /**
     * Copy memory from block into given target array.
     *
     * @param positionOffset position offset in block.
     * @param positionCount position count to copy in block.
     * @param targetArray the target array to store the copied value.
     * @param targetOffset the offset in target array.
     */
    default void copyToLongArray(int positionOffset, int positionCount, long[] targetArray, int targetOffset) {
        throw new UnsupportedOperationException();
    }

    /**
     * Performs a numerical sum over the elements in this block.
     *
     * @param groupSelected positions of selected elements.
     * @param selSize count of selected elements.
     * @param results store the sum results.
     * results[0] = sum result number
     * results[1] = sum state (E_DEC_OK, E_DEC_OVERFLOW, E_DEC_TRUNCATED)
     */
    default void sum(int[] groupSelected, int selSize, long[] results) {
        throw new UnsupportedOperationException();
    }

    /**
     * Performs a numerical sum over the elements in this block.
     *
     * @param startIndexIncluded (included) start position of elements to perform sum.
     * @param endIndexExcluded (excluded) end position of elements to perform sum.
     * @param results store the sum results.
     * results[0] = sum result number for decimal64 or decimal128-low
     * results[1] = sum result number for decimal128-high
     * results[2] = sum state (E_DEC_DEC64, E_DEC_DEC128, E_DEC_TRUNCATED)
     */
    default void sum(int startIndexIncluded, int endIndexExcluded, long[] results) {
        throw new UnsupportedOperationException();
    }

    default void sum(int startIndexIncluded, int endIndexExcluded, long[] sumResultArray, int[] sumStatusArray,
                     int[] normalizedGroupIds) {
        throw new UnsupportedOperationException();
    }

    default void appendTypedHashTable(TypedList typedList, int sourceIndex, int startIndexIncluded,
                                      int endIndexExcluded) {
        throw new UnsupportedOperationException();
    }

    default void count(int[] groupIds, int[] probePositions, int selSize, NullableLongGroupState state) {
        for (int i = 0; i < selSize; i++) {
            int position = probePositions[i];
            int groupId = groupIds[position];

            if (!isNull(position)) {
                state.set(groupId, state.get(groupId) + 1);
            }
        }
    }

    default void recycle() {
    }

    default boolean isRecyclable() {
        return false;
    }

    default <T> void setRecycler(DriverObjectPool.Recycler<T> recycler) {
    }

    /**
     * Try to remove null array if there is no TRUE value in it.
     *
     * @param force force to remove null array without check.
     */
    default void destroyNulls(boolean force) {
        // default: do nothing
    }

    default void addIntToBloomFilter(final int totalPartitionCount, RFBloomFilter[] RFBloomFilters) {
        final int positionCount = getPositionCount();
        for (int pos = 0; pos < positionCount; pos++) {

            // calc physical partition id.
            long hashVal = hashCodeUseXxhash(pos);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);

            // put hash code.
            RFBloomFilters[partition].putInt(hashCode(pos));
        }
    }

    default void addIntToBloomFilter(RFBloomFilter RFBloomFilter) {
        final int positionCount = getPositionCount();
        for (int pos = 0; pos < positionCount; pos++) {
            // put hash code.
            RFBloomFilter.putInt(hashCode(pos));
        }
    }

    default int mightContainsInt(RFBloomFilter RFBloomFilter, boolean[] bitmap) {
        return mightContainsInt(RFBloomFilter, bitmap, false);
    }

    default int mightContainsInt(RFBloomFilter RFBloomFilter, boolean[] bitmap, boolean isConjunctive) {
        int hitCount = 0;
        final int positionCount = getPositionCount();
        if (isConjunctive) {

            for (int pos = 0; pos < positionCount; pos++) {

                // Base on the original status in bitmap.
                if (bitmap[pos]) {
                    int hashCode = hashCode(pos);
                    bitmap[pos] &= RFBloomFilter.mightContainInt(hashCode);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        } else {

            for (int pos = 0; pos < positionCount; pos++) {
                int hashCode = hashCode(pos);
                bitmap[pos] = RFBloomFilter.mightContainInt(hashCode);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }

        }

        return hitCount;

    }

    default int mightContainsInt(final int totalPartitionCount, RFBloomFilter[] RFBloomFilters,
                                 boolean[] bitmap, boolean isPartitionConsistent) {
        int hitCount = 0;
        final int positionCount = getPositionCount();

        if (isPartitionConsistent) {
            // Find the consistent partition number.
            long hashVal = hashCodeUseXxhash(0);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
            RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

            for (int pos = 0; pos < positionCount; pos++) {
                int hashCode = hashCode(pos);
                bitmap[pos] = rfBloomFilter.mightContainInt(hashCode);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        } else {

            for (int pos = 0; pos < positionCount; pos++) {
                long hashVal = hashCodeUseXxhash(pos);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                int hashCode = hashCode(pos);
                bitmap[pos] = rfBloomFilter.mightContainInt(hashCode);
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        }

        return hitCount;
    }

    default int mightContainsInt(final int totalPartitionCount, RFBloomFilter[] RFBloomFilters,
                                 boolean[] bitmap, boolean isPartitionConsistent, boolean isConjunctive) {
        int hitCount = 0;
        final int positionCount = getPositionCount();

        if (isConjunctive) {

            if (isPartitionConsistent) {
                // Find the consistent partition number.
                long hashVal = hashCodeUseXxhash(0);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                for (int pos = 0; pos < positionCount; pos++) {

                    if (bitmap[pos]) {
                        int hashCode = hashCode(pos);
                        bitmap[pos] &= rfBloomFilter.mightContainInt(hashCode);
                        if (bitmap[pos]) {
                            hitCount++;
                        }
                    }

                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {

                    if (bitmap[pos]) {
                        long hashVal = hashCodeUseXxhash(pos);
                        int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                        RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                        int hashCode = hashCode(pos);
                        bitmap[pos] &= rfBloomFilter.mightContainInt(hashCode);
                        if (bitmap[pos]) {
                            hitCount++;
                        }

                    }

                }
            }

        } else {

            if (isPartitionConsistent) {
                // Find the consistent partition number.
                long hashVal = hashCodeUseXxhash(0);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                for (int pos = 0; pos < positionCount; pos++) {
                    int hashCode = hashCode(pos);
                    bitmap[pos] = rfBloomFilter.mightContainInt(hashCode);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {
                    long hashVal = hashCodeUseXxhash(pos);
                    int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                    RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                    int hashCode = hashCode(pos);
                    bitmap[pos] = rfBloomFilter.mightContainInt(hashCode);
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        }

        return hitCount;
    }

    default void addLongToBloomFilter(final int totalPartitionCount, RFBloomFilter[] RFBloomFilters) {
        final int positionCount = getPositionCount();
        for (int pos = 0; pos < positionCount; pos++) {

            // calc physical partition id.
            long hashVal = hashCodeUseXxhash(pos);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);

            // put hash code.
            RFBloomFilters[partition].putLong(getLong(pos));
        }
    }

    default void addLongToBloomFilter(RFBloomFilter RFBloomFilter) {
        final int positionCount = getPositionCount();
        for (int pos = 0; pos < positionCount; pos++) {
            // put hash code.
            RFBloomFilter.putLong(getLong(pos));
        }
    }

    default int mightContainsLong(RFBloomFilter RFBloomFilter, boolean[] bitmap) {
        return mightContainsLong(RFBloomFilter, bitmap, false);
    }

    default int mightContainsLong(RFBloomFilter RFBloomFilter, boolean[] bitmap, boolean isConjunctive) {
        int hitCount = 0;
        final int positionCount = getPositionCount();
        if (isConjunctive) {

            for (int pos = 0; pos < positionCount; pos++) {

                // Base on the original status in bitmap.
                if (bitmap[pos]) {
                    bitmap[pos] &= RFBloomFilter.mightContainLong(getLong(pos));
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        } else {

            for (int pos = 0; pos < positionCount; pos++) {
                bitmap[pos] = RFBloomFilter.mightContainLong(getLong(pos));
                if (bitmap[pos]) {
                    hitCount++;
                }
            }

        }

        return hitCount;

    }

    default int mightContainsLong(final int totalPartitionCount, RFBloomFilter[] RFBloomFilters,
                                  boolean[] bitmap, boolean isPartitionConsistent) {
        int hitCount = 0;
        final int positionCount = getPositionCount();

        if (isPartitionConsistent) {
            // Find the consistent partition number.
            long hashVal = hashCodeUseXxhash(0);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
            RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

            for (int pos = 0; pos < positionCount; pos++) {
                bitmap[pos] = rfBloomFilter.mightContainLong(getLong(pos));
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        } else {

            for (int pos = 0; pos < positionCount; pos++) {
                long hashVal = hashCodeUseXxhash(pos);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                bitmap[pos] = rfBloomFilter.mightContainLong(getLong(pos));
                if (bitmap[pos]) {
                    hitCount++;
                }
            }
        }

        return hitCount;
    }

    default int mightContainsLong(final int totalPartitionCount, RFBloomFilter[] RFBloomFilters,
                                  boolean[] bitmap, boolean isPartitionConsistent, boolean isConjunctive) {
        int hitCount = 0;
        final int positionCount = getPositionCount();

        if (isConjunctive) {

            if (isPartitionConsistent) {
                // Find the consistent partition number.
                long hashVal = hashCodeUseXxhash(0);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                for (int pos = 0; pos < positionCount; pos++) {

                    if (bitmap[pos]) {
                        bitmap[pos] &= rfBloomFilter.mightContainLong(getLong(pos));
                        if (bitmap[pos]) {
                            hitCount++;
                        }
                    }

                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {

                    if (bitmap[pos]) {
                        long hashVal = hashCodeUseXxhash(pos);
                        int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                        RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                        bitmap[pos] &= rfBloomFilter.mightContainLong(getLong(pos));
                        if (bitmap[pos]) {
                            hitCount++;
                        }

                    }

                }
            }

        } else {

            if (isPartitionConsistent) {
                // Find the consistent partition number.
                long hashVal = hashCodeUseXxhash(0);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                for (int pos = 0; pos < positionCount; pos++) {
                    bitmap[pos] = rfBloomFilter.mightContainLong(getLong(pos));
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            } else {

                for (int pos = 0; pos < positionCount; pos++) {
                    long hashVal = hashCodeUseXxhash(pos);
                    int partition = (int) ((hashVal & Long.MAX_VALUE) % totalPartitionCount);
                    RFBloomFilter rfBloomFilter = RFBloomFilters[partition];

                    bitmap[pos] = rfBloomFilter.mightContainLong(getLong(pos));
                    if (bitmap[pos]) {
                        hitCount++;
                    }
                }
            }

        }

        return hitCount;
    }
}

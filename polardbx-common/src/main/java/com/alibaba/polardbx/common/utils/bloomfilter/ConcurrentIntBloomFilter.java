/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.alibaba.polardbx.common.utils.bloomfilter;

import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;

import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class ConcurrentIntBloomFilter implements RFBloomFilter {

    public static final double DEFAULT_FPP = 0.03;
    private final LockFreeBitArray bits;
    private final int numHashFunctions;

    private ConcurrentIntBloomFilter(LockFreeBitArray bits, int numHashFunctions) {
        checkArgument(numHashFunctions > 0, "numHashFunctions (%s) must be > 0", numHashFunctions);
        checkArgument(numHashFunctions < 32, "numHashFunctions (%s) must be < 32", numHashFunctions);
        this.bits = checkNotNull(bits);
        this.numHashFunctions = numHashFunctions;
    }

    @Override
    public void putInt(int value) {
        int size = bits.bitSize();
        for (int i = 0; i < numHashFunctions; i++) {
            bits.set(Integer.remainderUnsigned(hash(value, i), size));
        }
    }

    @Override
    public boolean mightContainInt(int value) {
        int size = bits.bitSize();
        for (int i = 0; i < numHashFunctions; i++) {
            if (!bits.get(Integer.remainderUnsigned(hash(value, i), size))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void putLong(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mightContainLong(long value) {
        throw new UnsupportedOperationException();
    }

    private int hash(int value, int i) {
        return Integer.rotateRight(value, i * 5);
    }

    @Override
    public long sizeInBytes() {
        return bits.sizeInBytes();
    }

    @Override
    public void merge(RFBloomFilter other) {
        throw new UnsupportedOperationException();
    }

    public static long estimatedSizeInBytes(long expectedInsertions, double fpp) {
        if (expectedInsertions == 0) {
            expectedInsertions = 1;
        }
        int numBits = BloomFilterUtil.optimalNumOfBits(expectedInsertions, fpp);
        int arraySize = Ints.checkedCast(LongMath.divide(numBits, 64, RoundingMode.CEILING));
        return SizeOf.sizeOfLongArray(arraySize);
    }

    public static ConcurrentIntBloomFilter create(long expectedInsertions) {
        return create(expectedInsertions, DEFAULT_FPP); // FYI, for 3%, we always get 5 hash functions
    }

    public static ConcurrentIntBloomFilter create(long expectedInsertions, double fpp) {
        checkArgument(expectedInsertions >= 0, "Expected insertions (%s) must be >= 0", expectedInsertions);
        checkArgument(fpp > 0.0, "False positive probability (%s) must be > 0.0", fpp);
        checkArgument(fpp < 1.0, "False positive probability (%s) must be < 1.0", fpp);
        if (expectedInsertions == 0) {
            expectedInsertions = 1;
        }
        /*
         * TODO(user): Put a warning in the javadoc about tiny fpp values, since
         * the resulting size is proportional to -log(p), but there is not much
         * of a point after all, e.g. optimalM(1000, 0.0000000000000001) = 76680
         * which is less than 10kb. Who cares!
         */
        int numBits = BloomFilterUtil.optimalNumOfBits(expectedInsertions, fpp);
        int numHashFunctions = BloomFilterUtil.optimalNumOfHashFunctions(expectedInsertions, numBits);
        try {
            return new ConcurrentIntBloomFilter(new LockFreeBitArray(numBits), numHashFunctions);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not create BloomFilter of " + numBits + " bits", e);
        }
    }

    /**
     * Models a lock-free array of bits.
     * <p>
     * We use this instead of java.util.BitSet because we need access to the
     * array of longs and we need compare-and-swap.
     */
    static final class LockFreeBitArray {

        private static final int LONG_ADDRESSABLE_BITS = 6;

        private final AtomicLongArray data;

        LockFreeBitArray(int bits) {
            this(new long[Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING))]);
        }

        LockFreeBitArray(long[] data) {
            this.data = new AtomicLongArray(data);
        }

        /**
         * Returns true if the bit changed value.
         */
        boolean set(long bitIndex) {
            if (get(bitIndex)) {
                return false;
            }

            int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS);
            long mask = 1L << bitIndex; // only cares about low 6 bits of bitIndex

            long oldValue;
            long newValue;
            do {
                oldValue = data.get(longIndex);
                newValue = oldValue | mask;
                if (oldValue == newValue) {
                    return false;
                }
            } while (!data.compareAndSet(longIndex, oldValue, newValue));

            return true;
        }

        boolean get(long bitIndex) {
            return (data.get((int) (bitIndex >>> 6)) & (1L << bitIndex)) != 0;
        }

        int bitSize() {
            return data.length() * Long.SIZE;
        }

        long sizeInBytes() {
            return data.length() * Long.BYTES;
        }
    }
}

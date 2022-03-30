

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

package com.alibaba.polardbx.common.utils.bloomfilter;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;

import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class FastIntBloomFilter {

    private final LockFreeBitArray bits;
    private final int numHashFunctions;

    private FastIntBloomFilter(LockFreeBitArray bits, int numHashFunctions) {
        checkArgument(numHashFunctions > 0, "numHashFunctions (%s) must be > 0", numHashFunctions);
        checkArgument(numHashFunctions < 32, "numHashFunctions (%s) must be < 32", numHashFunctions);
        this.bits = checkNotNull(bits);
        this.numHashFunctions = numHashFunctions;
    }

    public void put(int value) {
        int size = bits.bitSize();
        for (int i = 0; i < numHashFunctions; i++) {
            bits.set(Integer.remainderUnsigned(hash(value, i), size));
        }
    }

    public boolean mightContain(int value) {
        int size = bits.bitSize();
        for (int i = 0; i < numHashFunctions; i++) {
            if (!bits.get(Integer.remainderUnsigned(hash(value, i), size))) {
                return false;
            }
        }
        return true;
    }

    private int hash(int value, int i) {
        return Integer.rotateRight(value, i * 5);
    }

    public long sizeInBytes() {
        return bits.sizeInBytes();
    }

    public static FastIntBloomFilter create(long expectedInsertions) {
        return create(expectedInsertions, 0.03);
    }

    public static FastIntBloomFilter create(long expectedInsertions, double fpp) {
        checkArgument(expectedInsertions >= 0, "Expected insertions (%s) must be >= 0", expectedInsertions);
        checkArgument(fpp > 0.0, "False positive probability (%s) must be > 0.0", fpp);
        checkArgument(fpp < 1.0, "False positive probability (%s) must be < 1.0", fpp);
        if (expectedInsertions == 0) {
            expectedInsertions = 1;
        }

        int numBits = BloomFilterUtil.optimalNumOfBits(expectedInsertions, fpp);
        int numHashFunctions = BloomFilterUtil.optimalNumOfHashFunctions(expectedInsertions, numBits);
        try {
            return new FastIntBloomFilter(new LockFreeBitArray(numBits), numHashFunctions);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not create BloomFilter of " + numBits + " bits", e);
        }
    }

    private static int optimalNumOfHashFunctions(long n, long m) {

        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    private static long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
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

        boolean set(long bitIndex) {
            if (get(bitIndex)) {
                return false;
            }

            int longIndex = (int) (bitIndex >>> LONG_ADDRESSABLE_BITS);
            long mask = 1L << bitIndex;

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

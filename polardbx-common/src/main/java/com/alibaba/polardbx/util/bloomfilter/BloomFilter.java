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

package com.alibaba.polardbx.util.bloomfilter;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteOrder;


@NotThreadSafe
public class BloomFilter {
    public static final ByteOrder PLATFORM_ENDIAN = ByteOrder.nativeOrder();

    public static final double DEFAULT_FPP = 0.03f;

    private final HashMethod hashMethod;
    private final int numHashFunctions;
    private final BitSet bitset;

    private final int numBits;

    private BloomFilter(HashMethod hashMethod, int numHashFunctions, BitSet bitset) {
        this.hashMethod = hashMethod;
        this.numHashFunctions = numHashFunctions;
        this.bitset = bitset;

        this.numBits = Math.multiplyExact(bitset.getData().length, Long.SIZE);
    }

    public TddlHasher newHasher() {
        return hashMethod.newHasher();
    }

    public void put(HashCode hashCode) {
        hashMethod.computeHashCodes(hashCode, numHashFunctions)
            .forEachOrdered(bitIndex -> bitset.set((int) (bitIndex % numBits)));
    }

    public boolean mightContain(HashCode hashCode) {
        return hashMethod.computeHashCodes(hashCode, numHashFunctions)
            .mapToInt(bitIdx -> (int) (bitIdx % numBits))
            .allMatch(bitset::get);
    }

    public void merge(BloomFilter other) {
        Preconditions.checkArgument(other.numBits == numBits, "Number of bits not match!");
        Preconditions.checkArgument(other.numHashFunctions == numHashFunctions, "Number of hash functions not match!");
        Preconditions.checkArgument(other.hashMethod.equals(hashMethod), "Hash method not match!");

        bitset.putAll(other.bitset);
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    public long[] getBitmap() {
        return bitset.getData();
    }

    public HashMethodInfo getHashMethodInfo() {
        return hashMethod.metadata();
    }

    public static BloomFilter createEmpty(HashMethodInfo hashMethodInfo, int numHashFunctions, int numBits) {
        HashMethod hashMethod = HashMethodFactory.build(hashMethodInfo);
        BitSet bitset = new BitSet(numBits);

        return new BloomFilter(hashMethod, numHashFunctions, bitset);
    }

    public static BloomFilter createEmpty(long expectedInsertions, double fpp) {
        int nb = optimalNumOfBits(expectedInsertions, fpp);
        int numBits = nb + (Long.SIZE - (nb % Long.SIZE));
        int numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);

        return createEmpty(HashMethodInfo.defaultHashMethod(), numHashFunctions, numBits);
    }

    public static BloomFilter createEmpty(long expectedInsertions) {
        return createEmpty(expectedInsertions, DEFAULT_FPP);
    }

    public static BloomFilter createWithData(HashMethodInfo hashMethodInfo, int numHashFunctions, long[] data) {
        return new BloomFilter(HashMethodFactory.build(hashMethodInfo), numHashFunctions, new BitSet(data));
    }

    public static int optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalNumOfHashFunctions(long n, long m) {

        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

}

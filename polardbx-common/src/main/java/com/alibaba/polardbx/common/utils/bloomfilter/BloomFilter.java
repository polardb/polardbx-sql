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

import com.alibaba.polardbx.common.utils.hash.HashMethodFactory;
import com.alibaba.polardbx.common.utils.hash.HashMethodInfo;
import com.alibaba.polardbx.common.utils.hash.HashResult128;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteOrder;


@NotThreadSafe
public class BloomFilter {
    public static final double DEFAULT_FPP = 0.03f;

    protected final BloomFilterHashMethod hashMethod;
    private final int numHashFunctions;
    private final BitSet bitset;

    private final int numBits;
    private final boolean is64Bit;

    private BloomFilter(BloomFilterHashMethod hashMethod, int numHashFunctions, BitSet bitset) {
        this.hashMethod = hashMethod;
        this.numHashFunctions = numHashFunctions;
        this.bitset = bitset;

        this.numBits = Math.multiplyExact(bitset.getData().length, Long.SIZE);
        this.is64Bit = hashMethod.is64Bit();
    }

    private IStreamingHasher getHasher() {
        return hashMethod.getHasher();
    }

    public IStreamingHasher newHasher() {
        return hashMethod.newHasher();
    }

    public void put(HashResult128 hashResult) {
        if (is64Bit) {
            put64(hashResult);
        } else {
            put128(hashResult);
        }
    }

    private void put64(HashResult128 hashResult) {
        int hash1 = (int) hashResult.getResult1();
        int hash2 = (int) (hashResult.getResult1() >>> 32);
        int combined = hash1 + hash2;
        for (int i = 0; i < numHashFunctions; i++) {
            if (combined < 0) {
                combined &= Integer.MAX_VALUE;
            }
            bitset.set(combined % numBits);
            combined += hash2;
        }
    }

    private void put128(HashResult128 hashResult) {
        long hash1 = hashResult.getResult1();
        long hash2 = hashResult.getResult2();
        long combined = hash1;
        for (int i = 0; i < numHashFunctions; i++) {
            long res = combined & Long.MAX_VALUE;
            int bits = (int) (res % numBits);
            bitset.set(bits);
            combined += hash2;
        }
    }

    public boolean mightContain(HashResult128 hashResult) {
        if (is64Bit) {
            return mightContain64(hashResult);
        } else {
            return mightContain128(hashResult);
        }
    }

    private boolean mightContain128(HashResult128 hashResult) {
        long hash1 = hashResult.getResult1();
        long hash2 = hashResult.getResult2();
        long combinedHash = hash1;
        for (int i = 0; i < numHashFunctions; i++) {
            long res = combinedHash & Long.MAX_VALUE;
            int bits = (int) (res % numBits);
            if (!bitset.get(bits)) {
                // 只要有一次不匹配 就不存在
                return false;
            }
            combinedHash += hash2;
        }
        return true;
    }

    private boolean mightContain64(HashResult128 hashResult) {
        int hash1 = (int) hashResult.getResult1();
        int hash2 = (int) (hashResult.getResult1() >>> 32);
        int combined = hash1 + hash2;
        for (int i = 0; i < numHashFunctions; i++) {
            if (combined < 0) {
                combined &= Integer.MAX_VALUE;
            }
            if (!bitset.get(combined % numBits)) {
                // 只要有一次不匹配 就不存在
                return false;
            }
            combined += hash2;
        }
        return true;
    }

    /**
     * NotThreadSafe
     */
    public void put(String str) {
        this.put(getHasher().putString(str).hash());
    }

    public void putSynchronized(String str) {
        synchronized (this) {
            this.put(getHasher().putString(str).hash());
        }
    }

    /**
     * NotThreadSafe
     */
    public boolean mightContain(String str) {
        return this.mightContain(getHasher().putString(str).hash());
    }

    public boolean mightContainSynchronized(String str) {
        synchronized (this) {
            return this.mightContain(getHasher().putString(str).hash());
        }
    }

    public void clear() {
        bitset.clear();
    }

    /**
     * NotThreadSafe
     */
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
        BloomFilterHashMethod hashMethod = HashMethodFactory.buildForBloomFilter(hashMethodInfo);
        BitSet bitset = new BitSet(numBits);

        return new BloomFilter(hashMethod, numHashFunctions, bitset);
    }

    public static BloomFilter createEmpty(HashMethodInfo hashMethodInfo, long expectedInsertions, double fpp) {
        int nb = BloomFilterUtil.optimalNumOfBits(expectedInsertions, fpp);
        int numBits = nb + (Long.SIZE - (nb % Long.SIZE));
        int numHashFunctions = BloomFilterUtil.optimalNumOfHashFunctions(expectedInsertions, numBits);

        return createEmpty(hashMethodInfo, numHashFunctions, numBits);
    }

    public static BloomFilter createEmpty(long expectedInsertions, double fpp) {
        return createEmpty(HashMethodInfo.defaultHashMethod(), expectedInsertions, fpp);
    }

    public static BloomFilter createEmpty(long expectedInsertions) {
        return createEmpty(expectedInsertions, DEFAULT_FPP);
    }

    public static BloomFilter createWithData(HashMethodInfo hashMethodInfo, int numHashFunctions, long[] data) {
        return new BloomFilter(HashMethodFactory.buildForBloomFilter(hashMethodInfo), numHashFunctions,
            new BitSet(data));
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

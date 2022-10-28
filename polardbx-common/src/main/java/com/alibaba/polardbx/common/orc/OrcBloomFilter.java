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

package com.alibaba.polardbx.common.orc;

import org.apache.orc.util.Murmur3;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

public class OrcBloomFilter {
    public static final double DEFAULT_FPP = 0.05;
    private final OrcBloomFilter.BitSet bitSet;
    private final int numBits;
    private final int numHashFunctions;

    static void checkArgument(boolean expression, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    public OrcBloomFilter(long expectedEntries) {
        this(expectedEntries, DEFAULT_FPP);
    }

    public OrcBloomFilter(long expectedEntries, double fpp) {
        expectedEntries = Math.max(expectedEntries, 1);
        checkArgument(fpp > 0.0 && fpp < 1.0, "False positive probability should be > 0.0 & < 1.0");
        int nb = optimalNumOfBits(expectedEntries, fpp);
        // make 'm' multiple of 64
        this.numBits = nb + (Long.SIZE - (nb % Long.SIZE));
        this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
        this.bitSet = new OrcBloomFilter.BitSet(numBits);
    }

    /**
     * A constructor to support rebuilding the BloomFilter from a serialized representation.
     *
     * @param bits the serialized bits
     * @param numFuncs the number of functions used
     */
    public OrcBloomFilter(long[] bits, int numFuncs) {
        super();
        bitSet = new OrcBloomFilter.BitSet(bits);
        this.numBits = (int) bitSet.bitSize();
        numHashFunctions = numFuncs;
    }

    static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    static int optimalNumOfBits(long n, double p) {
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    @Override
    public boolean equals(Object other) {
        return other != null &&
            other.getClass() == getClass() &&
            numBits == ((OrcBloomFilter) other).numBits &&
            numHashFunctions == ((OrcBloomFilter) other).numHashFunctions &&
            bitSet.equals(((OrcBloomFilter) other).bitSet);
    }

    @Override
    public int hashCode() {
        return bitSet.hashCode() + numHashFunctions * 5;
    }

    public void add(byte[] val) {
        addBytes(val, 0, val == null ? 0 : val.length);
    }

    public void addBytes(byte[] val, int offset, int length) {
        // We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
        // by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
        // implement a Bloom filter without any loss in the asymptotic false positive probability'

        // Lets split up 64-bit hashcode into two 32-bit hash codes and employ the technique mentioned
        // in the above paper
        long hash64 = val == null ? Murmur3.NULL_HASHCODE :
            Murmur3.hash64(val, offset, length);
        addHash(hash64);
    }

    private void addHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            bitSet.set(pos);
        }
    }

    public void addString(String val) {
        if (val == null) {
            add(null);
        } else {
            add(val.getBytes(Charset.defaultCharset()));
        }
    }

    public void addLong(long val) {
        addHash(getLongHash(val));
    }

    public void addDouble(double val) {
        addLong(Double.doubleToLongBits(val));
    }

    public boolean test(byte[] val) {
        return testBytes(val, 0, val == null ? 0 : val.length);
    }

    public boolean testBytes(byte[] val, int offset, int length) {
        long hash64 = val == null ? Murmur3.NULL_HASHCODE :
            Murmur3.hash64(val, offset, length);
        return testHash(hash64);
    }

    public boolean testHash(int hash1, int hash2) {
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    public boolean testHash(int[] combinedHashes) {
        for (int i = 1; i <= numHashFunctions; i++) {
            if (!bitSet.get(combinedHashes[i] % numBits)) {
                return false;
            }
        }
        return true;
    }

    public boolean testHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    public boolean testString(String val) {
        if (val == null) {
            return test(null);
        } else {
            return test(val.getBytes(Charset.defaultCharset()));
        }
    }

    public boolean testLong(long val) {
        return testHash(getLongHash(val));
    }

    // Thomas Wang's integer hash function
    // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
    private long getLongHash(long key) {
        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = (key + (key << 3)) + (key << 8); // key * 265
        key = key ^ (key >> 14);
        key = (key + (key << 2)) + (key << 4); // key * 21
        key = key ^ (key >> 28);
        key = key + (key << 31);
        return key;
    }

    public boolean testDouble(double val) {
        return testLong(Double.doubleToLongBits(val));
    }

    public long sizeInBytes() {
        return getBitSize() / 8;
    }

    public int getBitSize() {
        return bitSet.getData().length * Long.SIZE;
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    public long[] getBitSet() {
        return bitSet.getData();
    }

    @Override
    public String toString() {
        return "m: " + numBits + " k: " + numHashFunctions;
    }

    /**
     * Merge the specified bloom filter with current bloom filter.
     *
     * @param that - bloom filter to merge
     */
    public void merge(OrcBloomFilter that) {
        if (this != that && this.numBits == that.numBits && this.numHashFunctions == that.numHashFunctions) {
            this.bitSet.putAll(that.bitSet);
        } else {
            throw new IllegalArgumentException("BloomFilters are not compatible for merging." +
                " this - " + this.toString() + " that - " + that.toString());
        }
    }

    public void reset() {
        this.bitSet.clear();
    }

    /**
     * Serialize the bloom filter to output stream.
     *
     * @param out output stream
     * @param bloomFilter bloom filter instance
     * @return written bytes
     */
    public static int serialize(OutputStream out, OrcBloomFilter bloomFilter) throws IOException {
        int bytes = 0;
        /**
         * Serialized BloomFilter format:
         * 1 byte for the number of hash functions.
         * 1 big endian int(That is how OutputStream works) for the number of longs in the bitset
         * big endian longs in the BloomFilter bitset
         */
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        dataOutputStream.writeByte(bloomFilter.numHashFunctions);
        dataOutputStream.writeInt(bloomFilter.getBitSet().length);
        bytes += (Byte.BYTES + Integer.BYTES);

        for (long value : bloomFilter.getBitSet()) {
            dataOutputStream.writeLong(value);
            bytes += Long.BYTES;
        }

        return bytes;
    }

    /**
     * Deserialize a bloom filter
     * Read a byte stream, which was written by serialize(OutputStream, OrcBloomFilter)
     * into a {@code BloomFilter}
     *
     * @param in input bytestream
     * @return deserialized BloomFilter
     */
    public static OrcBloomFilter deserialize(InputStream in) throws IOException {
        if (in == null) {
            throw new IOException("Input stream is null");
        }

        try {
            DataInputStream dataInputStream = new DataInputStream(in);
            int numHashFunc = dataInputStream.readByte();
            int numLongs = dataInputStream.readInt();
            long[] data = new long[numLongs];
            for (int i = 0; i < numLongs; i++) {
                data[i] = dataInputStream.readLong();
            }
            return new OrcBloomFilter(data, numHashFunc);
        } catch (RuntimeException e) {
            IOException io = new IOException("Unable to deserialize BloomFilter");
            io.initCause(e);
            throw io;
        }
    }

    /**
     * Bare metal bit set implementation. For performance reasons, this implementation does not check
     * for index bounds nor expand the bit set size if the specified index is greater than the size.
     */
    public static class BitSet {
        private final long[] data;

        public BitSet(long bits) {
            this(new long[(int) Math.ceil((double) bits / (double) Long.SIZE)]);
        }

        /**
         * Deserialize long array as bit set.
         *
         * @param data - bit array
         */
        public BitSet(long[] data) {
            assert data.length > 0 : "data length is zero!";
            this.data = data;
        }

        /**
         * Sets the bit at specified index.
         *
         * @param index - position
         */
        public void set(int index) {
            data[index >>> 6] |= (1L << index);
        }

        /**
         * Returns true if the bit is set in the specified index.
         *
         * @param index - position
         * @return - value at the bit position
         */
        public boolean get(int index) {
            return (data[index >>> 6] & (1L << index)) != 0;
        }

        /**
         * Number of bits
         */
        public long bitSize() {
            return (long) data.length * Long.SIZE;
        }

        public long[] getData() {
            return data;
        }

        /**
         * Combines the two BitArrays using bitwise OR.
         */
        public void putAll(OrcBloomFilter.BitSet array) {
            assert data.length == array.data.length :
                "BitArrays must be of equal length (" + data.length + "!= " + array.data.length + ")";
            for (int i = 0; i < data.length; i++) {
                data[i] |= array.data[i];
            }
        }

        /**
         * Clear the bit set.
         */
        public void clear() {
            Arrays.fill(data, 0);
        }

        @Override
        public boolean equals(Object other) {
            return other != null &&
                other.getClass() == getClass() &&
                Arrays.equals(data, ((OrcBloomFilter.BitSet) other).data);
        }

        @Override
        public int hashCode() {
            int result = 0;
            for (long l : data) {
                result = (int) (result * 13 + l);
            }
            return result;
        }
    }
}

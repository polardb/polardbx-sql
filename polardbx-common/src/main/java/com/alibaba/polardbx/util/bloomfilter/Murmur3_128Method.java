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

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.PrimitiveIterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static com.alibaba.polardbx.util.bloomfilter.BloomFilter.PLATFORM_ENDIAN;
import static java.util.Spliterator.ORDERED;

public class Murmur3_128Method extends BaseHashMethod {
    public static final String METHOD_NAME = "murmur3_128";
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    private static final Murmur3_128Method SINGLETON = new Murmur3_128Method();

    private Murmur3_128Method() {
        super(new HashMethodInfo(METHOD_NAME), HASH_FUNCTION);
    }

    private static long lowerEight(HashCode hashCode) {
        byte[] bytes = hashCode.asBytes();
        if (ByteOrder.BIG_ENDIAN == PLATFORM_ENDIAN) {
            return Longs.fromBytes(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]);
        } else {
            return Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        }
    }

    private static long higherEight(HashCode hashCode) {
        byte[] bytes = hashCode.asBytes();
        if (ByteOrder.BIG_ENDIAN == PLATFORM_ENDIAN) {
            return Longs
                .fromBytes(bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
        } else {
            return Longs
                .fromBytes(bytes[15], bytes[14], bytes[13], bytes[12], bytes[11], bytes[10], bytes[9], bytes[8]);
        }
    }

    public static Murmur3_128Method create(Object... args) {
        return SINGLETON;
    }

    @Override
    public TddlHasher newHasher() {
        return new Murmur2_128TddlHasher();
    }

    @Override
    public LongStream computeHashCodes(HashCode hashCode, int numHashFunctions) {
        long hash1 = lowerEight(hashCode);
        long hash2 = higherEight(hashCode);

        return StreamSupport.longStream(
            () -> Spliterators.spliteratorUnknownSize(new HashCodeIterator(hash1, hash2, numHashFunctions), ORDERED),
            ORDERED, false);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    private static class HashCodeIterator implements PrimitiveIterator.OfLong {
        private final long hash2;
        private final int numHashFunctions;

        private long combined;
        private int cur;

        private HashCodeIterator(long hash1, long hash2, int numHashFunctions) {
            this.combined = hash1;
            this.hash2 = hash2;
            this.numHashFunctions = numHashFunctions;
            this.cur = 0;
        }

        @Override
        public long nextLong() {
            long ret = combined & Long.MAX_VALUE;
            combined += hash2;
            cur += 1;
            return ret;
        }

        @Override
        public boolean hasNext() {
            return cur < numHashFunctions;
        }
    }

    private static class Murmur2_128TddlHasher implements TddlHasher {
        private static final Field FIELD_H1;
        private static final Field FIELD_H2;
        private static final Field FIELD_LENGTH;
        private static final Field FIELD_BUFFER;

        private static final int CHUNK_SIZE = 16;

        static {
            try {
                Class<?> HASHER_CLASS =
                    Class.forName("com.google.common.hash.Murmur3_128HashFunction$Murmur3_128Hasher");
                FIELD_H1 = HASHER_CLASS.getDeclaredField("h1");
                FIELD_H1.setAccessible(true);
                FIELD_H2 = HASHER_CLASS.getDeclaredField("h2");
                FIELD_H2.setAccessible(true);
                FIELD_LENGTH = HASHER_CLASS.getDeclaredField("length");
                FIELD_LENGTH.setAccessible(true);

                FIELD_BUFFER = HASHER_CLASS.getSuperclass().getDeclaredField("buffer");
                FIELD_BUFFER.setAccessible(true);
            } catch (Exception e) {
                throw GeneralUtil.nestedException("Failed to init Murmur2_128TddlHasher", e);
            }
        }

        private final Hasher hasher = HASH_FUNCTION.newHasher();
        private final ByteBuffer innerBuffer;

        private Murmur2_128TddlHasher() {
            try {
                innerBuffer = (ByteBuffer) FIELD_BUFFER.get(hasher);
            } catch (IllegalAccessException e) {
                throw GeneralUtil.nestedException("Failed to init Murmur2_128TddlHasher inner buffer.", e);
            }
        }

        @Override
        public TddlHasher putByte(byte b) {
            hasher.putLong(b);
            return this;
        }

        @Override
        public TddlHasher putBytes(byte[] bytes, int off, int len) {
            hasher.putBytes(bytes, off, len);
            return this;
        }

        @Override
        public TddlHasher putBytes(ByteBuffer bytes) {
            hasher.putBytes(bytes);
            return this;
        }

        @Override
        public TddlHasher putLong(long l) {
            hasher.putLong(l);
            return this;
        }

        @Override
        public TddlHasher putDouble(double d) {
            hasher.putDouble(d);
            return this;
        }

        @Override
        public TddlHasher putBoolean(boolean b) {
            hasher.putBoolean(b);
            return this;
        }

        @Override
        public TddlHasher putChar(char c) {
            hasher.putChar(c);
            return this;
        }

        @Override
        public HashCode hash() {
            HashCode ret = hasher.hash();
            reset();
            return ret;
        }

        private void reset() {

            try {
                FIELD_H1.setLong(hasher, 0);
                FIELD_H2.setLong(hasher, 0);
                FIELD_LENGTH.setInt(hasher, 0);

                innerBuffer.flip();
                innerBuffer.position(0);
                innerBuffer.limit(CHUNK_SIZE);
            } catch (Exception e) {
                throw GeneralUtil.nestedException("Failed to reset Murmur2_128TddlHasher.", e);
            }
        }
    }
}

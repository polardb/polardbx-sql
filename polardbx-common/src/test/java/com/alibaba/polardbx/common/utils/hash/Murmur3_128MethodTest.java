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

package com.alibaba.polardbx.common.utils.hash;

import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static com.alibaba.polardbx.common.utils.hash.ByteUtil.PLATFORM_ENDIAN;

/**
 * 验证 Murmur3_128Hasher实现的正确性
 * 保证与guava库一致性
 * 注意: 整数一律以long处理
 */
public class Murmur3_128MethodTest {

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

    @Test
    public void testMyMurmur3BlockHash() {
        IBlockHasher myMurmur3 = new Murmur3_128Hasher(0);
        HashFunction hashFunction = Hashing.murmur3_128();

        HashResult128 myHashResult;
        HashCode hashCode;

        long l = -1364121479110812L;
        myHashResult = myMurmur3.hashLong(l);
        hashCode = hashFunction.hashLong(l);
        Assert.assertEquals(lowerEight(hashCode), myHashResult.getResult1());
        Assert.assertEquals(higherEight(hashCode), myHashResult.getResult2());

        int i = 132119;
        myHashResult = myMurmur3.hashInt(i);
        hashCode = hashFunction.hashLong(i);
        Assert.assertEquals(lowerEight(hashCode), myHashResult.getResult1());
        Assert.assertEquals(higherEight(hashCode), myHashResult.getResult2());

        short s = (short) 13211;
        myHashResult = myMurmur3.hashShort(s);
        hashCode = hashFunction.hashLong(s);
        Assert.assertEquals(lowerEight(hashCode), myHashResult.getResult1());
        Assert.assertEquals(higherEight(hashCode), myHashResult.getResult2());

        // 多个block
        String str = "asdzxcasdasdasdasew45123";
        myHashResult = myMurmur3.hashString(str);
        hashCode = hashFunction.hashBytes(str.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(lowerEight(hashCode), myHashResult.getResult1());
        Assert.assertEquals(higherEight(hashCode), myHashResult.getResult2());

        // 和String处理方式相同
        byte[] bytes = new byte[37];
        new Random().nextBytes(bytes);
        myHashResult = myMurmur3.hashBytes(bytes);
        hashCode = hashFunction.hashBytes(bytes);
        Assert.assertEquals(lowerEight(hashCode), myHashResult.getResult1());
        Assert.assertEquals(higherEight(hashCode), myHashResult.getResult2());
    }

    @Test
    public void testMyMurmur3StreamingHash() {
        IStreamingHasher myMurmur3 = new Murmur3_128Hasher(0);
        Hasher hashFunction = Hashing.murmur3_128().newHasher();

        HashResult128 myHashResult;
        HashCode hashCode;

        long l = -1364121479110812L;
        int i = 132119;
        short s = (short) 127;

        myHashResult = myMurmur3.putLong(l).putInt(i).putShort(s).hash();
        hashCode = hashFunction.putLong(l).putLong(i).putLong(s).hash();
        Assert.assertEquals(lowerEight(hashCode), myHashResult.getResult1());
        Assert.assertEquals(higherEight(hashCode), myHashResult.getResult2());

        // reset for guava hasher
        hashFunction = Hashing.murmur3_128().newHasher();
        // long enough for multiple blocks
        final byte[] tmpBytes = "asdz231zxcvsasd4555412zdxfvtjhstsdfdsf123你好".getBytes(StandardCharsets.UTF_8);
        myHashResult = myMurmur3.putBytes(tmpBytes).hash();
        hashCode = hashFunction.putBytes(tmpBytes).hash();
        Assert.assertEquals(lowerEight(hashCode), myHashResult.getResult1());
        Assert.assertEquals(higherEight(hashCode), myHashResult.getResult2());
    }

    private final HashFunction zeroSeedMurmur3hashFunc = Hashing.murmur3_128(0);

    /**
     * 测试与 {@link com.alibaba.polardbx.common.partition.MurmurHashUtils} 原返回结果一致
     */
    @Test
    public void testWithMurmurHashUtils() {
        long data;
        final int times = 100_000_000;
        Random random = new Random(System.currentTimeMillis());

        for (int i = 0; i < times; i++) {
            data = random.nextLong();
            equalWithGuavaHash(data);
        }

        equalWithGuavaHash(Long.MAX_VALUE);
        equalWithGuavaHash(Long.MIN_VALUE);
    }

    /**
     * 分片算法依赖guava的murmur3 hash
     * 要保障不出错
     */
    @Ignore
    @Test
    public void testAllWithMurmurHashUtils() {
        for (long i = Long.MIN_VALUE; i < Long.MAX_VALUE; i++) {
            equalWithGuavaHash(i);
        }
        equalWithGuavaHash(Long.MAX_VALUE);
    }

    private void equalWithGuavaHash(long data) {
        long originResult = zeroSeedMurmur3hashFunc.hashLong(data).asLong();
        long curResult = MurmurHashUtils.murmurHash128WithZeroSeed(data);
        Assert.assertEquals(originResult, curResult);
    }
}

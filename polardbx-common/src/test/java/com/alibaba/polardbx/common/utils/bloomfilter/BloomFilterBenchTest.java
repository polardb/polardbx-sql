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

import com.alibaba.polardbx.common.utils.hash.HashMethodInfo;
import com.alibaba.polardbx.common.utils.hash.HashResult128;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

/**
 * 简易模拟测试
 * 待引入JMH框架
 */
public class BloomFilterBenchTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterBenchTest.class);
    private static final int NUM_ELEMENT = 10_000_000;
    private static final double FPP = 0.03;
    private static final List<HashMethodInfo> SUPPORT_HASH_INFO = new ArrayList<HashMethodInfo>() {{
        add(HashMethodInfo.MURMUR3_METHOD);
        add(HashMethodInfo.XXHASH_METHOD);
    }};

    private List<BloomFilter> bloomFilters;
    private List<IStreamingHasher> hashers;
    private static long[] longData;
    private static int[] intData;

    @BeforeClass
    public static void setUpResources() {
        longData = new long[NUM_ELEMENT];
        intData = new int[NUM_ELEMENT];
        Random random = new Random(0);
        for (int i = 0; i < NUM_ELEMENT; i++) {
            longData[i] = random.nextLong();
            intData[i] = random.nextInt();
        }
    }

    @Before
    public void setUp() {
        bloomFilters = new ArrayList<>(SUPPORT_HASH_INFO.size());
        hashers = new ArrayList<>(SUPPORT_HASH_INFO.size());
        for (HashMethodInfo hashInfo : SUPPORT_HASH_INFO) {
            BloomFilter bloomFilter = BloomFilter.createEmpty(hashInfo, NUM_ELEMENT, FPP);
            bloomFilters.add(bloomFilter);
            hashers.add(bloomFilter.newHasher());
        }
    }

    public void putLongs(BloomFilter bloomFilter, IStreamingHasher hasher) {
        for (int i = 0; i < NUM_ELEMENT; i++) {
            bloomFilter.put(hasher.putLong(longData[i]).putInt(intData[i]).hash());
        }
    }

    public void probeAll(BloomFilter bloomFilter, IStreamingHasher hasher) {
        for (int i = 0; i < NUM_ELEMENT; i++) {
            bloomFilter.mightContain(hasher.putLong(longData[i]).putInt(intData[i]).hash());
        }
    }

    @Test
    public void testBuild() {
        for (int i = 0; i < SUPPORT_HASH_INFO.size(); i++) {
            long start = System.nanoTime();
            putLongs(bloomFilters.get(i), hashers.get(i));
            long end = System.nanoTime();
            LOGGER.info(String.format("[%s] Probe used %.4f s%n", SUPPORT_HASH_INFO.get(i).getMethodName(),
                (end - start) / 1000_000_000F));
        }
    }

    @Test
    public void testProbe() {
        for (int i = 0; i < SUPPORT_HASH_INFO.size(); i++) {
            putLongs(bloomFilters.get(i), hashers.get(i));
        }
        for (int i = 0; i < SUPPORT_HASH_INFO.size(); i++) {
            long start = System.nanoTime();
            probeAll(bloomFilters.get(i), hashers.get(i));
            long end = System.nanoTime();
            LOGGER.info(String.format("[%s] Probe used %.4f s%n", SUPPORT_HASH_INFO.get(i).getMethodName(),
                (end - start) / 1000_000_000F));
        }
    }

    @Test
    public void testFalsePositive() {
        for (int i = 0; i < SUPPORT_HASH_INFO.size(); i++) {
            putLongs(bloomFilters.get(i), hashers.get(i));
        }

        HashSet<Long> allValues = new HashSet<>(NUM_ELEMENT);
        for (long l : longData) {
            allValues.add(l);
        }
        final int testCount = NUM_ELEMENT;
        final double fppWithError = FPP * (1.005);
        for (int i = 0; i < SUPPORT_HASH_INFO.size(); i++) {
            long falsePositiveCount = 0;
            BloomFilter bloomFilter = bloomFilters.get(i);
            IStreamingHasher hasher = hashers.get(i);
            Random random = new Random(System.currentTimeMillis());
            for (int j = 0; j < testCount; j++) {
                long value = random.nextLong();
                boolean contains = allValues.contains(value);
                boolean guess = bloomFilter.mightContain(hasher.putLong(value).hash());

                if (!contains && guess) {
                    falsePositiveCount += 1;
                }
            }
            double falsePositiveRate = falsePositiveCount * 1.0 / testCount;
            Assert.assertTrue(String.format("[%s] False positive rate %.6f higher than expected %.6f",
                SUPPORT_HASH_INFO.get(i).getMethodName(), falsePositiveRate, FPP), falsePositiveRate <= fppWithError);
        }
    }

    @Test
    public void testHashCollision() {
        HashSet<HashResult128> allValuesAfterHash = new HashSet<>(NUM_ELEMENT);
        for (int i = 0; i < SUPPORT_HASH_INFO.size(); i++) {
            IStreamingHasher hasher = hashers.get(i);
            for (long l : longData) {
                allValuesAfterHash.add(hasher.putLong(l).hash());
            }
            double collisionRate = 1.0 - allValuesAfterHash.size() * 1.0 / NUM_ELEMENT;
            LOGGER.info(
                String.format("[%s] hash collision: %.7f", SUPPORT_HASH_INFO.get(i).getMethodName(), collisionRate));
            allValuesAfterHash.clear();
        }
    }
}

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

import com.alibaba.polardbx.util.bloomfilter.BloomFilter;
import com.alibaba.polardbx.util.bloomfilter.TddlHasher;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {
    private static final int NUM_ELEMENT = 1_000_000;

    @Test
    public void testCheckContainsAfterPut() {
        BloomFilter bloomFilter = BloomFilter.createEmpty(NUM_ELEMENT, 0.03);

        HashSet<Long> allValues = new HashSet<>(NUM_ELEMENT);
        Random random = new Random(0);

        TddlHasher hasher = bloomFilter.newHasher();

        for (int i = 0; i < NUM_ELEMENT; i++) {
            long value = random.nextLong();
            allValues.add(value);
            bloomFilter.put(hasher.putLong(value).hash());
        }

        for (Long value : allValues) {
            assertTrue(bloomFilter.mightContain(hasher.putLong(value).hash()));
        }

        long falsePositiveCount = 0;
        for (int i = 0; i < NUM_ELEMENT; i++) {
            long value = random.nextLong();
            boolean contains = allValues.contains(value);
            boolean guess = bloomFilter.mightContain(hasher.putLong(value).hash());

            if (!contains && guess) {
                falsePositiveCount += 1;
            }
        }

        assertTrue((falsePositiveCount * 1.0 / NUM_ELEMENT) < 0.04);
    }

    @Test
    public void testMerge() {
        Random random = new Random(0);
        HashSet<Long> values = new HashSet<>(20);

        BloomFilter bloomFilter1 = BloomFilter.createEmpty(1000, 0.03);
        TddlHasher hasher1 = bloomFilter1.newHasher();

        for (int i = 0; i < 10; i++) {
            long value = random.nextLong();
            values.add(value);
            bloomFilter1.put(hasher1.putLong(value).hash());
        }

        BloomFilter bloomFilter2 = BloomFilter.createEmpty(1000, 0.03);
        TddlHasher hasher2 = bloomFilter2.newHasher();

        for (int i = 0; i < 10; i++) {
            long value = random.nextLong();
            values.add(value);
            bloomFilter2.put(hasher2.putLong(value).hash());
        }

        bloomFilter1.merge(bloomFilter2);

        for (long value : values) {
            assertTrue(bloomFilter1.mightContain(hasher1.putLong(value).hash()));
        }
    }

    @Test
    public void testDifferentObjectTypes() {
        BloomFilter bloomFilter = BloomFilter.createEmpty(1000, 0.03);
        TddlHasher hasher = bloomFilter.newHasher();

        bloomFilter.put(hasher.putLong(1234).hash());
        bloomFilter.put(hasher.putString("abcd").hash());

        assertTrue(bloomFilter.mightContain(hasher.putLong(1234).hash()));
        assertTrue(bloomFilter.mightContain(hasher.putString("abcd").hash()));
        assertFalse(bloomFilter.mightContain(hasher.putString("dcba").hash()));
        assertFalse(bloomFilter.mightContain(hasher.putLong(4321).hash()));
    }
}

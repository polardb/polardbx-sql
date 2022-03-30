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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.utils.bloomfilter.FastIntBloomFilter;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FastIntBloomFilterTest {

    private static final int NUM_ELEMENT = 1000000;

    @Test
    public void test() {
        Random rand = new Random();

        IntOpenHashSet hashSet = new IntOpenHashSet(NUM_ELEMENT);
        FastIntBloomFilter bloomFilter = FastIntBloomFilter.create(NUM_ELEMENT);
        for (int i = 0; i < NUM_ELEMENT; i++) {
            int value = rand.nextInt();
            bloomFilter.put(value);
            hashSet.add(value);
        }

        IntIterator iterator = hashSet.iterator();
        while (iterator.hasNext()) {
            int value = iterator.nextInt();
            boolean result = bloomFilter.mightContain(value);
            assertTrue(result);
        }

        int countFalsePositive = 0;
        int countTotal = 0;
        for (int i = 0; i < NUM_ELEMENT; i++) {
            int value = rand.nextInt();
            boolean truth = hashSet.contains(value);
            boolean guess = bloomFilter.mightContain(value);
            if (truth == true && guess == false) {
                fail("false negative detected!");
            } else if (truth == false && guess == true) {
                countFalsePositive++;
            }
            countTotal++;
        }

        double fpp = (double) countFalsePositive / countTotal;
        System.out.println("fpp = " + fpp + ", expected = 0.03");
        assertTrue("FP rate too high", fpp < 0.035);
    }

}

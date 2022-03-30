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

package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Random;

/**
 * @author shengyu
 */
public class HistogramTest {
    /**
     * test binary search
     */
    @Test
    public void testFindBucket() throws Exception {
        IntegerType it = new IntegerType();

        Histogram h = new Histogram(7, it, 1);
        Integer[] list = new Integer[10000];
        Random r1 = new Random();
        for (int i = 0; i < list.length; i++) {
            list[i] = r1.nextInt(list.length * 100);
        }
        h.buildFromData(list);

        Method format = h.getClass().getDeclaredMethod("findBucket", Object.class);
        format.setAccessible(true);
        for (int i : list) {
            //i = i - 10;
            Histogram.Bucket bucket = (Histogram.Bucket) format.invoke(h, i);
            if (bucket == null) {
                continue;
            }
            Assert.assertTrue(Double.parseDouble(bucket.getUpper().toString()) >= i);
        }
    }
}

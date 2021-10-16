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

package com.alibaba.polardbx.optimizer.datastruct;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IntegerSegmentArrayListTest {

    @Test
    public void test() {
        IntegerSegmentArrayList a = new IntegerSegmentArrayList(1);
        IntArrayList gt = new IntArrayList();

        for (int i = 0; i < 10000; i++) {
            a.add(i);
            gt.add(i);
        }
        for (int i = 0; i < 10000; i += 2) {
            a.set(i, i * 3);
            gt.set(i, i * 3);
        }
        for (int i = 0; i < 10000; i++) {
            a.add(i * 0x9e3779b9);
            gt.add(i * 0x9e3779b9);
        }

        assertEquals(gt.size(), a.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(gt.getInt(i), a.get(i));
        }
    }
}

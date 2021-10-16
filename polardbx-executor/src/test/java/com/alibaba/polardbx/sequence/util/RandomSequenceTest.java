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

package com.alibaba.polardbx.sequence.util;

import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RandomSequenceTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test_randomIntSequence() {
        try {
            RandomSequence.randomIntSequence(0);
            Assert.assertTrue(false);
        } catch (SequenceException e) {
            Assert.assertTrue(true);
        }
        try {
            RandomSequence.randomIntSequence(-1);
            Assert.assertTrue(false);
        } catch (SequenceException e) {
            Assert.assertTrue(true);
        }
        int[] random1;
        try {
            random1 = RandomSequence.randomIntSequence(1);
            Assert.assertEquals(random1[0], 0);
        } catch (SequenceException e) {
        }

    }
}

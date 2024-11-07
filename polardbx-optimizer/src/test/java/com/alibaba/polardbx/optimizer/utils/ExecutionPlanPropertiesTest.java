/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.utils;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.BitSet;

public class ExecutionPlanPropertiesTest {
    @Test
    public void testGetPropertiesMaxValue() {
        int maxValue = ExecutionPlanProperties.getMaxPropertyValue();
        Assert.assertEquals(maxValue, ExecutionPlanProperties.MODIFY_FOREIGN_KEY + 1);
    }

    @Test
    public void testBitSetMaxValue() throws Exception {
        Field field = BitSet.class.getDeclaredField("sizeIsSticky");
        field.setAccessible(true);

        int maxValue = ExecutionPlanProperties.getMaxPropertyValue();
        BitSet bitSetWithMaxValue = new BitSet(maxValue);
        bitSetWithMaxValue.set(ExecutionPlanProperties.MODIFY_FOREIGN_KEY, true);

        boolean sizeIsSticky = (boolean) field.get(bitSetWithMaxValue);
        Assert.assertTrue(sizeIsSticky);

        BitSet bitSet = new BitSet();
        bitSet.set(ExecutionPlanProperties.MODIFY_FOREIGN_KEY, true);
        bitSet.clone();
        sizeIsSticky = (boolean) field.get(bitSet);
        Assert.assertFalse(sizeIsSticky);
    }

}

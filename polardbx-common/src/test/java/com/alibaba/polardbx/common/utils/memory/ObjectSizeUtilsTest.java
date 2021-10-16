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

package com.alibaba.polardbx.common.utils.memory;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class ObjectSizeUtilsTest {

    @Test
    public void testCalculateDataObjectSize() {
        List<Object> objects = Arrays.asList(
            (byte) 42,
            (short) 42,
            (int) 42,
            (long) 42,
            (float) 3.14,
            (double) 3.14,
            new BigInteger("42"),
            new BigDecimal("1551353537.444912"),
            new BigDecimal(12345L),
            "Hello, World!",
            new java.sql.Date(1551353537L),
            new java.sql.Timestamp(1551353537L),
            new java.sql.Time(1551353537L),
            "Hello, World!".getBytes()
        );
        for (Object object : objects) {
            long result = ObjectSizeUtils.calculateDataSize(object);
            long actual = ObjectSizeUtils.calculateObjectSize(object);
            if (result != actual) {
                System.err.println("not equals! estimated size = " + result + ", actual size = " + actual
                    + ", class = " + object.getClass().getCanonicalName());
            }
            assertTrue(result >= actual);
        }
    }
}

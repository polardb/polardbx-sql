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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.util.MySqlUtils;
import junit.framework.TestCase;

import java.math.BigDecimal;

public class MySqlUtils_decimal_0 extends TestCase {
    public void test_max_1() throws Exception {
        BigDecimal decimal = new BigDecimal("123.5123");
        assertEquals(new BigDecimal("9.9")
                ,MySqlUtils.decimal(decimal, 2, 1));
    }

    public void test_min_1() throws Exception {
        BigDecimal decimal = new BigDecimal("-123.5123");
        assertEquals(new BigDecimal("-9.9")
                ,MySqlUtils.decimal(decimal, 2, 1));
    }

    public void test_max_2() throws Exception {
        BigDecimal decimal = new BigDecimal("123.5123");
        assertEquals(new BigDecimal("9.99")
                ,MySqlUtils.decimal(decimal, 3, 2));
    }

    public void test_min_2() throws Exception {
        BigDecimal decimal = new BigDecimal("-123.5123");
        assertEquals(new BigDecimal("-9.99")
                ,MySqlUtils.decimal(decimal, 3, 2));
    }

    public void test_max_3() throws Exception {
        BigDecimal decimal = new BigDecimal("123.5123");
        assertEquals(new BigDecimal("9.999")
                ,MySqlUtils.decimal(decimal, 4, 3));
    }

    public void test_min_3() throws Exception {
        BigDecimal decimal = new BigDecimal("-123.5123");
        assertEquals(new BigDecimal("-9.999")
                ,MySqlUtils.decimal(decimal, 4, 3));
    }

    public void test_0() throws Exception {
        BigDecimal decimal = new BigDecimal("-123.51");
        assertEquals(new BigDecimal("-123.5")
                ,MySqlUtils.decimal(decimal, 4, 1));
    }

    public void test_1() throws Exception {
        BigDecimal decimal = new BigDecimal("-123.51");
        assertEquals(new BigDecimal("-123.5")
                ,MySqlUtils.decimal(decimal, 4, 1));
    }

    public void test_2() throws Exception {
        BigDecimal decimal = new BigDecimal("-123.5");
        assertSame(decimal
                ,MySqlUtils.decimal(decimal, 4, 1));
    }

    public void test_3() throws Exception {
        BigDecimal decimal = new BigDecimal("100");
        assertEquals(new BigDecimal("99.99")
                ,MySqlUtils.decimal(decimal, 4, 2));
    }

    public void test_4() throws Exception {
        BigDecimal decimal = new BigDecimal("1000");
        assertEquals(new BigDecimal("99.99")
                ,MySqlUtils.decimal(decimal, 4, 2));
    }

    public void test_5() throws Exception {
        BigDecimal decimal = new BigDecimal("10.001");
        assertEquals(new BigDecimal("10.00")
                ,MySqlUtils.decimal(decimal, 4, 2));
    }

    public void test_6() throws Exception {
        Exception error = null;
        try {
            BigDecimal decimal = new BigDecimal("10.001");
            assertEquals(new BigDecimal("10.00")
                    , MySqlUtils.decimal(decimal, 0, 0));
        } catch (Exception ex) {
            error = ex;
        }
        assertNotNull(error);
    }

    public void test_eq() throws Exception {
        System.out.println(
                eq(new BigDecimal("1.0"), new BigDecimal("1.00"))
        );
    }

    private static boolean eq(BigDecimal a, BigDecimal b) {
        int cmp = a.compareTo(b);
        return cmp == 0;
    }
}

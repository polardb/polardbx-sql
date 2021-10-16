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

package com.alibaba.polardbx.druid.bvt.sql.mysql.parseFloat;

import com.alibaba.polardbx.druid.util.NumberUtils;
import junit.framework.TestCase;

public class NumberUtils_parseFloat_min_max extends TestCase {
    public void test_parse_float_max() throws Exception {
        String str = Float.toString(Float.MAX_VALUE);
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

    public void test_parse_float_min() throws Exception {
        String str = Float.toString(Float.MIN_VALUE);
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

    public void test_parse_float_min_1() throws Exception {
        String str = Float.toString(Float.MIN_VALUE).toLowerCase();
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

    public void test_parse_float_int_max() throws Exception {
        String str = Integer.toString(Integer.MAX_VALUE);
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_int_min() throws Exception {
        String str = Integer.toString(Integer.MIN_VALUE);
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_long_max() throws Exception {
        String str = Long.toString(Long.MAX_VALUE);
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

    public void test_parse_float_long_min() throws Exception {
        String str = Long.toString(Long.MIN_VALUE);
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

    public void test_parse_float_long_max_1() throws Exception {
        String str = Long.toString(Long.MAX_VALUE) + ".1";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

    public void test_parse_float_long_min_1() throws Exception {
        String str = Long.toString(Long.MIN_VALUE) + ".1";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

    public void test_parse_double_max() throws Exception {
        String str = Double.toString(Double.MAX_VALUE);
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

    public void test_parse_double_min() throws Exception {
        String str = Double.toString(Double.MIN_VALUE);
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertTrue(Float.valueOf(str) == value);
    }

}

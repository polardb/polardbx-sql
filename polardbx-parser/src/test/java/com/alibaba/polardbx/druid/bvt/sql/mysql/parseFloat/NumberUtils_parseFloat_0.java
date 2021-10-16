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

public class NumberUtils_parseFloat_0 extends TestCase {
    public void test_parse_float_0() throws Exception {
        String str = "1979.0714";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_1() throws Exception {
        String str = "-1979.0714";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_3() throws Exception {
        String str = "1979";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_4() throws Exception {
        String str = "-1979";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_5() throws Exception {
        String str = "1979.";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_6() throws Exception {
        String str = "-1979.";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_7() throws Exception {
        String str = "1";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_8() throws Exception {
        String str = "+1";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_9() throws Exception {
        String str = "+1";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_10() throws Exception {
        String str = "-.474836e8";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_11() throws Exception {
        String str = ",,,,,,,,,,-2.1370637,,,,,,,,,,";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 10, bytes.length - 20);
        assertEquals(Float.valueOf("-2.1370637"), value);
    }

    public void test_parse_float_scale_0() throws Exception {
        String str = "1979.";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_1() throws Exception {
        String str = "1979.1";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_2() throws Exception {
        String str = "1979.12";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_3() throws Exception {
        String str = "1979.123";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_4() throws Exception {
        String str = "1979.1234";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_5() throws Exception {
        String str = "1979.12345";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_5_1() throws Exception {
        String str = "3.52145";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_6() throws Exception {
        String str = "1979.123456";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_7() throws Exception {
        String str = "1979.1234567";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_8() throws Exception {
        String str = ".12345678";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_9() throws Exception {
        String str = ".123456789";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_10() throws Exception {
        String str = ".0123456789";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_11() throws Exception {
        String str = ".10123456789";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_12() throws Exception {
        String str = ".210123456789";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_13() throws Exception {
        String str = ".3210123456789";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_14() throws Exception {
        String str = ".43210123456789";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

    public void test_parse_float_scale_15() throws Exception {
        String str = ".54210123456789";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }
    public void test_parse_float_scale_16() throws Exception {
        String str = ".654210123456789";
        byte[] bytes = str.getBytes();
        float value = NumberUtils.parseFloat(bytes, 0, bytes.length);
        assertEquals(Float.valueOf(str), value);
    }

}

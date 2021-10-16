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

public class NumberUtils_parseDouble_0 extends TestCase {
    public void test_parse_float_0() throws Exception {
        String str = "1979.0714";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_1() throws Exception {
        String str = "-1979.0714";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_3() throws Exception {
        String str = "1979";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_4() throws Exception {
        String str = "-1979";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_5() throws Exception {
        String str = "1979.";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_6() throws Exception {
        String str = "-1979.";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_7() throws Exception {
        String str = "1";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_8() throws Exception {
        String str = "+1";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_9() throws Exception {
        String str = "+1";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_10() throws Exception {
        String str = "-.474836e8";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_11() throws Exception {
        String str = "-3.9113533727339678";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }

    public void test_parse_float_12() throws Exception {
        String str = "1.8387091954385522";
        byte[] bytes = str.getBytes();
        double value = NumberUtils.parseDouble(bytes, 0, bytes.length);
        assertEquals(Double.valueOf(str), value);
    }


}

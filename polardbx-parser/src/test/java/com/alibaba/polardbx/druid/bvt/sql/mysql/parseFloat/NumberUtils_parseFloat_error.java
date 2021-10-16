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

public class NumberUtils_parseFloat_error extends TestCase {
    public void test_parse_float_0() throws Exception {
        Exception error = null;
        try {
            NumberUtils.parseFloat(null, 0, 1);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_1() throws Exception {
        Exception error = null;
        try {
            String str = "1979.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(null, 0, 16);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_2() throws Exception {
        Exception error = null;
        try {
            String str = "1979.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, -1, 16);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_3() throws Exception {
        Exception error = null;
        try {
            String str = "1979.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, -1, 0);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_4() throws Exception {
        Exception error = null;
        try {
            String str = "-a979.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_5() throws Exception {
        Exception error = null;
        try {
            String str = "-A979.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
   }

   public void test_parse_float_5_1() throws Exception {
        Exception error = null;
        try {
            String str = "-9-79.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
   }

   public void test_parse_float_5_2() throws Exception {
        Exception error = null;
        try {
            String str = "-9a79.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
   }

   public void test_parse_float_6() throws Exception {
        Exception error = null;
        try {
            String str = "--979.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
   }

   public void test_parse_float_7() throws Exception {
        Exception error = null;
        try {
            String str = "A979.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_8() throws Exception {
        Exception error = null;
        try {
            String str = "1979.a714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_9() throws Exception {
        Exception error = null;
        try {
            String str = "1979.A714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_10() throws Exception {
        Exception error = null;
        try {
            String str = "1979.-714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_11() throws Exception {
        Exception error = null;
        try {
            String str = "1979.71-4";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_12() throws Exception {
        Exception error = null;
        try {
            String str = "1979.7-14";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_13() throws Exception {
        Exception error = null;
        try {
            String str = "19-79.0714";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, bytes.length);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_14() throws Exception {
        Exception error = null;
        try {
            String str = "a1b";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 1, 2);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

   public void test_parse_float_15() throws Exception {
        Exception error = null;
        try {
            String str = ".123";
            byte[] bytes = str.getBytes();

            NumberUtils.parseFloat(bytes, 0, 1);
        } catch (IllegalArgumentException ex) {
            error = ex;
        }
        assertNotNull(error);
    }

}

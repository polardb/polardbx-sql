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

public class MySqlUtils_isNumber extends TestCase {
    public void test_true() throws Exception {
        assertTrue(MySqlUtils.isNumber("3.2"));
        assertTrue(MySqlUtils.isNumber("+3.2"));
        assertTrue(MySqlUtils.isNumber("-3.2"));
        assertTrue(MySqlUtils.isNumber("-3e2"));
        assertTrue(MySqlUtils.isNumber("-3e-2"));
        assertTrue(MySqlUtils.isNumber("-3.2e-2"));
        assertTrue(MySqlUtils.isNumber("-3.2e+2"));
        assertTrue(MySqlUtils.isNumber("100"));
        assertTrue(MySqlUtils.isNumber("-100"));
    }

    public void test_false() throws Exception {
        assertFalse(MySqlUtils.isNumber("3.2.2"));
        assertFalse(MySqlUtils.isNumber("-3e-2.0"));
        assertFalse(MySqlUtils.isNumber("-3e-2e3"));
        assertFalse(MySqlUtils.isNumber("-3a"));
        assertFalse(MySqlUtils.isNumber("a"));
        assertFalse(MySqlUtils.isNumber("a3"));
        assertFalse(MySqlUtils.isNumber("-3e"));
        assertFalse(MySqlUtils.isNumber(null));
        assertFalse(MySqlUtils.isNumber(""));
    }
}

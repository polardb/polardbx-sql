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

package com.alibaba.polardbx.druid.bvt.sql.mysql.parseDate;

import com.alibaba.polardbx.druid.util.MySqlUtils;
import junit.framework.TestCase;

import java.util.TimeZone;

public class MySqlUtils_parseTimezone extends TestCase {
    public void test_0() throws Exception {
        assertEquals(TimeZone.getDefault(), MySqlUtils.parseTimeZone("SYSTEM"));
    }

    public void test_utc() throws Exception {
        assertEquals(TimeZone.getTimeZone("UTC"), MySqlUtils.parseTimeZone("UTC"));
    }

    public void test_1() throws Exception {
        for (int i = 0; i < 11; ++i) {
            String str0 = "-" + i + " :00";
            assertEquals(TimeZone.getTimeZone(str0)
                    , MySqlUtils.parseTimeZone(str0));
//            String str1 = "+" + i + " :00";
        }

    }
}

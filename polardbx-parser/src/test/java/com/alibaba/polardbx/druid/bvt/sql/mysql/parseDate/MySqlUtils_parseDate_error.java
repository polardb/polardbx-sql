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

public class MySqlUtils_parseDate_error extends TestCase {

    public void test_err_0() throws Exception {
        String dateStr = "2018-12-23 12:21:80";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_1() throws Exception {
        String dateStr = "2018-12-23 12:61:40";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_2() throws Exception {
        String dateStr = "2018-12-23 25:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_3() throws Exception {
        String dateStr = "2018-12-2A 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_4() throws Exception {
        String dateStr = "2018-12-2a 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_5() throws Exception {
        String dateStr = "2018-12-41 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_6() throws Exception {
        String dateStr = "2018-12-a1 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_7() throws Exception {
        String dateStr = "2018-12-A1 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }
    public void test_err_8() throws Exception {
        String dateStr = "2018-1a-21 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_9() throws Exception {
        String dateStr = "2018-1A-21 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_10() throws Exception {
        String dateStr = "2018-A1-21 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_11() throws Exception {
        String dateStr = "2018-a1-21 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_12() throws Exception {
        String dateStr = "2018-11*21 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_13() throws Exception {
        String dateStr = "2018-00-21 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_14() throws Exception {
        String dateStr = "2018-01-00 21:31:42";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_15() throws Exception {
        String dateStr = "2018-01";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_16() throws Exception {
        String dateStr = "201a-01-01";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }

    public void test_err_17() throws Exception {
        String dateStr = "201A-01-01";

        TimeZone timeZone = TimeZone.getDefault();
        assertNull(dateStr
                , MySqlUtils.parseDate(dateStr, timeZone));
    }
}

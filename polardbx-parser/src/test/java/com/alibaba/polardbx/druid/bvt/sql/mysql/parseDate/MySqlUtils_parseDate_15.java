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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class MySqlUtils_parseDate_15 extends TestCase {
    public void test_datetime_15_0() throws Exception {
        String dateStr = "2016-1-2 7:6:31";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_15_1() throws Exception {
        String dateStr = "2016-1-2 7:16:3";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_15_2() throws Exception {
        String dateStr = "2016-1-2 17:6:3";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_15_3() throws Exception {
        String dateStr = "2016-1-12 1:6:3";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_15_4() throws Exception {
        String dateStr = "2016-11-2 1:6:3";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }
}

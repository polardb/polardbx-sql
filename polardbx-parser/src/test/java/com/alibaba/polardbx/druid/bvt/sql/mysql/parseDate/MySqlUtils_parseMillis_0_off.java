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

public class MySqlUtils_parseMillis_0_off extends TestCase {

    public void test_datetime_0() throws Exception {
        String dateStr = ",,,,,2018-1-1 0:0:10.100";

        TimeZone timeZone = TimeZone.getDefault();
        byte[] bytes = dateStr.getBytes();
        Date date = new Date(
                MySqlUtils.parseMillis(bytes, 5, bytes.length - 5, timeZone));
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s.SSS");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, ",,,,," + str2);
    }

    public void test_datetime_1() throws Exception {
        String dateStr = ",,,,,2018-12-23 12:21:34.789";

        TimeZone timeZone = TimeZone.getDefault();
        byte[] bytes = dateStr.getBytes();
        Date date = new Date(
                MySqlUtils.parseMillis(bytes, 5, bytes.length - 5, timeZone));
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s.SSS");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, ",,,,," + str2);
    }

    public void test_datetime_2() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("H:m:s");
        TimeZone timeZone = TimeZone.getDefault();
        format.setTimeZone(timeZone);

        String dateStr = ",,,,,12:11:13";

        byte[] bytes = dateStr.getBytes();
        Date date = new Date(
                MySqlUtils.parseMillis(bytes, 5, bytes.length - 5, timeZone));
        assertNotNull(dateStr, date);
        assertNotNull(dateStr, date);

        String str2 = format.format(date);
        assertEquals(dateStr, ",,,,," + str2);
    }
}

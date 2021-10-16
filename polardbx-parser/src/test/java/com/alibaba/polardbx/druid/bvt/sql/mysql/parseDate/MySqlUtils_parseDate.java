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

public class MySqlUtils_parseDate extends TestCase {
    public void test_date_0() throws Exception {
        for (int year = 1970; year <= 9999; ++year) {
            for (int month = 1; month <= 12; ++month) {
                String mm = (month < 10 ? "0" : "") + month;

                for (int day = 1; day <= 28; ++day) {
                    String dd = (day < 10 ? "0" : "") + day;

                    String dateStr = year + "" + mm + "" + dd;

                    TimeZone timeZone = TimeZone.getDefault();
                    Date date = MySqlUtils.parseDate(dateStr, timeZone);
                    assertNotNull(dateStr, date);

                    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                    format.setTimeZone(timeZone);

                    String str2 = format.format(date);
                    assertEquals(dateStr, str2);
                }
            }

            if (year > 2500) {
                year+=10;
            }
        }
    }

    public void test_date_1() throws Exception {
        for (int year = 1970; year <= 9999; ++year) {
            for (int month = 1; month <= 12; ++month) {

                for (int day = 1; day <= 28; ++day) {
                    String dateStr = year + "-" + month + "-" + day;

                    TimeZone timeZone = TimeZone.getDefault();
                    Date date = MySqlUtils.parseDate(dateStr, timeZone);
                    assertNotNull(dateStr, date);

                    SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d");
                    format.setTimeZone(timeZone);

                    String str2 = format.format(date);
                    assertEquals(dateStr, str2);
                }
            }

            if (year > 2500) {
                year+=10;
            }
        }
    }

    public void test_datetime_0() throws Exception {
            for (int hour = 0; hour < 24; ++hour) {
                String hh = (hour < 10 ? "0" : "") + hour;

                for (int minute = 0; minute < 60; ++minute) {
                    String mm = (minute < 10 ? "0" : "") + minute;

                    for (int second = 0; second < 60; ++second) {
                        String ss = (second < 10 ? "0" : "") + second;

                        String dateStr = "20181228" + hh + "" + mm + "" + ss;

                        TimeZone timeZone = TimeZone.getDefault();
                        Date date = MySqlUtils.parseDate(dateStr, timeZone);
                        assertNotNull(dateStr, date);

                        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
                        format.setTimeZone(timeZone);

                        String str2 = format.format(date);
                        assertEquals(dateStr, str2);
                    }
                }
            }
    }

    public void test_datetime_d0() throws Exception {
        String dateStr = "20181228";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr + "000000", str2);
    }

    public void test_datetime_d1() throws Exception {
        String dateStr = "2018-12-28";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr + " 00:00:00", str2);
    }

    public void test_datetime_x0() throws Exception {
        String dateStr = "20181228070613";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_x1() throws Exception {
        String dateStr = "2016-11-22 17:06:31";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_x1_T() throws Exception {
        String dateStr = "2016-11-22T17:06:31";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_x2() throws Exception {
        String dateStr = "2016-11-22 17:06:31.761";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_x2_T() throws Exception {
        String dateStr = "2016-11-22T17:06:31.761";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_x3() throws Exception {
        String dateStr = "2016-1-2 7:6:3";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals(dateStr, str2);
    }

    public void test_datetime_x4() throws Exception {
        String dateStr = "2498-11-25 02:22:08.000374056";

        TimeZone timeZone = TimeZone.getDefault();
        Date date = MySqlUtils.parseDate(dateStr, timeZone);
        assertNotNull(dateStr, date);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-M-d H:m:s.S");
        format.setTimeZone(timeZone);

        String str2 = format.format(date);
        assertEquals("2498-11-25 2:22:8.0", str2);
    }
}

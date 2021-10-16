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

package com.alibaba.polardbx.druid.bvt.util;

import com.alibaba.polardbx.druid.util.DataTimeUtils;
import com.alibaba.polardbx.druid.util.Jdk8TimeUtils;
import junit.framework.TestCase;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DataTimeUtilsTest extends TestCase {
    final static int COUNT = 1000 * 1000 * 10;
    public void test_0() throws Exception {
        long millis = System.currentTimeMillis();
        TimeZone timeZone = TimeZone.getDefault();
        String str = DataTimeUtils.formatDateTime19(millis, timeZone);
        String str1 = Jdk8TimeUtils.formatDateTime19(Instant.ofEpochMilli(millis), timeZone.toZoneId());

        System.out.println(str);
        System.out.println(str1);

        for (int i = 0; i < 5; ++i) {
//            perf_0(millis, timeZone); // 1977
//            perf_1(millis, timeZone); // 656
//            perf_2(millis, timeZone); // 9214
//            perf_3(millis, timeZone); // 2363
//            perf_4(millis, timeZone); // 2559
//            perf_5(millis, timeZone); // 1809 jodaa
//            perf_6(millis, timeZone); // 1196 jodaa
            perf_7(millis, timeZone); // cal
        }
    }

    public void perf_0(long millis, TimeZone timeZone) throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; ++i) {
            DataTimeUtils.formatDateTime19(millis, timeZone);
        }
        System.out.println("millis : " + (System.currentTimeMillis() - start));
    }

    public void perf_1(long millis, TimeZone timeZone) throws Exception {
        long start = System.currentTimeMillis();
        ZoneId zoneId = timeZone.toZoneId();
        for (int i = 0; i < COUNT; ++i) {
            Jdk8TimeUtils.formatDateTime19(Instant.ofEpochMilli(millis), zoneId);
        }
        System.out.println("millis : " + (System.currentTimeMillis() - start));
    }

    public void perf_2(long millis, TimeZone timeZone) throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; ++i) {
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(millis));
        }
        System.out.println("millis : " + (System.currentTimeMillis() - start));
    }

    public void perf_3(long millis, TimeZone timeZone) throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(millis);

        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; ++i) {
            dateFormat.format(date);
        }
        System.out.println("millis : " + (System.currentTimeMillis() - start));
    }

    public void perf_4(long millis, TimeZone timeZone) throws Exception {
        DateTimeFormatter f = DateTimeFormatter.ofPattern ( "yyyy-MM-dd HH:mm:ss" );
        ZoneId zoneId = timeZone.toZoneId();
        ZonedDateTime temporal = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId);

        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; ++i) {
            f.format(temporal);
        }
        System.out.println("millis : " + (System.currentTimeMillis() - start));
    }

    public void perf_5(long millis, TimeZone timeZone) throws Exception {
        org.joda.time.format.DateTimeFormatter f = org.joda.time.format.DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss" );

        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; ++i) {
            f.print(millis);
        }
        System.out.println("millis : " + (System.currentTimeMillis() - start));
    }

    public void perf_6(long millis, TimeZone timeZone) throws Exception {
        org.joda.time.format.DateTimeFormatter f = org.joda.time.format.DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss" );
        DateTimeZone dateTimeZone = DateTimeZone.forTimeZone(timeZone);

        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; ++i) {
            formatDateTime19(new org.joda.time.Instant(millis), dateTimeZone);
        }
        System.out.println("millis : " + (System.currentTimeMillis() - start));
    }

    public void perf_7(long millis, TimeZone timeZone) throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < COUNT; ++i) {
            formatDateTime19_cal(Calendar.getInstance(timeZone));
        }
        System.out.println("millis : " + (System.currentTimeMillis() - start));
    }

    public static String formatDateTime19(org.joda.time.Instant instant, DateTimeZone dateTimeZone) {
        org.joda.time.DateTime zt = new org.joda.time.DateTime(instant, dateTimeZone);

        int year = zt.get(DateTimeFieldType.year());;
        int month = zt.get(DateTimeFieldType.monthOfYear());;
        int dayOfMonth = zt.get(DateTimeFieldType.dayOfYear());;
        int hour = zt.get(DateTimeFieldType.hourOfDay());;
        int minute = zt.get(DateTimeFieldType.minuteOfHour());;
        int second = zt.get(DateTimeFieldType.secondOfMinute());;

        char[] chars = new char[19];
        chars[0] = (char) (year/1000 + '0');
        chars[1] = (char) ((year/100)%10 + '0');
        chars[2] = (char) ((year/10)%10 + '0');
        chars[3] = (char) (year%10 + '0');
        chars[4] = '-';
        chars[5] = (char) (month/10 + '0');
        chars[6] = (char) (month%10 + '0');
        chars[7] = '-';
        chars[8] = (char) (dayOfMonth/10 + '0');
        chars[9] = (char) (dayOfMonth%10 + '0');
        chars[10] = ' ';
        chars[11] = (char) (hour/10 + '0');
        chars[12] = (char) (hour%10 + '0');
        chars[13] = ':';
        chars[14] = (char) (minute/10 + '0');
        chars[15] = (char) (minute%10 + '0');
        chars[16] = ':';
        chars[17] = (char) (second/10 + '0');
        chars[18] = (char) (second%10 + '0');
        return new String(chars);
    }

    public static String formatDateTime19_cal(Calendar calendar) {

        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);

        char[] chars = new char[19];
        chars[0] = (char) (year/1000 + '0');
        chars[1] = (char) ((year/100)%10 + '0');
        chars[2] = (char) ((year/10)%10 + '0');
        chars[3] = (char) (year%10 + '0');
        chars[4] = '-';
        chars[5] = (char) (month/10 + '0');
        chars[6] = (char) (month%10 + '0');
        chars[7] = '-';
        chars[8] = (char) (dayOfMonth/10 + '0');
        chars[9] = (char) (dayOfMonth%10 + '0');
        chars[10] = ' ';
        chars[11] = (char) (hour/10 + '0');
        chars[12] = (char) (hour%10 + '0');
        chars[13] = ':';
        chars[14] = (char) (minute/10 + '0');
        chars[15] = (char) (minute%10 + '0');
        chars[16] = ':';
        chars[17] = (char) (second/10 + '0');
        chars[18] = (char) (second%10 + '0');
        return new String(chars);
    }
}

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

package com.alibaba.polardbx.druid.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.TimeZone;

import static java.time.temporal.ChronoUnit.DAYS;

public class Jdk8TimeUtils {
    public static Integer timestampDiff(long unitHash, String str1, String str2, TimeZone timeZone) {

        ChronoUnit unit = null;
        if (unitHash == FnvHash.Constants.DAY) {
            unit = ChronoUnit.DAYS;
        } else if (unitHash == FnvHash.Constants.MONTH) {
            unit = ChronoUnit.MONTHS;
        } else if (unitHash == FnvHash.Constants.HOUR) {
            unit = ChronoUnit.HOURS;
        } else if (unitHash == FnvHash.Constants.MINUTE) {
            unit = ChronoUnit.MINUTES;
        } else if (unitHash == FnvHash.Constants.SECOND) {
            unit = ChronoUnit.SECONDS;
        }

        if (unit == null) {
            return null;
        }

        Date d1 = MySqlUtils.parseDate(str1, timeZone);
        Date d2 = MySqlUtils.parseDate(str2, timeZone);

        if (d1 == null || d2 == null) {
            return null;
        }

        return (int) unit.between(d1.toInstant().atZone(timeZone.toZoneId())
                , d2.toInstant().atZone(timeZone.toZoneId()));
    }

    public static Integer dateDiff(String str0, String str1, TimeZone timeZone) {
        Date d0 = MySqlUtils.parseDate(str0, timeZone);
        Date d1 = MySqlUtils.parseDate(str1, timeZone);

        if (d0 == null || d1 == null) {
            return null;
        }

        ZoneId zoneId = timeZone != null ? timeZone.toZoneId() : ZoneId.systemDefault();

        ZonedDateTime zonedDateTime0 = d0.toInstant().atZone(zoneId).truncatedTo(ChronoUnit.DAYS);
        ZonedDateTime zonedDateTime1 = d1.toInstant().atZone(zoneId).truncatedTo(ChronoUnit.DAYS);
        return (int) DAYS.between(zonedDateTime1, zonedDateTime0);
    }


    public static Integer dateDiff1(long unitHash, String str1, String str2, TimeZone timeZone) {
        ChronoUnit unit = null;
        if (unitHash == FnvHash.Constants.DAY) {
            unit = ChronoUnit.DAYS;
        } else if (unitHash == FnvHash.Constants.MONTH) {
            unit = ChronoUnit.MONTHS;
        } else if (unitHash == FnvHash.Constants.HOUR) {
            unit = ChronoUnit.HOURS;
        } else if (unitHash == FnvHash.Constants.MINUTE) {
            unit = ChronoUnit.MINUTES;
        } else if (unitHash == FnvHash.Constants.SECOND) {
            unit = ChronoUnit.SECONDS;
        }

        if (unit == null) {
            return null;
        }

        Date d1 = MySqlUtils.parseDate(str1, timeZone);
        Date d2 = MySqlUtils.parseDate(str2, timeZone);

        if (d1 == null || d2 == null) {
            return null;
        }

        return (int) unit.between(d1.toInstant().atZone(timeZone.toZoneId())
                , d2.toInstant().atZone(timeZone.toZoneId()));
    }



    public static Integer monthBetween(String str1, String str2, TimeZone timeZone) {
        ChronoUnit unit = ChronoUnit.MONTHS;

        Date d1 = MySqlUtils.parseDate(str1, timeZone);
        Date d2 = MySqlUtils.parseDate(str2, timeZone);

        if (d1 == null || d2 == null) {
            return null;
        }

        return (int) unit.between(d1.toInstant().atZone(timeZone.toZoneId())
                , d2.toInstant().atZone(timeZone.toZoneId()));
    }

    public static String formatDateTime14(Instant instant, ZoneId zoneId) {
        ZonedDateTime zt = ZonedDateTime.ofInstant(instant, zoneId);

        int year = zt.get(ChronoField.YEAR);;
        int month = zt.get(ChronoField.MONTH_OF_YEAR);;
        int dayOfMonth = zt.get(ChronoField.DAY_OF_MONTH);;
        int hour = zt.get(ChronoField.HOUR_OF_DAY);;
        int minute = zt.get(ChronoField.MINUTE_OF_HOUR);;
        int second = zt.get(ChronoField.SECOND_OF_MINUTE);;

        char[] chars = new char[14];
        chars[0] = (char) (year/1000 + '0');
        chars[1] = (char) ((year/100)%10 + '0');
        chars[2] = (char) ((year/10)%10 + '0');
        chars[3] = (char) (year%10 + '0');
        chars[4] = (char) (month/10 + '0');
        chars[5] = (char) (month%10 + '0');
        chars[6] = (char) (dayOfMonth/10 + '0');
        chars[7] = (char) (dayOfMonth%10 + '0');
        chars[8] = (char) (hour/10 + '0');
        chars[9] = (char) (hour%10 + '0');
        chars[10] = (char) (minute/10 + '0');
        chars[11] = (char) (minute%10 + '0');
        chars[12] = (char) (second/10 + '0');
        chars[13] = (char) (second%10 + '0');
        return new String(chars);
    }

    public static String formatDateTime19(Instant instant, ZoneId zoneId) {
        ZonedDateTime zt = ZonedDateTime.ofInstant(instant, zoneId);

        int year = zt.get(ChronoField.YEAR);;
        int month = zt.get(ChronoField.MONTH_OF_YEAR);;
        int dayOfMonth = zt.get(ChronoField.DAY_OF_MONTH);;
        int hour = zt.get(ChronoField.HOUR_OF_DAY);;
        int minute = zt.get(ChronoField.MINUTE_OF_HOUR);;
        int second = zt.get(ChronoField.SECOND_OF_MINUTE);;

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

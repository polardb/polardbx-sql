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

import java.util.Calendar;
import java.util.TimeZone;

public class DataTimeUtils {

    public static String formatDateTime14(long millis, TimeZone timeZone) {
        Calendar cale = timeZone == null
                ? Calendar.getInstance()
                : Calendar.getInstance(timeZone);
        cale.setTimeInMillis(millis);

        int year = cale.get(Calendar.YEAR);
        int month = cale.get(Calendar.MONTH) + 1;
        int dayOfMonth = cale.get(Calendar.DAY_OF_MONTH);
        int hour = cale.get(Calendar.HOUR_OF_DAY);
        int minute = cale.get(Calendar.MINUTE);
        int second = cale.get(Calendar.SECOND);

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

    public static String formatDateTime19(long millis, TimeZone timeZone) {
        Calendar cale = timeZone == null
                ? Calendar.getInstance()
                : Calendar.getInstance(timeZone);
        cale.setTimeInMillis(millis);

        int year = cale.get(Calendar.YEAR);
        int month = cale.get(Calendar.MONTH) + 1;
        int dayOfMonth = cale.get(Calendar.DAY_OF_MONTH);
        int hour = cale.get(Calendar.HOUR_OF_DAY);
        int minute = cale.get(Calendar.MINUTE);
        int second = cale.get(Calendar.SECOND);

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

    public static String formatDate8(long millis, TimeZone timeZone) {
        Calendar cale = timeZone == null
                ? Calendar.getInstance()
                : Calendar.getInstance(timeZone);
        cale.setTimeInMillis(millis);

        int year = cale.get(Calendar.YEAR);
        int month = cale.get(Calendar.MONTH) + 1;
        int dayOfMonth = cale.get(Calendar.DAY_OF_MONTH);

        char[] chars = new char[14];
        chars[0] = (char) (year/1000 + '0');
        chars[1] = (char) ((year/100)%10 + '0');
        chars[2] = (char) ((year/10)%10 + '0');
        chars[3] = (char) (year%10 + '0');
        chars[4] = (char) (month/10 + '0');
        chars[5] = (char) (month%10 + '0');
        chars[6] = (char) (dayOfMonth/10 + '0');
        chars[7] = (char) (dayOfMonth%10 + '0');
        return new String(chars);
    }
}

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

package com.alibaba.polardbx.common.utils.time.old;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;

import java.util.Calendar;
import java.util.Date;


public class DateUtils {

    public static final int TIME_DATETIME_ONLY = 0;
    public static int[] internal_format_positions = {0, 1, 2, 3, 4, 5, 6, 255};
    public static final int MAX_DATE_PARTS = 8;
    private static final long[] LOG_10_INT = new long[] {
        1, 10, 100, 1000, 10000L, 100000L,
        1000000L, 10000000L, 100000000L};
    public static final int YY_PART_YEAR = 70;

    private static final String[] WEEK_DAYS = new String[] {
        "Sunday", "Monday", "Tuesday",
        "Wednesday", "Thursday", "Friday",
        "Saturday"};
    private static final String[] WEEK_DAYS_ABBR = new String[] {
        "Sun", "Mon", "Tue", "Wed", "Thu",
        "Fri", "Sat"};
    private static final String[] MONTHS = new String[] {
        "January", "February", "March", "April",
        "May", "June", "July", "August",
        "September", "October", "November",
        "December"};
    private static final String[] MONTHS_ABBR = new String[] {
        "Jan", "Feb", "Mar", "Apr", "May",
        "Jun", "Jul", "Aug", "Sep", "Oct",
        "Nov", "Dec"};

    private static long MAX_DAY_NUMBER = 3652424;

    public static final int MAX_HOUR = 838;
    public static final int MIN_HOUR = -838;
    public static final String MYSQL_TIME_FORMAT = "%H:%i:%s.%f";
    public static final String MYSQL_DATETIME_DEFAULT_FORMAT1 = "%Y-%m-%d %H:%i:%s.%f";
    public static final String MYSQL_DATETIME_DEFAULT_FORMAT2 = "%y-%m-%d %H:%i:%s.%f";
    private static final String ZERO_STR = "00000000000000000000";

    @SuppressWarnings({"deprecation", "unused"})
    public static Date str_to_time(String str) {
        int length = str.length();
        int was_cut;
        Date time = new Date(0);
        int field_length;
        int digits;
        int year_length = 0;
        int add_hours = 0, start_loop;
        long not_zero_date, allow_space;
        boolean is_internal_format;
        int pos;
        int last_field_pos = 0;
        int cur_pos = 0;
        int i;
        int[] date = new int[MAX_DATE_PARTS];
        int[] date_len = new int[MAX_DATE_PARTS];
        int number_of_fields;
        boolean found_delimitier = false, found_space = false;
        int frac_pos;
        int frac_len;

        was_cut = 0;


        str = str.trim();

        int end = str.length();
        if (str.isEmpty() || !isdigit(str.charAt(0))) {
            was_cut = 1;
            return null;
        }

        is_internal_format = false;

        int[] format_position = internal_format_positions;

        for (pos = 0; pos < str.length() && (isdigit(str.charAt(pos)) || str.charAt(pos) == 'T'); pos++) {
        }

        digits = pos;

        start_loop = 0;
        date_len[format_position[0]] = 0;

        if (pos == str.length() || str.charAt(pos) == '.') {

            year_length = (digits == 4 || digits == 8 || digits >= 14) ? 4 : 2;
            field_length = year_length;
            is_internal_format = true;
            format_position = internal_format_positions;
        } else {
            if (format_position[0] >= 3) {

                while (pos < end && !my_isspace(str.charAt(pos))) {
                    pos++;
                }
                while (pos < end && !isdigit(str.charAt(pos))) {
                    pos++;
                }
                if (pos == end) {

                    date[0] = date[1] = date[2] = date[3] = date[4] = 0;
                    start_loop = 5;
                }
            }

            field_length = format_position[0] == 0 ? 4 : 2;
        }

        i = max(format_position[0], format_position[1]);

        if (i < format_position[2]) {
            i = format_position[2];
        }

        allow_space = ((1 << i) | (1 << format_position[6]));
        allow_space &= (1 | 2 | 4 | 8);

        not_zero_date = 0;
        for (i = start_loop; i < MAX_DATE_PARTS - 1 && cur_pos != end && isdigit(str.charAt(cur_pos)); i++) {
            int start = cur_pos;
            int tmp_value = (str.charAt(cur_pos++) - '0');


            boolean scan_until_delim = !is_internal_format && ((i != format_position[6]));

            while (cur_pos != end && isdigit(str.charAt(cur_pos)) && (scan_until_delim || (--field_length != 0))) {
                tmp_value = tmp_value * 10 + (str.charAt(cur_pos) - '0');
                cur_pos++;
            }

            if (cur_pos != end && !isdigit(str.charAt(cur_pos)) && scan_until_delim) {

                if (i < 2 && str.charAt(cur_pos) != '-') {
                    return null;
                }
            }
            date_len[i] = (cur_pos - start);
            if (tmp_value > 999999) {
                was_cut = 1;
                return null;
            }
            date[i] = tmp_value;
            not_zero_date |= tmp_value;


            field_length = format_position[i + 1] == 0 ? 4 : 2;

            if ((last_field_pos = cur_pos) == end) {
                i++;
                break;
            }

            if (i == format_position[2] && str.charAt(cur_pos) == 'T') {
                cur_pos++;
                continue;
            }
            if (i == format_position[5]) {
                if (str.charAt(cur_pos) == '.') {
                    cur_pos++;
                    field_length = 6;
                }
                continue;
            }
            while (cur_pos != end && (my_ispunct(str.charAt(cur_pos)) || my_isspace(str.charAt(cur_pos)))) {
                if (my_isspace(str.charAt(cur_pos))) {
                    if (!((allow_space & (1 << i)) != 0)) {
                        was_cut = 1;
                        return null;
                    }
                    found_space = true;
                }
                cur_pos++;
                found_delimitier = true;
            }

            if (i == format_position[6]) {
                i++;
                if (format_position[7] != 255) {
                    if (cur_pos + 2 <= end && (str.charAt(cur_pos + 1) == 'M' || str.charAt(cur_pos) + 1 == 'm')) {
                        if (str.charAt(cur_pos) == 'p' || str.charAt(cur_pos) == 'P') {
                            add_hours = 12;
                        } else if (str.charAt(cur_pos) != 'a' || str.charAt(cur_pos) != 'A') {
                            continue;
                        }
                        str += 2;

                        while (cur_pos != end && my_isspace(str.charAt(cur_pos))) {
                            cur_pos++;
                        }
                    }
                }
            }
            last_field_pos = cur_pos;
        }

        cur_pos = last_field_pos;

        number_of_fields = i - start_loop;
        while (i < MAX_DATE_PARTS) {
            date_len[i] = 0;
            date[i++] = 0;
        }

        if (!is_internal_format) {
            year_length = date_len[format_position[0]];
            if (!(year_length != 0)) {
                was_cut = 1;
                return null;
            }
            time.setYear(date[format_position[0]] - 1900);

            time.setMonth(date[format_position[1]] - 1);

            time.setDate(date[format_position[2]]);

            time.setHours(date[format_position[3]]);
            time.setMinutes(date[format_position[4]]);
            time.setSeconds(date[format_position[5]]);

            frac_pos = format_position[6];
            frac_len = date_len[frac_pos];
            if (frac_len < 6) {
                date[frac_pos] *= LOG_10_INT[6 - frac_len];
            }
            time.setTime(time.getTime() + date[frac_pos] / 1000);

            if (format_position[7] != 255) {
                if (time.getHours() > 12) {
                    was_cut = 1;
                    return null;
                }
                time.setHours(time.getHours() % 12 + add_hours);
            }
        } else {
            time.setYear(date[0] - 1900);
            time.setMonth(date[1] - 1);
            time.setDate(date[2]);
            time.setHours(date[3]);
            time.setMinutes(date[4]);
            time.setSeconds(date[5]);
            if (date_len[6] < 6) {
                date[6] *= LOG_10_INT[6 - date_len[6]];
            }
            time.setTime(time.getTime() + date[6] / 1000);

        }

        if (year_length == 2 && (not_zero_date != 0)) {
            time.setYear(time.getYear() + (time.getYear() + 1900 < YY_PART_YEAR ? 2000 : 1900));
        }

        if (number_of_fields < 3 || time.getYear() > 9999 || time.getMonth() > 12 || time.getDate() > 31
            || time.getHours() > 23 || time.getMinutes() > 59 || time.getMinutes() > 59) {

            if (!(not_zero_date != 0)) {
                for (; cur_pos != end; cur_pos++) {
                    if (!my_isspace(str.charAt(cur_pos))) {
                        not_zero_date = 1;
                        break;
                    }
                }
            }

            return null;
        }

        if (check_date(time, not_zero_date != 0, was_cut)) {
            return null;
        }

        for (; cur_pos != end; cur_pos++) {
            if (!my_isspace(str.charAt(cur_pos))) {
                was_cut = 1;
                break;
            }
        }

        return time;
    }

    public static boolean check_date(Date time, boolean not_zero_date, int was_cut) {
        return false;
    }

    private static int[] uplimits = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    public static Boolean check(String str) {
        int length = str.length();
        int was_cut;
        Date time = new Date(0);
        int field_length;
        int digits;
        int year_length = 0;
        int add_hours = 0, start_loop;
        long not_zero_date, allow_space;
        boolean is_internal_format;
        int pos;
        int last_field_pos = 0;
        int cur_pos = 0;
        int i;
        int[] date = new int[MAX_DATE_PARTS];
        int[] date_len = new int[MAX_DATE_PARTS];
        int number_of_fields;
        boolean found_delimitier = false, found_space = false;
        int frac_pos;
        int frac_len;

        was_cut = 0;

        str = str.trim();
        int end = str.length();
        if (str.isEmpty() || !isdigit(str.charAt(0))) {
            was_cut = 1;
            return null;
        }

        is_internal_format = false;

        int[] format_position = internal_format_positions;

        for (pos = 0; pos < str.length() && (isdigit(str.charAt(pos)) || str.charAt(pos) == 'T'); pos++) {
        }

        digits = pos;

        start_loop = 0;
        date_len[format_position[0]] = 0;

        if (pos == str.length() || str.charAt(pos) == '.') {

            year_length = (digits == 4 || digits == 8 || digits >= 14) ? 4 : 2;
            field_length = year_length;
            is_internal_format = true;
            format_position = internal_format_positions;
        } else {
            if (format_position[0] >= 3) {

                while (pos < end && !my_isspace(str.charAt(pos))) {
                    pos++;
                }
                while (pos < end && !isdigit(str.charAt(pos))) {
                    pos++;
                }
                if (pos == end) {

                    date[0] = date[1] = date[2] = date[3] = date[4] = 0;
                    start_loop = 5;
                }
            }

            field_length = format_position[0] == 0 ? 4 : 2;
        }

        i = max(format_position[0], format_position[1]);

        if (i < format_position[2]) {
            i = format_position[2];
        }

        allow_space = ((1 << i) | (1 << format_position[6]));
        allow_space &= (1 | 2 | 4 | 8);

        not_zero_date = 0;
        for (i = start_loop; i < MAX_DATE_PARTS - 1 && cur_pos != end && isdigit(str.charAt(cur_pos)); i++) {
            int start = cur_pos;
            int tmp_value = (str.charAt(cur_pos++) - '0');

            boolean scan_until_delim = !is_internal_format && ((i != format_position[6]));

            while (cur_pos != end && isdigit(str.charAt(cur_pos)) && (scan_until_delim || (--field_length != 0))) {
                tmp_value = tmp_value * 10 + (str.charAt(cur_pos) - '0');
                cur_pos++;
            }
            date_len[i] = (cur_pos - start);
            if (tmp_value > 999999) {
                was_cut = 1;
                return null;
            }
            date[i] = tmp_value;
            not_zero_date |= tmp_value;

            field_length = format_position[i + 1] == 0 ? 4 : 2;

            if ((last_field_pos = cur_pos) == end) {
                i++;
                break;
            }

            if (i == format_position[2] && str.charAt(cur_pos) == 'T') {
                cur_pos++;
                continue;
            }
            if (i == format_position[5]) {
                if (str.charAt(cur_pos) == '.') {
                    cur_pos++;
                    field_length = 6;
                }
                continue;
            }
            while (cur_pos != end && (my_ispunct(str.charAt(cur_pos)) || my_isspace(str.charAt(cur_pos)))) {
                if (my_isspace(str.charAt(cur_pos))) {
                    if (!((allow_space & (1 << i)) != 0)) {
                        was_cut = 1;
                        return null;
                    }
                    found_space = true;
                }
                cur_pos++;
                found_delimitier = true;
            }

            if (i == format_position[6]) {
                i++;
                if (format_position[7] != 255) {
                    if (cur_pos + 2 <= end && (str.charAt(cur_pos + 1) == 'M' || str.charAt(cur_pos) + 1 == 'm')) {
                        if (str.charAt(cur_pos) == 'p' || str.charAt(cur_pos) == 'P') {
                            add_hours = 12;
                        } else if (str.charAt(cur_pos) != 'a' || str.charAt(cur_pos) != 'A') {
                            continue;
                        }
                        str += 2;

                        while (cur_pos != end && my_isspace(str.charAt(cur_pos))) {
                            cur_pos++;
                        }
                    }
                }
            }
            last_field_pos = cur_pos;
        }

        cur_pos = last_field_pos;

        number_of_fields = i - start_loop;
        while (i < MAX_DATE_PARTS) {
            date_len[i] = 0;
            date[i++] = 0;
        }

        if (!is_internal_format) {
            year_length = date_len[format_position[0]];
            if (!(year_length != 0)) {
                was_cut = 1;
                return null;
            }
            int year = date[format_position[0]];
            int month = date[format_position[1]];
            int day = date[format_position[2]];
            int hours = date[format_position[3]];
            int mins = date[format_position[4]];
            int seconds = date[format_position[5]];
            if (!checkDateTime(year_length, not_zero_date, year, month, day, hours, mins, seconds)) {
                return false;
            }
            time.setYear(date[format_position[0]] - 1900);

            time.setMonth(date[format_position[1]] - 1);

            time.setDate(date[format_position[2]]);

            time.setHours(date[format_position[3]]);
            time.setMinutes(date[format_position[4]]);
            time.setSeconds(date[format_position[5]]);

            frac_pos = format_position[6];
            frac_len = date_len[frac_pos];

            if (format_position[7] != 255) {
                if (time.getHours() > 12) {
                    was_cut = 1;
                    return null;
                }
                time.setHours(time.getHours() % 12 + add_hours);
            }
        } else {

            int year = date[0];
            int month = date[1];
            int day = date[2];
            int hours = date[3];
            int mins = date[4];
            int seconds = date[5];

            if (!checkDateTime(year_length, not_zero_date, year, month, day, hours, mins, seconds)) {
                return false;
            }

            time.setYear(date[0] - 1900);
            time.setMonth(date[1] - 1);
            time.setDate(date[2]);

            time.setHours(date[3]);
            time.setMinutes(date[4]);
            time.setSeconds(date[5]);
        }

        if (number_of_fields < 3 || time.getYear() > 9999 || time.getMonth() > 12 || time.getDate() > 31
            || time.getHours() > 23 || time.getMinutes() > 59 || time.getMinutes() > 59) {

            if (!(not_zero_date != 0)) {
                for (; cur_pos != end; cur_pos++) {
                    if (!my_isspace(str.charAt(cur_pos))) {
                        not_zero_date = 1;
                        break;
                    }
                }
            }

            return null;
        }

        return true;
    }

    private static boolean checkDateTime(int year_length, long not_zero_date, int year, int month, int day, int hours,
                                         int mins, int seconds) {

        if (month > 12 || month <= 0) {
            return false;
        }
        if (year_length == 2 && (not_zero_date != 0)) {
            year = year + (year < YY_PART_YEAR ? 2000 : 1900);
        }
        int uplimit = uplimits[month];

        if (day != 2) {
            if (day > uplimit || day <= 0) {
                return false;
            }
        } else {
            if (isLeap(year)) {
                if (day > 29 || day <= 0) {
                    return false;
                }
            } else {
                if (day > 28 || day <= 0) {
                    return false;
                }
            }
        }
        if (hours > 23 || hours < 0) {
            return false;
        }
        if (mins > 59 || mins < 0) {
            return false;
        }
        return seconds <= 59 && seconds >= 0;
    }

    private static boolean isLeap(int year) {
        return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);

    }

    public static boolean my_ispunct(char v) {
        switch (Character.getType(v)) {
        case Character.CONNECTOR_PUNCTUATION:
        case Character.DASH_PUNCTUATION:
        case Character.END_PUNCTUATION:
        case Character.FINAL_QUOTE_PUNCTUATION:
        case Character.INITIAL_QUOTE_PUNCTUATION:
        case Character.START_PUNCTUATION:
        case Character.OTHER_PUNCTUATION:
            return true;
        }

        return false;
    }

    public static int max(int i, int j) {
        return i > j ? i : j;
    }

    public static boolean my_isspace(char v) {
        return Character.isWhitespace(v);
    }

    public static boolean isdigit(char v) {
        return Character.isDigit(v);
    }

    public static Calendar getDateByDelta(Calendar calendar, int deltaVal) {
        calendar.add(Calendar.DATE, deltaVal);
        return calendar;
    }

    public static Date extractDateTime(String val, String format) {
        Calendar cal = strToCalendar(val, format);
        if (cal == null) {
            return null;
        }
        return cal.getTime();
    }

    public static Calendar strToCalendar(String val, String format) {
        MysqlDateTime mysqlTime = strToMySQLTime(val, format);
        if (mysqlTime == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(Calendar.YEAR, (int) mysqlTime.getYear());
        cal.set(Calendar.MONTH, (int) mysqlTime.getMonth() - 1);
        cal.set(Calendar.DATE, (int) mysqlTime.getDay());
        cal.set(Calendar.HOUR, (int) mysqlTime.getHour());
        cal.set(Calendar.MINUTE, (int) mysqlTime.getMinute());
        cal.set(Calendar.SECOND, (int) mysqlTime.getSecond());
        cal.set(Calendar.MILLISECOND, (int) mysqlTime.getSecondPart() / 1000);
        return cal;
    }

    public static MysqlDateTime strToMySQLTime(String val, String format) {

        if (val == null || format == null) {
            return null;
        }
        format = replaceAll(format, "%r", "%I:%i:%S %p");
        format = replaceAll(format, "%T", "%H:%i:%S");
        val = val.trim();
        format = format.trim();
        int valEnd = val.length();
        int formatEnd = format.length();
        int valPos = 0;
        boolean usaTime = false, sundayFirstNFirstWeekNonIso = false, strictWeekNumber = false,
            strictWeekNumberYearType = false, emptyFormat = true;
        int weekDay = 0, yearDay = 0, strictWeekNumberYear = 0, weekNumber = 0;
        long[] result = new long[3];
        MysqlDateTime mysqlTime = new MysqlDateTime();
        Calendar cal = Calendar.getInstance();
        if (val.isEmpty() || format.isEmpty()) {
            return null;
        }

        boolean includeYear = false;
        for (int pos = 0; pos < formatEnd && valPos < valEnd; pos++) {
            while (my_isspace(val.charAt(valPos)) && valPos < valEnd) {
                ++valPos;
            }
            if (valPos == valEnd) {
                break;
            }
            if (format.charAt(pos) == '%' && pos + 1 != formatEnd) {
                pos++;
                emptyFormat = false;
                switch (format.charAt(pos)) {
                case 'Y':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(4, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    if (result[2] - valPos <= 2) {
                        result[1] = result[1] + 1900;
                        if (result[1] < 1970) {
                            result[1] += 100;
                        }
                    }
                    valPos = (int) result[2];
                    mysqlTime.setYear(result[1]);
                    includeYear = true;
                    break;
                case 'y':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(2, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }

                    if (val.substring(valPos, (int) result[2]).trim().length() == 2) {
                        result[1] = result[1] + 1900;
                        if (result[1] < 1970) {
                            result[1] += 100;
                        }
                    }
                    valPos = (int) result[2];
                    mysqlTime.setYear(result[1]);
                    includeYear = true;
                    break;
                case 'm':
                case 'c':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(2, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    mysqlTime.setMonth(result[1]);
                    break;
                case 'M':
                    getMonthFromStr(MONTHS, 12, val, valPos, valEnd, false, result);
                    if (result[1] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    mysqlTime.setMonth((int) result[1]);
                    break;
                case 'b':
                    getMonthFromStr(MONTHS_ABBR, 12, val, valPos, valEnd, true, result);
                    if (result[1] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    mysqlTime.setMonth(result[1]);
                    break;
                case 'd':
                case 'e':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(2, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    mysqlTime.setDay(result[1]);
                    break;
                case 'D':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(2, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];

                    valPos += Math.min(valEnd - valPos, 2);
                    mysqlTime.setDay(result[1]);
                    break;
                case 'h':
                case 'I':
                case 'l':
                    usaTime = true;
                case 'k':
                case 'H':
                    result[0] = 0;
                    if (includeYear) {
                        getIntegerFromStr(val, valPos, valPos + Math.min(2, valEnd - valPos), result);
                    } else {
                        getIntegerFromStr(val, valPos, valEnd, result);
                    }
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    mysqlTime.setHour(result[1]);
                    break;
                case 'i':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(2, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    mysqlTime.setMinute(result[1]);
                    break;
                case 's':
                case 'S':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(2, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    mysqlTime.setSecond(result[1]);
                    break;
                case 'f':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(6, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    if (result[2] > valPos) {
                        result[1] = result[1] * LOG_10_INT[6 - (int) (result[2] - valPos)];
                    }
                    long nanoseconds = result[1] * 1000L;
                    mysqlTime.setSecondPart(nanoseconds);
                    valPos = (int) result[2];
                    break;
                case 'p':
                    if (valEnd - valPos < 2 || !usaTime) {
                        return null;
                    }
                    if (val.substring(valPos, valPos + 2).equalsIgnoreCase("PM")) {
                        if (mysqlTime.getHour() > 12 || mysqlTime.getHour() < 1) {
                            return null;
                        }
                        mysqlTime.setHour(mysqlTime.getHour() % 12 + 12);
                    } else if (!val.substring(valPos, valPos + 2).equalsIgnoreCase("AM")) {
                        return null;
                    }
                    valPos += 2;
                    usaTime = false;
                    break;
                case 'W':
                    getMonthFromStr(WEEK_DAYS, 7, val, valPos, valEnd, false, result);
                    if (result[1] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    weekDay = (int) result[1];
                    break;
                case 'a':
                    getMonthFromStr(WEEK_DAYS, 7, val, valPos, valEnd, false, result);
                    if (result[1] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    weekDay = (int) result[1];
                    break;
                case 'w':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + 1, result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    if (result[1] >= 7) {
                        return null;
                    }
                    weekDay = (int) (result[1] + 1);
                    break;
                case 'j':
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(3, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    yearDay = (int) (result[1]);
                    if (yearDay < 1) {
                        return null;
                    }
                    break;
                case 'V':
                case 'U':
                case 'v':
                case 'u':
                    sundayFirstNFirstWeekNonIso = (format.charAt(pos) == 'U' || format.charAt(pos) == 'V');
                    strictWeekNumber = (format.charAt(pos) == 'V' || format.charAt(pos) == 'v');
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(2, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    strictWeekNumberYear = (int) (result[1] + 1);
                    break;
                case 'X':
                case 'x':
                    strictWeekNumberYearType = (format.charAt(pos) == 'X');
                    result[0] = 0;
                    getIntegerFromStr(val, valPos, valPos + Math.min(4, valEnd - valPos), result);
                    if (result[0] == 0) {
                        return null;
                    }
                    valPos = (int) result[2];
                    weekNumber = (int) (result[1] + 1);
                    if (weekNumber < 0 || (strictWeekNumber && weekNumber == 0) || (weekNumber > 53)) {
                        return null;
                    }
                    break;

                default:
                    return null;
                }
            } else if (!my_isspace(format.charAt(pos)) && format.charAt(pos) != val.charAt(valPos++)) {
                return null;
            }
        }

        if (yearDay > 0) {
            cal.clear();
            cal.set(Calendar.YEAR, (int) mysqlTime.getYear());
            int days = cal.get(Calendar.DAY_OF_YEAR) + yearDay - 1;
            if (days <= 0 || days > MAX_DAY_NUMBER) {
                return null;
            }
            cal.set(Calendar.DAY_OF_YEAR, days);
            mysqlTime.setYear(cal.get(Calendar.YEAR));
            mysqlTime.setMonth(cal.get(Calendar.MONTH) + 1);
            mysqlTime.setDay(cal.get(Calendar.DAY_OF_MONTH));
        }
        if (weekNumber >= 0 && weekDay > 0) {

            if ((strictWeekNumber
                && (strictWeekNumberYear < 0 || strictWeekNumberYearType != sundayFirstNFirstWeekNonIso))
                || (!strictWeekNumber && strictWeekNumberYear >= 0)) {
                return null;
            }

            cal.clear();
            cal.set(Calendar.YEAR, strictWeekNumber ? strictWeekNumberYear : (int) mysqlTime.getYear());
            cal.set(Calendar.MONTH, 0);
            cal.set(Calendar.DATE, 1);
            cal.setFirstDayOfWeek(sundayFirstNFirstWeekNonIso ? Calendar.SUNDAY : Calendar.MONDAY);
            int days = cal.get(Calendar.DAY_OF_YEAR) + yearDay - 1;
            int weekDayB = cal.get(Calendar.DAY_OF_WEEK);
            if (sundayFirstNFirstWeekNonIso) {
                days += (weekDayB == 0 ? 0 : 7) - weekDayB + (weekNumber - 1) * 7 + (weekDay % 7);
            } else {
                days += (weekDayB == 0 ? 0 : 7) - weekDayB + (weekNumber - 1) * 7 + (weekDay - 1);
            }
            if (days <= 0 || days > MAX_DAY_NUMBER) {
                return null;
            }
            cal.set(Calendar.DAY_OF_YEAR, days);
            mysqlTime.setYear(cal.get(Calendar.YEAR));
            mysqlTime.setMonth(cal.get(Calendar.MONTH) + 1);
            mysqlTime.setDay(cal.get(Calendar.DAY_OF_MONTH));
        }
        if (mysqlTime.getMonth() > 12 || mysqlTime.getDay() > 31
            || (mysqlTime.getHour() > 23
            && (mysqlTime.getYear() != Long.MAX_VALUE || (mysqlTime.getMonth() != 0 || mysqlTime.getDay() != 0)))
            || mysqlTime.getMinute() > 59 || mysqlTime.getSecond() > 59 || emptyFormat) {
            return null;
        }

        if (mysqlTime.getYear() == Long.MAX_VALUE) {
            mysqlTime.setYear(0);
        }
        return mysqlTime;
    }

    public static String strToTimeString(String str) {
        str = str.trim();
        boolean includeDatePart = (str.indexOf(' ') != -1);
        if (includeDatePart) {
            return str;
        }
        boolean formatted = (str.indexOf(':') != -1);
        if (formatted) {
            return str;
        }
        boolean isNegative = false;
        if (!str.isEmpty() && str.charAt(0) == '-') {
            isNegative = true;
            str = str.substring(1);
        }
        int nanoIndex = str.indexOf('.');
        nanoIndex = nanoIndex > 0 ? nanoIndex : str.length();
        switch (nanoIndex) {
        case 0:
        case 1:
            str = "00:00:0" + str;
            break;
        case 2:
            str = "00:00:" + str;
            break;
        case 3:
            str = "00:0" + str.substring(0, 1) + ":" + str.substring(1);
            break;
        case 4:
            str = "00:" + str.substring(0, 2) + ":" + str.substring(2);
            break;
        case 5:
            str = "0" + str.substring(0, 1) + ":" + str.substring(1, 3) + ":" + str.substring(3);
            break;
        case 6:
            str = str.substring(0, 2) + ":" + str.substring(2, 4) + ":" + str.substring(4);
            break;
        case 7:
            str = str.substring(0, 3) + ":" + str.substring(3, 5) + ":" + str.substring(5);
            break;
        default:
            str = str.substring(0, nanoIndex - 4) + ":" + str.substring(nanoIndex - 4, nanoIndex - 2) + ":"
                + str.substring(nanoIndex - 2);
            break;
        }
        if (isNegative) {
            str = "-" + str;
        }
        return str;
    }

    public static String strToDateString(String val) {
        val = val.trim();
        if (val.indexOf('-') != -1) {
            return val;
        } else if (val.indexOf('/') != -1) {
            return replaceAll(val, "/", "-");
        } else {
            int i = 0;
            while (i < val.length() && val.charAt(i) == '0') {
                i++;
            }
            val = val.substring(i);
            int nanoIndex = val.indexOf('.');
            nanoIndex = nanoIndex > 0 ? nanoIndex : val.length();

            boolean simpleYear = false;
            switch (nanoIndex) {
            case 3:
                val = "00-0" + val.substring(0, 1) + "-" + val.substring(1, 3);
                simpleYear = true;
                break;
            case 4:
                val = "00-" + val.substring(0, 2) + "-" + val.substring(2, 4);
                simpleYear = true;
                break;
            case 5:
                val = "0" + val.substring(0, 1) + "-" + val.substring(1, 3) + "-" + val.substring(3, 5);
                simpleYear = true;
                break;
            case 6:
                val = val.substring(0, 2) + "-" + val.substring(2, 4) + "-" + val.substring(4, 6);
                simpleYear = true;
                break;
            case 7:
                val = "0" + val.substring(0, 3) + "-" + val.substring(3, 5) + "-" + val.substring(5, 7);
                simpleYear = false;
                break;
            case 8:
                val = val.substring(0, 4) + "-" + val.substring(4, 6) + "-" + val.substring(6, 8);
                simpleYear = false;
                break;
            case 9:
                val = "00-0" + val.substring(0, 1) + "-" + val.substring(1, 3) + " " + val.substring(3, 5) + ":"
                    + val.substring(5, 7) + ":" + val.substring(7);
                simpleYear = true;
                break;
            case 10:
                val = "00-" + val.substring(0, 2) + "-" + val.substring(2, 4) + " " + val.substring(4, 6) + ":"
                    + val.substring(6, 8) + ":" + val.substring(8);
                simpleYear = true;
                break;
            case 11:
                val = "0" + val.substring(0, 1) + "-" + val.substring(1, 3) + "-" + val.substring(3, 5) + " "
                    + val.substring(5, 7) + ":" + val.substring(7, 9) + ":" + val.substring(9);
                simpleYear = true;
                break;
            case 12:
                val = val.substring(0, 2) + "-" + val.substring(2, 4) + "-" + val.substring(4, 6) + " "
                    + val.substring(6, 8) + ":" + val.substring(8, 10) + ":" + val.substring(10);
                simpleYear = true;
                break;
            case 13:
                val = "0" + val.substring(0, 3) + "-" + val.substring(3, 5) + "-" + val.substring(5, 7) + " "
                    + val.substring(7, 9) + ":" + val.substring(9, 11) + ":" + val.substring(11);
                simpleYear = false;
                break;
            case 14:
                val = val.substring(0, 4) + "-" + val.substring(4, 6) + "-" + val.substring(6, 8) + " "
                    + val.substring(8, 10) + ":" + val.substring(10, 12) + ":" + val.substring(12);
                simpleYear = false;
                break;
            }
        }
        return val;
    }

    public static String strToFormatString(String val, String formatStr, boolean isDate) {
        String formator;
        Calendar cal;
        if (isDate) {
            val = strToDateString(val);
            formator = val.indexOf("-") <= 2 ? MYSQL_DATETIME_DEFAULT_FORMAT2 : MYSQL_DATETIME_DEFAULT_FORMAT1;
        } else {
            val = strToTimeString(val);
            formator = MYSQL_TIME_FORMAT;
        }
        MysqlDateTime mysqltime = strToMySQLTime(val, formator);
        cal = strToCalendar(val, formator);

        formatStr = replaceAll(formatStr, "%r", "%I:%i:%S %p");
        formatStr = replaceAll(formatStr, "%T", "%H:%i:%S");
        int formatEnd = formatStr.length();
        StringBuffer valBuff = new StringBuffer();
        for (int pos = 0; pos < formatEnd; pos++) {
            if (formatStr.charAt(pos) == '%' && pos + 1 != formatEnd) {
                pos++;
                switch (formatStr.charAt(pos)) {
                case 'Y':
                    valBuff.append(int2StringWithFixedSize(4, mysqltime.getYear()));
                    break;
                case 'y':
                    valBuff.append(int2StringWithFixedSize(2, mysqltime.getYear() / 1000));
                    break;
                case 'm':
                    valBuff.append(int2StringWithFixedSize(2, mysqltime.getMonth()));
                    break;
                case 'c':
                    valBuff.append(mysqltime.getMonth());
                    break;
                case 'M':
                    if (mysqltime.getMonth() > 12 || mysqltime.getMonth() < 1) {
                        return null;
                    }
                    valBuff.append(MONTHS[(int) mysqltime.getMonth() - 1]);
                    break;
                case 'b':
                    if (mysqltime.getMonth() > 12 || mysqltime.getMonth() < 1) {
                        return null;
                    }
                    valBuff.append(MONTHS_ABBR[(int) mysqltime.getMonth() - 1]);
                    break;
                case 'd':
                    valBuff.append(int2StringWithFixedSize(2, mysqltime.getDay()));
                    break;
                case 'e':
                    valBuff.append(mysqltime.getDay());
                    break;
                case 'D':
                    valBuff.append(mysqltime.getDay());
                    switch ((int) mysqltime.getDay()) {
                    case 2:
                        valBuff.append("nd");
                        break;
                    case 3:
                        valBuff.append("rd");
                        break;
                    default:
                        valBuff.append("st");
                        break;
                    }
                    break;
                case 'h':
                case 'I':
                    valBuff.append(
                        int2StringWithFixedSize(2, mysqltime.getHour() % 12 == 0 ? 12 : mysqltime.getHour() % 12));
                    break;
                case 'l':
                    valBuff.append(mysqltime.getHour() % 12 == 0 ? 12 : mysqltime.getHour() % 12);
                    break;
                case 'k':
                    valBuff.append(mysqltime.getHour());
                    break;
                case 'H':
                    valBuff.append(int2StringWithFixedSize(2, mysqltime.getHour()));
                    break;
                case 'i':
                    valBuff.append(int2StringWithFixedSize(2, mysqltime.getMinute()));
                    break;
                case 's':
                case 'S':
                    valBuff.append(int2StringWithFixedSize(2, mysqltime.getSecond()));
                    break;
                case 'f':
                    valBuff.append(int2StringWithFixedSize(6, mysqltime.getSecondPart()));
                    break;
                case 'p':
                    if (mysqltime.getHour() % 24 >= 12) {
                        valBuff.append("PM");
                    } else {
                        valBuff.append("AM");
                    }
                    break;
                case 'W':
                    if (!isDate) {
                        return null;
                    }
                    int weekIndex = cal.get(Calendar.DAY_OF_WEEK) - 1;
                    weekIndex = weekIndex < 0 ? 0 : weekIndex;
                    valBuff.append(WEEK_DAYS[weekIndex]);
                    break;
                case 'a':
                    if (!isDate) {
                        return null;
                    }
                    int weekIndexAbbr = cal.get(Calendar.DAY_OF_WEEK) - 1;
                    weekIndexAbbr = weekIndexAbbr < 0 ? 0 : weekIndexAbbr;
                    valBuff.append(WEEK_DAYS_ABBR[weekIndexAbbr]);
                    break;
                case 'w':
                    if (!isDate) {
                        return null;
                    }
                    valBuff.append(cal.get(Calendar.DAY_OF_WEEK));
                    break;
                case 'j':
                    if (!isDate) {
                        return null;
                    }
                    valBuff.append(int2StringWithFixedSize(3, cal.get(Calendar.DAY_OF_YEAR)));
                    break;
                case 'V':
                case 'U':
                    if (!isDate) {
                        return null;
                    }
                    cal.setFirstDayOfWeek(Calendar.SUNDAY);
                    valBuff.append(cal.get(Calendar.WEEK_OF_YEAR));
                    break;
                case 'v':
                case 'u':
                    if (!isDate) {
                        return null;
                    }
                    cal.setFirstDayOfWeek(Calendar.MONDAY);
                    valBuff.append(cal.get(Calendar.WEEK_OF_YEAR));
                    break;
                case 'X':
                case 'x':
                    if (!isDate) {
                        return null;
                    }
                    valBuff.append("0001");
                    break;
                default:
                    valBuff.append(formatStr.charAt(pos));
                    break;
                }
            } else {
                valBuff.append(formatStr.charAt(pos));
            }
        }
        if (valBuff.length() > 0) {
            return valBuff.toString();
        } else {
            return null;
        }

    }

    public static void getIntegerFromStr(String val, int pos, int endPos, long[] result) {
        int startValPos = pos;
        while (pos != endPos && (val.charAt(pos) == ' ' || val.charAt(pos) == '\t')) {
            pos++;
        }
        startValPos = pos;
        if (val.charAt(pos) == '-' || val.charAt(pos) == '+') {
            pos++;
        }
        while (pos < endPos && val.charAt(pos) - '0' <= 9 && val.charAt(pos) - '0' >= 0) {
            pos++;
        }
        if (pos == startValPos
            || ((val.charAt(startValPos) == '-' || val.charAt(startValPos) == '+') && pos - startValPos <= 1)) {
            result[0] = 0;
            return;
        }
        try {
            result[1] = Long.parseLong(val.substring(startValPos, pos));
        } catch (NumberFormatException e) {
            result[0] = 0;
            return;
        }
        result[0] = 1;
        result[2] = pos;
    }

    public static void getMonthFromStr(String[] lib, int len, String val, int startPos, int endPos, boolean isAbbr,
                                       long[] result) {
        int pos = startPos;
        result[1] = 0;
        while (endPos > pos && Character.isLetter(val.charAt(pos))) {
            pos++;
        }
        for (int i = 0; i < 12; i++) {
            if (lib[i].equalsIgnoreCase(val.substring(startPos, pos))) {
                result[1] = i + 1;
                break;
            }
        }
        result[2] = pos;
    }

    public static void normalizeTime(Date time) {
        Calendar cal = Calendar.getInstance();
        cal.clear();
        long diff = time.getTime() - cal.getTimeInMillis();
        long nano = (diff % 1000) * 1000;
        diff = diff / 1000;
        long second = diff % 60;
        diff = diff / 60;
        long minute = diff % 60;
        diff = diff / 60;
        long hour = diff;
        boolean outOfRange = (hour > MAX_HOUR || hour < MIN_HOUR)
            || ((hour == MAX_HOUR || hour == MIN_HOUR) && (second == 59 && minute == 59));
        if (outOfRange) {
            hour = hour > 0 ? MAX_HOUR : MIN_HOUR;
            second = hour > 0 ? 59 : -59;
            minute = hour > 0 ? 59 : -59;
            nano = 0;
        }
        diff = hour * 3600 * 1000 + minute * 60000 + second * 1000 + nano / 1000;
        time.setTime(diff);
    }

    public static void resetCalendar(Calendar cal) {
        cal.clear();
        cal.set(Calendar.YEAR, 0);
        cal.set(Calendar.MONTH, -1);
        cal.set(Calendar.DATE, 0);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
    }

    public static String replaceAll(String source, String pattern, String replaceStr) {
        StringBuilder sb = new StringBuilder();
        int index;
        int patternLen = pattern.length();
        for (int i = 0; i < source.length(); ) {
            index = source.indexOf(pattern, i);
            if (index != -1) {
                sb.append(source, i, index);
                sb.append(replaceStr);
                i = patternLen + index;
            } else {
                sb.append(source.substring(i));
                break;
            }
        }
        return sb.toString();
    }

    private static String int2StringWithFixedSize(int fixedSize, long val) {
        assert fixedSize < 20;
        String retVal = String.valueOf(val);
        if (retVal.length() < fixedSize) {
            retVal = ZERO_STR.substring(0, fixedSize - retVal.length()) + retVal;
        } else if (fixedSize < retVal.length()) {
            retVal = retVal.substring(0, fixedSize);
        }
        return retVal;
    }

    public static void main(String[] args) {
        String source =
            "time2DateTime#OKdw,lmoi09,lp[k99jij9fremionionu9023niodsanbhubltime2DateTime#OKdsnjknbuybuitime2DateTime#OKjioniion";
        String replaceStr = "10:20:22";
        String pattern = "time2DateTime#OK";
        System.out.println(replaceAll(source, pattern, replaceStr));
        System.out.println(replaceAll("2019-11-12   10:32:20", " ", ""));
    }
}


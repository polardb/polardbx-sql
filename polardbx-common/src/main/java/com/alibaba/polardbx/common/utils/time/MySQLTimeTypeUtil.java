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

package com.alibaba.polardbx.common.utils.time;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.mysql.jdbc.StringUtils;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.time.parser.MySQLTimeParserBase;
import com.alibaba.polardbx.common.utils.time.parser.NumericTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Optional;
import java.util.TimeZone;

import static com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags.FLAG_TIME_FUZZY_DATE;
import static com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags.FLAG_TIME_INVALID_DATES;
import static com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags.FLAG_TIME_NO_ZERO_DATE;
import static com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE;
import static com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags.FLAG_TIME_WARN_OUT_OF_RANGE;
import static com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags.FLAG_TIME_WARN_ZERO_DATE;
import static com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags.FLAG_TIME_WARN_ZERO_IN_DATE;

public class MySQLTimeTypeUtil {

    public static final int MAX_FRACTIONAL_SCALE = 6;
    public static final int MAX_DATE_WIDTH = 10;
    public static final int MAX_TIME_WIDTH = 10;
    public static final int MAX_DATETIME_WIDTH = 19;
    public static final int MAX_TIME_PRECISION = MAX_TIME_WIDTH + MAX_FRACTIONAL_SCALE + 1;
    public static final int MAX_DATETIME_PRECISION = MAX_DATETIME_WIDTH + MAX_FRACTIONAL_SCALE + 1;


    public static final long SEC_TO_NANO = 1000_000_000L;
    public static final Decimal SEC_TO_NANO_DEC = new Decimal(SEC_TO_NANO, 0);
    public static final int MAX_NANO_LENGTH = 6;
    public static final long SEC_TO_MICRO = 1000_000L;

    public static final int MAX_DAY_NUMBER = 3652424;
    public static final int TIMESTAMP_MAX_YEAR = 2038;
    public static final int TIMESTAMP_MIN_YEAR = 1969;

    public static final int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};

    public static final int YEAR_SQL_TYPE = 10001;
    public static final int DATETIME_SQL_TYPE = 10003;

    public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();
    public static final long DEFAULT_TIME_ZONE_OFFSET = DEFAULT_TIME_ZONE.getRawOffset();

    public static final boolean FAST_EPOCH_MILLIS = true;

    private final static ThreadLocal threadLocal = new ThreadLocal();

    public static Calendar getCalendar() {
        Calendar cal = (Calendar) threadLocal.get();
        if (cal == null) {
            cal = Calendar.getInstance();
            threadLocal.set(cal);
        }
        cal.clear();
        return cal;
    }

    public static int getScaleOfTime(long maxLength) {
        return maxLength == MAX_TIME_WIDTH ? 0 : (int) (maxLength - MAX_TIME_WIDTH - 1);
    }

    public static int getScaleOfDatetime(long maxLength) {
        return maxLength == MAX_DATETIME_WIDTH ? 0 : (int) (maxLength - MAX_DATETIME_WIDTH - 1);
    }

    public static int normalizeScale(int scale) {
        if (scale < 0) {
            return 0;
        }
        if (scale > 6) {
            return 6;
        }
        return scale;
    }

    public static int calcDaysInYear(int year) {
        return ((year & 3) == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0)) ?
            366 : 365);
    }

    public static boolean isLeapYear(long year) {
        return (year % 4) == 0
            && ((year % 100) != 0 || (year % 400) == 0);
    }

    public static boolean notZeroDate(MysqlDateTime t) {
        return t.getYear() != 0 || t.getMonth() != 0 || t.getDay() != 0;
    }

    public static long getLeaps(long year) {
        return (year / 4) - (year / 100) + (year / 400);
    }

    static int[][] MONTH_STARTS = new int[][]
        {
            {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
            {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335}
        };

    public static long toJavaEpochMillis(MysqlDateTime t, int sqlType) {
        int year = (int) t.getYear();
        int month = (int) t.getMonth();
        int day = (int) t.getDay();
        int hour = (int) t.getHour();
        int minute = (int) t.getMinute();
        int second = (int) t.getSecond();
        int secondPart = (int) t.getSecondPart();

        switch (sqlType) {
        case Types.TIME:
            year = 1970;
            month = 1;
            day = 1;
            secondPart = 0;
            break;
        case Types.DATE:
            hour = 0;
            minute = 0;
            second = 0;
            secondPart = 0;
            break;
        case DATETIME_SQL_TYPE:
        case Types.TIMESTAMP:
        default:
            break;
        }

        boolean isSafe = t.getYear() >= 1 && t.getYear() < 9999
            && t.getMonth() > 0 && t.getMonth() < 13
            && t.getDay() > 0 && t.getDay() < DAYS_IN_MONTH[(int) t.getMonth() - 1]
            && t.getHour() < 24 && t.getHour() >= 0
            && t.getMinute() < 60 && t.getMinute() >= 0
            && t.getSecond() < 60 && t.getSecond() >= 0;

        long millis;
        if (isSafe && FAST_EPOCH_MILLIS) {

            millis = MySQLTimeConverter.secondSinceEpoch(year, month, day, hour, minute, second) * 1000;

            int offset = DEFAULT_TIME_ZONE.getOffset(millis - DEFAULT_TIME_ZONE_OFFSET);
            millis -= offset;

            millis += secondPart / 1000_000;
            return millis;
        } else {
            Calendar cal = getCalendar();
            cal.clear();
            cal.set(year, month - 1, day, hour, minute, second);

            if (secondPart != 0) {
                cal.set(Calendar.MILLISECOND, secondPart / 1000_000);
            }

            millis = cal.getTimeInMillis();
        }
        return millis;
    }

    public static final int TIME_MAX_HOUR = 838;

    @Deprecated
    public static MysqlDateTime bytesToMysqlTime(byte[] timestampAsBytes, int sqlType) {
        int length = timestampAsBytes.length;
        boolean allZeroTimestamp = true;

        boolean onlyTimePresent = false;

        for (int i = 0; i < length; i++) {
            if (timestampAsBytes[i] == ':') {
                onlyTimePresent = true;
                break;
            }
        }

        for (int i = 0; i < length; i++) {
            byte b = timestampAsBytes[i];

            if (b == ' ' || b == '-' || b == '/') {
                onlyTimePresent = false;
            }

            if (b != '0' && b != ' ' && b != ':' && b != '-' && b != '/' && b != '.') {
                allZeroTimestamp = false;

                break;
            }
        }
        MysqlDateTime mysqlDateTime = new MysqlDateTime();

        if (onlyTimePresent || !allZeroTimestamp) {

            int year = 0;
            int month = 0;
            int day = 0;
            int hour = 0;
            int minutes = 0;
            int seconds = 0;
            int nanos = 0;

            int decimalIndex = -1;
            for (int i = 0; i < length; i++) {
                if (timestampAsBytes[i] == '.') {
                    decimalIndex = i;
                    break;
                }
            }

            if (decimalIndex == length - 1) {

                length--;

            } else if (decimalIndex != -1) {
                if ((decimalIndex + 2) <= length) {
                    nanos = StringUtils.getInt(timestampAsBytes, decimalIndex + 1, length);

                    int numDigits = (length) - (decimalIndex + 1);

                    if (numDigits < 9) {
                        int factor = (int) (Math.pow(10, 9 - numDigits));
                        nanos = nanos * factor;
                    }
                } else {
                    throw new IllegalArgumentException();

                }

                length = decimalIndex;
            }

            switch (length) {
            case 29:
            case 26:
            case 25:
            case 24:
            case 23:
            case 22:
            case 21:
            case 20:
            case 19: {
                year = StringUtils.getInt(timestampAsBytes, 0, 4);
                month = StringUtils.getInt(timestampAsBytes, 5, 7);
                day = StringUtils.getInt(timestampAsBytes, 8, 10);
                hour = StringUtils.getInt(timestampAsBytes, 11, 13);
                minutes = StringUtils.getInt(timestampAsBytes, 14, 16);
                seconds = StringUtils.getInt(timestampAsBytes, 17, 19);

                break;
            }

            case 14: {
                year = StringUtils.getInt(timestampAsBytes, 0, 4);
                month = StringUtils.getInt(timestampAsBytes, 4, 6);
                day = StringUtils.getInt(timestampAsBytes, 6, 8);
                hour = StringUtils.getInt(timestampAsBytes, 8, 10);
                minutes = StringUtils.getInt(timestampAsBytes, 10, 12);
                seconds = StringUtils.getInt(timestampAsBytes, 12, 14);

                break;
            }

            case 12: {
                year = StringUtils.getInt(timestampAsBytes, 0, 2);

                if (year <= 69) {
                    year = (year + 100);
                }

                year += 1900;

                month = StringUtils.getInt(timestampAsBytes, 2, 4);
                day = StringUtils.getInt(timestampAsBytes, 4, 6);
                hour = StringUtils.getInt(timestampAsBytes, 6, 8);
                minutes = StringUtils.getInt(timestampAsBytes, 8, 10);
                seconds = StringUtils.getInt(timestampAsBytes, 10, 12);

                break;
            }

            case 10: {
                boolean hasDash = false;

                for (int i = 0; i < length; i++) {
                    if (timestampAsBytes[i] == '-') {
                        hasDash = true;
                        break;
                    }
                }

                if (sqlType == Types.DATE || hasDash) {
                    year = StringUtils.getInt(timestampAsBytes, 0, 4);
                    month = StringUtils.getInt(timestampAsBytes, 5, 7);
                    day = StringUtils.getInt(timestampAsBytes, 8, 10);
                    hour = 0;
                    minutes = 0;
                } else {
                    year = StringUtils.getInt(timestampAsBytes, 0, 2);

                    if (year <= 69) {
                        year = (year + 100);
                    }

                    month = StringUtils.getInt(timestampAsBytes, 2, 4);
                    day = StringUtils.getInt(timestampAsBytes, 4, 6);
                    hour = StringUtils.getInt(timestampAsBytes, 6, 8);
                    minutes = StringUtils.getInt(timestampAsBytes, 8, 10);

                    year += 1900;
                }

                break;
            }

            case 8: {
                boolean hasColon = false;

                for (int i = 0; i < length; i++) {
                    if (timestampAsBytes[i] == ':') {
                        hasColon = true;
                        break;
                    }
                }

                if (hasColon) {
                    hour = StringUtils.getInt(timestampAsBytes, 0, 2);
                    minutes = StringUtils.getInt(timestampAsBytes, 3, 5);
                    seconds = StringUtils.getInt(timestampAsBytes, 6, 8);

                    year = 1970;
                    month = 1;
                    day = 1;

                    break;
                }

                year = StringUtils.getInt(timestampAsBytes, 0, 4);
                month = StringUtils.getInt(timestampAsBytes, 4, 6);
                day = StringUtils.getInt(timestampAsBytes, 6, 8);

                year -= 1900;
                month--;

                break;
            }

            case 6: {
                year = StringUtils.getInt(timestampAsBytes, 0, 2);

                if (year <= 69) {
                    year = (year + 100);
                }

                year += 1900;

                month = StringUtils.getInt(timestampAsBytes, 2, 4);
                day = StringUtils.getInt(timestampAsBytes, 4, 6);

                break;
            }

            case 4: {
                year = StringUtils.getInt(timestampAsBytes, 0, 2);

                if (year <= 69) {
                    year = (year + 100);
                }

                month = StringUtils.getInt(timestampAsBytes, 2, 4);

                day = 1;

                break;
            }

            case 2: {
                year = StringUtils.getInt(timestampAsBytes, 0, 2);

                if (year <= 69) {
                    year = (year + 100);
                }

                year += 1900;
                month = 1;
                day = 1;

                break;
            }

            default:

                return null;
            }
            mysqlDateTime.setYear(year);
            mysqlDateTime.setMonth(month);
            mysqlDateTime.setDay(day);
            mysqlDateTime.setHour(hour);
            mysqlDateTime.setMinute(minutes);
            mysqlDateTime.setSecond(seconds);
            mysqlDateTime.setSecondPart(nanos);
        }
        return mysqlDateTime;
    }

    public static Time bytesToTime(byte[] timestampAsBytes, int sqlType, boolean allowModification) {
        return Optional.ofNullable(timestampAsBytes)
            .map(bs -> StringTimeParser.parseString(timestampAsBytes, sqlType))
            .map(
                t -> {
                    if (!allowModification) {
                        return createOriginalTime(t);
                    } else {
                        return createJavaTime(t);
                    }
                }
            )
            .orElse(null);
    }

    public static Time createOriginalTime(MysqlDateTime t) {
        if (t == null) {
            return null;
        }
        return new OriginalTime(t);
    }

    public static Time createJavaTime(MysqlDateTime t) {
        if (t == null ||
            t.getHour() < 0 || t.getHour() > 24 ||
            t.getMinute() < 0 || t.getMinute() > 59 ||
            t.getSecond() < 0 || t.getSecond() > 59) {
            return null;
        }
        Calendar cal = getCalendar();

        cal.clear();

        cal.set(1970, 0, 1, (int) t.getHour(), (int) t.getMinute(), (int) t.getSecond());

        long timeAsMillis = cal.getTimeInMillis();

        return new Time(timeAsMillis);
    }

    public static Timestamp bytesToDatetime(byte[] timestampAsBytes, int sqlType,
                                            boolean allowModification) {
        return bytesToDatetime(
            timestampAsBytes,
            sqlType,
            getCalendar(),
            allowModification,
            false
        );
    }

    public static Timestamp bytesToDatetime(byte[] timestampAsBytes, int sqlType, Calendar calendar,
                                            boolean allowModification, boolean allowInvalidity) {

        MysqlDateTime mysqlDateTime = StringTimeParser.parseString(timestampAsBytes, sqlType);
        if (mysqlDateTime == null) {
            return null;
        }
        if (!allowModification) {
            return createOriginalTimestamp(mysqlDateTime);
        } else {
            int year = (int) mysqlDateTime.getYear();
            int month = (int) mysqlDateTime.getMonth();
            int day = (int) mysqlDateTime.getDay();
            int hour = (int) mysqlDateTime.getHour();
            int minute = (int) mysqlDateTime.getMinute();
            int second = (int) mysqlDateTime.getSecond();
            int secondPart = (int) mysqlDateTime.getSecondPart();
            return createJavaTimestamp(calendar, year, month, day, hour, minute, second, secondPart);
        }
    }

    public static Timestamp createOriginalTimestamp(MysqlDateTime t) {
        if (t == null) {
            return null;
        }

        return new OriginalTimestamp(t);
    }

    public static Timestamp createJavaTimestamp(Calendar cal, int year, int month, int day, int hour, int minute,
                                                int seconds, int secondsPart) {
        cal.clear();
        cal.set(year, month - 1, day, hour, minute, seconds);
        if (secondsPart != 0) {
            cal.set(Calendar.MILLISECOND, secondsPart / 1000000);
        }
        long tsAsMillis = cal.getTimeInMillis();

        Timestamp ts = new Timestamp(tsAsMillis);
        ts.setNanos(secondsPart);
        return ts;
    }

    public static Date bytesToDate(byte[] dateAsBytes, int sqlType, boolean allowModification) {
        return Optional.ofNullable(dateAsBytes)
            .map(bs -> StringTimeParser.parseString(dateAsBytes, sqlType))
            .map(
                t -> {
                    if (!allowModification) {
                        return createOriginalDate(t);
                    } else {
                        return createJavaDate(t);
                    }
                }
            )
            .orElse(null);
    }

    public static Date createJavaDate(MysqlDateTime t) {
        Calendar dateCal = getCalendar();

        dateCal.clear();
        dateCal.set(Calendar.MILLISECOND, 0);

        dateCal.set((int) t.getYear(), (int) (t.getMonth() - 1), (int) t.getDay(), 0, 0, 0);

        long dateAsMillis = dateCal.getTimeInMillis();

        return new Date(dateAsMillis);
    }

    public static Date createOriginalDate(MysqlDateTime t) {
        if (t == null) {
            return null;
        }

        return new OriginalDate(t);
    }

    public static int nanoOfTime(Time time) {
        if (time instanceof OriginalTime) {
            MysqlDateTime t = ((OriginalTime) time).getMysqlDateTime();
            return (int) t.getSecondPart();
        }
        return 0;
    }

    public static Timestamp toJavaTimestamp(Timestamp timestamp) {
        if (timestamp instanceof OriginalTimestamp) {
            long tsAsMillis = timestamp.getTime();

            Timestamp ret = new Timestamp(tsAsMillis);
            ret.setNanos(timestamp.getNanos());
            return ret;
        }
        return timestamp;
    }

    public static MysqlDateTime toMysqlDateTime(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        }
        if (timestamp instanceof OriginalTimestamp) {
            return ((OriginalTimestamp) timestamp).getMysqlDateTime();
        } else {
            return Optional.ofNullable(timestamp)
                .map(Timestamp::toString)
                .map(String::getBytes)
                .map(
                    bs -> StringTimeParser.parseDatetime(bs)
                ).orElse(null);
        }
    }

    public static MysqlDateTime toMysqlDateTime(Timestamp timestamp, int flags) {
        if (timestamp == null) {
            return null;
        }
        if (timestamp instanceof OriginalTimestamp) {
            return ((OriginalTimestamp) timestamp).getMysqlDateTime();
        } else {
            return Optional.ofNullable(timestamp)
                .map(Timestamp::toString)
                .map(String::getBytes)
                .map(
                    bs -> StringTimeParser.parseDatetime(bs, flags)
                ).orElse(null);
        }
    }

    public static Time toJavaTime(Time time) {
        if (time == null) {
            return null;
        }
        if (time instanceof OriginalTime) {
            long t = time.getTime();
            return new Time(t);
        }
        return time;
    }

    public static MysqlDateTime toMysqlTime(Time time) {
        if (time == null) {
            return null;
        }
        if (time instanceof OriginalTime) {
            return ((OriginalTime) time).getMysqlDateTime();
        } else {
            return Optional.ofNullable(time)
                .map(Time::toString)
                .map(String::getBytes)
                .map(
                    StringTimeParser::parseTime
                )
                .orElse(null);
        }
    }

    public static MysqlDateTime toMysqlTime(Time time, int flags) {
        if (time == null) {
            return null;
        }
        if (time instanceof OriginalTime) {
            return ((OriginalTime) time).getMysqlDateTime();
        } else {
            return Optional.ofNullable(time)
                .map(Time::toString)
                .map(String::getBytes)
                .map(
                    bs -> StringTimeParser.parseTime(bs, flags)
                )
                .orElse(null);
        }
    }

    public static MysqlDateTime toMysqlDate(Date date) {
        if (date == null) {
            return null;
        }
        if (date instanceof OriginalDate) {
            return ((OriginalDate) date).getMysqlDateTime();
        } else {
            return Optional.ofNullable(date)
                .map(Date::toString)
                .map(String::getBytes)
                .map(
                    bs -> StringTimeParser.parseDatetime(bs)
                ).orElse(null);
        }
    }

    public static MysqlDateTime toMysqlDate(Date date, int flags) {
        if (date == null) {
            return null;
        }
        if (date instanceof OriginalDate) {
            return ((OriginalDate) date).getMysqlDateTime();
        } else {
            return Optional.ofNullable(date)
                .map(Date::toString)
                .map(String::getBytes)
                .map(
                    bs -> StringTimeParser.parseDatetime(bs, flags)
                ).orElse(null);
        }
    }

    public static Date toJavaDate(Date date) {
        if (date == null) {
            return null;
        }
        if (date instanceof OriginalDate) {
            long t = date.getTime();
            return new Date(t);
        }
        return date;
    }

    public static boolean isTimeRangeInvalid(MysqlDateTime t) {
        if (t == null) {
            return true;
        }
        return t.getMinute() >= 60
            || t.getSecond() >= 60
            || t.getSecondPart() >= 999999999;
    }

    public static boolean checkTimeRangeQuick(MysqlDateTime t) {
        long hour = t.getHour() + 24 * t.getDay();
        return hour <= TIME_MAX_HOUR
            && (hour != TIME_MAX_HOUR
            || t.getMinute() != 59
            || t.getSecond() != 59
            || t.getSecondPart() == 0);
    }

    public static boolean isDatetimeRangeInvalid(MysqlDateTime t) {
        if (t == null) {
            return true;
        }
        return t.getYear() > 9999 || t.getMonth() > 12 || t.getDay() > 31
            || t.getMinute() > 59 || t.getSecond() > 59 || t.getSecondPart() > 999999999
            || t.getHour() > (t.getSqlType() == Types.TIME ? TIME_MAX_HOUR : 23);
    }

    public static boolean isDateInvalid(MysqlDateTime t, boolean notZeroDate, int flags) {
        return isDateInvalid(
            t,
            notZeroDate,
            TimeParserFlags.check(flags, FLAG_TIME_NO_ZERO_IN_DATE),
            TimeParserFlags.check(flags, FLAG_TIME_FUZZY_DATE),
            TimeParserFlags.check(flags, FLAG_TIME_INVALID_DATES),
            TimeParserFlags.check(flags, FLAG_TIME_NO_ZERO_DATE)
        );
    }

    public static boolean isDateInvalid(MysqlDateTime t, boolean notZeroDate, int flags,
                                        TimeParseStatus status) {
        return isDateInvalid(
            t,
            notZeroDate,
            TimeParserFlags.check(flags, FLAG_TIME_NO_ZERO_IN_DATE),
            TimeParserFlags.check(flags, FLAG_TIME_FUZZY_DATE),
            TimeParserFlags.check(flags, FLAG_TIME_INVALID_DATES),
            TimeParserFlags.check(flags, FLAG_TIME_NO_ZERO_DATE),
            status
        );
    }

    public static boolean isDateInvalid(MysqlDateTime t, boolean notZeroDate,
                                        boolean timeNoZeroInDate,
                                        boolean timeFuzzyDate,
                                        boolean timeInvalidDates,
                                        boolean timeNoZeroDate) {
        if (t == null) {
            return true;
        }
        if (notZeroDate) {
            if ((timeNoZeroInDate || !timeFuzzyDate)
                && (t.getMonth() == 0 || t.getDay() == 0)) {
                return true;
            } else if (!timeInvalidDates) {
                return t.getMonth() > 0
                    && t.getDay() > DAYS_IN_MONTH[(int) (t.getMonth() - 1)]
                    && !(t.getMonth() == 2 && t.getDay() == 29
                    && calcDaysInYear((int) t.getYear()) == 366);
            }
        }
        return timeNoZeroDate;
    }

    public static boolean isDateInvalid(MysqlDateTime t, boolean notZeroDate,
                                        boolean timeNoZeroInDate,
                                        boolean timeFuzzyDate,
                                        boolean timeInvalidDates,
                                        boolean timeNoZeroDate,
                                        TimeParseStatus status) {
        if (t == null) {
            return true;
        }
        if (notZeroDate) {
            if ((timeNoZeroInDate || !timeFuzzyDate)
                && (t.getMonth() == 0 || t.getDay() == 0)) {
                if (status != null) {
                    status.addWarning(FLAG_TIME_WARN_ZERO_IN_DATE);
                }
                return true;
            } else if (!timeInvalidDates
                && t.getMonth() > 0
                && t.getDay() > DAYS_IN_MONTH[(int) (t.getMonth() - 1)]
                && !(t.getMonth() == 2 && t.getDay() == 29
                && calcDaysInYear((int) t.getYear()) == 366)) {
                if (status != null) {
                    status.addWarning(FLAG_TIME_WARN_OUT_OF_RANGE);
                }
                return true;
            }
        } else if (timeNoZeroDate) {
            if (status != null) {
                status.addWarning(FLAG_TIME_WARN_ZERO_DATE);
            }
            return true;
        }
        return false;
    }

    public static boolean checkTimestampRange(MysqlDateTime t) {
        return t != null && t.getYear() <= TIMESTAMP_MAX_YEAR && t.getYear() >= TIMESTAMP_MIN_YEAR
            && (t.getYear() != TIMESTAMP_MAX_YEAR || (t.getMonth() <= 1 && t.getDay() <= 19))
            && (t.getYear() != TIMESTAMP_MIN_YEAR || (t.getMonth() >= 12 && t.getDay() >= 31));
    }

    public static MysqlDateTime fromZonedDatetime(ZonedDateTime zoned) {
        MysqlDateTime t = new MysqlDateTime();
        t.setYear(zoned.getYear());
        t.setMonth(zoned.getMonthValue());
        t.setDay(zoned.getDayOfMonth());
        t.setHour(zoned.getHour());
        t.setMinute(zoned.getMinute());
        t.setSecond(zoned.getSecond());
        t.setSecondPart(zoned.getNano());
        t.setSqlType(Types.TIMESTAMP);
        t.setNeg(false);
        return t;
    }

    public static ZonedDateTime toZonedDatetime(MysqlDateTime t, ZoneId zoneId) {
        if (isDatetimeRangeInvalid(t)) {
            return null;
        } else {
            try {
                return ZonedDateTime.of(
                    (int) t.getYear(),
                    (int) t.getMonth(),
                    (int) t.getDay(),
                    (int) t.getHour(),
                    (int) t.getMinute(),
                    (int) t.getSecond(),
                    (int) t.getSecondPart(),
                    zoneId
                );
            } catch (Throwable e) {
                return null;
            }
        }

    }
}

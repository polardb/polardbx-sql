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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.parser.TimeParseStatus;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;

import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags.*;


public class MySQLTimeConverter {

    protected static MySQLTimeVal convertDatetimeToTimestampInternal(MysqlDateTime t, TimeParseStatus status,
                                                                     ZoneId zoneId) {
        MySQLTimeVal timeVal = new MySQLTimeVal();

        if (t.getMonth() == 0) {
            if (t.getHour() != 0 || t.getMinute() != 0 || t.getSecond() != 0 || t.getSecondPart() != 0) {

                if (status != null) {
                    status.addWarning(FLAG_TIME_WARN_TRUNCATED);
                }
                return null;
            }
            timeVal.setSeconds(0L);
            timeVal.setNano(0L);
            return timeVal;
        }


        ZonedDateTime zonedDateTime;
        if (!MySQLTimeTypeUtil.checkTimestampRange(t)) {
            if (status != null) {
                status.addWarning(FLAG_TIME_WARN_OUT_OF_RANGE);
            }
            zonedDateTime = null;
        } else {
            try {
                zonedDateTime = ZonedDateTime.of(
                    (int) t.getYear(),
                    (int) t.getMonth(),
                    (int) t.getDay(),
                    (int) t.getHour(),
                    (int) t.getMinute(),
                    (int) t.getSecond(),
                    0,
                    zoneId
                );
            } catch (Throwable e) {
                zonedDateTime = null;
            }
        }

        long epochSec;
        if (zonedDateTime == null
            || (epochSec = zonedDateTime.toEpochSecond()) > 0x7FFFFFFFL || epochSec < 1L) {

            if (status != null) {
                status.addWarning(FLAG_TIME_WARN_OUT_OF_RANGE);
            }
            return null;
        }

        timeVal.setSeconds(epochSec);
        timeVal.setNano(t.getSecondPart());
        return timeVal;
    }

    public static MySQLTimeVal convertDatetimeToTimestamp(MysqlDateTime t, TimeParseStatus status, ZoneId zoneId) {

        boolean isNonZeroDate = t.getYear() != 0 || t.getMonth() != 0 || t.getDay() != 0;
        boolean isInvalid = MySQLTimeTypeUtil.isDateInvalid(t, isNonZeroDate, FLAG_TIME_NO_ZERO_IN_DATE);
        if (isInvalid) {

            return null;
        }
        return convertDatetimeToTimestampInternal(t, status, zoneId);
    }

    public static MySQLTimeVal convertDatetimeToTimestampWithoutCheck(MysqlDateTime t, TimeParseStatus status,
                                                                      ZoneId zoneId) {
        return convertDatetimeToTimestampInternal(t, status, zoneId);
    }

    public static MysqlDateTime convertTimestampToDatetime(MySQLTimeVal timeVal, ZoneId zoneId) {
        long second = timeVal.getSeconds();
        Instant instant = Instant.ofEpochSecond(second);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);

        MysqlDateTime mysqlDateTime = new MysqlDateTime();
        mysqlDateTime.setYear(zonedDateTime.getYear());
        mysqlDateTime.setMonth(zonedDateTime.getMonthValue());
        mysqlDateTime.setDay(zonedDateTime.getDayOfMonth());
        mysqlDateTime.setHour(zonedDateTime.getHour());
        mysqlDateTime.setMinute(zonedDateTime.getMinute());
        mysqlDateTime.setSecond(zonedDateTime.getSecond());

        mysqlDateTime.setSecondPart(timeVal.getNano());
        return mysqlDateTime;
    }

    public static Number convertTemporalToNumeric(MysqlDateTime t, int fromSqlType, int toSqlType) {
        if (t == null) {
            return null;
        }
        switch (fromSqlType) {

        case Types.TIME: {
            switch (toSqlType) {
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.BIGINT:
            case Types.TINYINT:
            case Types.NUMERIC:
            case Types.BOOLEAN:
                return timeToLongRound(t);
            default:
                return timeToDecimal(t);
            }
        }

        case Types.TIMESTAMP:
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE: {
            switch (toSqlType) {
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.BIGINT:
            case Types.TINYINT:
            case Types.NUMERIC:
            case Types.BOOLEAN:
                return datetimeToLongRound(t);
            default:
                return datetimeToDecimal(t);
            }
        }

        case Types.DATE: {
            switch (toSqlType) {
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.BIGINT:
            case Types.TINYINT:
            case Types.NUMERIC:
            case Types.BOOLEAN:
                return dateToLong(t);
            default:
                return dateToDecimal(t);
            }
        }

        default:
            return 0L;
        }
    }

    /**
     * From date to decimal
     */
    public static Decimal dateToDecimal(MysqlDateTime t) {
        long quot = dateToLong(t);
        int sign = t.isNeg() ? -1 : 1;
        return Decimal.fromLong(quot).multiply(Decimal.fromLong(sign));
    }

    /**
     * From datetime to decimal
     */
    public static Decimal datetimeToDecimal(MysqlDateTime t) {
        long quot = datetimeToLong(t);
        long rem = t.getSecondPart();
        int sign = t.isNeg() ? -1 : 1;
        Decimal result = Decimal.fromLong(quot)
            .add(Decimal.fromString(Double.valueOf(rem / (MySQLTimeTypeUtil.SEC_TO_NANO * 1.0D)).toString()));
        result = result.multiply(Decimal.fromLong(sign));
        return result;
    }

    /**
     * From time to decimal
     */
    public static Decimal timeToDecimal(MysqlDateTime t) {
        long quot = timeToLong(t);
        long rem = t.getSecondPart();
        int sign = t.isNeg() ? -1 : 1;
        Decimal result = Decimal.fromLong(quot)
            .add(Decimal.fromString(Double.valueOf(rem / (MySQLTimeTypeUtil.SEC_TO_NANO * 1.0D)).toString()));
        result = result.multiply(Decimal.fromLong(sign));
        return result;
    }

    /**
     * Convert time value to integer in YYYYMMDDHHMMSS.
     */
    public static Long datetimeToLong(MysqlDateTime t) {
        if (t == null) {
            return null;
        }
        return (t.getYear() * 10000L + t.getMonth() * 100L + t.getDay()) * 1000000L + t.getHour() * 10000L
            + t.getMinute() * 100L + t.getSecond();
    }

    public static Long dateToLong(MysqlDateTime t) {
        if (t == null) {
            return null;
        }
        return t.getYear() * 10000L + t.getMonth() * 100L + t.getDay();
    }

    public static Long timeToLong(MysqlDateTime t) {
        if (t == null) {
            return null;
        }
        return t.getHour() * 10000L + t.getMinute() * 100L + t.getSecond();
    }

    public static MysqlDateTime convertTemporalToTemporal(MysqlDateTime t, int fromSqlType, int toSqlType) {
        if (t == null) {
            return null;
        }
        switch (fromSqlType) {

        case Types.TIME: {
            switch (toSqlType) {
            case Types.TIMESTAMP:
            case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
                return timeToDatetime(t);
            case Types.DATE:
                MysqlDateTime datetime = timeToDatetime(t);
                return dateTimeToDate(datetime);
            default:
                return t;
            }
        }

        case Types.TIMESTAMP:
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE: {
            switch (toSqlType) {
            case Types.TIME:
                return datetimeToTime(t);
            case Types.DATE:
                return dateTimeToDate(t);
            default:
                return t;
            }
        }

        case Types.DATE: {
            switch (toSqlType) {
            case Types.TIMESTAMP:
            case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
                return dateToDatetime(t);
            case Types.TIME:
                return datetimeToTime(t);
            default:
                return t;
            }
        }

        default:
            return t;
        }
    }

    public static Long datetimeToLongRound(MysqlDateTime t) {
        if (t == null) {
            return null;
        }

        if (t.getSecondPart() < 500000L * 1000) {
            return datetimeToLong(t);
        } else if (t.getSecond() < 59L) {
            return datetimeToLong(t) + 1;
        }

        MysqlDateTime tmp = t.clone();
        MySQLTimeCalculator.roundDatetime(tmp, 0);
        long ret = datetimeToLong(tmp);
        return ret;
    }

    public static Long timeToLongRound(MysqlDateTime t) {
        if (t == null) {
            return null;
        }

        if (t.getSecondPart() < 500000L * 1000) {
            return datetimeToLong(t);
        } else if (t.getSecond() < 59L) {
            return datetimeToLong(t) + 1;
        }

        MysqlDateTime tmp = t.clone();
        MySQLTimeCalculator.roundTime(tmp, 0);
        long ret = timeToLong(tmp);
        return ret;
    }

    public static MysqlDateTime datetimeToTime(MysqlDateTime t) {
        if (t == null) {
            return null;
        }
        t.setYear(0);
        t.setMonth(0);
        t.setDay(0);
        t.setSqlType(Types.TIME);
        return t;
    }

    public static MysqlDateTime dateTimeToDate(MysqlDateTime t) {
        if (t == null) {
            return null;
        }
        t.setHour(0);
        t.setMinute(0);
        t.setSecond(0);
        t.setSecondPart(0);
        t.setSqlType(Types.DATE);
        return t;
    }

    public static MysqlDateTime dateToDatetime(MysqlDateTime t) {
        t.setSqlType(Types.TIMESTAMP);
        return t;
    }

    public static MysqlDateTime timeToDatetime(MysqlDateTime t) {

        ZonedDateTime now = ZonedDateTime.now();
        MysqlDateTime nowDate = new MysqlDateTime();

        nowDate.setYear(now.getYear());
        nowDate.setMonth(now.getMonthValue());
        nowDate.setDay(now.getDayOfMonth());

        if (!t.isNeg() && t.getHour() < 24) {

            nowDate.setHour(t.getHour());
            nowDate.setMinute(t.getMinute());
            nowDate.setSecond(t.getSecond());
            nowDate.setSecondPart(t.getSecondPart());
        } else {

            int sign = t.isNeg() ? 1 : -1;
            MysqlDateTime diff = MySQLTimeCalculator.calTimeDiff(nowDate, t, !t.isNeg());
            nowDate.setNeg(diff.isNeg());

            long days = diff.getSecond() / (24 * 3600L);
            long seconds = diff.getSecond() % 86400L;

            nowDate.setYear(0);
            nowDate.setMonth(0);
            nowDate.setDay(0);
            nowDate.setHour(seconds / 3600L);
            long tSec = seconds % 3600L;
            nowDate.setMinute(tSec / 60L);
            nowDate.setSecond(tSec % 60L);
            nowDate.setSecondPart(diff.getSecondPart());

            MySQLTimeCalculator.getDateFromDayNumber(days, nowDate);
        }
        nowDate.setSqlType(Types.TIMESTAMP);
        return nowDate;
    }

    public static MysqlDateTime secToTime(long seconds, long nano) {
        return secToTime(seconds, nano, true);
    }

    public static MysqlDateTime secToTime(long seconds, long nano, boolean convertWarningToNull) {
        MysqlDateTime t = new MysqlDateTime();
        t.setSqlType(Types.TIME);

        if (seconds < 0 || nano < 0) {
            t.setNeg(true);
            seconds = -seconds;
            nano = -nano;
        }

        if (seconds > MySQLTimeTypeUtil.TIME_MAX_HOUR * 3600L + 59 * 60L + 59) {
            t.setHour(MySQLTimeTypeUtil.TIME_MAX_HOUR);
            t.setMinute(59);
            t.setSecond(59);

            return convertWarningToNull ? null : t;
        }

        t.setHour(seconds / 3600);
        long sec = seconds % 3600;
        t.setMinute(sec / 60);
        t.setSecond(sec % 60);

        MySQLTimeCalculator.datetimeAddNanoWithRound(t, (int) nano);

        if (t != null && !MySQLTimeTypeUtil.checkTimeRangeQuick(t)) {
            t.setDay(0);
            t.setSecondPart(0);
            t.setHour(MySQLTimeTypeUtil.TIME_MAX_HOUR);
            t.setMinute(59);
            t.setSecond(59);
            t.setSecondPart(0);

            return convertWarningToNull ? null : t;
        }
        return t;
    }

    public static long secondSinceEpoch(int year, int month, int day, int hour, int minute, int second) {

        long dayNumber = year * 365
            - 1970 * 365
            + MySQLTimeTypeUtil.getLeaps(year - 1)
            - MySQLTimeTypeUtil.getLeaps(1970 - 1);
        boolean isLeapYear = MySQLTimeTypeUtil.isLeapYear(year);
        dayNumber += MySQLTimeTypeUtil.MONTH_STARTS[isLeapYear ? 1 : 0][month - 1];
        dayNumber += (day - 1);

        return ((dayNumber * 24 + hour) * 60 + minute) * 60 + second;
    }

    public static long periodToMonth(long period) {
        if (period == 0) {
            return 0L;
        }
        long a = period / 100L;
        if (a < 70) {

            a += 2000;
        } else if (a < 100) {

            a += 1900;
        }
        long b = period % 100;
        return a * 12 + b - 1;
    }

    public static long monthToPeriod(long month) {
        if (month == 0L) {
            return 0L;
        }
        long year = month / 12;
        if (year < 100) {
            year += (year < 70 ? 2000 : 1900);
        }
        return year * 100 + month % 12 + 1;
    }

    public static int weekMode(int mode) {
        int week_format = (mode & 7);
        if ((week_format & FLAG_WEEK_MONDAY_FIRST) == 0) {
            week_format ^= FLAG_WEEK_FIRST_WEEKDAY;
        }
        return week_format;
    }

    public static long[] datetimeToWeek(MysqlDateTime t, int formatFlags) {
        long[] ret = new long[2];
        long dayNumber = MySQLTimeCalculator.calDayNumber(
            t.getYear(),
            t.getMonth(),
            t.getDay());
        long firstDayNumber = MySQLTimeCalculator.calDayNumber(t.getYear(), 1, 1);
        boolean mondayFirst = TimeParserFlags.check(formatFlags, FLAG_WEEK_MONDAY_FIRST);
        boolean weekYear = TimeParserFlags.check(formatFlags, FLAG_WEEK_YEAR);
        boolean firstWeekDay = TimeParserFlags.check(formatFlags, FLAG_WEEK_FIRST_WEEKDAY);

        long weekDay = Integer.toUnsignedLong((int) ((firstDayNumber + 5 + (!mondayFirst ? 1L : 0L)) % 7));

        long year = t.getYear();

        long days;

        if (t.getMonth() == 1L
            && t.getDay() <= (7 - weekDay)) {
            if (!weekYear
                && ((firstWeekDay && weekDay != 0)
                || (!firstWeekDay && weekDay >= 4))) {
                ret[0] = 0;
                ret[1] = year;
                return ret;
            }
            weekYear = true;
            year--;
            days = Integer.toUnsignedLong(MySQLTimeCalculator.calDaysInYears(year));
            firstDayNumber -= days;
            weekDay = Integer.toUnsignedLong((int) ((weekDay + 53 * 7 - days) % 7));
        }

        if ((firstWeekDay && weekDay != 0) || (!firstWeekDay && weekDay >= 4)) {
            days = Integer.toUnsignedLong((int) (dayNumber - (firstDayNumber + 7 - weekDay)));
        } else {
            days = Integer.toUnsignedLong((int) (dayNumber - (firstDayNumber - weekDay)));
        }

        if (weekYear && days >= 52 * 7) {
            weekDay = (weekDay + MySQLTimeCalculator.calDaysInYears(year)) % 7;
            if ((!firstWeekDay && weekDay < 4) || (firstWeekDay && weekDay == 0)) {
                year++;
                ret[0] = 1;
                ret[1] = year;
                return ret;
            }
        }
        long week = days / 7 + 1;
        ret[0] = week;
        ret[1] = year;
        return ret;
    }
}

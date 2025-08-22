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

package com.alibaba.polardbx.common.utils.time.calculator;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;

import java.sql.Types;

public class MySQLTimeCalculator {
    public static int[] NANO_ROUND_ADD = new int[] {
        500000000,
        50000000,
        5000000,
        500000,
        50000,
        5000,
        0
    };

    public static boolean needToRound(int secondPart, int decimal) {
        int add = NANO_ROUND_ADD[decimal];
        int scale = (add == 0 ? 1000 : add * 2);

        long nano = secondPart + add;
        nano = (nano / scale) * scale;
        return nano >= 1000_000_000L || nano != secondPart;
    }

    public static int getScale(int nano) {
        Preconditions.checkArgument(nano > 0);
        int scale = 1000;
        while (scale <= 1000_000_000) {
            if (nano / scale == 0) {
                return scale;
            }
            scale *= 10;
        }
        return 1000_000_000;
    }

    public static MysqlDateTime roundDatetime(MysqlDateTime t, int decimal) {
        int nano = NANO_ROUND_ADD[decimal];
        t = datetimeAddNanoWithRound(t, nano);
        return timeTruncate(t, decimal);
    }

    public static MysqlDateTime roundTime(MysqlDateTime t, int decimal) {
        int nano = NANO_ROUND_ADD[decimal];
        t = timeAddNanoWithRound(t, nano);
        return timeTruncate(t, decimal);
    }

    public static MysqlDateTime timeTruncate(MysqlDateTime t, int decimals) {
        if (t == null) {
            return null;
        }
        long micro0 =
            (t.getSecondPart() / 1000) % StringTimeParser.LOG_10[MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE - decimals];
        long micro = t.getSecondPart() / 1000 - micro0;
        t.setSecondPart(micro * 1000);
        return t;
    }

    public static MysqlDateTime datetimeAddNanoWithRound(MysqlDateTime t, int nano) {

        if (nano < 500) {
            return t;
        }

        long micro = t.getSecondPart() / 1000 + (nano + 500) / 1000;
        if (micro < MySQLTimeTypeUtil.SEC_TO_MICRO) {
            t.setSecondPart(micro * 1000);
            return t;
        }

        t.setSecondPart((micro % MySQLTimeTypeUtil.SEC_TO_MICRO) * 1000);


        boolean notZeroDate = MySQLTimeTypeUtil.notZeroDate(t);
        boolean invalid = MySQLTimeTypeUtil.isDateInvalid(t,
            notZeroDate,
            true,
            StringTimeParser.TIME_FUZZY_DATE,
            StringTimeParser.TIME_INVALID_DATES,
            true
        );
        if (invalid) {
            return null;
        }

        MySQLInterval interval = new MySQLInterval();
        interval.setSecond(1);
        return addInterval(t, MySQLIntervalType.INTERVAL_SECOND, interval);
    }

    public static MysqlDateTime timeAddNanoWithRound(MysqlDateTime t, int nano) {

        if (nano < 500) {
            return t;
        }

        long micro = t.getSecondPart() / 1000 + (nano + 500) / 1000;
        if (micro < MySQLTimeTypeUtil.SEC_TO_MICRO) {
            t.setSecondPart(micro * 1000);
            return t;
        }

        t.setSecondPart((micro % MySQLTimeTypeUtil.SEC_TO_MICRO) * 1000);

        long hour = t.getHour();
        long minute = t.getMinute();
        long second = t.getSecond();

        if (second < 59) {
            t.setSecond(++second);
            return t;
        }

        t.setSecond(0);
        if (minute < 59) {
            t.setMinute(++minute);
            return t;
        }

        t.setMinute(0);
        t.setHour(++hour);
        return t;
    }

    public static MysqlDateTime roundWithoutAddInterval(MysqlDateTime mysqlDateTime, int decimal) {
        long year = mysqlDateTime.getYear();
        long month = mysqlDateTime.getMonth();
        long day = mysqlDateTime.getDay();
        long hour = mysqlDateTime.getHour();
        long minute = mysqlDateTime.getMinute();
        long second = mysqlDateTime.getSecond();
        long secondPart = mysqlDateTime.getSecondPart();

        MysqlDateTime ret = new MysqlDateTime(year, month, day, hour, minute, second, secondPart);

        int add = NANO_ROUND_ADD[decimal];
        int scale = add * 2;

        long nano = secondPart + add;
        nano = (nano / scale) * scale;

        if (nano < 1000_000_000L) {
            ret.setSecondPart(nano);
            return ret;
        }


        if (StringTimeParser.TIME_NO_ZERO_IN_DATE || !StringTimeParser.TIME_FUZZY_DATE) {
            if (month == 0 || day == 0) {
                return null;
            }
        }

        ret.setSecondPart(0);
        if (second < 59) {
            ret.setSecond(++second);
            return ret;
        }

        ret.setSecond(0);
        if (minute < 59) {
            ret.setMinute(++minute);
            return ret;
        }

        ret.setMinute(0);
        if (hour < 23) {
            ret.setHour(++hour);
            return ret;
        }

        ret.setHour(0);
        if (day <
            (MySQLTimeTypeUtil.calcDaysInYear((int) year) == 366 && month - 1 == 2
                ? 29
                : MySQLTimeTypeUtil.DAYS_IN_MONTH[(int) (month - 1)])) {
            ret.setDay(++day);
            return ret;
        }

        ret.setDay(1);
        if (month < 12) {
            ret.setMonth(++month);
            return ret;
        }

        ret.setMonth(1);
        if (year < 9999) {
            ret.setYear(++year);
            return ret;
        }

        GeneralUtil.nestedException("invalid datetime to round: " + mysqlDateTime.toString());
        return null;
    }

    public static MysqlDateTime addInterval(MysqlDateTime src, MySQLIntervalType intervalType,
                                            MySQLInterval interval) {
        switch (intervalType) {
        case INTERVAL_SECOND:
        case INTERVAL_SECOND_MICROSECOND:
        case INTERVAL_MICROSECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_HOUR:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_DAY_MICROSECOND:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_HOUR: {
            return handleTimeInterval(src, interval);
        }
        case INTERVAL_DAY:
        case INTERVAL_WEEK: {
            return handleDayAndWeekInterval(src, interval);
        }
        case INTERVAL_YEAR: {
            return handleYearInterval(src, interval);
        }
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_QUARTER:
        case INTERVAL_MONTH: {
            return handleMonthInterval(src, interval);
        }
        default:
            return null;
        }
    }

    private static MysqlDateTime handleTimeInterval(MysqlDateTime src, MySQLInterval interval) {
        long sign = interval.isNeg() ? -1 : 1;
        MysqlDateTime ret = new MysqlDateTime();

        ret.setSqlType(Types.TIMESTAMP);

        long nano = src.getSecondPart() + sign * interval.getSecondPart();
        long extraSec = nano / MySQLTimeTypeUtil.SEC_TO_NANO;
        nano = nano % MySQLTimeTypeUtil.SEC_TO_NANO;

        long srcSec = (src.getDay() - 1) * 3600 * 24
            + src.getHour() * 3600
            + src.getMinute() * 60
            + src.getSecond();
        long intervalSec = interval.getDay() * 3600 * 24
            + interval.getHour() * 3600
            + interval.getMinute() * 60
            + interval.getSecond();

        intervalSec *= sign;
        long sec = srcSec + intervalSec + extraSec;
        if (nano < 0) {
            nano += MySQLTimeTypeUtil.SEC_TO_NANO;
            sec--;
        }

        long day = sec / (3600 * 24);
        sec -= day * 3600 * 24;
        if (sec < 0) {
            day--;
            sec += 3600 * 24;
        }

        ret.setSecondPart(Math.abs(nano));
        ret.setSecond(Math.abs(sec % 60));
        ret.setMinute(Math.abs((sec / 60) % 60));
        ret.setHour(Math.abs(sec / 3600));

        long dayNumber = calDayNumber(src.getYear(), src.getMonth(), 1) + day;
        if (dayNumber < 0 || dayNumber > MySQLTimeTypeUtil.MAX_DAY_NUMBER) {
            return null;
        }
        getDateFromDayNumber(dayNumber, ret);
        return ret;
    }

    private static MysqlDateTime handleDayAndWeekInterval(MysqlDateTime src, MySQLInterval interval) {
        MysqlDateTime ret = new MysqlDateTime();

        ret.setSqlType(src.getSqlType());

        int sign = interval.isNeg() ? -1 : 1;
        long period =
            calDayNumber(src.getYear(), src.getMonth(), src.getDay()) + sign * interval.getDay();
        if (period < 0 || period > MySQLTimeTypeUtil.MAX_DAY_NUMBER) {
            return null;
        }
        getDateFromDayNumber(period, ret);

        ret.setSecondPart(src.getSecondPart());
        ret.setSecond(src.getSecond());
        ret.setMinute(src.getMinute());
        ret.setHour(src.getHour());
        return ret;
    }

    private static MysqlDateTime handleYearInterval(MysqlDateTime src, MySQLInterval interval) {
        MysqlDateTime ret = new MysqlDateTime();

        ret.setSqlType(src.getSqlType());

        int sign = interval.isNeg() ? -1 : 1;
        long year = src.getYear() + sign * interval.getYear();
        long month = src.getMonth();
        long day = src.getDay();

        if (year < 0 || year > 10000L) {
            return null;
        }
        if (month > 0
            && day > MySQLTimeTypeUtil.DAYS_IN_MONTH[(int) (month - 1)]
            && !(month == 2 && day == 29 && MySQLTimeTypeUtil.calcDaysInYear((int) year) == 366)) {
            day = 28;
        }
        ret.setYear(year);
        ret.setMonth(month);
        ret.setDay(day);

        ret.setSecondPart(src.getSecondPart());
        ret.setSecond(src.getSecond());
        ret.setMinute(src.getMinute());
        ret.setHour(src.getHour());

        return ret;
    }

    private static final MysqlDateTime handleMonthInterval(MysqlDateTime src, MySQLInterval interval) {
        MysqlDateTime ret = new MysqlDateTime();

        ret.setSqlType(src.getSqlType());

        int sign = interval.isNeg() ? -1 : 1;
        long period = src.getYear() * 12 + sign * interval.getYear() * 12
            + src.getMonth() - 1 + sign * interval.getMonth();
        if (period < 0 || period > 120000L) {
            return null;
        }
        long year = Math.abs(period / 12);
        long month = Math.abs(period % 12) + 1;
        long day = src.getDay();

        if (day > MySQLTimeTypeUtil.DAYS_IN_MONTH[(int) (month - 1)]) {
            day = MySQLTimeTypeUtil.DAYS_IN_MONTH[(int) (month - 1)];
            if (month == 2 && MySQLTimeTypeUtil.calcDaysInYear((int) year) == 366) {
                day++;
            }
        }
        ret.setYear(year);
        ret.setMonth(month);
        ret.setDay(day);

        ret.setSecondPart(src.getSecondPart());
        ret.setSecond(src.getSecond());
        ret.setMinute(src.getMinute());
        ret.setHour(src.getHour());

        return ret;
    }

    public static long calDayNumber(long year, long month, long day) {
        if (year == 0 && month == 0) {
            return 0;
        }
        long sum = 365 * year + 31 * (month - 1) + day;
        if (month <= 2) {
            year--;
        } else {
            sum -= (month * 4 + 23) / 10;
        }
        return sum + year / 4 - ((year / 100 + 1) * 3 / 4);
    }

    public static void getDateFromDayNumber(long dayNumber, MysqlDateTime target) {
        if (dayNumber <= 365 || dayNumber > 3652500) {
            target.setYear(0);
            target.setMonth(0);
            target.setDay(0);
        } else {

            long year = Math.abs(dayNumber * 100 / 36525L);
            long dayOfYear = Math.abs(dayNumber - year * 365 - (year - 1) / 4 + (((year - 1) / 100 + 1) * 3) / 4);
            int daysInYear = 0;
            while (dayOfYear > (daysInYear = MySQLTimeTypeUtil.calcDaysInYear((int) year))) {
                dayOfYear -= daysInYear;
                year++;
            }

            int leapDay = 0;
            if (daysInYear == 366) {
                if (dayOfYear > 31 + 28) {
                    dayOfYear--;
                    if (dayOfYear == 31 + 28) {
                        leapDay = 1;
                    }
                }
            }

            long month = 1;
            int monthIdx = 0;
            while (dayOfYear > MySQLTimeTypeUtil.DAYS_IN_MONTH[monthIdx]) {
                dayOfYear -= MySQLTimeTypeUtil.DAYS_IN_MONTH[monthIdx];
                monthIdx++;
                month++;
            }

            target.setYear(year);
            target.setMonth(month);
            target.setDay(dayOfYear + leapDay);
        }
    }

    public static MysqlDateTime calTimeDiff(MysqlDateTime t1, MysqlDateTime t2, boolean isNeg) {
        MysqlDateTime ret = new MysqlDateTime();
        long days;
        int sign = isNeg ? -1 : 1;

        if (t1.getSqlType() == Types.TIME) {
            days = t1.getDay() - t2.getDay() * sign;
        } else {
            days = calDayNumber(t1.getYear(), t1.getMonth(), t1.getDay());
            if (t2.getSqlType() == Types.TIME) {
                days -= sign * t2.getDay();
            } else {
                days -= sign * calDayNumber(t2.getYear(), t2.getMonth(), t2.getDay());
            }
        }

        long microSec = (days * 24 * 60 * 60
            + (t1.getHour() * 3600 + t1.getMinute() * 60 + t1.getSecond())
            - (t2.getHour() * 3600 + t2.getMinute() * 60 + t2.getSecond()) * sign) * MySQLTimeTypeUtil.SEC_TO_MICRO
            + t1.getSecondPart() / 1000 - (t2.getSecondPart() / 1000) * sign;

        if (microSec < 0) {
            microSec = -microSec;
            ret.setNeg(true);
        }
        ret.setSecond(microSec / MySQLTimeTypeUtil.SEC_TO_MICRO);
        ret.setSecondPart((microSec % MySQLTimeTypeUtil.SEC_TO_MICRO) * 1000);
        return ret;
    }

    public static int calDaysInYears(long year) {
        return (year & 3) == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0))
            ? 366 : 365;
    }

    public static int calWeekDay(long dayNumber, boolean isSundayFirstDayOfWeek) {
        return (int) ((dayNumber + 5L + (isSundayFirstDayOfWeek ? 1L : 0L)) % 7);
    }

    /**
     * Cacl the day of the year for date,  in the range 1 to 366.
     *
     * @return Returns 0 if date is invalid
     */
    public static long calDayOfYear(long year, long month, long day) {
        if (year == 0) {
            return calDayNumber(0L, month, day);
        } else {
            return calDayNumber(year, month, day) - calDayNumber(year - 1, 12, 31);
        }
    }

    /**
     * Calculate the weekday index for date
     *
     * @return 1 = Sunday, 2 = Monday..., 7 = Saturday
     */
    public static long calDayOfWeek(long year, long month, long day) {
        long dayNumber = calDayNumber(year, month, day);
        return calWeekDay(dayNumber, true) + 1;
    }

    /**
     * Cacl the weekIdx of the year, in the range 1 to 53.
     * This function comply with mysql, which means Monday is the first day of week,
     * and week 1 is the first week of the year only if it contains 4 or more days.
     * reference: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
     *
     * @return the week of this year in the range 1 to 53.
     */
    public static long calWeekOfYear(long year, long month, long day) {
        long dayNumberOfFirstWeek = calDayNumberOfFirstWeekDayOfYear(year);
        long dayNumber = calDayNumber(year, month, day);
        if (dayNumber >= dayNumberOfFirstWeek) {
            return (int) ((dayNumber - dayNumberOfFirstWeek) / 7 + 1);
        } else {
            //today belongs to last year's week
            long dayNumberOfFirstWeekLastYear = calDayNumberOfFirstWeekDayOfYear(year - 1);
            return (int) ((dayNumber - dayNumberOfFirstWeekLastYear) / 7 + 1);
        }
    }

    /**
     * Cacl the day number of the first week's first day
     * This function comply with mysql, which means Monday is the first day of week,
     * and week 1 is the first week of the year only if it contains 4 or more days.
     */
    private static long calDayNumberOfFirstWeekDayOfYear(long year) {
        long dayNumberOfNewYearDay = calDayNumber(year, 1L, 1L);
        long weekDayOfNewYearDay = calWeekDay(dayNumberOfNewYearDay, false) + 1;
        if (weekDayOfNewYearDay <= 4) {
            return dayNumberOfNewYearDay - weekDayOfNewYearDay + 1;
        } else {
            return dayNumberOfNewYearDay + (7 - weekDayOfNewYearDay + 1);
        }
    }

    /**
     * Cacl the weeks from year 0.
     */
    public static long calToWeeks(long year, long month, long day) {
        long dayNumber = calDayNumber(year, month, day);
        return (dayNumber - 1) / 7;
    }

    /**
     * Cacl the months from year 0.
     */
    public static long calToMonths(long year, long month, long day) {
        return year * 12 + month;
    }

}

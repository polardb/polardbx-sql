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

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;

import java.sql.Types;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class PartitionFunctionTimeCaculator extends MySQLTimeCalculator {
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

    /**
     * Change a Day number to year, month and day
     * This function is a little different with MySQLTimeCalculator.getDateFromDayNumber
     *
     * @param dayNumber must be long value.
     * @param target datetime to modify.
     */
    public static void getDateFromDayNumber(long dayNumber, MysqlDateTime target) {
        if (dayNumber > 3652500) {
            target.setYear(0);
            target.setMonth(0);
            target.setDay(0);
        } else {
            // handle year and day in year
            long year = Math.abs(dayNumber * 100 / 36525L);
            long dayOfYear = Math.abs(dayNumber - year * 365 - (year - 1) / 4 + (((year - 1) / 100 + 1) * 3) / 4);
            int daysInYear = 0;
            while (dayOfYear > (daysInYear = MySQLTimeTypeUtil.calcDaysInYear((int) year))) {
                dayOfYear -= daysInYear;
                year++;
            }

            // handle leap day
            int leapDay = 0;
            if (daysInYear == 366) {
                if (dayOfYear > 31 + 28) {
                    dayOfYear--;
                    if (dayOfYear == 31 + 28) {
                        leapDay = 1;
                    }
                }
            }

            // handle month
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

    protected static MysqlDateTime handleTimeInterval(MysqlDateTime src, MySQLInterval interval) {
        long sign = interval.isNeg() ? -1 : 1;
        MysqlDateTime ret = new MysqlDateTime();
        // must be timestamp type
        ret.setSqlType(Types.TIMESTAMP);

        // for nano
        long nano = src.getSecondPart() + sign * interval.getSecondPart();
        long extraSec = nano / MySQLTimeTypeUtil.SEC_TO_NANO;
        nano = nano % MySQLTimeTypeUtil.SEC_TO_NANO;

        // for second
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

        // for day
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

        // check validity
        long dayNumber = calDayNumber(src.getYear(), src.getMonth(), 1) + day;
        if (dayNumber < 0 || dayNumber > MySQLTimeTypeUtil.MAX_DAY_NUMBER) {
            return null;
        }
        getDateFromDayNumber(dayNumber, ret);
        return ret;
    }

    protected static MysqlDateTime handleDayAndWeekInterval(MysqlDateTime src, MySQLInterval interval) {
        MysqlDateTime ret = new MysqlDateTime();
        // according to source datetime sql type
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

    protected static MysqlDateTime handleYearInterval(MysqlDateTime src, MySQLInterval interval) {
        MysqlDateTime ret = new MysqlDateTime();
        // according to source datetime sql type
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

    protected static MysqlDateTime handleMonthInterval(MysqlDateTime src, MySQLInterval interval) {
        MysqlDateTime ret = new MysqlDateTime();
        // according to source datetime sql type
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
        // adjust day if the new month doesn't have enough days
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

}

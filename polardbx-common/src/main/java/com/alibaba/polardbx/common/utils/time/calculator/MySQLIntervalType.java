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

import com.alibaba.polardbx.common.datatype.DivStructure;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.utils.time.parser.MySQLTimeParserBase;

import java.util.Arrays;

public enum MySQLIntervalType {
    INTERVAL_YEAR("YEAR", 0),
    INTERVAL_QUARTER("QUARTER", 1),
    INTERVAL_MONTH("MONTH", 2),
    INTERVAL_WEEK("WEEK", 3),
    INTERVAL_DAY("DAY", 4),
    INTERVAL_HOUR("HOUR", 5),
    INTERVAL_MINUTE("MINUTE", 6),
    INTERVAL_SECOND("SECOND", 7),
    INTERVAL_MICROSECOND("MICROSECOND", 8),
    INTERVAL_YEAR_MONTH("YEAR_MONTH", 9),
    INTERVAL_DAY_HOUR("DAY_HOUR", 10),
    INTERVAL_DAY_MINUTE("DAY_MINUTE", 11),
    INTERVAL_DAY_SECOND("DAY_SECOND", 12),
    INTERVAL_HOUR_MINUTE("HOUR_MINUTE", 13),
    INTERVAL_HOUR_SECOND("HOUR_SECOND", 14),
    INTERVAL_MINUTE_SECOND("MINUTE_SECOND", 15),
    INTERVAL_DAY_MICROSECOND("DAY_MICROSECOND", 16),
    INTERVAL_HOUR_MICROSECOND("HOUR_MICROSECOND", 17),
    INTERVAL_MINUTE_MICROSECOND("MINUTE_MICROSECOND", 18),
    INTERVAL_SECOND_MICROSECOND("SECOND_MICROSECOND", 19),
    INTERVAL_LAST("LAST", 20);

    final String name;
    final int id;

    MySQLIntervalType(String name, int id) {
        this.name = name;
        this.id = id;
    }

    public static MySQLIntervalType of(String intervalName) {
        return Arrays.stream(values())
            .filter(v -> v.name.equalsIgnoreCase(normalize(intervalName)))
            .findFirst()
            .orElse(null);
    }

    public static boolean isDate(MySQLIntervalType intervalType) {
        switch (intervalType) {
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_QUARTER:
        case INTERVAL_MONTH:
        case INTERVAL_WEEK:
        case INTERVAL_DAY:
            return true;
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_SECOND:
        case INTERVAL_MICROSECOND:
        case INTERVAL_DAY_MICROSECOND:
        case INTERVAL_HOUR_MICROSECOND:
        case INTERVAL_MINUTE_MICROSECOND:
        case INTERVAL_SECOND_MICROSECOND:
        default:
            return false;
        }
    }

    private static String normalize(String intervalName) {
        if (intervalName == null) {
            return null;
        }
        if (intervalName.startsWith("INTERVAL_")
            || intervalName.startsWith("interval_")) {
            return intervalName.substring(9);
        }
        return intervalName;
    }

    public String getName() {
        return name;
    }

    public static MySQLInterval parseInterval(String value, MySQLIntervalType intervalType) {
        if (value == null) {
            return null;
        }
        MySQLInterval interval = new MySQLInterval();
        int intValue = 0;

        if (intervalType == INTERVAL_SECOND && value.contains(".")) {
            Decimal decimalValue = Decimal.fromString(value);
            DivStructure divStructure = DivStructure.fromDecimal(decimalValue);
            long[] div = divStructure.getDiv();

            if (div == null) {
                return null;
            }
            interval.setZero(div[0] == 0 && div[1] == 0);

            if (div[0] >= 0 && div[1] >= 0) {
                interval.setNeg(false);
                interval.setSecond((int) div[0]);
                interval.setSecondPart((int) div[1]);
            } else {
                interval.setNeg(true);
                interval.setSecond(-(int) div[0]);
                interval.setSecondPart(-(int) div[1]);
            }
            return interval;
        } else if (intervalType.id <= INTERVAL_MICROSECOND.id) {
            try {
                intValue = Integer.valueOf(value);
                if (intValue < 0) {
                    interval.setNeg(true);
                    intValue = -intValue;
                }
            } catch (Throwable t) {
                throwError(value, intervalType, t);
            }
        }

        long[] data = null;
        if (intervalType.id > INTERVAL_MICROSECOND.id) {
            data = new long[5];
        }
        boolean isNeg;

        switch (intervalType) {
        case INTERVAL_YEAR:
            interval.setYear(intValue);
            break;
        case INTERVAL_QUARTER:
            interval.setMonth(intValue * 3);
            break;
        case INTERVAL_MONTH:
            interval.setMonth(intValue);
            break;
        case INTERVAL_WEEK:
            interval.setDay(intValue * 7);
            break;
        case INTERVAL_DAY:
            interval.setDay(intValue);
            break;
        case INTERVAL_HOUR:
            interval.setHour(intValue);
            break;
        case INTERVAL_MINUTE:
            interval.setMinute(intValue);
            break;
        case INTERVAL_SECOND:
            interval.setSecond(intValue);
            break;
        case INTERVAL_MICROSECOND:
            interval.setSecondPart(intValue * 1000);
            break;
        case INTERVAL_YEAR_MONTH:

            isNeg = getIntervalData(value, data, 2, false, intervalType);
            interval.setNeg(isNeg);
            interval.setYear(data[0]);
            interval.setMonth(data[1]);
            break;
        case INTERVAL_DAY_HOUR:
            isNeg = getIntervalData(value, data, 2, false, intervalType);
            interval.setNeg(isNeg);
            interval.setDay(data[0]);
            interval.setHour(data[1]);
            break;
        case INTERVAL_DAY_MINUTE:
            isNeg = getIntervalData(value, data, 3, false, intervalType);
            interval.setNeg(isNeg);
            interval.setDay(data[0]);
            interval.setHour(data[1]);
            interval.setMinute(data[2]);
            break;
        case INTERVAL_DAY_SECOND:
            isNeg = getIntervalData(value, data, 4, false, intervalType);
            interval.setNeg(isNeg);
            interval.setDay(data[0]);
            interval.setHour(data[1]);
            interval.setMinute(data[2]);
            interval.setSecond(data[3]);
            break;
        case INTERVAL_HOUR_MINUTE:
            isNeg = getIntervalData(value, data, 2, false, intervalType);
            interval.setNeg(isNeg);
            interval.setHour(data[0]);
            interval.setMinute(data[1]);
            break;
        case INTERVAL_HOUR_SECOND:
            isNeg = getIntervalData(value, data, 3, false, intervalType);
            interval.setNeg(isNeg);
            interval.setHour(data[0]);
            interval.setMinute(data[1]);
            interval.setSecond(data[2]);
            break;
        case INTERVAL_MINUTE_SECOND:
            isNeg = getIntervalData(value, data, 2, false, intervalType);
            interval.setNeg(isNeg);
            interval.setMinute(data[0]);
            interval.setSecond(data[1]);
            break;
        case INTERVAL_DAY_MICROSECOND:
            isNeg = getIntervalData(value, data, 5, true, intervalType);
            interval.setNeg(isNeg);
            interval.setDay(data[0]);
            interval.setHour(data[1]);
            interval.setMinute(data[2]);
            interval.setSecond(data[3]);
            interval.setSecondPart(data[4]);
            break;
        case INTERVAL_HOUR_MICROSECOND:
            isNeg = getIntervalData(value, data, 4, true, intervalType);
            interval.setNeg(isNeg);
            interval.setHour(data[0]);
            interval.setMinute(data[1]);
            interval.setSecond(data[2]);
            interval.setSecondPart(data[3]);
            break;
        case INTERVAL_MINUTE_MICROSECOND:
            isNeg = getIntervalData(value, data, 3, true, intervalType);
            interval.setNeg(isNeg);
            interval.setMinute(data[0]);
            interval.setSecond(data[1]);
            interval.setSecondPart(data[2]);
            break;
        case INTERVAL_SECOND_MICROSECOND:
            isNeg = getIntervalData(value, data, 2, true, intervalType);
            interval.setNeg(isNeg);
            interval.setSecond(data[0]);
            interval.setSecondPart(data[1]);
            break;
        case INTERVAL_LAST:
            break;
        default:
            GeneralUtil.nestedException("Unsupported time unit: " + intervalType);
        }
        return interval;
    }

    private static boolean getIntervalData(String value, long[] data, int arraySize, boolean hasSecondPart,
                                           MySQLIntervalType intervalType) {
        Preconditions.checkNotNull(value);
        boolean isNeg = false;
        int pos = 0;
        final int len = value.length();

        while (pos < len && value.charAt(pos) == ' ') {
            pos++;
        }

        if (pos < len && value.charAt(pos) == '-') {
            isNeg = true;
            pos++;
        }

        while (pos < len && !isDigit(value.charAt(pos))) {
            pos++;
        }

        int microLen = 0;
        for (int i = 0; i < arraySize; i++) {

            long currentVal;
            int start = pos;
            for (currentVal = 0; pos < len && isDigit(value.charAt(pos)); pos++) {
                currentVal = currentVal * 10 + (value.charAt(pos) - '0');
            }

            microLen = 6 - (pos - start);
            data[i] = currentVal;

            while (pos < len && !isDigit(value.charAt(pos))) {
                pos++;
            }

            if (pos == len && i != arraySize - 1) {
                i++;

                for (int j = arraySize - 1; j >= arraySize - i; j--) {
                    data[j] = data[j - (arraySize - i)];
                }
                for (int j = 0; j < arraySize - i; j++) {
                    data[j] = 0;
                }
                break;
            }
        }

        if (hasSecondPart && microLen >= 0) {
            data[arraySize - 1] *= MySQLTimeParserBase.LOG_10[microLen];

            data[arraySize - 1] *= 1000L;
        }

        if (pos != len) {
            GeneralUtil.nestedException("error value: [" + value + "] for " + intervalType.toString());
        }

        return isNeg;
    }

    private static boolean isDigit(char ch) {
        return ch >= '0' && ch <= '9';
    }

    private static void throwError(String value, MySQLIntervalType intervalType, Throwable t) {
        GeneralUtil.nestedException("error value: [" + value + "] for " + intervalType.toString(), t);
    }
}

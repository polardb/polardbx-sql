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

package com.alibaba.polardbx.common.utils.time.parser;

import com.alibaba.polardbx.common.datatype.DivStructure;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.datatype.Decimal;

import java.sql.Types;


public class NumericTimeParser extends MySQLTimeParserBase {
    public static long DIV_MIN = -1000000000000000000L;
    public static long DIV_MAX = 1000000000000000000L;
    public static long TIME_MAX_VALUE = MySQLTimeTypeUtil.TIME_MAX_HOUR * 10000 + 59 * 100 + 59;

    public static MysqlDateTime parseNumeric(Number n, int sqlType) {

        switch (sqlType) {

        case Types.TIME:
            if (n instanceof Double || n instanceof Float) {
                double value = n.doubleValue();
                return parseTimeFromReal(value);
            } else if (n instanceof Short || n instanceof Integer || n instanceof Long) {
                long value = n.longValue();
                return parseTimeFromInteger(value);
            } else if (n instanceof Decimal) {
                Decimal value = (Decimal) n;
                return parseTimeFromDecimal(value);
            }
            break;
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
        case Types.TIMESTAMP:
        case Types.DATE:
        default:
            if (n instanceof Double || n instanceof Float) {
                double value = n.doubleValue();
                return parseDatetimeFromReal(value);
            } else if (n instanceof Short || n instanceof Integer || n instanceof Long) {
                long value = n.longValue();
                return parseDatetimeFromInteger(value);
            } else if (n instanceof Decimal) {
                Decimal value = (Decimal) n;
                return parseDatetimeFromDecimal(value);
            }
        }
        return null;
    }

    public static MysqlDateTime parseNumeric(Number n, int sqlType, int flags) {

        switch (sqlType) {

        case Types.TIME:
            if (n instanceof Double || n instanceof Float) {
                double value = n.doubleValue();
                return parseTimeFromReal(value);
            } else if (n instanceof Short || n instanceof Integer || n instanceof Long) {
                long value = n.longValue();
                return parseTimeFromInteger(value);
            } else if (n instanceof Decimal) {
                Decimal value = (Decimal) n;
                return parseTimeFromDecimal(value);
            }
            break;
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
        case Types.TIMESTAMP:
        case Types.DATE:
        default:
            if (n instanceof Double || n instanceof Float) {
                double value = n.doubleValue();
                return parseDatetimeFromReal(value, flags);
            } else if (n instanceof Short || n instanceof Integer || n instanceof Long) {
                long value = n.longValue();
                return parseDatetimeFromInteger(value, flags);
            } else if (n instanceof Decimal) {
                Decimal value = (Decimal) n;
                return parseDatetimeFromDecimal(value, flags);
            }
        }
        return null;
    }

    public static MysqlDateTime parseTimeFromReal(Double value) {
        if (value == null) {
            return null;
        }
        DivStructure div = DivStructure.fromDouble(value);
        if (div == null) {
            return null;
        }
        long quot = div.getQuot();
        long rem = div.getRem();
        MysqlDateTime t = divToMysqlTime(quot, rem);

        return t;
    }

    public static MysqlDateTime parseTimeFromInteger(Long value) {
        if (value == null) {
            return null;
        }
        MysqlDateTime t = longToMysqlTime(value);
        return t;
    }

    public static MysqlDateTime parseTimeFromDecimal(Decimal value) {
        if (value == null) {
            return null;
        }
        DivStructure div = DivStructure.fromDecimal(value);
        if (div == null) {
            return null;
        }
        long quot = div.getQuot();
        long rem = div.getRem();
        MysqlDateTime t = divToMysqlTime(quot, rem);
        return t;
    }

    public static MysqlDateTime parseDatetimeFromReal(Double value) {
        return parseDatetimeFromReal(value, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
    }

    public static MysqlDateTime parseDatetimeFromReal(Double value, int flags) {
        if (value == null) {
            return null;
        }
        DivStructure div = DivStructure.fromDouble(value);
        if (div == null) {
            return null;
        }
        long quot = div.getQuot();
        long rem = div.getRem();
        MysqlDateTime t = divToMysqlDatetime(quot, rem, flags);
        return t;
    }

    public static MysqlDateTime parseDatetimeFromInteger(Long value) {
        return parseDatetimeFromInteger(value, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
    }

    public static MysqlDateTime parseDatetimeFromInteger(Long value, int flags) {
        if (value == null) {
            return null;
        }
        MysqlDateTime t = longToMysqlDatetime(value, flags);
        return t;
    }

    public static MysqlDateTime parseDatetimeFromDecimal(Decimal value) {
        return parseDatetimeFromDecimal(value, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
    }

    public static MysqlDateTime parseDatetimeFromDecimal(Decimal value, int flags) {
        if (value == null) {
            return null;
        }
        DivStructure div = DivStructure.fromDecimal(value);
        if (div == null) {
            return null;
        }
        long quot = div.getQuot();
        long rem = div.getRem();
        MysqlDateTime t = divToMysqlDatetime(quot, rem, flags);
        return t;
    }

    public static MysqlDateTime divToMysqlDatetime(long quot, long rem, int flags) {
        if (rem < 0) {

            return null;
        }
        MysqlDateTime t = longToMysqlDatetime(quot, flags);
        if (t == null) {
            return null;
        } else if (t.getSqlType() == Types.DATE) {

        } else if (!TimeParserFlags.check(flags, TimeParserFlags.FLAG_TIME_NO_NANO_ROUNDING)) {
            t.setSecondPart(rem / 1000 * 1000);
            t = MySQLTimeCalculator.datetimeAddNanoWithRound(t, (int) (rem % 1000));
        }

        return t;
    }

    public static MysqlDateTime divToMysqlTime(long quot, long rem) {
        MysqlDateTime t = longToMysqlTime(quot);
        if (t == null) {
            return null;
        }

        t.setNeg(t.isNeg() || (rem < 0));
        if (t.isNeg()) {
            rem = -rem;
        }
        t.setSecondPart(rem / 1000 * 1000);
        MySQLTimeCalculator.timeAddNanoWithRound(t, (int) (rem % 1000));

        return t;
    }

    private static MysqlDateTime longToMysqlDatetime(Long value, int flags) {
        MysqlDateTime t = new MysqlDateTime();
        t.setSqlType(Types.DATE);

        if (value == 0L || value >= 10000101000000L) {
            t.setSqlType(Types.TIMESTAMP);
            if (value > 99999999999999L) {

                return null;
            }
            return parseLong0(value, t, flags);
        }
        if (value < 101) {
            return null;
        }
        if (value <= (70 - 1) * 10000L + 1231L) {

            value = (value + 20000000L) * 1000000L;
            return parseLong0(value, t, flags);
        }
        if (value < 70 * 10000L + 101L) {
            return null;
        }
        if (value <= 991231L) {

            value = (value + 19000000L) * 1000000L;
            return parseLong0(value, t, flags);
        }

        if (value < 10000101L && !TimeParserFlags.check(flags, TimeParserFlags.FLAG_TIME_FUZZY_DATE)) {
            return null;
        }
        if (value <= 99991231L) {
            value = value * 1000000L;
            return parseLong0(value, t, flags);
        }
        if (value < 101000000L) {
            return null;
        }
        t.setSqlType(Types.TIMESTAMP);

        if (value <= (70 - 1) * 10000000000L + 1231235959L) {

            value = value + 20000000000000L;
            return parseLong0(value, t, flags);
        }
        if (value < 70 * 10000000000L + 101000000L) {
            return null;
        }
        if (value <= 991231235959L) {

            value = value + 19000000000000L;
        }
        return parseLong0(value, t, flags);
    }

    private static MysqlDateTime parseLong0(Long value, MysqlDateTime t, int flags) {
        long part1 = value / 1000000L;
        long part2 = (value - part1 * 1000000L);
        t.setYear(part1 / 10000L);
        part1 %= 10000L;
        t.setMonth(part1 / 100);
        t.setDay(part1 % 100);
        t.setHour(part2 / 10000L);
        part2 %= 10000L;
        t.setMinute(part2 / 100);
        t.setSecond(part2 % 100);

        if (!MySQLTimeTypeUtil.isDatetimeRangeInvalid(t)
            && !MySQLTimeTypeUtil.isDateInvalid(t, (value != 0), flags)) {
            return t;
        }
        return null;
    }

    private static MysqlDateTime longToMysqlTime(Long value) {
        MysqlDateTime t;
        if (value > TIME_MAX_VALUE) {

            if (value >= 10000000000L) {

                t = longToMysqlDatetime(value, 0);
                return t;
            }
            return null;
        } else if (value < -TIME_MAX_VALUE) {
            return null;
        }

        t = new MysqlDateTime();
        if (value < 0) {
            value = -value;
            t.setNeg(true);
        }

        if (value % 100 >= 60 || value / 100 % 100 >= 60) {
            return null;
        }

        t.setSqlType(Types.TIME);
        t.setYear(0);
        t.setMonth(0);
        t.setDay(0);

        t.setSecond(value % 100);
        t.setMinute((value / 100) % 100);
        t.setHour(value / 10000);
        t.setSecondPart(0);
        return t;
    }

}

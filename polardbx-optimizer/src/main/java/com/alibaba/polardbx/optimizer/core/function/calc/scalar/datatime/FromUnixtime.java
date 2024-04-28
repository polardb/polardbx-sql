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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DivStructure;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.google.common.base.Preconditions;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Returns a representation of the unix_timestamp argument as a value in
 * 'YYYY-MM-DD HH:MM:SS' or YYYYMMDDHHMMSS format, depending on whether the
 * function is used in a string or numeric context. The value is expressed in
 * the current time zone. unix_timestamp is an internal timestamp value such as
 * is produced by the UNIX_TIMESTAMP() function. If format is given, the result
 * is formatted according to the format string, which is used the same way as
 * listed in the entry for the DATE_FORMAT() function.
 *
 * <pre>
 * mysql> SELECT FROM_UNIXTIME(1196440219);
 *         -> '2007-11-30 10:30:19'
 * mysql> SELECT FROM_UNIXTIME(1196440219) + 0;
 *         -> 20071130103019.000000
 * mysql> SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(),
 *     ->                      '%Y %D %M %h:%i:%s %x');
 *         -> '2007 30th November 10:30:59 2007'
 * </pre>
 * <p>
 * Note: If you use UNIX_TIMESTAMP() and FROM_UNIXTIME() to convert between
 * TIMESTAMP values and Unix timestamp values, the conversion is lossy because
 * the mapping is not one-to-one in both directions. For details, see the
 * description of the UNIX_TIMESTAMP() function.
 *
 * @author jianghang 2014-4-17 上午12:24:11
 * @since 5.0.7
 */
public class FromUnixtime extends AbstractScalarFunction {
    public FromUnixtime(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private static final long MAX_TIME_VALUE_IN_MYSQL80 = 32536771199L;

    private static final String[] ORDER_SUFFIX = {"th", "st", "nd", "rd"};

    protected static String predealing(String format) {
        StringBuilder builder = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < format.length(); ++i) {
            if ('%' == format.charAt(i)) {
                if (inQuotes) {
                    inQuotes = false;
                    builder.append('\'');
                }
                builder.append('%');
                if (i + 1 >= format.length()) {
                    break;
                } else {
                    ++i;
                    builder.append(format.charAt(i));
                }
            } else {
                if (!inQuotes) {
                    inQuotes = true;
                    builder.append('\'').append(format.charAt(i));
                } else {
                    builder.append(format.charAt(i));
                }
                if ('\'' == format.charAt(i)) {
                    builder.append('\'');
                }
            }
        }
        if (inQuotes) {
            inQuotes = false;
            builder.append('\'');
        }
        return builder.toString();
    }

    protected static String computNotSuppotted(Timestamp timestamp, String format) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timestamp);

        StringBuilder builder = new StringBuilder();

        for (int idx = 0; idx < format.length(); ++idx) {
            if ('%' == format.charAt(idx)) {
                if (idx + 1 < format.length()) {
                    switch (format.charAt(idx + 1)) {
                    case 'D': {
                        // Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
                        String res = String.format("%d", dayOfMonth);
                        if (dayOfMonth % 10 < 4 && dayOfMonth / 10 != 1) {
                            res = res + ORDER_SUFFIX[dayOfMonth % 10];
                        } else {
                            res = res + "th";
                        }
                        builder.append(res);
                        break;
                    }

                    case 'w': {
                        // Day of the week (0=Sunday..6=Saturday)
                        String res = String.format("%d", calendar.get(Calendar.DAY_OF_WEEK) - 1);
                        builder.append(res);
                        break;
                    }

                    case 'U': {
                        // Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
                        calendar.setFirstDayOfWeek(Calendar.SUNDAY);
                        calendar.setMinimalDaysInFirstWeek(7);
                        int woy = calendar.get(Calendar.WEEK_OF_YEAR);
                        if (woy >= 50 && calendar.get(Calendar.DAY_OF_YEAR) <= 7) {
                            woy = 0;
                        }
                        String res = String.format("%02d", woy);
                        builder.append(res);
                        break;
                    }

                    case 'u': {
                        // Week (00..53), where Monday is the first day of the week; WEEK() mode 1
                        calendar.setFirstDayOfWeek(Calendar.MONDAY);
                        calendar.setMinimalDaysInFirstWeek(4);
                        int woy = calendar.get(Calendar.WEEK_OF_YEAR);
                        if (woy >= 50 && calendar.get(Calendar.DAY_OF_YEAR) <= 7) {
                            woy = 0;
                        }
                        String res = String.format("%02d", woy);
                        builder.append(res);
                        break;
                    }

                    case 'V': {
                        // Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
                        calendar.setFirstDayOfWeek(Calendar.SUNDAY);
                        calendar.setMinimalDaysInFirstWeek(7);
                        String res = String.format("%02d", calendar.get(Calendar.WEEK_OF_YEAR));
                        builder.append(res);
                        break;
                    }

                    case 'v': {
                        // Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
                        calendar.setFirstDayOfWeek(Calendar.MONDAY);
                        calendar.setMinimalDaysInFirstWeek(4);
                        String res = String.format("%02d", calendar.get(Calendar.WEEK_OF_YEAR));
                        builder.append(res);
                        break;
                    }

                    case 'X': {
                        // Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                        calendar.setFirstDayOfWeek(Calendar.SUNDAY);
                        String res = String.format("%04d", calendar.getWeekYear());
                        builder.append(res);
                        break;
                    }

                    case 'x': {
                        calendar.setFirstDayOfWeek(Calendar.MONDAY);
                        String res = String.format("%04d", calendar.getWeekYear());
                        builder.append(res);
                        break;
                    }

                    default:
                        builder.append(format.charAt(idx));
                        builder.append(format.charAt(idx + 1));
                        break;
                    }
                    ++idx;
                    continue;
                }
            }
            builder.append(format.charAt(idx));
        }

        return builder.toString();
    }

    protected static String convertToJavaDataFormat(String format) {
        StringBuilder builder = new StringBuilder();

        for (int idx = 0; idx < format.length(); ++idx) {
            if ('%' == format.charAt(idx)) {
                if (idx + 1 < format.length()) {
                    switch (format.charAt(idx + 1)) {
                    case 'a':
                        builder.append("EEE");
                        break;
                    case 'b':
                        builder.append("MMM");
                        break;
                    case 'c':
                        builder.append("M");
                        break;
                    case 'd':
                        builder.append("dd");
                        break;
                    case 'e':
                        builder.append("d");
                        break;
                    case 'f':
                        builder.append("SSSSSS");
                        break;
                    case 'H':
                        builder.append("HH");
                        break;
                    case 'h':
                    case 'I':
                        builder.append("hh");
                        break;
                    case 'i':
                        builder.append("mm");
                        break;
                    case 'j':
                        builder.append("DDD");
                        break;
                    case 'k':
                        builder.append("H");
                        break;
                    case 'l':
                        builder.append("h");
                        break;
                    case 'M':
                        builder.append("MMMM");
                        break;
                    case 'm':
                        builder.append("MM");
                        break;
                    case 'p':
                        builder.append("a");
                        break;
                    case 'r':
                        builder.append("hh:mm:ss a");
                        break;
                    case 'S':
                    case 's':
                        builder.append("ss");
                        break;
                    case 'T':
                        builder.append("HH:mm:ss");
                        break;
                    case 'W':
                        builder.append("EEEE");
                        break;
                    case 'Y':
                        builder.append("yyyy");
                        break;
                    case 'y':
                        builder.append("yy");
                        break;
                    case '%':
                        builder.append("%");
                        break;

                    default:
                        builder.append(format.charAt(idx));
                        builder.append(format.charAt(idx + 1));
                        break;
                    }
                    ++idx;
                    continue;
                }
            }
            builder.append(format.charAt(idx));
        }

        return builder.toString();
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 1) {
            GeneralUtil.nestedException("FromUnixtime must have at least one argument");
        }
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        if (args.length == 1) {
            final ZoneId zoneId = ec.getTimeZone() == null
                ? TimeZone.getDefault().toZoneId()
                : ec.getTimeZone().getZoneId();

            MysqlDateTime mysqlDateTime = evaluateOneParam(args[0], zoneId);

            return mysqlDateTime == null ? null : new OriginalTimestamp(mysqlDateTime);

        } else if (args.length >= 2) {
            // old logic
            Long time = DataTypes.LongType.convertFrom(args[0]);
            java.sql.Timestamp timestamp = new java.sql.Timestamp(time * 1000);

            TimeZone timeZone = null;
            if (ec.getTimeZone() != null) {
                timeZone = ec.getTimeZone().getTimeZone();
            }

            String format = DataTypes.StringType.convertFrom(args[1]);
            try {
                SimpleDateFormat dateFormat = new SimpleDateFormat(
                    convertToJavaDataFormat(predealing(computNotSuppotted(timestamp, format))),
                    Locale.ENGLISH);
                if (ec.getTimeZone() != null) {
                    dateFormat.setTimeZone(timeZone);
                }
                return dateFormat.format(timestamp);
            } catch (IllegalArgumentException e) {
                return format;
            }
        }

        return null;
    }

    private MysqlDateTime evaluateOneParam(Object arg, ZoneId zoneId) {
        Preconditions.checkNotNull(arg);

        boolean hasFractional;
        // get fractions after point.
        if (arg instanceof OriginalTemporalValue) {
            hasFractional = ((OriginalTemporalValue) arg).getMysqlDateTime().getSecondPart() != 0;
        } else if (arg instanceof Decimal) {
            hasFractional = ((Decimal) arg).scale() > 0;
        } else if (arg instanceof Number && !(arg instanceof Double || arg instanceof Float)) {
            hasFractional = false;
        } else {
            hasFractional = true;
        }

        long upperBound = InstanceVersion.isMYSQL80() ? MAX_TIME_VALUE_IN_MYSQL80 : Integer.MAX_VALUE;

        DivStructure divStructure;
        if (hasFractional) {
            // convert to decimal structure and get div structure
            Decimal decimal = DataTypes.DecimalType.convertFrom(arg);
            if (decimal == null) {
                return null;
            }

            divStructure = DivStructure.fromDecimal(decimal);
            if (divStructure == null) {
                return null;
            }
        } else {
            Long longVal = DataTypes.LongType.convertFrom(arg);
            divStructure = new DivStructure();
            divStructure.setQuot(longVal);
            divStructure.setRem(0);
        }

        // check boundary of div structure.
        if (divStructure.getQuot() > upperBound
            || divStructure.getQuot() < 0
            || divStructure.getRem() < 0) {
            return null;
        }

        // Try convert
        MySQLTimeVal timeVal = new MySQLTimeVal(divStructure.getQuot(), 0);
        MysqlDateTime mysqlDateTime = MySQLTimeConverter.convertTimestampToDatetime(timeVal, zoneId);
        if (mysqlDateTime == null) {
            return null;
        }

        // get micro and nano part to round
        long micro = !hasFractional ? 0 : divStructure.getRem() / 1000;
        int nano = (int) (divStructure.getRem() % 1000);

        mysqlDateTime.setSecondPart(micro * 1000);
        MysqlDateTime result = MySQLTimeCalculator.datetimeAddNanoWithRound(mysqlDateTime, nano);
        return result;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"FROM_UNIXTIME"};
    }
}

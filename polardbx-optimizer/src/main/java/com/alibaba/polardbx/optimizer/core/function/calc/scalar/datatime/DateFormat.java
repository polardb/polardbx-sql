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

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

/**
 * DATE_FORMAT(date,format)
 * Formats the date value according to the format string.
 * The specifiers shown in the following table may be used in the format string. The % character
 * is required before format specifier characters. The specifiers apply to other functions as well:
 * STR_TO_DATE(), TIME_FORMAT(), UNIX_TIMESTAMP().
 */
public class DateFormat extends AbstractScalarFunction {
    public DateFormat(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private SimpleDateFormat cachedFormat;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Object timeObj = args[0];
        MysqlDateTime t =
            DataTypeUtil.toMySQLDatetimeByFlags(timeObj, Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_FUZZY_DATE);

        if (t == null) {
            return null;
        }

        java.sql.Timestamp timestamp = new OriginalTimestamp(t);
        // to time in default time zone
        java.sql.Timestamp timestampInDefaultZone = java.sql.Timestamp.valueOf(timestamp.toString());

        String format = DataTypes.StringType.convertFrom(args[1]);
        try {
            SimpleDateFormat dateFormat =
                new SimpleDateFormat(convertToJavaDataFormat(predealing(computNotSuppotted(timestamp, format))),
                    Locale.ENGLISH);
            return dateFormat.format(timestampInDefaultZone);
        } catch (IllegalArgumentException e) {
            return format;
        }

    }

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
    public String[] getFunctionNames() {
        return new String[] {"DATE_FORMAT"};
    }
}

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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.IntervalType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.exception.FunctionException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * mysql interval函数
 *
 * <pre>
 * The INTERVAL keyword and the unit specifier are not case sensitive.
 *
 * The following table shows the expected form of the expr argument for each unit value.
 *
 * unit Value  Expected expr Format
 * MICROSECOND MICROSECONDS
 * SECOND  SECONDS
 * MINUTE  MINUTES
 * HOUR    HOURS
 * DAY DAYS
 * WEEK    WEEKS
 * MONTH   MONTHS
 * QUARTER QUARTERS
 * YEAR    YEARS
 * SECOND_MICROSECOND  'SECONDS.MICROSECONDS'
 * MINUTE_MICROSECOND  'MINUTES:SECONDS.MICROSECONDS'
 * MINUTE_SECOND   'MINUTES:SECONDS'
 * HOUR_MICROSECOND    'HOURS:MINUTES:SECONDS.MICROSECONDS'
 * HOUR_SECOND 'HOURS:MINUTES:SECONDS'
 * HOUR_MINUTE 'HOURS:MINUTES'
 * DAY_MICROSECOND 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
 * DAY_SECOND  'DAYS HOURS:MINUTES:SECONDS'
 * DAY_MINUTE  'DAYS HOURS:MINUTES'
 * DAY_HOUR    'DAYS HOURS'
 * YEAR_MONTH  'YEARS-MONTHS'
 * </pre>
 *
 * @author jianghang 2014-4-16 下午1:34:00
 * @since 5.0.7
 */
public class Interval extends AbstractScalarFunction {
    public Interval(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public enum Interval_Unit {

        MICROSECOND(null, "S"), SECOND(null, "s"), MINUTE(null, "m"), HOUR(null, "H"),
        DAY(null, "d"), WEEK(null, "w"), MONTH(null, "M"), QUARTER(null, null),
        YEAR(null, "yyyy"),
        /**
         *
         */
        SECOND_MICROSECOND("^\\s*(\\d+)?\\.?(\\d+)?\\s*$", "sSSSSSS"),
        /**
         *
         */
        MINUTE_MICROSECOND("^\\s*(\\d+)?\\:?(\\d+)?\\.?(\\d+)?\\s*$", "mssSSSSSS"),
        /**
         *
         */
        MINUTE_SECOND("^\\s*(\\d+)?\\:?(\\d+)?\\s*$", "mss"),
        /**
         *
         */
        HOUR_MICROSECOND("^\\s*(\\d+)?\\:?(\\d+)?\\:?(\\d+)?\\.?(\\d+)?\\s*$", "HmmssSSSSSS"),
        /**
         *
         */
        HOUR_SECOND("^^\\s*(\\d+)?\\:?(\\d+)?\\:?(\\d+)?\\s*$", "Hmmss"),
        /**
         *
         */
        HOUR_MINUTE("^\\s*(\\d+)\\:(\\d+)\\s*$", "Hmm"),
        /**
         *
         */
        DAY_MICROSECOND("^\\s*((\\d+)(\\s+|.)(\\d+)\\:(\\d+)\\:)?(\\d+)(\\.(\\d+))?\\s*$",
            "dHHmmssSSSSSS"),
        /**
         *
         */
        DAY_SECOND("^\\s*(\\d+)\\s+(\\d+)\\:(\\d+)\\:(\\d+)\\s*$", "dHHmmss"),
        /**
         *
         */
        DAY_MINUTE("^\\s*(\\d+)\\s+(\\d+)\\:(\\d+)\\s*$", "dHHmm"),
        /**
         *
         */
        DAY_HOUR("^\\s*(\\d+)\\s+(\\d+)\\s*$", "dHH"),
        /**
         *
         */
        YEAR_MONTH("^\\s*(\\d+)-(\\d+)\\s*$", "yyyyMM");

        Interval_Unit() {
            this(null, null);
        }

        Interval_Unit(String pattern, String format) {
            this.pattern = pattern;
            this.format = format;
        }

        String pattern;
        String format;
    }

    private static LoadingCache<String, Pattern> patterns = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, Pattern>() {

            @Override
            public Pattern load(String regex) throws Exception {
                return Pattern.compile(regex);
            }
        });

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        return paseIntervalDate(args[0], args[1]);
    }

    private static String removeSign(IntervalType interval, String str) {
        if (str.length() > 0) {
            switch (str.charAt(0)) {
            case '-':
                interval.setFactor(-1);
            case '+':
                str = str.substring(1);
                break;

            default:
                break;
            }
        }
        return str;
    }

    private static String formatMicros(String microStr) {
        final String zeros = "000000";
        if (microStr.length() >= 6) {
            return microStr;
        }
        return microStr + zeros.substring(microStr.length());
    }

    public static IntervalType paseIntervalDate(Object value, Object unitObj) {
        try {
            String unit = DataTypes.StringType.convertFrom(unitObj);
            IntervalType interval = new IntervalType();
            if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MICROSECOND.name())) {
                interval.setMicrosecond(DataTypes.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.SECOND.name())) {
                interval.setSecond(DataTypes.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MINUTE.name())) {
                interval.setMinute(DataTypes.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.HOUR.name())) {
                interval.setHour(DataTypes.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY.name())) {
                interval.setDay(DataTypes.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.WEEK.name())) {
                interval.setDay(DataTypes.IntegerType.convertFrom(value) * 7);// 7天
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MONTH.name())) {
                interval.setMonth(DataTypes.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.QUARTER.name())) {
                interval.setMonth(DataTypes.IntegerType.convertFrom(value) * 3);// 3个月
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.YEAR.name())) {
                interval.setYear(DataTypes.IntegerType.convertFrom(value));
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.SECOND_MICROSECOND.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.SECOND_MICROSECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String sec = match.group(1);
                if (sec != null) {
                    interval.setSecond(DataTypes.IntegerType.convertFrom(sec));
                }
                String mic = match.group(2);
                if (mic != null) {
                    interval.setMicrosecond(DataTypes.IntegerType.convertFrom(formatMicros(mic)));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MINUTE_MICROSECOND.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.MINUTE_MICROSECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String min = match.group(1);
                if (min != null) {
                    interval.setMinute(DataTypes.IntegerType.convertFrom(min));
                }
                String sec = match.group(2);
                if (sec != null) {
                    interval.setSecond(DataTypes.IntegerType.convertFrom(sec));
                }
                String mic = match.group(3);
                if (mic != null) {
                    interval.setMicrosecond(DataTypes.IntegerType.convertFrom(formatMicros(mic)));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.MINUTE_SECOND.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.MINUTE_SECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String min = match.group(1);
                if (min != null) {
                    interval.setMinute(DataTypes.IntegerType.convertFrom(min));
                }
                String sec = match.group(2);
                if (sec != null) {
                    interval.setSecond(DataTypes.IntegerType.convertFrom(sec));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.HOUR_MICROSECOND.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.HOUR_MICROSECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String hour = match.group(1);
                if (hour != null) {
                    interval.setHour(DataTypes.IntegerType.convertFrom(hour));
                }
                String min = match.group(2);
                if (min != null) {
                    interval.setMinute(DataTypes.IntegerType.convertFrom(min));
                }
                String sec = match.group(3);
                if (sec != null) {
                    interval.setSecond(DataTypes.IntegerType.convertFrom(sec));
                }
                String mic = match.group(4);
                if (mic != null) {
                    interval.setMicrosecond(DataTypes.IntegerType.convertFrom(formatMicros(mic)));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.HOUR_SECOND.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.HOUR_SECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String hour = match.group(1);
                if (hour != null) {
                    interval.setHour(DataTypes.IntegerType.convertFrom(hour));
                }
                String min = match.group(2);
                if (min != null) {
                    interval.setMinute(DataTypes.IntegerType.convertFrom(min));
                }
                String sec = match.group(3);
                if (sec != null) {
                    interval.setSecond(DataTypes.IntegerType.convertFrom(sec));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.HOUR_MINUTE.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.HOUR_MINUTE.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String hour = match.group(1);
                if (hour != null) {
                    interval.setHour(DataTypes.IntegerType.convertFrom(hour));
                }
                String min = match.group(2);
                if (min != null) {
                    interval.setMinute(DataTypes.IntegerType.convertFrom(min));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY_MICROSECOND.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.DAY_MICROSECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String day = match.group(1);
                if (day != null) {
                    interval.setDay(DataTypes.IntegerType.convertFrom(day));
                }
                String hour = match.group(4);
                if (hour != null) {
                    interval.setHour(DataTypes.IntegerType.convertFrom(hour));
                }
                String min = match.group(5);
                if (min != null) {
                    interval.setMinute(DataTypes.IntegerType.convertFrom(min));
                }
                String sec = match.group(6);
                if (sec != null) {
                    interval.setSecond(DataTypes.IntegerType.convertFrom(sec));
                }
                String mic = match.group(8);
                if (mic != null) {
                    interval.setMicrosecond(DataTypes.IntegerType
                        .convertFrom(formatMicros(mic.substring(0, mic.length() > 7 ? 7 : mic.length()))));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY_SECOND.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.DAY_SECOND.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String day = match.group(1);
                if (day != null) {
                    interval.setDay(DataTypes.IntegerType.convertFrom(day));
                }
                String hour = match.group(2);
                if (hour != null) {
                    interval.setHour(DataTypes.IntegerType.convertFrom(hour));
                }
                String min = match.group(3);
                if (min != null) {
                    interval.setMinute(DataTypes.IntegerType.convertFrom(min));
                }
                String sec = match.group(4);
                if (sec != null) {
                    interval.setSecond(DataTypes.IntegerType.convertFrom(sec));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY_MINUTE.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.DAY_MINUTE.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String day = match.group(1);
                if (day != null) {
                    interval.setDay(DataTypes.IntegerType.convertFrom(day));
                }
                String hour = match.group(2);
                if (hour != null) {
                    interval.setHour(DataTypes.IntegerType.convertFrom(hour));
                }
                String min = match.group(3);
                if (min != null) {
                    interval.setMinute(DataTypes.IntegerType.convertFrom(min));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.DAY_HOUR.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.DAY_HOUR.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String day = match.group(1);
                if (day != null) {
                    interval.setDay(DataTypes.IntegerType.convertFrom(day));
                }
                String hour = match.group(2);
                if (hour != null) {
                    interval.setHour(DataTypes.IntegerType.convertFrom(hour));
                }
            } else if (StringUtils.equalsIgnoreCase(unit, Interval_Unit.YEAR_MONTH.name())) {
                String str = DataTypes.StringType.convertFrom(value);
                str = removeSign(interval, str);
                Matcher match = patterns.get(Interval_Unit.YEAR_MONTH.pattern).matcher(str);
                if (!match.matches()) {
                    throw new FunctionException("interval parser error");
                }

                String year = match.group(1);
                if (year != null) {
                    interval.setYear(DataTypes.IntegerType.convertFrom(year));
                }
                String mon = match.group(2);
                if (mon != null) {
                    interval.setMonth(DataTypes.IntegerType.convertFrom(mon));
                }
            }

            return interval;
        } catch (ExecutionException e) {
            throw new FunctionException(e);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"INTERVAL_PRIMARY"};
    }
}

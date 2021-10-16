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

import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.util.List;

/**
 * EXTRACT(unit FROM date)
 * The EXTRACT() function uses the same kinds of unit specifiers as DATE_ADD()
 * or DATE_SUB(), but extracts parts from the date rather than performing date
 * arithmetic.
 */
public class Extract extends AbstractScalarFunction {
    public Extract(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private MySQLIntervalType cachedIntervalType;
    private Boolean cachedIsDate = null;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Object intervalObj = args[0];
        Object timeObj = args[1];
        // prepare interval type
        if (cachedIntervalType == null || cachedIsDate == null) {
            String unit = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, intervalObj);
            cachedIntervalType = MySQLIntervalType.of(unit);
            if (cachedIntervalType == null) {
                // invalid interval type.
                return null;
            }
            cachedIsDate = isDate(cachedIntervalType);
        }
        MySQLIntervalType intervalType = cachedIntervalType;
        boolean isDate = cachedIsDate.booleanValue();

        MysqlDateTime t;
        int sign;
        if (isDate) {
            t = DataTypeUtil.toMySQLDatetimeByFlags(timeObj, Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
            if (t == null) {
                return null;
            }
            sign = 1;
        } else {
            t = DataTypeUtil.toMySQLDatetime(timeObj, Types.TIME);
            if (t == null) {
                return null;
            }
            sign = t.isNeg() ? -1 : 1;
        }

        switch (intervalType) {
        case INTERVAL_YEAR:
            return t.getYear();
        case INTERVAL_YEAR_MONTH:
            return t.getYear() * 100L + t.getMonth();
        case INTERVAL_QUARTER:
            return (t.getMonth() + 2) / 3;
        case INTERVAL_MONTH:
            return t.getMonth();
        case INTERVAL_WEEK: {
            long[] weekAndYear = MySQLTimeConverter.datetimeToWeek(t, TimeParserFlags.FLAG_WEEK_FIRST_WEEKDAY);
            return weekAndYear[0];
        }
        case INTERVAL_DAY:
            return t.getDay();
        case INTERVAL_DAY_HOUR:
            return (t.getDay() * 100L + t.getHour()) * sign;
        case INTERVAL_DAY_MINUTE:
            return (t.getDay() * 10000L + t.getHour() * 100L + t.getMinute()) * sign;
        case INTERVAL_DAY_SECOND:
            return (t.getDay() * 1000000L + (t.getHour() * 10000L + t.getMinute() * 100 + t.getSecond())) * sign;
        case INTERVAL_HOUR:
            return t.getHour() * sign;
        case INTERVAL_HOUR_MINUTE:
            return (t.getHour() * 100 + t.getMinute()) * sign;
        case INTERVAL_HOUR_SECOND:
            return (t.getHour() * 10000 + t.getMinute() * 100 + t.getSecond()) * sign;
        case INTERVAL_MINUTE:
            return t.getMinute() * sign;
        case INTERVAL_MINUTE_SECOND:
            return (t.getMinute() * 100 + t.getSecond()) * sign;
        case INTERVAL_SECOND:
            return t.getSecond() * sign;
        case INTERVAL_MICROSECOND:
            return t.getSecondPart() / 1000L * sign;
        case INTERVAL_DAY_MICROSECOND:
            return ((t.getDay() * 1000000L + t.getHour() * 10000L + t.getMinute() * 100 + t.getSecond()) * 1000000L + t
                .getSecondPart() / 1000L) * sign;
        case INTERVAL_HOUR_MICROSECOND:
            return ((t.getHour() * 10000L + t.getMinute() * 100 + t.getSecond()) * 1000000L + t.getSecondPart() / 1000L)
                * sign;
        case INTERVAL_MINUTE_MICROSECOND:
            return (((t.getMinute() * 100 + t.getSecond())) * 1000000L + t.getSecondPart() / 1000L) * sign;
        case INTERVAL_SECOND_MICROSECOND:
            return (t.getSecond() * 1000000L + t.getSecondPart() / 1000L) * sign;
        default:
            break;
        }

        return null;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"EXTRACT"};
    }

    private boolean isDate(MySQLIntervalType intervalType) {
        boolean isDate;
        switch (cachedIntervalType) {
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_QUARTER:
        case INTERVAL_MONTH:
        case INTERVAL_WEEK:
        case INTERVAL_DAY:
            isDate = true;
            break;
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
            isDate = false;
        }
        return isDate;
    }
}

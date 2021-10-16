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

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * TIMESTAMPDIFF(unit,datetime_expr1,datetime_expr2)
 * Returns datetime_expr2 − datetime_expr1, where datetime_expr1 and datetime_expr2
 * are date or datetime expressions. One expression may be a date and the other a datetime; a date
 * value is treated as a datetime having the time part '00:00:00' where necessary. The unit for the
 * result (an integer) is given by the unit argument. The legal values for unit are the same as those
 * listed in the description of the TIMESTAMPADD() function.
 */
public class TimestampDiff extends AbstractScalarFunction {
    public TimestampDiff(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private MySQLIntervalType cachedIntervalType;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        // prepare interval type
        if (cachedIntervalType == null) {
            String unit = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
            cachedIntervalType = MySQLIntervalType.of(unit);
            if (cachedIntervalType == null) {
                // invalid interval type.
                return null;
            }
        }
        MySQLIntervalType intervalType = cachedIntervalType;

        // calculate the diff btw t1 and t2
        MysqlDateTime t1 = DataTypeUtil.toMySQLDatetime(args[1]);
        MysqlDateTime t2 = DataTypeUtil.toMySQLDatetime(args[2]);
        if (t1 == null || t2 == null) {
            return null;
        }

        // NOTE: t2 - t1
        MysqlDateTime diff = MySQLTimeCalculator.calTimeDiff(t2, t1, false);
        boolean isNeg = diff.isNeg();
        int sign = isNeg ? -1 : 1;
        long diffSec = diff.getSecond();
        long diffMicro = diff.getSecondPart() / 1000;

        // calc month & year
        long months = 0;
        if (intervalType == MySQLIntervalType.INTERVAL_YEAR
            || intervalType == MySQLIntervalType.INTERVAL_QUARTER
            || intervalType == MySQLIntervalType.INTERVAL_MONTH) {
            long yearBeg, yearEnd, monthBeg, monthEnd, dayBeg, dayEnd;
            long secondBeg, secondEnd, microsecondBeg, microsecondEnd;
            if (isNeg) {
                yearBeg = t2.getYear();
                yearEnd = t1.getYear();
                monthBeg = t2.getMonth();
                monthEnd = t1.getMonth();
                dayBeg = t2.getDay();
                dayEnd = t1.getDay();
                secondBeg = t2.getHour() * 3600 + t2.getMinute() * 60 + t2.getSecond();
                secondEnd = t1.getHour() * 3600 + t1.getMinute() * 60 + t1.getSecond();
                microsecondBeg = t2.getSecondPart() / 1000;
                microsecondEnd = t1.getSecondPart() / 1000;
            } else {
                yearBeg = t1.getYear();
                yearEnd = t2.getYear();
                monthBeg = t1.getMonth();
                monthEnd = t2.getMonth();
                dayBeg = t1.getDay();
                dayEnd = t2.getDay();
                secondBeg = t1.getHour() * 3600 + t1.getMinute() * 60 + t1.getSecond();
                secondEnd = t2.getHour() * 3600 + t2.getMinute() * 60 + t2.getSecond();
                microsecondBeg = t1.getSecondPart() / 1000;
                microsecondEnd = t2.getSecondPart() / 1000;
            }

            // calc year
            long years = yearEnd - yearBeg;
            if (monthEnd < monthBeg
                || (monthEnd == monthBeg && dayEnd < dayBeg)) {
                years--;
            }

            // calc month
            months = 12 * years;
            if (monthEnd < monthBeg
                || (monthEnd == monthBeg && dayEnd < dayBeg)) {
                months += (12 - (monthBeg - monthEnd));
            } else {
                months += (monthEnd - monthBeg);
            }

            if (dayEnd < dayBeg) {
                months--;
            } else if ((dayEnd == dayBeg)
                && ((secondEnd < secondBeg) || (secondEnd == secondBeg && microsecondEnd < microsecondBeg))) {
                months--;
            }
        }

        switch (intervalType) {
        case INTERVAL_YEAR:
            return months / 12 * sign;
        case INTERVAL_QUARTER:
            return months / 3 * sign;
        case INTERVAL_MONTH:
            return months * sign;
        case INTERVAL_WEEK:
            return diffSec / (3600 * 24L) / 7L * sign;
        case INTERVAL_DAY:
            return diffSec / (3600 * 24L) * sign;
        case INTERVAL_HOUR:
            return diffSec / 3600L * sign;
        case INTERVAL_MINUTE:
            return diffSec / 60L * sign;
        case INTERVAL_SECOND:
            return diffSec * sign;
        case INTERVAL_MICROSECOND:
            return (diffSec * MySQLTimeTypeUtil.SEC_TO_MICRO + diffMicro) * sign;
        default:
            break;
        }
        return null;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TIMESTAMPDIFF"};
    }
}

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
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLInterval;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.OriginalTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * ADDDATE(date,INTERVAL expr unit), ADDDATE(expr,days) When invoked with the
 * INTERVAL form of the second argument, ADDDATE() is a synonym for DATE_ADD().
 * The related function SUBDATE() is a synonym for DATE_SUB(). For information
 * on the INTERVAL unit argument, see the discussion for DATE_ADD().
 *
 * <pre>
 * mysql> SELECT DATE_ADD('2008-01-02', INTERVAL 31 DAY);
 *         -> '2008-02-02'
 * mysql> SELECT ADDDATE('2008-01-02', INTERVAL 31 DAY);
 *         -> '2008-02-02'
 * </pre>
 * <p>
 * When invoked with the days form of the second argument, MySQL treats it as an
 * integer number of days to be added to expr.
 *
 * <pre>
 * mysql> SELECT ADDDATE('2008-01-02', 31);
 *         -> '2008-02-02'
 * </pre>
 *
 * @author jianghang 2014-4-16 下午4:00:49
 * @since 5.0.7
 */
public class DateAdd extends AbstractScalarFunction {
    public DateAdd(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    protected MySQLIntervalType cachedIntervalType;
    protected MySQLInterval cachedInterval;

    //
    //       DATE_ADD                                 DATE_ADD
    //       /      \                               /    |     \
    //   TIME   INTERVAL_PRIMARY        =>      TIME    VALUE   TIME_UNIT
    //                 /    \
    //           VALUE        TIME_UNIT
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        // the time to compute
        Object timeObj = args[0];
        // the interval value
        Object valueObj = args[1];

        // prepare interval type
        if (cachedIntervalType == null) {
            String unit = DataTypeUtil.convert(operandTypes.get(2), DataTypes.StringType, args[2]);
            cachedIntervalType = MySQLIntervalType.of(unit);
            if (cachedIntervalType == null) {
                // invalid interval type.
                return null;
            }
        }
        MySQLIntervalType intervalType = cachedIntervalType;

        // prepare interval object
        MySQLInterval interval = getInterval(intervalType, valueObj);
        if (interval == null) {
            return null;
        }

        // prepare mysql datetime object
        MysqlDateTime mysqlDateTime = DataTypeUtil.toMySQLDatetime(timeObj);
        if (mysqlDateTime == null) {
            return null;
        }

        MysqlDateTime ret;
        if (!DataTypeUtil.equalsSemantically(resultType, DataTypes.TimeType)) {
            // date_add_interval cannot handle bad dates
            boolean invalid = MySQLTimeTypeUtil.isDateInvalid(
                mysqlDateTime,
                MySQLTimeTypeUtil.notZeroDate(mysqlDateTime),
                true,
                StringTimeParser.TIME_FUZZY_DATE,
                StringTimeParser.TIME_INVALID_DATES,
                true);

            if (invalid) {
                return null;
            }

            ret = MySQLTimeCalculator.addInterval(mysqlDateTime, intervalType, interval);
        } else {
            // for time type
            long microSec1 = (((mysqlDateTime.getDay() * 24
                + mysqlDateTime.getHour()) * 60
                + mysqlDateTime.getMinute()) * 60
                + mysqlDateTime.getSecond()) * 1000000L
                + mysqlDateTime.getSecondPart() / 1000L;
            microSec1 *= mysqlDateTime.isNeg() ? -1 : 1;

            long microSec2 = (((interval.getDay() * 24
                + interval.getHour()) * 60
                + interval.getMinute()) * 60
                + interval.getSecond()) * 1000000L
                + interval.getSecondPart() / 1000L;
            microSec2 *= interval.isNeg() ? -1 : 1;

            long diff = microSec1 + microSec2;
            long sec = diff / 1000000L;
            long nano = diff % 1000000 * 1000; // nano

            ret = MySQLTimeConverter.secToTime(sec, nano);
            if (interval.getYear() != 0 || interval.getMonth() != 0 || ret == null) {
                return null;
            }
        }

        // convert mysql datetime to proper type.
        if (ret == null || MySQLTimeTypeUtil.isDatetimeRangeInvalid(ret)) {
            // for null or invalid time
            return null;
        } else if (DataTypeUtil.anyMatchSemantically(resultType, DataTypes.TimestampType, DataTypes.DatetimeType)) {
            // for timestamp / datetime
            return new OriginalTimestamp(ret);
        } else if (DataTypeUtil.equalsSemantically(resultType, DataTypes.DateType)) {
            // for date
            return new OriginalDate(ret);
        } else if (DataTypeUtil.equalsSemantically(resultType, DataTypes.TimeType)) {
            // for time
            return new OriginalTime(ret);
        } else if (DataTypeUtil.isStringType(resultType)) {
            // for varchar
            return ret.toStringBySqlType();
        } else {
            return ret.toDatetimeString(MySQLTimeTypeUtil.MAX_FRACTIONAL_SCALE);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DATE_ADD", "ADDDATE", "DATETIME_PLUS"};
    }

    protected MySQLInterval getInterval(MySQLIntervalType intervalType, Object valueObj) {
        if (cachedInterval == null) {
            String intervalValue = DataTypes.StringType.convertFrom(valueObj);
            try {
                cachedInterval = MySQLIntervalType.parseInterval(intervalValue, intervalType);
            } catch (Throwable t) {
                // for invalid interval value
                return null;
            }
        }
        return cachedInterval;
    }
}

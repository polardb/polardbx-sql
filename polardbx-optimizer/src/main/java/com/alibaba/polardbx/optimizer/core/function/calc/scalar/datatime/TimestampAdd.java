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
 * TIMESTAMPADD(unit,interval,datetime_expr) Adds the integer expression
 * interval to the date or datetime expression datetime_expr. The unit for
 * interval is given by the unit argument, which should be one of the following
 * values: MICROSECOND (microseconds), SECOND, MINUTE, HOUR, DAY, WEEK, MONTH,
 * QUARTER, or YEAR. The unit value may be specified using one of keywords as
 * shown, or with a prefix of SQL_TSI_. For example, DAY and SQL_TSI_DAY both
 * are legal.
 *
 * <pre>
 * mysql> SELECT TIMESTAMPADD(MINUTE,1,'2003-01-02');
 *         -> '2003-01-02 00:01:00'
 * mysql> SELECT TIMESTAMPADD(WEEK,1,'2003-01-02');
 *         -> '2003-01-09'
 * </pre>
 *
 * @author jianghang 2014-4-16 下午11:11:30
 * @since 5.0.7
 */
public class TimestampAdd extends AbstractScalarFunction {
    public TimestampAdd(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private MySQLIntervalType cachedIntervalType;
    private MySQLInterval cachedInterval;
    //
    //      TIMESTAMP_ADD
    //      /    |    \
    //   UNIT   VALUE  TIME

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        // the interval value
        Object valueObj = args[1];

        // the time to compute
        Object timeObj = args[2];

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

        // prepare interval object
        if (cachedInterval == null) {
            String intervalValue = DataTypes.StringType.convertFrom(valueObj);
            try {
                cachedInterval = MySQLIntervalType.parseInterval(intervalValue, intervalType);
            } catch (Throwable t) {
                // for invalid interval value
                return null;
            }
        }
        MySQLInterval interval = cachedInterval;
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
        return new String[] {"TIMESTAMPADD"};
    }
}

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
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.util.List;

/**
 * Returns a date, given year and day-of-year values. dayofyear must be greater
 * than 0 or the result is NULL.
 */
public class Makedate extends AbstractScalarFunction {
    public Makedate(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        //  As arguments are integers, we can't know if the year is a 2 digit or 4 digit year.
        //  In this case we treat all years < 100 as 2 digit years. Ie, this is not safe
        //  for dates between 0000-01-01 and 0099-12-31
        Long year = DataTypes.LongType.convertFrom(args[0]);
        Long dayNumber = DataTypes.LongType.convertFrom(args[1]);
        if (year < 0 || year > 9999 || dayNumber <= 0) {
            return null;
        }

        // handle year of 1970 or 2000
        if (year < 100) {
            year = year + 1900;
            if (year < 1970) {
                year += 100;
            }
        }

        long days = MySQLTimeCalculator.calDayNumber(year.longValue(), 1, 1) + dayNumber - 1;
        MysqlDateTime t = new MysqlDateTime();
        // Day number from year 0 to 9999-12-31
        if (days >= 0 && days <= MySQLTimeTypeUtil.MAX_DAY_NUMBER) {
            MySQLTimeCalculator.getDateFromDayNumber(days, t);
            t.setNeg(false);
            t.setHour(0);
            t.setMinute(0);
            t.setSecond(0);
            t.setSecondPart(0);
            t.setSqlType(Types.DATE);
            return DataTypeUtil.fromMySQLDatetime(resultType, t, InternalTimeZone.DEFAULT_TIME_ZONE);
        }

        return null;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"MAKEDATE"};
    }
}

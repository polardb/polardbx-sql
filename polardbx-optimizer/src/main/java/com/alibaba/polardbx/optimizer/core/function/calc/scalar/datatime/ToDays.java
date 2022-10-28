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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;

import java.util.List;

/**
 * TO_DAYS(date)
 * Given a date date, returns a day number (the number of days since year 0).
 * TO_DAYS() is not intended for use with values that precede the advent of the Gregorian calendar
 * (1582), because it does not take into account the days that were lost when the calendar was
 * changed. For dates before 1582 (and possibly a later year in other locales), results from this function
 * are not reliable.
 */
public class ToDays extends AbstractScalarFunction {
    public ToDays(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TO_DAYS"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_FUNCTION, "param invalid");
        }

        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        MysqlDateTime t = DataTypeUtil.toMySQLDatetimeByFlags(args[0], Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);
        if (t == null) {
            return null;
        }

        long dayNumber = MySQLTimeCalculator.calDayNumber(
            t.getYear(),
            t.getMonth(),
            t.getDay());

        return dayNumber;
    }
}

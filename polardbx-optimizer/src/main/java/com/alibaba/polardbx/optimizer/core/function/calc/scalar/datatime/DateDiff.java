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

import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * DATEDIFF(expr1,expr2)
 * DATEDIFF() returns expr1 − expr2 expressed as a value in days from one date to the other.
 * expr1 and expr2 are date or date-and-time expressions. Only the date parts of the values are used
 * in the calculation.
 */
public class DateDiff extends AbstractScalarFunction {
    public DateDiff(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private static final Long DAY = 24 * 60 * 60 * 1000L;

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        MysqlDateTime t1 = DataTypeUtil.toMySQLDatetimeByFlags(args[0], TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);
        MysqlDateTime t2 = DataTypeUtil.toMySQLDatetimeByFlags(args[1], TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);
        if (t1 == null || t2 == null) {
            return null;
        }

        long dayNumber1 = MySQLTimeCalculator.calDayNumber(
            t1.getYear(),
            t1.getMonth(),
            t1.getDay());

        long dayNumber2 = MySQLTimeCalculator.calDayNumber(
            t2.getYear(),
            t2.getMonth(),
            t2.getDay());

        long res = dayNumber1 - dayNumber2;
        return res;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DATEDIFF"};
    }
}

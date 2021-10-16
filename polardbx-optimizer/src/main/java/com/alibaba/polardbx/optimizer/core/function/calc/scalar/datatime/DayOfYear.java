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

import java.sql.Types;
import java.util.List;

/**
 * Returns the day of the year for date, in the range 1 to 366.
 */
public class DayOfYear extends AbstractScalarFunction {
    public DayOfYear(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        MysqlDateTime t =
            DataTypeUtil.toMySQLDatetimeByFlags(args[0], Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);

        if (t == null) {
            return null;
        } else {
            return MySQLTimeCalculator.calDayNumber(t.getYear(), t.getMonth(), t.getDay())
                - MySQLTimeCalculator.calDayNumber(t.getYear(), 1, 1)
                + 1;
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DAYOFYEAR"};
    }
}

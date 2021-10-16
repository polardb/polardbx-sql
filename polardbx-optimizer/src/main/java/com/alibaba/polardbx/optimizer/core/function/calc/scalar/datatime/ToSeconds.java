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
 * Created by chuanqin on 17/12/20. Assuming 1 year = 365 days
 */
public class ToSeconds extends AbstractScalarFunction {
    public ToSeconds(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Object timeObj = args[0];
        MysqlDateTime t =
            DataTypeUtil.toMySQLDatetimeByFlags(timeObj, Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);
        if (t == null) {
            return null;
        }
        long sec = t.getHour() * 3600L + t.getMinute() * 60 + t.getSecond();
        if (t.isNeg()) {
            sec = -sec;
        }
        long days = MySQLTimeCalculator.calDayNumber(t.getYear(), t.getMonth(), t.getDay());
        return sec + days * 24L * 3600L;

    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TO_SECONDS"};
    }
}

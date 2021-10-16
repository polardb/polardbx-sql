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
 * Returns the day of the month for date, in the range 1 to 31, or 0 for dates
 * such as '0000-00-00' or '2008-00-00' that have a zero day part.
 */
public class DayOfMonth extends AbstractScalarFunction {
    public DayOfMonth(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        MysqlDateTime t = DataTypeUtil
            .toMySQLDatetimeByFlags(args[0], Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
        if (t == null) {
            return null;
        } else {
            return t.getDay();
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DAYOFMONTH"};
    }
}

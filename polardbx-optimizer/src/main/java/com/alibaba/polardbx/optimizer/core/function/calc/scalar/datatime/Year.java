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

import java.sql.Types;
import java.util.List;
import java.util.regex.Pattern;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

/**
 * YEAR(date)
 * Returns the year for date, in the range 1000 to 9999, or 0 for the “zero”
 * date.
 */
public class Year extends AbstractScalarFunction {
    public Year(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private static Pattern zeroMonthPattern = Pattern.compile("-0+-");
    private static Pattern zeroDayAndMonthPattern = Pattern.compile("-0+-0+-");
    private static Pattern zeroDayPattern = Pattern.compile("0+-");

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        MysqlDateTime t =
            DataTypeUtil.toMySQLDatetimeByFlags(args[0], Types.TIMESTAMP, TimeParserFlags.FLAG_TIME_FUZZY_DATE);
        if (t == null) {
            return null;
        } else {
            return t.getYear();
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"YEAR"};
    }
}

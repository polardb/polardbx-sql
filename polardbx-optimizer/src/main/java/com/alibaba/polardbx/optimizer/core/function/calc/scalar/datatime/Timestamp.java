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
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.util.List;

/**
 * With a single argument, this function returns the date or datetime expression
 * expr as a datetime value. With two arguments, it adds the time expression
 * expr2 to the date or datetime expression expr1 and returns the result as a
 * datetime value.
 */
public class Timestamp extends AddTime {
    public Timestamp(List<DataType> operandTypes, DataType resultType) {
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

        if (args.length == 1) {
            // timestamp(expr) = cast(? as datetime(?))
            MysqlDateTime t = DataTypeUtil.toMySQLDatetimeByFlags(
                timeObj,
                Types.TIMESTAMP,
                TimeParserFlags.FLAG_TIME_FUZZY_DATE | TimeParserFlags.FLAG_TIME_NO_DATE_FRAC_WARN);
            if (t == null) {
                return null;
            }
            t.setSqlType(Types.TIMESTAMP);
            int scale = resultType.getScale();

            t = MySQLTimeCalculator.roundDatetime(t, scale);
            if (t == null) {
                return null;
            }
            return DataTypeUtil.fromMySQLDatetime(resultType, t, InternalTimeZone.DEFAULT_TIME_ZONE);
        } else if (args.length == 2) {
            // timestamp(expr1, expr2) = addtime(expr1, expr2)
            return super.compute(args, ec);
        }
        return null;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TIMESTAMP"};
    }

    @Override
    protected int getInitialSign() {
        return 1;
    }
}

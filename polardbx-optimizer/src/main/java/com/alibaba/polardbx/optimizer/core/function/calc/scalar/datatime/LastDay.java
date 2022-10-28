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
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.sql.Types;
import java.util.Calendar;
import java.util.List;

/**
 * Takes a date or datetime value and returns the corresponding value for the
 * last day of the month. Returns NULL if the argument is invalid.
 *
 * <pre>
 * mysql> SELECT LAST_DAY('2003-02-05');
 *         -> '2003-02-28'
 * mysql> SELECT LAST_DAY('2004-02-05');
 *         -> '2004-02-29'
 * mysql> SELECT LAST_DAY('2004-01-01 01:01:01');
 *         -> '2004-01-31'
 * mysql> SELECT LAST_DAY('2003-03-32');
 *         -> NULL
 * </pre>
 *
 * @author jianghang 2014-4-16 下午6:24:45
 * @since 5.0.7
 */
public class LastDay extends AbstractScalarFunction {
    private final static int[] DAYS_IN_MONTH = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};


    public LastDay(List<DataType> operandTypes, DataType resultType) {
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

        if (t == null || t.getMonth() == 0) {
            // Cannot calculate last day for zero month.
            return null;
        }

        int monthIndex = (int) (t.getMonth() - 1);
        t.setDay(DAYS_IN_MONTH[monthIndex]);

        // fix 2 month.
        if (monthIndex == 1 && MySQLTimeTypeUtil.calcDaysInYear((int) t.getYear()) == 366) {
            t.setDay(29);
        }

        t.setHour(0);
        t.setMinute(0);
        t.setSecond(0);
        t.setSecondPart(0);

        return new OriginalDate(t);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LAST_DAY"};
    }
}

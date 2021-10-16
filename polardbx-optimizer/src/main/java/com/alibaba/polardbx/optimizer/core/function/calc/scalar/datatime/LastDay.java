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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

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

        java.sql.Timestamp timestamp = DataTypes.TimestampType.convertFrom(args[0]);
        if (timestamp == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);

        cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
        return resultType.convertFrom(cal.getTime());
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LAST_DAY"};
    }
}

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

import java.text.SimpleDateFormat;
import java.util.List;

public class TimeFormat extends AbstractScalarFunction {

    public TimeFormat(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Time time = DataTypes.TimeType.convertFrom(args[0]);
        String format = DataTypes.StringType.convertFrom(args[1]);

        // 这里其实是沿用了以往的错误做法
        // date format 没有设置时区，只能把utc epoch millis 转成默认时区的时间
        java.sql.Time timeInDefaultZone = java.sql.Time.valueOf(time.toString());

        SimpleDateFormat dateFormat = new SimpleDateFormat(DateFormat.convertToJavaDataFormat(format));
        return dateFormat.format(timeInDefaultZone);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TIME_FORMAT"};
    }
}

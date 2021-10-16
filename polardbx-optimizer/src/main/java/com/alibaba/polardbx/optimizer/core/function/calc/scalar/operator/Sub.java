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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.operator;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.Calculator;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.IntervalType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * @since 5.0.0
 */
public class Sub extends AbstractScalarFunction {
    public Sub(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Calculator cal = resultType.getCalculator();
        // 如果加法中有出现IntervalType类型，转到时间函数的加法处理
        for (Object arg : args) {
            if (arg instanceof IntervalType && !(resultType instanceof TimestampType
                || resultType instanceof DateType)) {
                cal = DataTypes.TimestampType.getCalculator();
            }
        }

        if (resultType.getCalculator() != cal) {
            return resultType.convertFrom(cal.sub(args[0], args[1]));
        } else {
            return cal.sub(args[0], args[1]);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SUB", "-"};
    }
}

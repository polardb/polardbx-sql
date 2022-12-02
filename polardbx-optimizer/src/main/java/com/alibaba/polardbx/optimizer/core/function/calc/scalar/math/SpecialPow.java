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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.math;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Origin purpose of this function is to cooperate with statistics aggregate function: e.g. stddev.
 * In order to get better performance, @AggregateReduceFunctionRule.reduceStddev() is used to reduce stddev,
 * but this may return confused result in some situation. For example, stddev(x,x,x) returned Nan instead of 0
 * because Pow(-0.0000000000001, 0.5) is invalid, and this function is used to solve this problem.
 *
 * The only different thing when compared with Pow is that SpecialPow(x, y) returns 0 instead of Nan when x <= 0 and y >= 0 and y < 1;
*/
public class SpecialPow extends Pow {
    public SpecialPow(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (args.length < 2) {
            return null;
        }

        if (FunctionUtils.isNull(args[0]) || FunctionUtils.isNull(args[1])) {
            return null;
        }

        Double x = DataTypes.DoubleType.convertFrom(args[0]);
        Double y = DataTypes.DoubleType.convertFrom(args[1]);

        if (x <= 0 && y >= 0 && y < 1) {
            return 0;
        }
        return Math.pow(x, y);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SPECIAL_POW"};
    }
}
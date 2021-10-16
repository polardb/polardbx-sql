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
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Returns the value of X raised to the power of Y.
 *
 * <pre>
 * mysql> SELECT POW(2,2);
 *         -> 4
 * mysql> SELECT POW(2,-2);
 *         -> 0.25
 * </pre>
 *
 * @author jianghang 2014-4-14 下午10:45:20
 * @since 5.0.7
 */
public class Pow extends AbstractScalarFunction {
    public Pow(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length < 2) {
            return null;
        }

        if (FunctionUtils.isNull(args[0]) || FunctionUtils.isNull(args[1])) {
            return null;
        }

        Double x = DataTypes.DoubleType.convertFrom(args[0]);
        Double y = DataTypes.DoubleType.convertFrom(args[1]);
        return Math.pow(x, y);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"POW", "POWER"};
    }
}

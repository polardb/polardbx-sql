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
 * Returns the arc tangent of X, that is, the value whose tangent is X.
 *
 * <pre>
 * mysql> SELECT ATAN(2);
 *         -> 1.1071487177941
 * mysql> SELECT ATAN(-2);
 *         -> -1.1071487177941
 *
 * mysql> SELECT ATAN(-2,2);
 *         -> -0.78539816339745
 * mysql> SELECT ATAN2(PI(),0);
 *         -> 1.5707963267949
 * </pre>
 *
 * @author jianghang 2014-4-14 下午9:43:36
 * @since 5.0.7
 */
public class Atan extends AbstractScalarFunction {
    public Atan(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        Double d = DataTypes.DoubleType.convertFrom(args[0]);
        if (args.length >= 2) {
            if (FunctionUtils.isNull(args[1])) {
                return null;
            }

            Double y = DataTypes.DoubleType.convertFrom(args[1]);
            return Math.atan2(d, y);
        } else {
            return Math.atan(d);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ATAN", "ATAN2"};
    }
}

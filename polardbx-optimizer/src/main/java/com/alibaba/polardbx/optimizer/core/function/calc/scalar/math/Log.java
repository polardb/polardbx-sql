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
 * If called with one parameter, this function returns the natural logarithm of
 * X. If X is less than or equal to 0, then NULL is returned. The inverse of
 * this function (when called with a single argument) is the EXP() function.
 *
 * <pre>
 * mysql> SELECT LOG(2);
 *         -> 0.69314718055995
 * mysql> SELECT LOG(-2);
 *         -> NULL
 * </pre>
 * <p>
 * If called with two parameters, this function returns the logarithm of X to
 * the base B. If X is less than or equal to 0, or if B is less than or equal to
 * 1, then NULL is returned.
 *
 * <pre>
 * mysql> SELECT LOG(2,65536);
 *         -> 16
 * mysql> SELECT LOG(10,100);
 *         -> 2
 * mysql> SELECT LOG(1,100);
 *         -> NULL
 * </pre>
 * <p>
 * LOG(B,X) is equivalent to LOG(X) / LOG(B).
 *
 * @author jianghang 2014-4-14 下午10:30:20
 * @since 5.0.7
 */
public class Log extends AbstractScalarFunction {
    public Log(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        Double d = DataTypes.DoubleType.convertFrom(args[0]);
        if (d <= 0) {
            return null;
        }

        if (args.length >= 2) {
            if (FunctionUtils.isNull(args[1])) {
                return null;
            }
            Double b = DataTypes.DoubleType.convertFrom(args[1]);
            if (b <= 0) {
                return null;
            }

            Double t = Math.log(d);
            if (t == 0.0) {
                return null;
            }
            return Math.log(b) / t;
        } else {
            return Math.log(d);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LOG"};
    }
}

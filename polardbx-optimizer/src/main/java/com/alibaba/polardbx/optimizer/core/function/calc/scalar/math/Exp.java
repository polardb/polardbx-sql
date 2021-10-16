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
 * Returns the value of e (the base of natural logarithms) raised to the power
 * of X. The inverse of this function is LOG() (using a single argument only) or
 * LN().
 *
 * <pre>
 * mysql> SELECT EXP(2);
 *         -> 7.3890560989307
 * mysql> SELECT EXP(-2);
 *         -> 0.13533528323661
 * mysql> SELECT EXP(0);
 *         -> 1
 * </pre>
 *
 * @author jianghang 2014-4-14 下午10:18:31
 * @since 5.0.7
 */
public class Exp extends AbstractScalarFunction {
    public Exp(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        Double d = DataTypes.DoubleType.convertFrom(args[0]);
        return Math.exp(d);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"EXP"};
    }
}

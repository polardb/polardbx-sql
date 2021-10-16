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
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Returns the absolute value of X.
 *
 * <pre>
 * mysql> SELECT ABS(2);
 *         -> 2
 * mysql> SELECT ABS(-32);
 *         -> 32
 * </pre>
 * <p>
 * This function is safe to use with BIGINT values.
 *
 * @author jianghang 2014-4-14 下午9:35:18
 * @since 5.0.7
 */
public class Abs extends AbstractScalarFunction {
    public Abs(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private Object computeInner(Object[] args) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        Object arg = resultType.convertFrom(args[0]);
        Object zero = resultType.convertFrom(0);

        if (resultType.compare(arg, zero) < 0) {
            return resultType.getCalculator().multiply(arg, -1);
        } else {
            return arg;
        }
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return this.computeInner(args);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ABS"};
    }
}

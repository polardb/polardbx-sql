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
 * Modulo operation. Returns the remainder of N divided by M.
 *
 * <pre>
 * mysql> SELECT MOD(234, 10);
 *         -> 4
 * mysql> SELECT 253 % 7;
 *         -> 1
 * mysql> SELECT MOD(29,9);
 *         -> 2
 * mysql> SELECT 29 MOD 9;
 *         -> 2
 * </pre>
 * <p>
 * This function is safe to use with BIGINT values. MOD() also works on values
 * that have a fractional part and returns the exact remainder after division:
 *
 * <pre>
 * mysql> SELECT MOD(34.5,3);
 *         -> 1.5
 * </pre>
 * <p>
 * MOD(N,0) returns NULL.
 */
public class Mod extends AbstractScalarFunction {
    public Mod(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private Object computeInner(Object[] args) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        return resultType.getCalculator().mod(args[0], args[1]);

    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return this.computeInner(args);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"MOD", "%"};
    }
}

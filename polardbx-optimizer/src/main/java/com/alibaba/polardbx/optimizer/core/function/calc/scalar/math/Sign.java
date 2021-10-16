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
 * Returns the sign of the argument as -1, 0, or 1, depending on whether X is
 * negative, zero, or positive.
 *
 * <pre>
 * mysql> SELECT SIGN(-32);
 *         -> -1
 * mysql> SELECT SIGN(0);
 *         -> 0
 * mysql> SELECT SIGN(234);
 *         -> 1
 * </pre>
 *
 * @author jianghang 2014-4-14 下午10:50:20
 * @since 5.0.7
 */
public class Sign extends AbstractScalarFunction {
    public Sign(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private Object computeInner(Object[] args) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        Object arg = resultType.convertFrom(args[0]);
        Object zero = resultType.convertFrom(0);

        return resultType.convertFrom(resultType.compare(arg, zero));
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return this.computeInner(args);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SIGN"};
    }
}

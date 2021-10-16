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
 * Returns the base-10 logarithm of X.
 *
 * <pre>
 * mysql> SELECT LOG10(2);
 *         -> 0.30102999566398
 * mysql> SELECT LOG10(100);
 *         -> 2
 * mysql> SELECT LOG10(-100);
 *         -> NULL
 * </pre>
 * <p>
 * LOG10(X) is equivalent to LOG(10,X).
 *
 * @author jianghang 2014-4-14 下午10:40:54
 * @since 5.0.7
 */
public class Log10 extends AbstractScalarFunction {

    public Log10(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        Double d = DataTypes.DoubleType.convertFrom(args[0]);
        Long l = d.longValue();
        if (l <= 0) {
            return null;
        }
        return Math.log10(d);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LOG10"};
    }
}

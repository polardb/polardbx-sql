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
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * <pre>
 * Returns the value of π (pi). The default number of decimal places displayed is seven, but MySQL uses the full double-precision value internally.
 *
 * mysql> SELECT PI();
 *         -> 3.141593
 * mysql> SELECT PI()+0.000000000000000000;
 *         -> 3.141592653589793116
 * </pre>
 *
 * @author jianghang 2014-4-14 下午10:44:26
 * @since 5.0.7
 */
public class Pi extends AbstractScalarFunction {
    public Pi(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return Math.PI;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"PI"};
    }
}

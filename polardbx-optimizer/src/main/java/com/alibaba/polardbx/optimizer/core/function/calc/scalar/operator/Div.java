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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.operator;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * 对应mysql的DIV函数，区别于/的除法
 *
 * <pre>
 * Integer division. Similar to FLOOR(), but is safe with BIGINT values.
 * In MySQL 5.6, if either operand has a noninteger type,
 * the operands are converted to DECIMAL and divided using DECIMAL arithmetic before converting the result to BIGINT.
 * If the result exceeds BIGINT range, an error occurs.
 *
 * mysql> SELECT 5 DIV 2;
 *         -> 2
 * </pre>
 *
 * @author jianghang 2014-2-13 上午11:55:29
 * @since 5.0.0
 */
public class Div extends AbstractScalarFunction {
    public Div(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        return resultType.getCalculator().divide(args[0], args[1]);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DIV"};
    }
}

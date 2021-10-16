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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.cast;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * The BINARY operator casts the string following it to a binary string. This is
 * an easy way to force a column comparison to be done byte by byte rather than
 * character by character. This causes the comparison to be case sensitive even
 * if the column is not defined as BINARY or BLOB. BINARY also causes trailing
 * spaces to be significant.
 *
 * @author jianghang 2014-7-1 上午11:08:14
 * @since 5.1.6
 */
public class Binary extends AbstractScalarFunction {
    public Binary(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        return resultType.convertFrom(args[0]);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "BINARY" };
    }
}

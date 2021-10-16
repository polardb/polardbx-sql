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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;
import java.util.Optional;

public class Charset extends AbstractScalarFunction {
    public Charset(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object arg = args[0];
        if (FunctionUtils.isNull(arg)) {
            return null;
        }
        return Optional.ofNullable(operandTypes)
            .map(types -> types.get(0))
            .map(DataType::getCharsetName)
            .map(Enum::name)
            .map(String::toLowerCase)
            .orElseGet(
                () -> Optional.ofNullable(ec.getEncoding())
                    .map(CharsetName::of)
                    .map(Enum::name)
                    .map(String::toLowerCase)
                    .orElse("utf8mb4")
            );
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CHARSET"};
    }
}

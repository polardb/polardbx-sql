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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Created by chuanqin on 18/3/26.
 */
public class NotBetween extends AbstractCollationScalarFunction {
    public NotBetween(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }
        DataType type = getCompareType();
        if ((args[2] == null && type.compare(args[0], args[1]) >= 0)
            || (args[1] == null && type.compare(args[0], args[2]) <= 0)) {
            return null;
        }
        return (type.compare(args[0], args[1]) < 0) || (type.compare(args[0], args[2]) > 0);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "NOT BETWEEN ASYMMETRIC" };
    }
}

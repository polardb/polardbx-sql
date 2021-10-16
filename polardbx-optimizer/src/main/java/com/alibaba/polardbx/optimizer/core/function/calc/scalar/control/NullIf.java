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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.control;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Returns NULL if expr1 = expr2 is true, otherwise returns expr1. This is the
 * same as CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END.
 *
 * <pre>
 * mysql> SELECT NULLIF(1,1);
 *         -> NULL
 * mysql> SELECT NULLIF(1,2);
 *         -> 1
 * </pre>
 * <p>
 * Note that MySQL evaluates expr1 twice if the arguments are not equal.
 *
 * @author jianghang 2014-4-15 上午11:01:37
 * @since 5.0.7
 */
public class NullIf extends AbstractCollationScalarFunction {
    public NullIf(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        } else if (FunctionUtils.isNull(args[1])) {
            return args[0];
        } else {
            DataType type = getCompareType();
            Object t1 = type.convertFrom(args[0]);
            Object t2 = type.convertFrom(args[1]);

            if (type.compare(t1, t2) == 0) {
                return null;
            } else {
                return args[0];
            }
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "NULLIF" };
    }
}

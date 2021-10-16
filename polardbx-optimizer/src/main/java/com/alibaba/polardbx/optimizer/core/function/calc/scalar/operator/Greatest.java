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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * With two or more arguments, returns the largest (maximum-valued) argument.
 * The arguments are compared using the same rules as for LEAST().
 *
 * <pre>
 * mysql> SELECT GREATEST(2,0);
 *         -> 2
 * mysql> SELECT GREATEST(34.0,3.0,5.0,767.0);
 *         -> 767.0
 * mysql> SELECT GREATEST('B','A','C');
 *         -> 'C'
 * </pre>
 *
 * @author jianghang 2014-4-21 下午6:00:26
 * @since 5.0.7
 */
public class Greatest extends AbstractScalarFunction {

    private DataType compareType;

    public Greatest(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
        this.compareType = resultType;
        if (operandTypes != null) {
            for (DataType operandType : operandTypes) {
                if (DataTypeUtil.isDateType(operandType)) {
                    compareType = operandType;
                    break;
                }
            }
        }
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        Object first = resultType.convertFrom(args[0]);
        Object max = first;
        for (int i = 1; i < args.length; i++) {
            Object cp = compareType.convertFrom(args[i]);
            max = (compareType.compare(max, cp) >= 0 ? max : cp);
        }

        return resultType.convertFrom(max);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"GREATEST"};
    }
}

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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.math.BigInteger;
import java.util.List;

/**
 * <pre>
 *  Returns a string representation of the octal value of N, where N is a longlong (BIGINT) number. This is equivalent to CONV(N,10,8). Returns NULL if N is NULL.
 *
 * mysql> SELECT OCT(12);
 *         -> '14'
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午6:42:47
 * @since 5.1.0
 */
public class Oct extends AbstractScalarFunction {
    public Oct(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"OCT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object arg = args[0];
        if (FunctionUtils.isNull(arg)) {
            return null;
        }

        BigInteger longlong = (BigInteger) DataTypes.ULongType.convertJavaFrom(arg);
        if (longlong == null) {
            return "0";
        }

        return longlong.toString(8);
    }
}

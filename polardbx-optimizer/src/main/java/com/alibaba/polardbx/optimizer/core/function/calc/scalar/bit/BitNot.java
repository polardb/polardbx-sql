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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.bit;

import com.google.common.primitives.UnsignedLong;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.NumberType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * 对应mysql的bitNot '~'
 *
 * @author jianghang 2014-2-13 上午11:55:29
 * @since 5.0.0
 */
public class BitNot extends AbstractScalarFunction {
    public BitNot(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        if (resultType instanceof NumberType) {
            Long i1 = DataTypes.LongType.convertFrom(args[0]);
            String bits = Long.toBinaryString(~i1);
            UnsignedLong unsignedInteger = UnsignedLong.valueOf(bits, 2);
            return resultType.convertFrom(unsignedInteger.bigIntegerValue());
        } else {
            return resultType.getCalculator().bitNot(args[0]);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"~"};
    }
}

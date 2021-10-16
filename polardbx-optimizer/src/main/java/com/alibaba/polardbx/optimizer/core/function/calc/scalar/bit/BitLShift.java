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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.math.BigInteger;
import java.util.List;

/**
 * <pre>
 * Shifts a longlong (BIGINT) number to the left.
 *
 * mysql> SELECT 1 << 2;
 *         -> 4
 * </pre>
 *
 * @author jianghang 2014-2-13 下午1:13:41
 * @since 5.0.0
 */
public class BitLShift extends AbstractScalarFunction {
    public BitLShift(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        BigInteger o1 = (BigInteger)DataTypes.ULongType.convertJavaFrom(args[0]);
        Integer o2 = DataTypes.IntegerType.convertFrom(args[1]);
        // <0 means that o2 has overflowed.
        if (o2 > 63 || o2 < 0) {
            return 0;
        }
        return o1.shiftLeft(o2);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"<<"};
    }
}

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
 * Shifts a longlong (BIGINT) number to the right.
 *
 * mysql> SELECT 4 >> 2;
 *         -> 1
 * </pre>
 *
 * @author jianghang 2014-2-13 下午1:13:06
 * @since 5.0.0
 */
public class BitRShift extends AbstractScalarFunction {
    public BitRShift(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        BigInteger o1 = (BigInteger) DataTypes.ULongType.convertJavaFrom(args[0]);
        BigInteger o2 = (BigInteger) DataTypes.ULongType.convertJavaFrom(args[1]);
        if (o2.compareTo(BigInteger.valueOf(63)) == 1) {
            return 0;
        }
        return o1.shiftRight(o2.intValue());
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {">>"};
    }
}

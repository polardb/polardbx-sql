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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.math.BigInteger;
import java.util.List;

/**
 * <pre>
 * ORD(str)
 *
 * If the leftmost character of the string str is a multi-byte character, returns the code for that character, calculated from the numeric values of its constituent bytes using this formula:
 *
 *   (1st byte code)
 * + (2nd byte code * 256)
 * + (3rd byte code * 2562) ...
 *
 * If the leftmost character is not a multi-byte character, ORD() returns the same value as the ASCII() function.
 *
 * mysql> SELECT ORD('2');
 *         -> 50
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午5:16:15
 * @since 5.1.0
 */
public class Ord extends AbstractScalarFunction {
    public Ord(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ORD"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object arg = args[0];

        if (FunctionUtils.isNull(arg)) {
            return null;
        }

        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        if (TStringUtil.isEmpty(str)) {
            return 0;
        }

        Character leftMost = str.charAt(0);
        byte[] bytes = leftMost.toString().getBytes();
        BigInteger value = BigInteger.valueOf(0l);
        for (int i = 0; i < bytes.length; i++) {
            value = value.add(BigInteger.valueOf(bytes[i] & 0xff).pow(i + 1));
        }

        return value;
    }
}

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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.math;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Converts numbers between different number bases. Returns a string
 * representation of the number N, converted from base from_base to base
 * to_base. Returns NULL if any argument is NULL. The argument N is interpreted
 * as an integer, but may be specified as an integer or a string. The minimum
 * base is 2 and the maximum base is 36. If to_base is a negative number, N is
 * regarded as a signed number. Otherwise, N is treated as unsigned. CONV()
 * works with 64-bit precision.
 *
 * <pre>
 * mysql> SELECT CONV('a',16,2);
 *         -> '1010'
 * mysql> SELECT CONV('6E',18,8);
 *         -> '172'
 * mysql> SELECT CONV(-17,10,-18);
 *         -> '-H'
 * mysql> SELECT CONV(10+'10'+'10'+0xa,10,10);
 *         -> '40'
 * </pre>
 *
 * @author jianghang 2014-4-14 下午9:52:58
 * @since 5.0.7
 */
public class Conv extends AbstractScalarFunction {
    public Conv(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    // The maximum possible number with the given (radix - 2).
    private static final char[] MAX_NUMBER = "123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        String n = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        Integer f = DataTypes.IntegerType.convertFrom(args[1]);
        Integer t = DataTypes.IntegerType.convertFrom(args[2]);

        int unsignedF = Math.abs(f);
        int unsignedT = Math.abs(t);
        // check the boundaries of from_base and to_base.
        if (unsignedF > Character.MAX_RADIX || unsignedF < Character.MIN_RADIX || unsignedT > Character.MAX_RADIX
            || unsignedT < Character.MIN_RADIX) {
            return null;
        }

        // Truncate the invalid char
        n = n.toLowerCase();
        int truncatedIndex = -1;
        final char maxChar = MAX_NUMBER[unsignedF - 2];
        for (int i = 0; i < n.length(); i++) {
            char ch = n.charAt(i);
            if (ch == '-' && i == 0) {
                continue;
            }
            if (ch > maxChar || ch < '0' || (ch > '9' && ch < 'a')) {
                truncatedIndex = i;
                break;
            }
        }
        if (truncatedIndex >= 0) {
            n = n.substring(0, truncatedIndex);
            if ("-".equals(n)) {
                return null;
            }
            // empty string should return 0 instead of null
            if ("".equals(n)) {
                return "0";
            }
        }

        Object result;
        Long d = Long.parseLong(n, unsignedF);

        if (t < 0) {
            result = Long.toString(d, unsignedT).toUpperCase();
        } else {
            result = Long.toUnsignedString(d, unsignedT).toUpperCase();
        }

        return result;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CONV"};
    }
}

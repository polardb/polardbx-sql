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
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * MAKE_SET(bits,str1,str2,...)
 *
 * Returns a set value (a string containing substrings separated by “,” characters) consisting of the strings that have the corresponding bit in bits set. str1 corresponds to bit 0, str2 to bit 1, and so on. NULL values in str1, str2, ... are not appended to the result.
 *
 * mysql> SELECT MAKE_SET(1,'a','b','c');
 *         -> 'a'
 * mysql> SELECT MAKE_SET(1 | 4,'hello','nice','world');
 *         -> 'hello,world'
 * mysql> SELECT MAKE_SET(1 | 4,'hello','nice',NULL,'world');
 *         -> 'hello'
 * mysql> SELECT MAKE_SET(0,'a','b','c');
 *         -> ''
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午6:51:11
 * @since 5.1.0
 */
public class MakeSet extends AbstractScalarFunction {
    public MakeSet(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"MAKE_SET"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }
        BigInteger bitsValue = (BigInteger)DataTypes.ULongType.convertJavaFrom(args[0]);
        String bitsStringReverse = TStringUtil.reverse(bitsValue.toString(2));
        List<String> sets = new ArrayList();
        for (int i = 1; i < args.length && i - 1 < bitsStringReverse.length(); i++) {

            if (FunctionUtils.isNull(args[i])) {
                continue;
            }

            if (bitsStringReverse.charAt(i - 1) == '1') {
                sets.add(DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]));
            }
        }

        return TStringUtil.join(sets, ",");
    }
}

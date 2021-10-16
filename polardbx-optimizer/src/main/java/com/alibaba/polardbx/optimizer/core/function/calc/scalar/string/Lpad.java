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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;

/**
 * <pre>
 * LPAD(str,len,padstr)
 *
 * Returns the string str, left-padded with the string padstr to a length of len characters. If str is longer than len, the return value is shortened to len characters.
 *
 * mysql> SELECT LPAD('hi',4,'??');
 *         -> '??hi'
 * mysql> SELECT LPAD('hi',1,'??');
 *         -> 'h'
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午6:18:14
 * @since 5.1.0
 */
public class Lpad extends AbstractScalarFunction {
    public Lpad(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LPAD"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        Integer len = DataTypes.IntegerType.convertFrom(args[1]);
        String padStr = DataTypeUtil.convert(operandTypes.get(2), DataTypes.StringType, args[2]);

        if (len == str.length()) {
            return str;
        }

        if (len < 0) {
            return null;
        }
        if (len < str.length()) {
            return str.substring(0, len);
        }

        return TStringUtil.leftPad(str, len, padStr);
    }
}

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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * <pre>
 * RIGHT(str,len)
 *
 * Returns the rightmost len characters from the string str, or NULL if any argument is NULL.
 *
 * mysql> SELECT RIGHT('foobarbar', 4);
 *         -> 'rbar'
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午6:11:40
 * @since 5.1.0
 */
public class Right extends AbstractScalarFunction {
    public Right(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"RIGHT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        Long len = DataTypes.LongType.convertFrom(args[1]);
        int effectLen;
        if (len < 0) {
            return "";
        }
        int length = str.length();
        if (len > length) {
            effectLen = length;
        } else {
            effectLen = len.intValue();
        }
        return str.substring(str.length() - effectLen, str.length());
    }
}

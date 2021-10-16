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

import java.util.List;

/**
 * <pre>
 * ASCII(str)
 *
 * Returns the numeric value of the leftmost character of the string str. Returns 0 if str is the empty string. Returns NULL if str is NULL. ASCII() works for 8-bit characters.
 *
 * mysql> SELECT ASCII('2');
 *         -> 50
 * mysql> SELECT ASCII(2);
 *         -> 50
 * mysql> SELECT ASCII('dx');
 *         -> 100
 *
 * See also the ORD() function.
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午1:33:36
 * @since 5.1.0
 */
public class Ascii extends AbstractScalarFunction {
    public Ascii(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ASCII"};
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

        char leftMost = str.charAt(0);
        return ((int) ((byte) leftMost)) & 0xFF;
    }

}

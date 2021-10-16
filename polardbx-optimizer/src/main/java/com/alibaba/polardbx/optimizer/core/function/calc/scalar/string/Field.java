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
 * FIELD(str,str1,str2,str3,...)
 *
 * Returns the index (position) of str in the str1, str2, str3, ... list. Returns 0 if str is not found.
 *
 * All arguments are compared as strings.
 *
 * If str is NULL, the return value is 0 because NULL fails equality comparison with any value. FIELD() is the complement of ELT().
 *
 * mysql> SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo');
 *         -> 2
 * mysql> SELECT FIELD('fo', 'Hej', 'ej', 'Heja', 'hej', 'foo');
 *         -> 0
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午2:18:25
 * @since 5.1.0
 */
public class Field extends AbstractScalarFunction {
    public Field(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"FIELD"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return 0;
        }

        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);

        for (int i = 1; i < args.length; i++) {
            if (FunctionUtils.isNull(args[i])) {
                continue;
            }

            if (TStringUtil.equals(str, DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]))) {
                return i;
            }
        }
        return 0;
    }
}

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
 * INSERT(str,pos,len,newstr)
 *
 * Returns the string str, with the substring beginning at position pos and len characters long replaced by the string newstr. Returns the original string if pos is not within the length of the string. Replaces the rest of the string from position pos if len is not within the length of the rest of the string. Returns NULL if any argument is NULL.
 *
 * mysql> SELECT INSERT('Quadratic', 3, 4, 'What');
 *         -> 'QuWhattic'
 * mysql> SELECT INSERT('Quadratic', -1, 4, 'What');
 *         -> 'Quadratic'
 * mysql> SELECT INSERT('Quadratic', 3, 100, 'What');
 *         -> 'QuWhat'
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午4:17:42
 * @since 5.1.0
 */
public class Insert extends AbstractScalarFunction {
    public Insert(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"INSERT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;

            }
        }

        String str1 = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        Integer pos = DataTypes.IntegerType.convertFrom(args[1]);
        Integer len = DataTypes.IntegerType.convertFrom(args[2]);
        String str2 = DataTypeUtil.convert(operandTypes.get(3), DataTypes.StringType, args[3]);

        if (pos <= 0 || pos > str1.length()) {
            return str1;
        }
        StringBuilder newStr = new StringBuilder();
        if (pos + len > str1.length()) {

            newStr.append(str1.substring(0, pos - 1));
            newStr.append(str2);

            return newStr.toString();
        } else {
            newStr.append(str1.substring(0, pos - 1));
            newStr.append(str2);
            newStr.append(str1.substring(pos + len - 1, str1.length()));

            return newStr.toString();
        }
    }
}

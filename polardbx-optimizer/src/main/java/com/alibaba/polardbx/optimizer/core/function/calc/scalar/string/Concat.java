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
 * CONCAT(str1,str2,...)
 *
 * Returns the string that results from concatenating the arguments. May have one or more arguments. If all arguments are nonbinary strings, the result is a nonbinary string. If the arguments include any binary strings, the result is a binary string. A numeric argument is converted to its equivalent string form. This is a nonbinary string as of MySQL 5.5.3. Before 5.5.3, it is a binary string; to to avoid that and produce a nonbinary string, you can use an explicit type cast, as in this example:
 *
 * SELECT CONCAT(CAST(int_col AS CHAR), char_col);
 *
 * CONCAT() returns NULL if any argument is NULL.
 *
 * mysql> SELECT CONCAT('My', 'S', 'QL');
 *         -> 'MySQL'
 * mysql> SELECT CONCAT('My', NULL, 'QL');
 *         -> NULL
 * mysql> SELECT CONCAT(14.3);
 *         -> '14.3'
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午1:27:46
 * @since 5.1.0
 */
public class Concat extends AbstractScalarFunction {
    public Concat(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CONCAT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        StringBuilder str = new StringBuilder();

        for (int i = 0; i < args.length; i++) {
            String argStr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
            if (!TStringUtil.isEmpty(argStr)) {
                str.append(argStr);
            }
        }
        return str.toString();
    }
}

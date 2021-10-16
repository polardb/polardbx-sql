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
 * SUBSTRING(str,pos), SUBSTRING(str FROM pos), SUBSTRING(str,pos,len), SUBSTRING(str FROM pos FOR len)
 *
 * The forms without a len argument return a substring from string str starting at position pos. The forms with a len argument return a substring len characters long from string str, starting at position pos. The forms that use FROM are standard SQL syntax. It is also possible to use a negative value for pos. In this case, the beginning of the substring is pos characters from the end of the string, rather than the beginning. A negative value may be used for pos in any of the forms of this function.
 *
 * For all forms of SUBSTRING(), the position of the first character in the string from which the substring is to be extracted is reckoned as 1.
 *
 * mysql> SELECT SUBSTRING('Quadratically',5);
 *         -> 'ratically'
 * mysql> SELECT SUBSTRING('foobarbar' FROM 4);
 *         -> 'barbar'
 * mysql> SELECT SUBSTRING('Quadratically',5,6);
 *         -> 'ratica'
 * mysql> SELECT SUBSTRING('Sakila', -3);
 *         -> 'ila'
 * mysql> SELECT SUBSTRING('Sakila', -5, 3);
 *         -> 'aki'
 * mysql> SELECT SUBSTRING('Sakila' FROM -4 FOR 2);
 *         -> 'ki'
 *
 * If len is less than 1, the result is the empty string.
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午1:39:12
 * @since 5.1.0
 */
public class SubString extends AbstractScalarFunction {
    public SubString(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SUBSTRING"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object argv : args) {
            if (FunctionUtils.isNull(argv)) {
                return null;
            }
        }

        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);

        Integer pos = DataTypes.IntegerType.convertFrom(args[1]);

        Integer len = null;

        if (args.length == 3) {
            len = DataTypes.IntegerType.convertFrom(args[2]);
        }

        if (pos == 0) {
            return "";
        }

        if (pos < 0) {
            pos = str.length() + pos + 1;
        }
        if (len == null) {
            return TStringUtil.substring(str, pos - 1);
        } else {

            if (len < 1) {
                return "";

            } else {
                return TStringUtil.substring(str, pos - 1, pos - 1 + len);
            }
        }
    }
}

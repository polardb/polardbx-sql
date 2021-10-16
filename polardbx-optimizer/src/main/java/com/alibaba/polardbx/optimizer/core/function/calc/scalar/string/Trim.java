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
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * <pre>
 * TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)
 *
 * Returns the string str with all remstr prefixes or suffixes removed. If none of the specifiers BOTH, LEADING, or TRAILING is given, BOTH is assumed. remstr is optional and, if not specified, spaces are removed.
 *
 * mysql> SELECT TRIM('  bar   ');
 *         -> 'bar'
 * mysql> SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');
 *         -> 'barxxx'
 * mysql> SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');
 *         -> 'bar'
 * mysql> SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');
 *         -> 'barx'
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午4:07:06
 * @since 5.1.0
 */
public class Trim extends AbstractScalarFunction {
    public Trim(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TRIM"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);

        if (args.length == 2) {
            str = TStringUtil.trim(str);
        }

        String trimStr = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
        if (args.length == 3 && trimStr != null && trimStr.length() > 0) {
            String direction = DataTypeUtil.convert(operandTypes.get(2), DataTypes.StringType, args[2]);

            if ("BOTH".equals(direction)) {
                while (str.endsWith(trimStr)) {
                    str = TStringUtil.removeEnd(str, trimStr);
                }

                while (str.startsWith(trimStr)) {
                    str = TStringUtil.removeStart(str, trimStr);
                }
            } else if ("TRAILING".equals(direction)) {
                while (str.endsWith(trimStr)) {
                    str = TStringUtil.removeEnd(str, trimStr);
                }

            } else if ("LEADING".equals(direction)) {

                while (str.startsWith(trimStr)) {
                    str = TStringUtil.removeStart(str, trimStr);
                }

            }

        }

        return str;
    }
}

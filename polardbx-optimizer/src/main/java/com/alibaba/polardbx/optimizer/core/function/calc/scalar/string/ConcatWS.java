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

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * CONCAT_WS(separator,str1,str2,...)
 *
 * CONCAT_WS() stands for Concatenate With Separator and is a special form of CONCAT(). The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is NULL, the result is NULL.
 *
 * mysql> SELECT CONCAT_WS(',','First name','Second name','Last Name');
 *         -> 'First name,Second name,Last Name'
 * mysql> SELECT CONCAT_WS(',','First name',NULL,'Last Name');
 *         -> 'First name,Last Name'
 *
 * CONCAT_WS() does not skip empty strings. However, it does skip any NULL values after the separator argument.
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午1:26:56
 * @since 5.1.0
 */
public class ConcatWS extends AbstractScalarFunction {
    public ConcatWS(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CONCAT_WS"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        Object arg0 = args[0];

        if (FunctionUtils.isNull(arg0)) {
            return null;
        }

        String sep = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);

        List<String> strs = new ArrayList(args.length - 1);

        for (int i = 1; i < args.length; i++) {
            if (FunctionUtils.isNull(args[i])) {
                continue;
            }
            String argStr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
            strs.add(argStr);
        }
        return TStringUtil.join(strs, sep);
    }
}

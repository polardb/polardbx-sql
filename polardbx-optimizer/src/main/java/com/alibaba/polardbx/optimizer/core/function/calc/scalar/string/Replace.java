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
 * REPLACE(str,from_str,to_str)
 *
 * Returns the string str with all occurrences of the string from_str replaced by the string to_str. REPLACE() performs a case-sensitive match when searching for from_str.
 *
 * mysql> SELECT REPLACE('www.mysql.com', 'w', 'Ww');
 *         -> 'WwWwWw.mysql.com'
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午5:32:36
 * @since 5.1.0
 */
public class Replace extends AbstractScalarFunction {
    public Replace(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"REPLACE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        String fromStr = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
        String toStr = DataTypeUtil.convert(operandTypes.get(2), DataTypes.StringType, args[2]);

        return TStringUtil.replace(str, fromStr, toStr);
    }
}

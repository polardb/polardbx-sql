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
 * CHAR_LENGTH(str)
 *
 * Returns the length of the string str, measured in characters. A multi-byte character counts as a single character. This means that for a string containing five 2-byte characters, LENGTH() returns 10, whereas CHAR_LENGTH() returns 5.
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午5:18:53
 * @since 5.1.0
 */
public class CharLength extends AbstractScalarFunction {
    public CharLength(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CHARACTER_LENGTH", "CHAR_LENGTH"};
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

        return str.length();
    }

}

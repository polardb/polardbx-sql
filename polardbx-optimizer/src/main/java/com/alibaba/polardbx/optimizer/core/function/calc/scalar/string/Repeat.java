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
 * REPEAT(str,count)
 *
 * Returns a string consisting of the string str repeated count times. If count is less than 1, returns an empty string. Returns NULL if str or count are NULL.
 *
 * mysql> SELECT REPEAT('MySQL', 3);
 *         -> 'MySQLMySQLMySQL'
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午6:39:43
 * @since 5.1.0
 */
public class Repeat extends AbstractScalarFunction {
    public Repeat(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    /**
     * The max length of the value returned by function REPEAT
     */
    private static final Long REPEAT_MAX_RETURN_LENGTH = 1024 * 1024 * 16L;

    @Override
    public String[] getFunctionNames() {
        return new String[] {"REPEAT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        Integer count = DataTypes.IntegerType.convertFrom(args[1]);
        if (count == null || count < 1) {
            return "";
        }
        if (str != null && count * str.length() > REPEAT_MAX_RETURN_LENGTH) {
            return null;
        }

        return TStringUtil.repeat(str, count);
    }
}

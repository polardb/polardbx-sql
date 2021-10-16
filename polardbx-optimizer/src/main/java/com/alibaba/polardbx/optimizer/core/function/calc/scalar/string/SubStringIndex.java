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
 * SUBSTRING_INDEX(str,delim,count)
 *
 * Returns the substring from string str before count occurrences of the delimiter delim. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned. SUBSTRING_INDEX() performs a case-sensitive match when searching for delim.
 *
 * mysql> SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);
 *         -> 'www.mysql'
 * mysql> SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);
 *         -> 'mysql.com'
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午2:02:15
 * @since 5.1.0
 */
public class SubStringIndex extends AbstractScalarFunction {
    public SubStringIndex(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SUBSTRING_INDEX"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object argv : args) {
            if (FunctionUtils.isNull(argv)) {
                return null;
            }
        }

        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        String delim = DataTypes.StringType.convertFrom(args[1]);
        Integer count = DataTypes.IntegerType.convertFrom(args[2]);
        if (count == 0) {
            return "";
        } else if (count > 0) {
            Integer len = TStringUtil.ordinalIndexOf(str, delim, count);
            if (len == -1) {
                return str;
            }
            return TStringUtil.substring(str, 0, len);
        } else {
            count = -count;
            Integer pos = TStringUtil.lastOrdinalIndexOf(str, delim, count);
            return TStringUtil.substring(str, pos + 1);
        }
    }
}

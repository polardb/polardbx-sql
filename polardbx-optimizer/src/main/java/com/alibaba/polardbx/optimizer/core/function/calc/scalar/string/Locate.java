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
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;
import java.util.Optional;

/**
 * <pre>
 * LOCATE(substr,str), LOCATE(substr,str,pos)
 *
 * The first syntax returns the position of the first occurrence of substring substr in string str. The second syntax returns the position of the first occurrence of substring substr in string str, starting at position pos. Returns 0 if substr is not in str.
 *
 * mysql> SELECT LOCATE('bar', 'foobarbar');
 *         -> 4
 * mysql> SELECT LOCATE('xbar', 'foobar');
 *         -> 0
 * mysql> SELECT LOCATE('bar', 'foobarbar', 5);
 *         -> 7
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午2:37:28
 * @since 5.1.0
 */
public class Locate extends AbstractCollationScalarFunction {
    public Locate(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LOCATE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        String subStr = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        String str = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);

        Integer pos = null;
        if (args.length == 3) {
            pos = DataTypes.IntegerType.convertFrom(args[2]);
        }

        // fix the position
        if (pos != null) {
            if (pos <= 0 || pos > str.length()) {
                return 0;
            }
            str = str.substring(pos - 1);
        } else {
            pos = 1;
        }

        // use instr function of collation handler.
        String finalStr = str;
        Integer finalPos = pos;
        return Optional.ofNullable(getCompareType())
            .filter(SliceType.class::isInstance)
            .map(SliceType.class::cast)
            .map(t -> t.instr(finalStr, subStr))
            .map(index -> index == 0 ? 0 : index + finalPos - 1)
            .orElseGet(
                () -> Optional.ofNullable(TStringUtil.indexOf(finalStr, subStr))
                    .map(idx -> idx == -1 ? 0 : idx + finalPos)
                    .orElse(0)
            );
    }
}

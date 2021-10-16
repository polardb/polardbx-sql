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
 * INSTR(str,substr)
 *  Returns the position of the first occurrence of substring substr in string str. This is the same as the two-argument form of LOCATE(), except that the order of the arguments is reversed.
 *
 * mysql> SELECT INSTR('foobarbar', 'bar');
 *         -> 4
 * mysql> SELECT INSTR('xbar', 'foobar');
 *         -> 0
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午4:56:34
 * @since 5.1.0
 */
public class Instr extends AbstractCollationScalarFunction {
    public Instr(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"INSTR"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        String str = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        String subStr = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);

        return Optional.ofNullable(getCompareType())
            .filter(SliceType.class::isInstance)
            .map(SliceType.class::cast)
            .map(t -> t.instr(str, subStr))
            .orElseGet(
                () -> TStringUtil.indexOf(str, subStr) + 1
            );
    }
}

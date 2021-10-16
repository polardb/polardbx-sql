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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * <pre>
 * ELT(N,str1,str2,str3,...)
 *
 * ELT() returns the Nth element of the list of strings: str1 if N = 1, str2 if N = 2, and so on. Returns NULL if N is less than 1 or greater than the number of arguments. ELT() is the complement of FIELD().
 *
 * mysql> SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo');
 *         -> 'ej'
 * mysql> SELECT ELT(4, 'ej', 'Heja', 'hej', 'foo');
 *         -> 'foo'
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月11日 下午1:36:05
 * @since 5.1.0
 */
public class Elt extends AbstractScalarFunction {
    public Elt(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ELT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        Integer index = DataTypes.IntegerType.convertFrom(args[0]);

        if (index < 1 || index > args.length - 1) {
            return null;
        }

        Object resEle = args[index];

        if (FunctionUtils.isNull(resEle)) {
            return null;
        }

        return DataTypeUtil.convert(operandTypes.get(index), DataTypes.StringType, args[index]);
    }
}

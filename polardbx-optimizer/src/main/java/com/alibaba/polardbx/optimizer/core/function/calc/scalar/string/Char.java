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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * http://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_char
 *
 * @author jianghang 2014-4-9 下午6:48:23
 * @since 5.0.7
 */
public class Char extends AbstractScalarFunction {
    public Char(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CHAR"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        List<Character> chs = new ArrayList<Character>();
        for (Object obj : args) {
            if (!FunctionUtils.isNull(obj)) {
                Long data = DataTypes.LongType.convertFrom(obj);
                chs.addAll(appendChars(data));
            }
        }

        if (chs.size() == 0) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        for (Character ch : chs) {
            builder.append(ch.charValue());
        }

        return builder.toString();
    }

    private List<Character> appendChars(long data) {
        // Limit input value as unsigned int
        data = Math.min(Math.max(data, 0L), 0xffffffffL);

        List<Character> chs = new ArrayList<Character>();
        do {
            Character ch = (char) (data % 256);
            chs.add(ch);
        } while ((data >>= 8) != 0);

        if (chs.size() <= 1) {
            return chs;
        }

        List<Character> result = new ArrayList<Character>();
        for (int i = chs.size() - 1; i >= 0; i--) {
            result.add(chs.get(i));
        }
        return result;
    }
}

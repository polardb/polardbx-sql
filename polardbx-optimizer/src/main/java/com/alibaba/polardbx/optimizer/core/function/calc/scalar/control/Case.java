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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.control;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * <pre>
 * The first version returns the result where value=compare_value. The second version returns the result for the first condition that is true. If there was no matching result value, the result after ELSE is returned, or NULL if there is no ELSE part.
 *
 * mysql> SELECT CASE 1 WHEN 1 THEN 'one'
 *     ->     WHEN 2 THEN 'two' ELSE 'more' END;
 *         -> 'one'
 * mysql> SELECT CASE WHEN 1>0 THEN 'true' ELSE 'false' END;
 *         -> 'true'
 * mysql> SELECT CASE BINARY 'B'
 *     ->     WHEN 'a' THEN 1 WHEN 'b' THEN 2 END;
 *         -> NULL
 * </pre>
 * <p>
 * The return type of a CASE expression is the compatible aggregated type of all
 * return values, but also depends on the context in which it is used. If used
 * in a string context, the result is returned as a string. If used in a numeric
 * context, the result is returned as a decimal, real, or integer value.
 *
 * @author jianghang 2014-4-15 上午11:22:01
 * @since 5.0.7
 */
public class Case extends AbstractScalarFunction {
    public Case(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Boolean existComparee = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(args[0]));
        Boolean existElse = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(args[args.length - 2]));

        DataType type = null;
        Object comparee = null;
        if (existComparee) {
            comparee = operandTypes.get(0).convertFrom(args[1]);
        }

        int size = args.length;
        if (existElse) {
            size -= 2;
        }

        for (int i = 2; i < size; i += 2) {
            if (existComparee) {
                Object when = type.convertFrom(args[i]);
                Object then = args[i + 1];
                if (comparee == null && when == null) {
                    return then;
                } else if (comparee != null && comparee.equals(when)) {
                    return then;
                }
            } else {
                // 不存在comparee
                Boolean when = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(args[i]));
                if (when) {
                    return args[i + 1];
                }
            }
        }

        if (existElse) {
            return args[args.length - 1];
        } else {
            return null;
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CASE_WHEN"};
    }
}

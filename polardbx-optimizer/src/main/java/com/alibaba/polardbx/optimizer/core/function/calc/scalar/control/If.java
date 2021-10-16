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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * If expr1 is TRUE (expr1 <> 0 and expr1 <> NULL) then IF() returns expr2;
 * otherwise it returns expr3. IF() returns a numeric or string value, depending
 * on the context in which it is used.
 *
 * <pre>
 * mysql> SELECT IF(1>2,2,3);
 *         -> 3
 * mysql> SELECT IF(1<2,'yes','no');
 *         -> 'yes'
 * mysql> SELECT IF(STRCMP('test','test1'),'no','yes');
 *         -> 'no'
 * </pre>
 * <p>
 * If only one of expr2 or expr3 is explicitly NULL, the result type of the IF()
 * function is the type of the non-NULL expression.
 *
 * @author jianghang 2014-4-15 上午10:57:54
 * @since 5.0.7
 */
public class If extends AbstractScalarFunction {
    public If(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args[0] == null) {
            return args[2];
        }

        Long f = DataTypes.LongType.convertFrom(args[0]);
        if (f != 0) {
            return args[1];
        } else {
            return args[2];
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"IF"};
    }
}

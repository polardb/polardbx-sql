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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.operator;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * INTERVAL(N,N1,N2,N3,...) <br/>
 * Returns 0 if N < N1, 1 if N < N2 and so on or -1 if N is NULL. All arguments
 * are treated as integers. It is required that N1 < N2 < N3 < ... < Nn for this
 * function to work correctly. This is because a binary search is used (very
 * fast).
 *
 * <pre>
 * mysql> SELECT INTERVAL(23, 1, 15, 17, 30, 44, 200);
 *         -> 3
 * mysql> SELECT INTERVAL(10, 1, 10, 100, 1000);
 *         -> 2
 * mysql> SELECT INTERVAL(22, 23, 30, 44, 200);
 *         -> 0
 * </pre>
 *
 * @author jianghang 2014-4-21 下午4:55:16
 * @since 5.0.7
 */
public class Interval extends AbstractScalarFunction {
    public Interval(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return -1;
        }

        Long n = DataTypes.LongType.convertFrom(args[0]);
        for (int i = 1; i < args.length; i++) {
            if (!FunctionUtils.isNull(args[i])) {
                Long p = DataTypes.LongType.convertFrom(args[i]);
                if (n < p) {
                    return i - 1;
                }
            }
        }

        return args.length - 1;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"INTERVAL"};
    }
}

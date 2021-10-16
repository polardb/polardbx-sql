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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.math;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;
import java.util.Random;

public class Rand extends AbstractScalarFunction {
    public Rand(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length > 0) {
            if (!FunctionUtils.isNull(args[0])) {
                Long d = DataTypes.LongType.convertFrom(args[0]);
                Random rand = new Random(d);
                return rand.nextDouble();
            } else {
                Random rand = new Random(0);
                return rand.nextDouble();
            }
        } else {
            Random rand = new Random();
            return rand.nextDouble();
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"RAND"};
    }
}

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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.bean.LobVal;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * 对应select 1中的1常量
 *
 * @author jianghang 2014-4-15 上午11:33:08
 * @since 5.0.7
 */
public class Constant extends AbstractScalarFunction {

    public Constant(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CONSTANT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        if (args[0] instanceof LobVal) {
            LobVal lob = (LobVal) args[0];
            return lob.evaluation();
        }
        return args[0];
    }

}

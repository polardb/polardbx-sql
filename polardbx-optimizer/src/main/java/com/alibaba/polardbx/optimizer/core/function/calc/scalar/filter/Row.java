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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter;

import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.RowType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @since 5.0.0
 */
public class Row extends AbstractCollationScalarFunction {
    public Row(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, new RowType(operandTypes));
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        List<Object> list = new ArrayList<>(args.length);
        for (Object arg : args) {
            list.add(arg);
        }
        return new RowValue(list);
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.RowType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ROW"};
    }
}

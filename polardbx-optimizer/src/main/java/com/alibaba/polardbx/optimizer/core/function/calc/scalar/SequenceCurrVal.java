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

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

public class SequenceCurrVal extends AbstractScalarFunction {

    public SequenceCurrVal(List<DataType> operandTypes,
                              DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        int argSize = args.length;
        String schemnName = null;
        String seqName = null;
        if (argSize == 2) {
            String tempSchema = ec != null ? ec.getSchemaName() : null;
            schemnName = OptimizerContext.getContext(tempSchema).getSchemaName();
            seqName = DataTypes.StringType.convertFrom(args[0]);
        } else if (argSize == 3) {
            schemnName = DataTypes.StringType.convertFrom(args[0]);
            seqName = DataTypes.StringType.convertFrom(args[1]);
        }

        return SequenceManagerProxy.getInstance().currValue(schemnName, seqName);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"CURRVAL"};
    }

}

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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Created by chuanqin on 18/3/21.
 */
public class Between extends AbstractCollationScalarFunction {

    private DataType firstComType;
    private DataType secondComType;
    private DataType commonComType;

    public Between(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return compute(args);
    }

    public Object compute(Object[] args) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        if (FunctionUtils.isNull(args[1]) && FunctionUtils.isNull(args[2])) {
            return null;
        } else if (FunctionUtils.isNull(args[1]) && !FunctionUtils.isNull(args[2])) {
            if (secondComType == null) {
                secondComType = calculateCompareType(operandTypes.get(1));
            }

            if (secondComType.compare(args[0], args[2]) > 0) {
                return false;
            } else {
                return null;
            }
        } else if (!FunctionUtils.isNull(args[1]) && FunctionUtils.isNull(args[2])) {
            if (firstComType == null) {
                firstComType = calculateCompareType(operandTypes.get(0));
            }
            if (firstComType.compare(args[0], args[1]) < 0) {
                return false;
            } else {
                return null;
            }
        } else {
            if (commonComType == null) {
                commonComType = calculateCompareType(operandTypes.get(0), operandTypes.get(1));
            }
            return (commonComType.compare(args[0], args[1]) >= 0) && (commonComType.compare(args[0], args[2]) <= 0);
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"BETWEEN ASYMMETRIC"};
    }
}

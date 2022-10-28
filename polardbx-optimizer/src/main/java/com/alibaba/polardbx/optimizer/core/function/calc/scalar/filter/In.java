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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @since 5.0.0
 */
public class In extends AbstractCollationScalarFunction {
    public In(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object left = args[0];
        Object right = args[1];

        if (FunctionUtils.isNull(left)) {
            return null;
        }
        if (args[1] instanceof Row.RowValue) {
            right = ((Row.RowValue) args[1]).getValues();
        }

        if (right instanceof List) {

            if (FunctionUtils.allElementsNull((List) right)) {
                // MySQL behavior
                return null;
            }

            if (!GeneralUtil.isEmpty((Collection) right)) {
                if (((List) right).get(0) instanceof List) {
                    right = ((List) right).get(0);
                }
            }
            for (Object eachRight : (List) right) {
                // 是否出现(id,name) in ((1,'a'),(2,'b'))
                if (left instanceof Row.RowValue) {
                    List<Object> rightArgs = null;
                    if (eachRight instanceof Row.RowValue) {
                        rightArgs = ((Row.RowValue) eachRight).getValues();
                    } else {
                        rightArgs = (List<Object>) eachRight;
                    }

                    List<Object> leftArgs = ((Row.RowValue) left).getValues();

                    if (leftArgs.size() != rightArgs.size()) {
                        throw new NotSupportException("impossible");
                    }

                    boolean notMatch = false;
                    if (operandTypes.get(0).compare(left, eachRight) != 0) {
                        // 不匹配
                        notMatch = true;
                    }
                    if (!notMatch) {
                        return 1L;
                    }
                } else {
                    DataType type = getEqualType(Arrays.asList(left, eachRight));
                    if (type.compare(left, eachRight) == 0) {
                        return 1L;
                    }
                }
            }
        }

        if (FunctionUtils.atLeastOneElementsNull((List<Object>) right)) {
            return null;
        }
        return 0L;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"IN"};
    }
}

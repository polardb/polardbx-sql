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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * Returns the number X, truncated to D decimal places. If D is 0, the result
 * has no decimal point or fractional part. D can be negative to cause D digits
 * left of the decimal point of the value X to become zero.
 */
public class Truncate extends Round {

    public Truncate(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    protected boolean isTruncate() {
        return true;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TRUNCATE"};
    }
}

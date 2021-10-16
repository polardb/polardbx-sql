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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * WEEKOFYEAR(date)
 * Returns the calendar week of the date as a number in the range from 1 to 53. WEEKOFYEAR() is a
 * compatibility function that is equivalent to WEEK(date,3).
 */
public class WeekOfYear extends Week {
    public WeekOfYear(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    protected int defaultWeekMode() {
        return 3;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"WEEKOFYEAR"};
    }
}

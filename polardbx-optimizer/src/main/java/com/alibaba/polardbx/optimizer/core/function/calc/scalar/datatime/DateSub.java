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

import com.alibaba.polardbx.common.utils.time.calculator.MySQLInterval;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.List;

public class DateSub extends DateAdd {
    public DateSub(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    protected MySQLInterval getInterval(MySQLIntervalType intervalType, Object valueObj) {
        if (cachedInterval == null) {
            String intervalValue = DataTypes.StringType.convertFrom(valueObj);
            try {
                cachedInterval = MySQLIntervalType.parseInterval(intervalValue, intervalType);
                if (cachedInterval != null) {
                    boolean isNeg = cachedInterval.isNeg();
                    cachedInterval.setNeg(!isNeg);
                }
            } catch (Throwable t) {
                // for invalid interval value
                return null;
            }
        }
        return cachedInterval;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"DATE_SUB", "SUBDATE"};
    }
}

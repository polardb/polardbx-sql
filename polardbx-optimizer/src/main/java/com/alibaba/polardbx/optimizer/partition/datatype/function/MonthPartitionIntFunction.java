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

package com.alibaba.polardbx.optimizer.partition.datatype.function;

import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;

import java.util.List;

import static com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity.NON_MONOTONIC;

public class MonthPartitionIntFunction extends PartitionIntFunction {
    public MonthPartitionIntFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        return NON_MONOTONIC;
    }

    @Override
    public MySQLIntervalType getIntervalType() {
        return MySQLIntervalType.INTERVAL_MONTH;
    }

    @Override
    public long evalInt(PartitionField partitionField, SessionProperties sessionProperties) {
        MysqlDateTime t = partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());

        if (t == null) {
            return 0L;
        } else {
            return t.getMonth();
        }
    }

    @Override
    public long evalIntEndpoint(PartitionField partitionField, SessionProperties sessionProperties,
                                boolean[] endpoints) {
        MysqlDateTime t = partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());

        if (t == null) {
            return SINGED_MIN_LONG;
        } else {
            return t.getMonth();
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"MonthPartitionInt"};
    }
}

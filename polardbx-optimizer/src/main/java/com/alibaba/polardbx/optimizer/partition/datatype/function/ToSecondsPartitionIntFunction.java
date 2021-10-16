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

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;

import java.sql.Types;
import java.util.List;

public class ToSecondsPartitionIntFunction extends PartitionIntFunction {

    public ToSecondsPartitionIntFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        if (fieldType.getSqlType() == Types.DATE
            || fieldType.getSqlType() == MySQLTimeTypeUtil.DATETIME_SQL_TYPE) {
            return Monotonicity.MONOTONIC_STRICT_INCREASING_NOT_NULL;
        }
        return Monotonicity.NON_MONOTONIC;
    }

    @Override
    public MySQLIntervalType getIntervalType() {
        return MySQLIntervalType.INTERVAL_SECOND;
    }

    @Override
    public long evalInt(PartitionField partitionField, SessionProperties sessionProperties) {
        MysqlDateTime t =
            partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_NO_ZERO_DATE, SessionProperties.empty());

        if (t == null) {
            return 0L;
        }
        long sec = t.getHour() * 3600L + t.getMinute() * 60 + t.getSecond();
        if (t.isNeg()) {
            sec = -sec;
        }
        long days = MySQLTimeCalculator.calDayNumber(t.getYear(), t.getMonth(), t.getDay());
        return sec + days * 24L * 3600L;
    }

    @Override
    public long evalIntEndpoint(PartitionField partitionField, SessionProperties sessionProperties,
                                boolean[] endpoints) {
        MysqlDateTime t = partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());

        if (t == null) {
            // got NULL, leave the incl_endp intact
            return SINGED_MIN_LONG;
        }

        long sec = t.getHour() * 3600L + t.getMinute() * 60 + t.getSecond();
        if (t.isNeg()) {
            sec = -sec;
        }
        long days = MySQLTimeCalculator.calDayNumber(t.getYear(), t.getMonth(), t.getDay());
        sec += days * 24L * 3600L;

        // Set to NULL if invalid date, but keep the value
        boolean isNonZeroDate = t.getYear() != 0 || t.getMonth() != 0 || t.getDay() != 0;
        boolean invalid = MySQLTimeTypeUtil.isDateInvalid(t, isNonZeroDate,
            TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE | TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);

        // Even if the evaluation return NULL, seconds is useful for pruning
        return sec;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ToSecondsPartitionInt"};
    }
}

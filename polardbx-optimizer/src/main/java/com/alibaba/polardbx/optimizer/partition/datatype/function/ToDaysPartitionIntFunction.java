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

import static com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity.MONOTONIC_INCREASING_NOT_NULL;
import static com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity.MONOTONIC_STRICT_INCREASING_NOT_NULL;
import static com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity.NON_MONOTONIC;

public class ToDaysPartitionIntFunction extends PartitionIntFunction {
    public ToDaysPartitionIntFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }


    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        if (fieldType.getSqlType() == Types.DATE) {
            return MONOTONIC_STRICT_INCREASING_NOT_NULL;
        } else if (fieldType.getSqlType() == MySQLTimeTypeUtil.DATETIME_SQL_TYPE) {
            return MONOTONIC_INCREASING_NOT_NULL;
        } else {
            return NON_MONOTONIC;
        }
    }

    @Override
    public MySQLIntervalType getIntervalType() {
        return MySQLIntervalType.INTERVAL_DAY;
    }

    @Override
    public long evalInt(PartitionField partitionField, SessionProperties sessionProperties) {
        // read datetime value from partition field.
        MysqlDateTime t =
            partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_NO_ZERO_DATE, SessionProperties.empty());
        // invalid datetime string
        if (t == null) {
            return 0L;
        }

        long dayNumber = MySQLTimeCalculator.calDayNumber(
            t.getYear(),
            t.getMonth(),
            t.getDay());

        return dayNumber;
    }

    @Override
    public long evalIntEndpoint(PartitionField partitionField, SessionProperties sessionProperties,
                                boolean[] endpoints) {
        // read datetime value from partition field.
        MysqlDateTime t =
            partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_NO_ZERO_DATE, SessionProperties.empty());
        // invalid datetime string
        if (t == null) {
            // got NULL, leave the incl_endp intact
            return SINGED_MIN_LONG;
        }

        long dayNumber = MySQLTimeCalculator.calDayNumber(
            t.getYear(),
            t.getMonth(),
            t.getDay());

        // Set to NULL if invalid date, but keep the value
        boolean isNonZeroDate = t.getYear() != 0 || t.getMonth() != 0 || t.getDay() != 0;
        boolean invalid = MySQLTimeTypeUtil.isDateInvalid(t, isNonZeroDate,
            TimeParserFlags.FLAG_TIME_NO_ZERO_IN_DATE | TimeParserFlags.FLAG_TIME_NO_ZERO_DATE);

        int sqlType = partitionField.dataType().getSqlType();
        if (invalid) {
            // Even if the evaluation return NULL, the calc_daynr is useful for pruning
            if (sqlType == Types.DATE) {
                endpoints[1] = true;
            }
            return dayNumber;
        }

        if (sqlType == Types.DATE) {
            // TO_DAYS() is strictly monotonic for dates, leave incl_endp intact
            return dayNumber;
        }

        //    Handle the special but practically useful case of datetime values that
        //    point to day bound ("strictly less" comparison stays intact):
        //
        //      col < '2007-09-15 00:00:00'  -> TO_DAYS(col) <  TO_DAYS('2007-09-15')
        //      col > '2007-09-15 23:59:59'  -> TO_DAYS(col) >  TO_DAYS('2007-09-15')
        //
        //    which is different from the general case ("strictly less" changes to
        //    "less or equal"):
        //
        //      col < '2007-09-15 12:34:56'  -> TO_DAYS(col) <= TO_DAYS('2007-09-15')
        boolean leftEndpoint = endpoints[0];
        boolean isStrict = (!leftEndpoint && !(t.getHour() != 0 || t.getMinute() != 0 || t.getSecond() != 0 ||
            t.getSecondPart() != 0)) ||
            (leftEndpoint && t.getHour() == 23 && t.getMinute() == 59 &&
                t.getSecond() == 59);

        if (!isStrict) {
            endpoints[1] = true;
        }
        return dayNumber;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"ToDaysPartitionInt"};
    }
}

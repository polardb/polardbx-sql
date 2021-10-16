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
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;

import java.sql.Types;
import java.util.List;

public class YearPartitionIntFunction extends PartitionIntFunction {
    public YearPartitionIntFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        if (fieldType.getSqlType() == Types.DATE
            || fieldType.getSqlType() == MySQLTimeTypeUtil.DATETIME_SQL_TYPE) {
            return Monotonicity.MONOTONIC_INCREASING;
        }
        return Monotonicity.NON_MONOTONIC;
    }

    @Override
    public MySQLIntervalType getIntervalType() {
        return MySQLIntervalType.INTERVAL_YEAR;
    }

    @Override
    public long evalInt(PartitionField partitionField, SessionProperties sessionProperties) {
        MysqlDateTime mysqlDateTime =
            partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());
        if (mysqlDateTime != null) {
            return mysqlDateTime.getYear();
        } else {
            return 0;
        }
    }

    @Override
    public long evalIntEndpoint(PartitionField partitionField, SessionProperties sessionProperties,
                                boolean[] endpoints) {
        MysqlDateTime mysqlDateTime =
            partitionField.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());
        if (mysqlDateTime == null) {
            return SINGED_MIN_LONG;
        }

        //    Handle the special but practically useful case of datetime values that
        //    point to year bound ("strictly less" comparison stays intact) :
        //
        //      col < '2007-01-01 00:00:00'  -> YEAR(col) <  2007
        //
        //    which is different from the general case ("strictly less" changes to
        //    "less or equal"):
        //
        //      col < '2007-09-15 23:00:00'  -> YEAR(col) <= 2007

        boolean leftEnd = endpoints[0];
        boolean isStrict = !leftEnd && mysqlDateTime.getDay() == 1 && mysqlDateTime.getMonth() == 1 &&
            !(mysqlDateTime.getHour() != 0 || mysqlDateTime.getMinute() != 0
                || mysqlDateTime.getSecond() != 0 || mysqlDateTime.getSecondPart() != 0);
        if (!isStrict) {
            endpoints[1] = true;
        }

        return mysqlDateTime.getYear();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"YearPartitionInt"};
    }
}

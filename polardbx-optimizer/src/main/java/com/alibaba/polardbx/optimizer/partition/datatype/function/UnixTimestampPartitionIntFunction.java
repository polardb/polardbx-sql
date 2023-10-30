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
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import org.apache.calcite.sql.SqlOperator;

import java.sql.Types;
import java.util.List;

public class UnixTimestampPartitionIntFunction extends PartitionIntFunction {
    public UnixTimestampPartitionIntFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Monotonicity getMonotonicity(DataType<?> fieldType) {
        if (fieldType.getSqlType() == Types.TIMESTAMP) {
            return Monotonicity.MONOTONIC_INCREASING;
        }
        return Monotonicity.NON_MONOTONIC;
    }

    @Override
    public MySQLIntervalType getIntervalType() {
        return null;
    }

    @Override
    public long evalInt(PartitionField partitionField, SessionProperties sessionProperties) {
        MySQLTimeVal timeVal = partitionField.timestampValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, sessionProperties);
        if (timeVal == null) {
            return 0L;
        }

        long epochSec = timeVal.getSeconds();

        // It's distinct from unix_timestamp, only get integer part of the second value.
        return epochSec;
    }

    @Override
    public long evalIntEndpoint(PartitionField partitionField, SessionProperties sessionProperties,
                                boolean[] endpoints) {
        // Leave the incl_endp intact
        return evalInt(partitionField, sessionProperties);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UnixTimestampPartitionInt"};
    }

    @Override
    public SqlOperator getSqlOperator() {
        return TddlOperatorTable.UNIX_TIMESTAMP;
    }
}

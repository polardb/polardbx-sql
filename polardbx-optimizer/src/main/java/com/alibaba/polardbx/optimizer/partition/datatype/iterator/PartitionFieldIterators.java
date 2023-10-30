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

package com.alibaba.polardbx.optimizer.partition.datatype.iterator;

import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.datatype.function.*;

public class PartitionFieldIterators {
    public static PartitionFieldIterator getIterator(DataType fieldType, MySQLIntervalType intervalType,
                                                     PartitionIntFunction partitionIntFunction) {
        if (intervalType == null) {
            if (DataTypeUtil.equalsSemantically(fieldType, DataTypes.DecimalType)) {
                return new DecimalInIterator(fieldType);
            } else {
                return new SimpleInIterator(fieldType);
            }
        } else if (intervalType == MySQLIntervalType.INTERVAL_SECOND) {
            if (partitionIntFunction instanceof ToSecondsPartitionIntFunction) {
                return new SecondIterator(fieldType);
            }
        } else if (intervalType == MySQLIntervalType.INTERVAL_DAY) {
            if (partitionIntFunction instanceof DayOfMonthPartitionIntFunction) {
                return new DayIterator(fieldType);
            } else if (partitionIntFunction instanceof DayOfWeekPartitionIntFunction) {
                return new DayOfWeekIterator(fieldType);
            } else if (partitionIntFunction instanceof DayOfYearPartitionIntFunction) {
                return new DayOfYearIterator(fieldType);
            } else if (partitionIntFunction instanceof ToDaysPartitionIntFunction) {
                return new ToDaysIterator(fieldType);
            }
        } else if (intervalType == MySQLIntervalType.INTERVAL_WEEK) {
            if (partitionIntFunction instanceof ToWeeksPartitionIntFunction) {
                return new ToWeeksIterator(fieldType);
            } else if (partitionIntFunction instanceof WeekOfYearPartitionIntFunction) {
                return new WeekOfYearIterator(fieldType);
            }
        } else if (intervalType == MySQLIntervalType.INTERVAL_MONTH) {
            if (partitionIntFunction instanceof MonthPartitionIntFunction) {
                return new MonthIterator(fieldType);
            } else if (partitionIntFunction instanceof ToMonthsPartitionIntFunction) {
                return new ToMonthsIterator(fieldType);
            }
        } else if (intervalType == MySQLIntervalType.INTERVAL_YEAR) {
            if (partitionIntFunction instanceof YearPartitionIntFunction) {
                return new YearIterator(fieldType);
            }
        }

        return null;
    }

    public static PartitionFieldIterator getIterator(DataType fieldType) {
        if (DataTypeUtil.equalsSemantically(fieldType, DataTypes.DecimalType)) {
            return new DecimalInIterator(fieldType);
        } else {
            return new SimpleInIterator(fieldType);
        }
    }
}

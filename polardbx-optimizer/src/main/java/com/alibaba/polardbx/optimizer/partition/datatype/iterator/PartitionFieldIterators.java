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

public class PartitionFieldIterators {
    public static PartitionFieldIterator getIterator(DataType fieldType, MySQLIntervalType intervalType) {
        if (intervalType == null) {
            return new SimpleInIterator(fieldType);
        }
        switch (intervalType) {
        case INTERVAL_MONTH:
            return new MonthIterator(fieldType);
        case INTERVAL_DAY:
            return new DayIterator(fieldType);
        case INTERVAL_YEAR:
            return new YearIterator(fieldType);
        case INTERVAL_SECOND:
            return new SecondIterator(fieldType);
        default:
            return null;
        }
    }

    public static PartitionFieldIterator getIterator(DataType fieldType) {
        return new SimpleInIterator(fieldType);
    }
}

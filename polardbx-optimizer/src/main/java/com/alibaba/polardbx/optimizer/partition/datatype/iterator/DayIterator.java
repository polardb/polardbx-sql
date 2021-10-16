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
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.google.common.base.Preconditions;

public class DayIterator extends AbstractDateIterator {
    public DayIterator(DataType fieldType) {
        super(fieldType);
    }

    @Override
    public boolean range(PartitionField from, PartitionField to, boolean lowerBoundIncluded, boolean upperBoundIncluded) {
        if (!checkBound(from, to)) {
            return false;
        }

        this.lowerBoundIncluded = lowerBoundIncluded;
        this.upperBoundIncluded = upperBoundIncluded;
        this.firstEnumerated = false;

        MysqlDateTime minDatetime = from.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());
        MysqlDateTime maxDatetime = to.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());

        maxDatetime.setHour(0);
        maxDatetime.setMinute(0);
        maxDatetime.setHour(0);
        maxDatetime.setSecond(0);
        maxDatetime.setSecondPart(0);

        minDatetime.setHour(0);
        minDatetime.setMinute(0);
        minDatetime.setHour(0);
        minDatetime.setSecond(0);
        minDatetime.setSecondPart(0);

        // NOTE: t2 - t1
        long packedLong = TimeStorage.writeTimestamp(maxDatetime);
        MysqlDateTime diff = MySQLTimeCalculator.calTimeDiff(maxDatetime, minDatetime, false);
        boolean isNeg = diff.isNeg();
        int sign = isNeg ? -1 : 1;
        long diffSec = diff.getSecond();

        // calc day

        // set the status of this iterator
        long dayDiff = diffSec / (3600 * 24L) * sign;
        count = (int) dayDiff + 1;
        if (!lowerBoundIncluded) {
            count--;
        }
        if (!upperBoundIncluded) {
            count--;
        }
        currentDatetime = minDatetime;
        endPackedLong = packedLong;
        currentCount = 0;
        return true;
    }

    @Override
    public boolean hasNext() {
        if (currentCount >= count) {
            return false;
        }

        // enumerate the first value if the lower bound is included and the first value has not been enumerated.
        if (lowerBoundIncluded && !firstEnumerated) {
            firstEnumerated = true;
            currentCount++;
            return true;
        }
        // try to calc the next datetime value, by add 1 month to current datetime.
        MysqlDateTime newDatetime = MySQLTimeCalculator.addInterval(
            currentDatetime,
            MySQLIntervalType.INTERVAL_DAY,
            DAY_INTERVAL_VALUE);
        long packedLongToCmp = TimeStorage.writeTimestamp(newDatetime);
        if ((packedLongToCmp < endPackedLong)
            || (upperBoundIncluded && packedLongToCmp <= endPackedLong)) {
            currentDatetime = newDatetime;
            currentCount++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Long next() {
        // get the current datetime value.
        if (currentDatetime != null) {
            return currentDatetime.getDay();
        } else {
            return INVALID_NEXT_VALUE;
        }
    }
}

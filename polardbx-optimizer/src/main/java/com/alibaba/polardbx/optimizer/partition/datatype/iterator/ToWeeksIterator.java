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
import com.alibaba.polardbx.common.utils.time.calculator.PartitionFunctionTimeCaculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;

/**
 * Created by zhuqiwei.
 */

/**
 * This Iterator is only used for partition enumeration,
 * because the enumeration of the endpoint is not accurate.
 */
public class ToWeeksIterator extends AbstractDateIterator {

    public ToWeeksIterator(DataType fieldType) {
        super(fieldType);
    }

    @Override
    public boolean range(PartitionField from, PartitionField to, boolean lowerBoundIncluded,
                         boolean upperBoundIncluded) {
        if (!checkBound(from, to)) {
            return false;
        }

        this.lowerBoundIncluded = lowerBoundIncluded;
        this.upperBoundIncluded = upperBoundIncluded;
        this.firstEnumerated = false;

        MysqlDateTime minDatetime = from.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());
        MysqlDateTime maxDatetime = to.datetimeValue(TimeParserFlags.FLAG_TIME_FUZZY_DATE, SessionProperties.empty());

        // NOTE: t2 - t1
        long packedLong1 = TimeStorage.writeTimestamp(minDatetime);
        long packedLong2 = TimeStorage.writeTimestamp(maxDatetime);
        boolean isNeg = packedLong1 > packedLong2;
        if (isNeg) {
            MysqlDateTime temp = maxDatetime;
            maxDatetime = minDatetime;
            minDatetime = temp;
        }
        this.currentDatetime = minDatetime;
        this.endPackedLong = isNeg ? packedLong1 : packedLong2;
        this.currentCount = 0;

        long toweeksMin = PartitionFunctionTimeCaculator.calToWeeks(
            minDatetime.getYear(),
            minDatetime.getMonth(),
            minDatetime.getDay()
        );
        long toweeksMax = PartitionFunctionTimeCaculator.calToWeeks(
            maxDatetime.getYear(),
            maxDatetime.getMonth(),
            maxDatetime.getDay()
        );
        this.count = toweeksMax - toweeksMin + 1;

        return true;
    }

    @Override
    public boolean hasNext() {
        if (currentCount >= count) {
            return false;
        }

        if (!firstEnumerated) {
            firstEnumerated = true;
            currentCount++;
            return true;
        }

        MysqlDateTime newDatetime = PartitionFunctionTimeCaculator.addInterval(
            currentDatetime,
            MySQLIntervalType.INTERVAL_WEEK,
            WEEK_INTERVAL_VALUE);

        this.currentDatetime = newDatetime;
        this.currentCount++;
        return true;
    }

    @Override
    public Long next() {
        if (currentDatetime != null) {
            return PartitionFunctionTimeCaculator.calToWeeks(
                currentDatetime.getYear(),
                currentDatetime.getMonth(),
                currentDatetime.getDay()
            );
        } else {
            return INVALID_NEXT_VALUE;
        }
    }
}

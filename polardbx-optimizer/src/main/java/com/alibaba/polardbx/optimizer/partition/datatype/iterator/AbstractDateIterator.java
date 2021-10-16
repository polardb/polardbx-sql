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

import com.alibaba.polardbx.common.utils.time.calculator.MySQLInterval;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.google.common.base.Preconditions;

public abstract class AbstractDateIterator implements PartitionFieldIterator {
    protected static final MySQLInterval YEAR_INTERVAL_VALUE = new MySQLInterval();
    protected static final MySQLInterval MONTH_INTERVAL_VALUE = new MySQLInterval();
    protected static final MySQLInterval DAY_INTERVAL_VALUE = new MySQLInterval();
    protected static final MySQLInterval SECOND_INTERVAL_VALUE = new MySQLInterval();

    static {
        YEAR_INTERVAL_VALUE.setYear(1L);
        MONTH_INTERVAL_VALUE.setMonth(1L);
        DAY_INTERVAL_VALUE.setDay(1L);
        SECOND_INTERVAL_VALUE.setSecond(1L);
    }

    protected boolean lowerBoundIncluded;
    protected boolean upperBoundIncluded;
    protected boolean firstEnumerated;

    protected int currentCount;
    protected long count;
    protected MysqlDateTime currentDatetime;
    protected long endPackedLong;

    protected final DataType fieldType;

    protected AbstractDateIterator(DataType fieldType) {
        this.fieldType = fieldType;
    }

    boolean checkBound(PartitionField from, PartitionField to) {
        Preconditions.checkArgument(from.dataType().getScale() == to.dataType().getScale());
        Preconditions.checkArgument(from.mysqlStandardFieldType() == to.mysqlStandardFieldType());

        if (from.lastStatus() != TypeConversionStatus.TYPE_OK
            || to.lastStatus() != TypeConversionStatus.TYPE_OK) {
            count = INVALID_COUNT;
            return false;
        }
        return true;
    }

    // for unit test
    MysqlDateTime nextDatetime() {
        // get the current datetime value.
        if (currentDatetime != null) {
            return currentDatetime;
        } else {
            return null;
        }
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void clear() {
        count = 0;
        currentDatetime = null;
        endPackedLong = -1;
        upperBoundIncluded = false;
        lowerBoundIncluded = false;
        firstEnumerated = false;
    }
}

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

package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.StorageField;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedLong;
import com.google.common.primitives.UnsignedLongs;
import io.airlift.slice.Slice;
import org.apache.calcite.sql.SqlKind;

import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;

public abstract class AbstractPartitionField implements PartitionField {
    static final SqlKind[][] COMPARISON_KINDS = {
        {LESS_THAN, LESS_THAN_OR_EQUAL},
        {GREATER_THAN, GREATER_THAN_OR_EQUAL}
    };
    protected StorageField field;
    protected PredicateBoolean lastPredicateBoolean;

    @Override
    public MySQLStandardFieldType mysqlStandardFieldType() {
        return field.standardFieldType();
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public int packetLength() {
        return field.packetLength();
    }

    @Override
    public TypeConversionStatus store(Object value, DataType<?> resultType, SessionProperties sessionProperties) {
        return cacheEqualPredicateBoolean(field.store(value, resultType, sessionProperties));
    }

    @Override
    public TypeConversionStatus store(Object value, DataType<?> resultType) {
        return store(value, resultType, SessionProperties.empty());
    }

    @Override
    public TypeConversionStatus store(Object value, DataType<?> resultType, boolean[] endpoints, AtomicInteger diff) {
        return store(value, resultType, SessionProperties.empty(), endpoints, diff);
    }

    @Override
    public TypeConversionStatus store(Object value, DataType<?> resultType, SessionProperties sessionProperties,
                                      boolean[] endpoints, AtomicInteger diff) {
        Preconditions.checkArgument(endpoints.length == 2);

        // store to field.
        TypeConversionStatus typeConversionStatus = field.store(value, resultType, sessionProperties);

        SqlKind comparisonKind = COMPARISON_KINDS[endpoints[0] ? 1 : 0][endpoints[1] ? 1 : 0];

        PredicateBoolean isAlwaysTrueOrFalse =
            PartitionDataTypeUtils.isAlwaysTrueOrFalse(this, typeConversionStatus, comparisonKind);
        this.lastPredicateBoolean = isAlwaysTrueOrFalse;

        // don't handle endpoint status if range predicate is always false or true.
        if (isAlwaysTrueOrFalse != PredicateBoolean.IS_NOT_ALWAYS_TRUE_OR_FALSE) {
            return typeConversionStatus;
        }

        // compare stored value & original value.
        int diffValue =
            PartitionDataTypeUtils.compareStoredFieldToValue(this, value, resultType, sessionProperties);
        if (diff != null) {
            // cache if needed.
            diff.set(diffValue);
        }

        // initialize the endpoint state to close.
        endpoints[1] = true;
        switch (comparisonKind) {
        case LESS_THAN:
            // If the stored field is equal to original value, open the right endpoint.
            // On the contrary, if the stored field is not equal to original value,
            // The endpoint should be close, e.g.:
            // int field < 10.3 => int field <= 10
            // int field < 10.6 => int field <= 11
            if (diffValue == 0) {
                // field < value => field in (-inf, value)
                // not including the right endpoint
                endpoints[1] = false;
            }

            // fall through to next case.
        case LESS_THAN_OR_EQUAL:
            // we don't support nullable check util now.
            // but we should preserve this branch to handle nullable check of field.
            break;
        case GREATER_THAN:
            // int field > 10.2 => stored(10) <= original(10.2) => int field > 10
            // int field > 10.5 => stored(11) > original(10.5) => int field >= 11
            if (diffValue <= 0) {
                // field > value => field in (value, +inf)
                // not including the left endpoint
                endpoints[1] = false;
            }
            break;
        case GREATER_THAN_OR_EQUAL:
            // int field >= 10.2 => stored(10) < original(10.2) => int field > 10
            if (diffValue < 0) {
                endpoints[1] = false;
            }
        default:
            break;
        }
        return typeConversionStatus;
    }

    @Override
    public TypeConversionStatus store(Object value, DataType<?> resultType, SessionProperties sessionProperties,
                                      boolean[] endpoints) {
        return store(value, resultType, sessionProperties, endpoints, null);
    }

    @Override
    public TypeConversionStatus store(Object value, DataType<?> resultType, boolean[] endpoints) {
        return store(value, resultType, SessionProperties.empty(), endpoints);
    }

    @Override
    public TypeConversionStatus store(ResultSet rs, int columnIndex, SessionProperties sessionProperties) {
        return cacheEqualPredicateBoolean(field.store(rs, columnIndex, sessionProperties));
    }

    @Override
    public TypeConversionStatus store(ResultSet rs, int columnIndex) {
        return cacheEqualPredicateBoolean(field.store(rs, columnIndex));
    }

    @Override
    public TypeConversionStatus lastStatus() {
        return field.lastStatus();
    }

    @Override
    public PredicateBoolean lastPredicateBoolean() {
        return lastPredicateBoolean;
    }

    @Override
    public void reset() {
        field.reset();
    }

    @Override
    public void setNull() {
        field.setNull();
    }

    @Override
    public void hash(long[] numbers) {
        field.hash(numbers);
    }

    @Override
    public long xxHashCode() {
        return field.xxHashCode();
    }

    @Override
    public byte[] rawBytes() {
        return field.rawBytes();
    }

    @Override
    public MysqlDateTime datetimeValue() {
        return field.datetimeValue(0, SessionProperties.empty());
    }

    @Override
    public MysqlDateTime datetimeValue(int timeParseFlags, SessionProperties sessionProperties) {
        return field.datetimeValue(timeParseFlags, sessionProperties);
    }

    @Override
    public MysqlDateTime timeValue() {
        return field.timeValue();
    }

    @Override
    public MysqlDateTime timeValue(int timeParseFlags, SessionProperties sessionProperties) {
        return field.timeValue(timeParseFlags, sessionProperties);
    }

    @Override
    public MySQLTimeVal timestampValue(int timeParseFlags, SessionProperties sessionProperties) {
        return field.timestampValue(timeParseFlags, sessionProperties);
    }

    @Override
    public MySQLTimeVal timestampValue() {
        return field.timestampValue();
    }

    @Override
    public long longValue(SessionProperties sessionProperties) {
        return field.longValue(sessionProperties);
    }

    @Override
    public long longValue() {
        return field.longValue();
    }

    @Override
    public Decimal decimalValue() {
        return decimalValue(SessionProperties.empty());
    }

    @Override
    public Decimal decimalValue(SessionProperties sessionProperties) {
        long longValue = longValue(sessionProperties);
        if (dataType().isUnsigned()) {
            String unsignedLongStr = UnsignedLongs.toString(longValue);
            return Decimal.fromString(unsignedLongStr);
        } else {
            return Decimal.fromLong(longValue);
        }
    }

    @Override
    public double doubleValue() {
        return doubleValue(SessionProperties.empty());
    }

    @Override
    public double doubleValue(SessionProperties sessionProperties) {
        long longValue = longValue(sessionProperties);
        if (dataType().isUnsigned()) {
            double dValue = (double) (longValue & 0x7fffffffffffffffL);
            if (longValue < 0) {
                dValue += 0x1.0p63;
            }
            return dValue;
        } else {
            return longValue;
        }
    }

    @Override
    public Slice stringValue(SessionProperties sessionProperties) {
        return field.stringValue(sessionProperties);
    }

    @Override
    public Slice stringValue() {
        return field.stringValue();
    }

    @Override
    public boolean isNull() {
        return field.isNull();
    }

    @Override
    public void setNull(boolean isNull) {
        field.setNull(isNull);
    }

    @Override
    public PartitionField maxValue() {
        return null;
    }

    @Override
    public PartitionField minValue() {
        return null;
    }

    protected int memCmp(byte[] left, byte[] right, int length) {
        for (int i = 0; i < length; i++) {
            if (left[i] != right[i]) {
                return (left[i] & 0xFF) - (right[i] & 0xFF);
            }
        }
        return 0;
    }

    private TypeConversionStatus cacheEqualPredicateBoolean(TypeConversionStatus conversionStatus) {
        PredicateBoolean isAlwaysTrueOrFalse =
            PartitionDataTypeUtils.isAlwaysTrueOrFalse(this, conversionStatus, EQUALS);
        this.lastPredicateBoolean = isAlwaysTrueOrFalse;
        return conversionStatus;
    }
}

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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.google.common.base.Preconditions;

import java.math.BigDecimal;

import static com.alibaba.polardbx.optimizer.core.field.AbstractNumericField.INT_64_MAX;

public class DecimalInIterator implements PartitionFieldIterator {

    private boolean lowerBoundIncluded;

    private boolean upperBoundIncluded;

    private boolean firstEnumerated;

    private int currentCount;

    private Decimal currentDecimal;

    private long count;

    private Decimal endDecimal;

    private final DataType fieldType;

    private final boolean isUnsigned;

    private DecimalStructure INT_64_MAX_DECIMAL = Decimal.fromLong(INT_64_MAX).getDecimalStructure();

    private DecimalStructure DECIMAL_0 = Decimal.fromLong(0).getDecimalStructure();

    private DecimalStructure DECIMAL_1 = Decimal.fromLong(1).getDecimalStructure();

    public DecimalInIterator(DataType fieldType) {
        this.fieldType = fieldType;
        this.isUnsigned = fieldType.isUnsigned();
    }

    @Override
    public boolean range(PartitionField from, PartitionField to, boolean lowerBoundIncluded,
                         boolean upperBoundIncluded) {
        Preconditions.checkArgument(from.mysqlStandardFieldType() == to.mysqlStandardFieldType());

        // We have stored an invalid value into the field.
        if (from.lastStatus() != TypeConversionStatus.TYPE_OK
            || to.lastStatus() != TypeConversionStatus.TYPE_OK) {
            count = INVALID_COUNT;
            return false;
        } else if (from.decimalValue().getDecimalStructure().getFractions() != 0 //scale must be 0
            || from.decimalValue().getDecimalStructure().getFractions() != 0) {
            count = INVALID_COUNT;
            return false;
        } else if (from.compareTo(to) > 0) {
            count = INVALID_COUNT;
            return true;
        }

        Decimal minValue = from.decimalValue();
        Decimal maxValue = to.decimalValue();
        DecimalStructure toValue = new DecimalStructure();
        FastDecimalUtils.sub(maxValue.getDecimalStructure(), minValue.getDecimalStructure(), toValue);
        FastDecimalUtils.add(toValue, DECIMAL_1, toValue);

        if (FastDecimalUtils.compare(toValue, INT_64_MAX_DECIMAL) > 0) {
            count = INVALID_COUNT;
            return false;
        } else if (FastDecimalUtils.compare(toValue, DECIMAL_0) < 0) {
            count = INVALID_COUNT;
            return true;
        } else {
            count = DecimalConverter.decimalToULong(toValue)[0];
        }

        if (!lowerBoundIncluded) {
            count--;
        }
        if (!upperBoundIncluded) {
            count--;
        }
        this.lowerBoundIncluded = lowerBoundIncluded;
        this.upperBoundIncluded = upperBoundIncluded;
        this.firstEnumerated = false;
        this.currentCount = 0;
        this.currentDecimal = minValue;
        this.endDecimal = maxValue;
        return true;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public void clear() {
        count = 0;
        currentCount = 0;
        currentDecimal = Decimal.fromLong(0);
        endDecimal = Decimal.fromLong(-1);
        upperBoundIncluded = false;
        lowerBoundIncluded = false;
        firstEnumerated = false;
    }

    @Override
    public boolean hasNext() {
        if (count == 0 || currentCount >= count) {
            return false;
        }

        // enumerate the first value if the lower bound is included and the first value has not been enumerated.
        if (lowerBoundIncluded && !firstEnumerated) {
            firstEnumerated = true;
            currentCount++;
            return true;
        }

        // try to calc the next value, by add 1
        Decimal newDecimalValue = currentDecimal.add(Decimal.fromLong(1));
        if ((newDecimalValue.compareTo(endDecimal) < 0) ||
            (upperBoundIncluded && newDecimalValue.compareTo(endDecimal) <= 0)) {
            currentDecimal = newDecimalValue;
            currentCount++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public BigDecimal next() {
        if (currentDecimal.getDecimalStructure().getFractions() != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "DecimalInIterator scale must be 0");
        }
        return currentDecimal.toBigDecimal();
    }

}

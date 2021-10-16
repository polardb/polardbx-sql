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

package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.UIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.junit.Assert;
import org.junit.Test;

public class IntFieldTest extends FieldTestBase {
    String minInt = "-2147483648";
    String maxInt = "2147483647";
    String maxIntPlus1 = "2147483648";
    String maxUint = "4294967295";

    String minIntMinus1 = "-2147483649";
    String veryHigh = "999999999999999";
    String veryLow = "-999999999999999";

    String maxUintPlus1 = "4294967296";

    StorageField intField = new IntField(new IntegerType());
    StorageField uintField = new IntField(new UIntegerType());

    @Test
    public void storeLegalIntValues() {
        checkLongInternal(intField, 0, 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(intField, 5, 5L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(intField, -1, -1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(intField, AbstractNumericField.INT_32_MIN, (long) AbstractNumericField.INT_32_MIN, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(intField, AbstractNumericField.INT_32_MAX, (long) AbstractNumericField.INT_32_MAX, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        // test set null
        intField.setNull();
        Assert.assertEquals((int) intField.longValue(), 0);
        Assert.assertTrue(intField.isNull());
    }

    @Test
    public void storeOutOfRangeIntValues() {
        // for signed
        checkLongInternal(intField, (long) AbstractNumericField.INT_32_MAX + 1L, (long) AbstractNumericField.INT_32_MAX, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkLongInternal(intField, (long) AbstractNumericField.INT_32_MIN - 1L, (long) AbstractNumericField.INT_32_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        // for unsigned
        checkLongInternal(uintField, -1L, 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkLongInternal(uintField, AbstractNumericField.INT_32_MIN, 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);
    }

    @Test
    public void storeLegalStringValues() {
        // for signed
        checkStringInternal(intField, "0", 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(intField, "1", 1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(intField, "-1", -1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(intField, maxInt, (long) AbstractNumericField.INT_32_MAX, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(intField, minInt, (long) AbstractNumericField.INT_32_MIN, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        // for unsigned
        checkStringInternal(uintField, maxIntPlus1, (long) AbstractNumericField.INT_32_MAX + 1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(uintField, maxUint, Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_32_MAX), DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void storeIllegalStringValues() {
        // For signed. Stored value is INT_MIN32/INT_MAX32 depending on sign of string to store
        checkStringInternal(intField, maxIntPlus1, (long) AbstractNumericField.INT_32_MAX, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(intField, veryHigh, (long) AbstractNumericField.INT_32_MAX, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(intField, minIntMinus1, (long) AbstractNumericField.INT_32_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(intField, veryLow, (long) AbstractNumericField.INT_32_MIN, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);


        // For unsigned. Stored value is 0/UINT_MAX32 depending on sign of string to store
        checkStringInternal(uintField, maxUintPlus1, Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_32_MAX), DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uintField, veryHigh, Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_32_MAX), DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uintField, "-1", 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uintField, minInt, 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uintField, veryLow, 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uintField, "foo", 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);
    }

    private void checkLongInternal(StorageField field, Object value, Object expectedValue,
                                   SessionProperties sessionProperties,
                                   TypeConversionStatus typeOk) {
        checkInternal(field, new LongType(), value, expectedValue, sessionProperties, typeOk);
    }

    private void checkStringInternal(StorageField field, Object value, Object expectedValue,
                                     SessionProperties sessionProperties,
                                     TypeConversionStatus typeOk) {
        checkInternal(field, new VarcharType(), value, expectedValue, sessionProperties, typeOk);
    }

    private void checkInternal(StorageField field, DataType resultType, Object value, Object expectedValue,
                               SessionProperties sessionProperties,
                               TypeConversionStatus typeOk) {
        checkStore(
            field,
            sessionProperties,
            resultType,
            value,
            expectedValue,
            false,
            typeOk,
            f -> f.longValue(sessionProperties)
        );
    }
}

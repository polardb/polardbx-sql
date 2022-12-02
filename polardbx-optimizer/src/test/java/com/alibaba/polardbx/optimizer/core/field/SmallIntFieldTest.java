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
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.SmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.USmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.junit.Assert;
import org.junit.Test;

public class SmallIntFieldTest extends FieldTestBase {
    String maxSmallInt = "32767";
    String minSmallInt = "-32768";
    String maxSmallIntPlus1 = "32768";
    String minSmallIntMinus1 = "-32769";

    String maxUSmallInt = "65535";
    String maxUSmallIntPlus1 = "65536";

    String veryHigh = "999999999999999";
    String veryLow = "-999999999999999";

    StorageField SmallIntField = new SmallIntField(new SmallIntType());
    StorageField uSmallintField = new SmallIntField(new USmallIntType());

    @Test
    public void storeLegalIntValues() {
        checkLongInternal(SmallIntField, 0, 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(SmallIntField, 5, 5L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(SmallIntField, -1, -1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(SmallIntField, AbstractNumericField.INT_16_MIN, (long) AbstractNumericField.INT_16_MIN,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(SmallIntField, AbstractNumericField.INT_16_MAX, (long) AbstractNumericField.INT_16_MAX,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        // test set null
        SmallIntField.setNull();
        Assert.assertEquals((int) SmallIntField.longValue(), 0);
        Assert.assertTrue(SmallIntField.isNull());
    }

    @Test
    public void storeOutOfRangeIntValues() {
        // for signed
        checkLongInternal(SmallIntField, (long) AbstractNumericField.INT_16_MAX + 1L,
            (long) AbstractNumericField.INT_16_MAX, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkLongInternal(SmallIntField, (long) AbstractNumericField.INT_16_MIN - 1L,
            (long) AbstractNumericField.INT_16_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        // for unsigned
        checkLongInternal(uSmallintField, -1L, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkLongInternal(uSmallintField, AbstractNumericField.INT_16_MIN, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);
    }

    @Test
    public void storeLegalStringValues() {
        // for signed
        checkStringInternal(SmallIntField, "0", 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(SmallIntField, "1", 1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(SmallIntField, "-1", -1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(SmallIntField, maxSmallInt, (long) AbstractNumericField.INT_16_MAX,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(SmallIntField, minSmallInt, (long) AbstractNumericField.INT_16_MIN,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        // for unsigned
        checkStringInternal(uSmallintField, maxSmallIntPlus1, (long) AbstractNumericField.INT_16_MAX + 1L,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(uSmallintField, maxUSmallInt,
            Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_16_MAX), DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void storeIllegalStringValues() {
        // For signed. Stored value is INT_MIN16/INT_MAX16 depending on sign of string to store
        checkStringInternal(SmallIntField, maxSmallIntPlus1, (long) AbstractNumericField.INT_16_MAX,
            DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(SmallIntField, veryHigh, (long) AbstractNumericField.INT_16_MAX, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(SmallIntField, minSmallIntMinus1, (long) AbstractNumericField.INT_16_MIN,
            DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(SmallIntField, veryLow, (long) AbstractNumericField.INT_16_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        // For unsigned. Stored value is 0/UINT_MAX16 depending on sign of string to store
        checkStringInternal(uSmallintField, maxUSmallIntPlus1,
            Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_16_MAX), DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uSmallintField, veryHigh, Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_16_MAX),
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uSmallintField, "-1", 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uSmallintField, minSmallInt, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uSmallintField, veryLow, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uSmallintField, "foo", 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_ERR_BAD_VALUE);
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

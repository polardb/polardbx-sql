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
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UTinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.junit.Assert;
import org.junit.Test;

public class TinyIntFieldTest extends FieldTestBase {
    String maxTinyInt = "127";
    String minTinyInt = "-128";
    String maxTinyIntPlus1 = "128";
    String minTinyIntMinus1 = "-129";

    String maxUTinyInt = "255";
    String maxUTinyIntPlus1 = "256";

    String veryHigh = "999999999999999";
    String veryLow = "-999999999999999";

    StorageField TinyIntField = new TinyIntField(new TinyIntType());
    StorageField uTinyintField = new TinyIntField(new UTinyIntType());

    @Test
    public void storeLegalIntValues() {
        checkLongInternal(TinyIntField, 0, 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(TinyIntField, 5, 5L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(TinyIntField, -1, -1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(TinyIntField, AbstractNumericField.INT_8_MIN, (long) AbstractNumericField.INT_8_MIN,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(TinyIntField, AbstractNumericField.INT_8_MAX, (long) AbstractNumericField.INT_8_MAX,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        // test set null
        TinyIntField.setNull();
        Assert.assertEquals((int) TinyIntField.longValue(), 0);
        Assert.assertTrue(TinyIntField.isNull());
    }

    @Test
    public void storeOutOfRangeIntValues() {
        // for signed
        checkLongInternal(TinyIntField, (long) AbstractNumericField.INT_8_MAX + 1L,
            (long) AbstractNumericField.INT_8_MAX, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkLongInternal(TinyIntField, (long) AbstractNumericField.INT_8_MIN - 1L,
            (long) AbstractNumericField.INT_8_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        // for unsigned
        checkLongInternal(uTinyintField, -1L, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkLongInternal(uTinyintField, AbstractNumericField.INT_8_MIN, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);
    }

    @Test
    public void storeLegalStringValues() {
        // for signed
        checkStringInternal(TinyIntField, "0", 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(TinyIntField, "1", 1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(TinyIntField, "-1", -1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(TinyIntField, maxTinyInt, (long) AbstractNumericField.INT_8_MAX, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_OK);

        checkStringInternal(TinyIntField, minTinyInt, (long) AbstractNumericField.INT_8_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_OK);

        // for unsigned
        checkStringInternal(uTinyintField, maxTinyIntPlus1, (long) AbstractNumericField.INT_8_MAX + 1L,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(uTinyintField, maxUTinyInt, Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_8_MAX),
            DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void storeIllegalStringValues() {
        // For signed. Stored value is INT_MIN8/INT_MAX8 depending on sign of string to store
        checkStringInternal(TinyIntField, maxTinyIntPlus1, (long) AbstractNumericField.INT_8_MAX,
            DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(TinyIntField, veryHigh, (long) AbstractNumericField.INT_8_MAX, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(TinyIntField, minTinyIntMinus1, (long) AbstractNumericField.INT_8_MIN,
            DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(TinyIntField, veryLow, (long) AbstractNumericField.INT_8_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        // For unsigned. Stored value is 0/UINT_MAX8 depending on sign of string to store
        checkStringInternal(uTinyintField, maxUTinyIntPlus1,
            Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_8_MAX), DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uTinyintField, veryHigh, Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_8_MAX),
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uTinyintField, "-1", 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uTinyintField, minTinyInt, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uTinyintField, veryLow, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uTinyintField, "foo", 0L, DEFAULT_SESSION_PROPERTIES,
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

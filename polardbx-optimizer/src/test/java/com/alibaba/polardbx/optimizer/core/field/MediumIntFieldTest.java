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
import com.alibaba.polardbx.optimizer.core.datatype.MediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UMediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.junit.Assert;
import org.junit.Test;

public class MediumIntFieldTest extends FieldTestBase {
    private static final int INT_24_MAX = 0x007FFFFF;
    private static final int INT_24_MIN = ~0x007FFFFF;
    private static final int UNSIGNED_INT_24_MAX = 0x00FFFFFF;
    private static final int UNSIGNED_INT_24_MIN = 0;

    String maxMediumInt = "8388607";
    String minMediumInt = "-8388608";
    String maxMediumIntPlus1 = "8388608";
    String minMediumIntMinus1 = "-8388609";

    String maxUMediumInt = "16777215";
    String maxUMediumIntPlus1 = "16777216";

    String veryHigh = "999999999999999";
    String veryLow = "-999999999999999";

    StorageField MediumIntField = new MediumIntField(new MediumIntType());
    StorageField uMediumintField = new MediumIntField(new UMediumIntType());

    @Test
    public void storeLegalIntValues() {
        checkLongInternal(MediumIntField, 0, 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(MediumIntField, 5, 5L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(MediumIntField, -1, -1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(MediumIntField, AbstractNumericField.INT_24_MIN, (long) AbstractNumericField.INT_24_MIN,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkLongInternal(MediumIntField, AbstractNumericField.INT_24_MAX, (long) AbstractNumericField.INT_24_MAX,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        // test set null
        MediumIntField.setNull();
        Assert.assertEquals((int) MediumIntField.longValue(), 0);
        Assert.assertTrue(MediumIntField.isNull());
    }

    @Test
    public void storeOutOfRangeIntValues() {
        // for signed
        checkLongInternal(MediumIntField, (long) AbstractNumericField.INT_24_MAX + 1L,
            (long) AbstractNumericField.INT_24_MAX, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkLongInternal(MediumIntField, (long) AbstractNumericField.INT_24_MIN - 1L,
            (long) AbstractNumericField.INT_24_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        // for unsigned
        checkLongInternal(uMediumintField, -1L, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkLongInternal(uMediumintField, AbstractNumericField.INT_24_MIN, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);
    }

    @Test
    public void storeLegalStringValues() {
        // for signed
        checkStringInternal(MediumIntField, "0", 0L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(MediumIntField, "1", 1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(MediumIntField, "-1", -1L, DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(MediumIntField, maxMediumInt, (long) AbstractNumericField.INT_24_MAX,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(MediumIntField, minMediumInt, (long) AbstractNumericField.INT_24_MIN,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        // for unsigned
        checkStringInternal(uMediumintField, maxMediumIntPlus1, (long) AbstractNumericField.INT_24_MAX + 1L,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkStringInternal(uMediumintField, maxUMediumInt,
            Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_24_MAX), DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void storeIllegalStringValues() {
        // For signed. Stored value is INT_MIN24/INT_MAX24 depending on sign of string to store
        checkStringInternal(MediumIntField, maxMediumIntPlus1, (long) AbstractNumericField.INT_24_MAX,
            DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(MediumIntField, veryHigh, (long) AbstractNumericField.INT_24_MAX,
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(MediumIntField, minMediumIntMinus1, (long) AbstractNumericField.INT_24_MIN,
            DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(MediumIntField, veryLow, (long) AbstractNumericField.INT_24_MIN, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        // For unsigned. Stored value is 0/UINT_MAX24 depending on sign of string to store
        checkStringInternal(uMediumintField, maxUMediumIntPlus1,
            Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_24_MAX), DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uMediumintField, veryHigh, Integer.toUnsignedLong(AbstractNumericField.UNSIGNED_INT_24_MAX),
            DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uMediumintField, "-1", 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uMediumintField, minMediumInt, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uMediumintField, veryLow, 0L, DEFAULT_SESSION_PROPERTIES,
            TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkStringInternal(uMediumintField, "foo", 0L, DEFAULT_SESSION_PROPERTIES,
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

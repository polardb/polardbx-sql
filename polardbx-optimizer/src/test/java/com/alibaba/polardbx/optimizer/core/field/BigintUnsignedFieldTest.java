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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.ULongType;
import org.junit.Test;

import java.time.ZoneId;

public class BigintUnsignedFieldTest extends FieldTestBase {
    @Test
    public void storeSignedLong() {
        StorageField field = new BigintField(new ULongType());
        DataType resultType = new LongType();
        SessionProperties sessionProperties =
            new SessionProperties(ZoneId.systemDefault(), CharsetName.defaultCharset(), 0,
                FieldCheckLevel.CHECK_FIELD_IGNORE);

        checkStore(field, sessionProperties, resultType, AbstractNumericField.INT_8_MAX, (long) AbstractNumericField.INT_8_MAX, false, TypeConversionStatus.TYPE_OK, f -> f.longValue());
        checkStore(field, sessionProperties, resultType, AbstractNumericField.INT_16_MAX, (long) AbstractNumericField.INT_16_MAX, false, TypeConversionStatus.TYPE_OK, f -> f.longValue());
        checkStore(field, sessionProperties, resultType, AbstractNumericField.INT_32_MAX, (long) AbstractNumericField.INT_32_MAX, false, TypeConversionStatus.TYPE_OK, f -> f.longValue());
        checkStore(field, sessionProperties, resultType, AbstractNumericField.INT_64_MAX, AbstractNumericField.INT_64_MAX, false, TypeConversionStatus.TYPE_OK, f -> f.longValue());

        checkStore(field, sessionProperties, resultType, AbstractNumericField.INT_8_MIN, 0L, false, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE, f -> f.longValue());
        checkStore(field, sessionProperties, resultType, AbstractNumericField.INT_16_MIN, 0L, false, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE, f -> f.longValue());
        checkStore(field, sessionProperties, resultType, AbstractNumericField.INT_32_MIN, 0L, false, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE, f -> f.longValue());
        checkStore(field, sessionProperties, resultType, AbstractNumericField.INT_64_MIN, 0L, false, TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE, f -> f.longValue());

        checkStore(field, sessionProperties, resultType, null, 0L, true, TypeConversionStatus.TYPE_OK, f -> f.longValue());
    }
}

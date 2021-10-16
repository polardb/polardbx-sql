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
import org.junit.Assert;

import java.time.ZoneId;
import java.util.function.Function;

public class FieldTestBase {
    protected static final SessionProperties DEFAULT_SESSION_PROPERTIES = new SessionProperties(
        ZoneId.systemDefault(),
        CharsetName.defaultCharset(),
        0,
        FieldCheckLevel.CHECK_FIELD_WARN
    );

    /**
     * Check the correctness of the field storage.
     *
     * @param field storage field.
     * @param sessionProperties the session properties (including the thread variables)
     * @param fromType The data type of value to be stored.
     * @param value The value to be stored.
     * @param expectResult The expect result of the reading.
     * @param expectedStatus The expect status of the reading.
     * @param getter The function to get the result.
     */
    protected void checkStore(StorageField field,
                              SessionProperties sessionProperties, DataType<?> fromType, Object value,
                              Object expectResult, boolean isExpectedNull, TypeConversionStatus expectedStatus,
                              Function<StorageField, Object> getter) {
        field.reset();
        field.store(value, fromType, sessionProperties);

        // check storage status
        TypeConversionStatus actualStatus = field.lastStatus();
        Assert.assertEquals(expectedStatus, actualStatus);

        // check reading result.
        Object actualResult = getter.apply(field);
        Assert.assertEquals(expectResult, actualResult);

        // check the result is null or not.
        boolean isNull = field.isNull();
        Assert.assertEquals(isExpectedNull, isNull);
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    protected static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}

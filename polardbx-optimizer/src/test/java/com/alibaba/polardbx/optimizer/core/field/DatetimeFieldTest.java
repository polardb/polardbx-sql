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
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.junit.Test;

import java.time.ZoneId;

import static com.alibaba.polardbx.common.SQLModeFlags.*;

public class DatetimeFieldTest extends FieldTestBase {
    DataType fieldType = new DateTimeType(0);

    DataType resultType = new VarcharType();

    StorageField datetimeField = new DatetimeField(fieldType);

    int modeSize = 3;

    long[] strictModes = {
        MODE_STRICT_TRANS_TABLES,
        MODE_STRICT_ALL_TABLES,
        MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES
    };

    @Test
    public void storeLegalString() {
        checkInternal("2001-01-01 00:00:01", "2001-01-01 00:00:01", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkInternal("0000-00-00 00:00:00", "0000-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkInternal("0001-00-00 00:00:00", "0001-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void storeIllegal() {
        // Bad year
        checkInternal("99999-01-01 00:00:01", "0000-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad month
        checkInternal("2001-13-01 00:00:01", "0000-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad day
        checkInternal("2001-01-32 00:00:01", "0000-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad hour
        checkInternal("2001-01-01 72:00:01", "0000-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad minute
        checkInternal("2001-01-01 00:72:01", "0000-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad second
        checkInternal("2001-01-01 00:00:72", "0000-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Not a day
        checkInternal("foo", "0000-00-00 00:00:00", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

    }

    @Test
    public void storeZeroDateSqlModeNoZeroRestrictions() {
        checkSqlModeInternal(0L, "0000-00-00 00:00:00", "0000-00-00 00:00:00", TypeConversionStatus.TYPE_OK);

        checkSqlModeInternal(0L, "0000-01-01 00:00:00", "0000-01-01 00:00:00", TypeConversionStatus.TYPE_OK);

        checkSqlModeInternal(0L, "2001-00-01 00:00:00", "2001-00-01 00:00:00", TypeConversionStatus.TYPE_OK);

        checkSqlModeInternal(0L, "2001-01-00 00:00:00", "2001-01-00 00:00:00", TypeConversionStatus.TYPE_OK);

    }

    @Test
    public void storeZeroDateSqlModeNoZeroDate() {
        // With "MODE_NO_ZERO_DATE" set - Errors if date is all null
        checkSqlModeInternal(MODE_NO_ZERO_DATE, "0000-00-00 00:00:00", "0000-00-00 00:00:00", TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Zero year, month or day is fine
        checkSqlModeInternal(MODE_NO_ZERO_DATE, "0000-01-01 00:00:00", "0000-01-01 00:00:00", TypeConversionStatus.TYPE_OK);

        checkSqlModeInternal(MODE_NO_ZERO_DATE, "2001-00-01 00:00:00", "2001-00-01 00:00:00", TypeConversionStatus.TYPE_OK);

        checkSqlModeInternal(MODE_NO_ZERO_DATE, "2001-01-00 00:00:00", "2001-01-00 00:00:00", TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void storeZeroDateSqlModeNoZeroInDate() {
        // With "MODE_NO_ZERO_IN_DATE" set - Entire date zero is ok
        checkSqlModeInternal(MODE_NO_ZERO_IN_DATE, "0000-00-00 00:00:00", "0000-00-00 00:00:00", TypeConversionStatus.TYPE_OK);

        // Year 0 is valid in strict mode too
        checkSqlModeInternal(MODE_NO_ZERO_IN_DATE, "0000-01-01 00:00:00", "0000-01-01 00:00:00", TypeConversionStatus.TYPE_OK);

        // Month 0 is NOT valid in strict mode, stores all-zero date
        checkSqlModeInternal(MODE_NO_ZERO_IN_DATE, "2001-00-01 00:00:00", "0000-00-00 00:00:00", TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Day 0 is NOT valid in strict mode, stores all-zero date
        checkSqlModeInternal(MODE_NO_ZERO_IN_DATE, "2001-01-00 00:00:00", "0000-00-00 00:00:00", TypeConversionStatus.TYPE_ERR_BAD_VALUE);
    }

    protected void checkInternal(String value, String expectedValue, SessionProperties sessionProperties,
                                 TypeConversionStatus typeOk) {
        checkStore(
            datetimeField,
            sessionProperties,
            resultType,
            value,
            expectedValue,
            false,
            typeOk,
            f -> f.stringValue(sessionProperties).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );
    }

    protected void checkSqlModeInternal(long extraFlag, String value, String expectedValue,
                                        TypeConversionStatus typeOk) {
        for (int i = 0; i < modeSize; i++) {
            SessionProperties sessionProperties = new SessionProperties(
                ZoneId.systemDefault(),
                CharsetName.defaultCharset(),
                extraFlag | strictModes[i],
                FieldCheckLevel.CHECK_FIELD_WARN);

            checkInternal(value, expectedValue, sessionProperties, typeOk);
        }
    }

}

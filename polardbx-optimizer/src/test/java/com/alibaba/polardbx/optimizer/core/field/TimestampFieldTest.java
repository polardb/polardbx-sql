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
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;

import static com.alibaba.polardbx.common.SQLModeFlags.*;

public class TimestampFieldTest extends FieldTestBase {
    DataType fieldType = new TimestampType(5);

    DataType resultType = new VarcharType();

    StorageField timestampField = new TimestampField(fieldType);

    int modeSize = 3;

    long[] strictModes = {
        MODE_STRICT_TRANS_TABLES,
        MODE_STRICT_ALL_TABLES,
        MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES
    };

    @Test
    public void storeNumber() {
        // illegal number
        checkStore(
            timestampField,
            DEFAULT_SESSION_PROPERTIES,
            new LongType(),
            1L,
            "0x00000000000000",
            false,
            TypeConversionStatus.TYPE_ERR_BAD_VALUE,
            f -> "0x" + bytesToHex(f.rawBytes())
        );

        // legal number
        checkStore(
            timestampField,
            DEFAULT_SESSION_PROPERTIES,
            new LongType(),
            20001112131415L,
            "0x3A0E2727000000",
            false,
            TypeConversionStatus.TYPE_OK,
            f -> "0x" + bytesToHex(f.rawBytes())
        );

        // legal number
        checkStore(
            timestampField,
            DEFAULT_SESSION_PROPERTIES,
            new LongType(),
            791112131415L,
            "0x128D04A7000000",
            false,
            TypeConversionStatus.TYPE_OK,
            f -> "0x" + bytesToHex(f.rawBytes())
        );

        // illegal number
        checkStore(
            timestampField,
            DEFAULT_SESSION_PROPERTIES,
            new LongType(),
            2000111213141512L,
            "0x00000000000000",
            false,
            TypeConversionStatus.TYPE_ERR_BAD_VALUE,
            f -> "0x" + bytesToHex(f.rawBytes())
        );
    }

    @Test
    public void storeLegalString() {
        checkInternal("2001-01-01 00:00:00", "0x3A4F5800000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);

        checkInternal("0000-00-00 00:00:00", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_OK);
    }

    @Test
    public void storeIllegalString() {
        // for timestamp, no zero in date!
        checkInternal("0001-00-00 00:00:00", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_NOTE_TIME_TRUNCATED);

        // Bad year
        checkInternal("99999-01-01 00:00:01", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad month
        checkInternal("2001-13-01 00:00:01", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad day
        checkInternal("2001-01-32 00:00:01", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad hour
        checkInternal("2001-01-01 72:00:01", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad minute
        checkInternal("2001-01-01 00:72:01", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Bad second
        checkInternal("2001-01-01 00:00:72", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        // Not a day
        checkInternal("foo", "0x00000000000000", DEFAULT_SESSION_PROPERTIES, TypeConversionStatus.TYPE_ERR_BAD_VALUE);
    }

    @Test
    public void storeZeroDateSqlModeNoZeroRestrictions() {
        // zero date is legal in strict mode
        checkSqlModeInternal(0L, "0000-00-00 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_OK);

        checkSqlModeInternal(0L, "1970-01-02 00:00:02", "0x0000E102000000", TypeConversionStatus.TYPE_OK);

        checkSqlModeInternal(0L, "2038-01-19 03:14:08", "0x7FFF8F80000000", TypeConversionStatus.TYPE_OK);

        // out of range
        checkSqlModeInternal(0L, "0000-01-01 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkSqlModeInternal(0L, "1970-1-1 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkSqlModeInternal(0L, "1969-12-31 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkSqlModeInternal(0L, "2038-01-20 03:14:08", "0x00000000000000", TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        // for timestamp, no zero in date!
        checkSqlModeInternal(0L, "2001-00-01 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        checkSqlModeInternal(0L, "2001-01-00 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_ERR_BAD_VALUE);
    }

    @Test
    public void storeZeroDateSqlModeNoZeroDate() {
        // With "MODE_NO_ZERO_DATE" set - Errors if date is all null
        checkSqlModeInternal(MODE_NO_ZERO_DATE, "0000-00-00 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        checkSqlModeInternal(MODE_NO_ZERO_DATE, "0000-01-01 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE);

        checkSqlModeInternal(MODE_NO_ZERO_DATE, "2001-00-01 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_ERR_BAD_VALUE);

        checkSqlModeInternal(MODE_NO_ZERO_DATE, "2001-01-00 00:00:00", "0x00000000000000", TypeConversionStatus.TYPE_ERR_BAD_VALUE);
    }

    @Test
    public void test() {
        Assert.assertFalse(MySQLTimeTypeUtil.checkTimestampRange(new MysqlDateTime(
            2038, 01, 20, 03, 14, 07, 0
        )));
    }

    protected void checkInternal(String value, String expectedValue, SessionProperties sessionProperties,
                                 TypeConversionStatus typeOk) {
        checkStore(
            timestampField,
            sessionProperties,
            resultType,
            value,
            expectedValue,
            false,
            typeOk,
            f -> "0x" + bytesToHex(f.rawBytes())
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

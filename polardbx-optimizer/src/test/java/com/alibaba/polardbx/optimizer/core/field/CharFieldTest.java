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
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import org.junit.Test;

public class CharFieldTest extends FieldTestBase {
    String shortStr = "abc";
    String longStr = "abcd";
    String spaceStr = "abc ";
    String badChar = "𝌆abc";
    String badCharEnd = "abc𝌆";

    DataType shortStrType = new VarcharType(CollationName.LATIN1_SWEDISH_CI, shortStr.length());
    DataType longStrType = new VarcharType(CollationName.LATIN1_SWEDISH_CI, longStr.length());
    DataType spaceStrType = new VarcharType(CollationName.LATIN1_SWEDISH_CI, spaceStr.length());
    DataType badCharType = new VarcharType(CollationName.BINARY, badChar.length());
    DataType badCharEndType = new VarcharType(CollationName.BINARY, badCharEnd.length());

    StorageField charField = new CharField(new VarcharType(CollationName.LATIN1_SWEDISH_CI, 3));
    StorageField charFieldGbk = new CharField(new VarcharType(CollationName.GBK_CHINESE_CI, 20));

    StorageField varcharField = new VarcharField(new VarcharType(CollationName.LATIN1_SWEDISH_CI, 3));
    StorageField varcharFieldGbk = new VarcharField(new VarcharType(CollationName.GBK_CHINESE_CI, 20));

    @Test
    public void testChar() {
        checkStore(
            charField,
            DEFAULT_SESSION_PROPERTIES,
            shortStrType,
            shortStr,
            shortStr,
            false,
            TypeConversionStatus.TYPE_OK,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );

        checkStore(
            charField,
            DEFAULT_SESSION_PROPERTIES,
            longStrType,
            longStr,
            "abc",
            false,
            TypeConversionStatus.TYPE_WARN_TRUNCATED,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );

        checkStore(
            charField,
            DEFAULT_SESSION_PROPERTIES,
            spaceStrType,
            spaceStr,
            "abc",
            false,
            TypeConversionStatus.TYPE_OK,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );

        checkStore(
            charFieldGbk,
            DEFAULT_SESSION_PROPERTIES,
            badCharType,
            badChar,
            "",
            false,
            TypeConversionStatus.TYPE_WARN_INVALID_STRING,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );

        checkStore(
            charFieldGbk,
            DEFAULT_SESSION_PROPERTIES,
            badCharEndType,
            badCharEnd,
            "",
            false,
            TypeConversionStatus.TYPE_WARN_INVALID_STRING,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );
    }

    @Test
    public void testVarcharField() {
        checkStore(
            varcharField,
            DEFAULT_SESSION_PROPERTIES,
            shortStrType,
            shortStr,
            shortStr,
            false,
            TypeConversionStatus.TYPE_OK,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );

        checkStore(
            varcharField,
            DEFAULT_SESSION_PROPERTIES,
            longStrType,
            longStr,
            "abc",
            false,
            TypeConversionStatus.TYPE_WARN_TRUNCATED,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );

        checkStore(
            varcharField,
            DEFAULT_SESSION_PROPERTIES,
            spaceStrType,
            spaceStr,
            "abc",
            false,
            TypeConversionStatus.TYPE_NOTE_TRUNCATED,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );

        checkStore(
            varcharFieldGbk,
            DEFAULT_SESSION_PROPERTIES,
            badCharType,
            badChar,
            "",
            false,
            TypeConversionStatus.TYPE_WARN_INVALID_STRING,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );

        checkStore(
            varcharFieldGbk,
            DEFAULT_SESSION_PROPERTIES,
            badCharEndType,
            badCharEnd,
            "",
            false,
            TypeConversionStatus.TYPE_WARN_INVALID_STRING,
            f -> f.stringValue(DEFAULT_SESSION_PROPERTIES).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK)
        );
    }

    @Test
    public void testEmpty() {
        checkStore(
            varcharField,
            DEFAULT_SESSION_PROPERTIES,
            spaceStrType,
            "",
            0L,
            false,
            TypeConversionStatus.TYPE_OK,
            f -> f.longValue(DEFAULT_SESSION_PROPERTIES)
        );
    }
}

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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import org.junit.Ignore;
import org.junit.Test;

public class MixOfCollationOfBetweenColTest extends DataTypeTestBase {
    @Ignore
    @Test
    public void testConcat() {
        sql("select concat(v_utf8mb4, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_utf8mb4_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4, v_utf8mb4_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf8mb4, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_ascii) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4, v_utf8) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf8mb4, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_gbk) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_big5) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4, v_big5_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf8mb4_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_general_ci, v_utf8mb4_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_general_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf8mb4_general_ci, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_ascii) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_general_ci, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_general_ci, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_general_ci, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_general_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf8mb4_general_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_gbk) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_big5) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_general_ci, v_big5_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf8mb4) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_unicode_ci, v_utf8mb4_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_unicode_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf8mb4_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_unicode_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf8mb4_unicode_ci, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_ascii) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_unicode_ci, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_unicode_ci, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_unicode_ci, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_unicode_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf8mb4_unicode_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_gbk) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_big5) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_unicode_ci, v_big5_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8mb4_bin, v_utf8mb4) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_bin, v_utf8mb4_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_bin, v_utf8mb4_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_bin, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf8mb4_bin, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_ascii) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_bin, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_bin, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_bin, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8mb4_bin, v_utf8) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8mb4_bin, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf8mb4_bin, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8mb4_bin, v_latin1) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_gbk) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_big5) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8mb4_bin, v_big5_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_binary, v_utf8mb4) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_ascii_bin) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_ascii_general_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_ascii) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf16) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf16_bin) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf16_general_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf8) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf8_bin) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf8_general_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf16le) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf16le_bin) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1_bin) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1_general_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1_general_cs) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_gbk) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_gbk_bin) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_big5) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_binary, v_big5_bin) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_ascii_bin, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_ascii_bin, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_ascii_bin, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_ascii_bin, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_ascii_bin, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_ascii_bin, v_ascii_bin) from collation_test")
            .type(CharsetName.ASCII, CollationName.ASCII_BIN);
        sql("select concat(v_ascii_bin, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_ascii_bin, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_ascii_bin, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_ascii_bin, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_ascii_bin, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_ascii_bin, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_ascii_bin, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_ascii_bin, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_ascii_bin, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_ascii_bin, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_ascii_bin, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_ascii_bin, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_bin, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_ascii_general_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_ascii_general_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_ascii_general_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_ascii_general_ci) from collation_test")
            .type(CharsetName.ASCII, CollationName.ASCII_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_ascii) from collation_test")
            .type(CharsetName.ASCII, CollationName.ASCII_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_ascii_general_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_ascii_general_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_ascii_general_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_ascii_general_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_ascii_general_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_ascii_general_ci, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii_general_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_ascii, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_ascii, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_ascii, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_ascii, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_ascii, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_ascii_general_ci) from collation_test")
            .type(CharsetName.ASCII, CollationName.ASCII_GENERAL_CI);
        sql("select concat(v_ascii, v_ascii) from collation_test")
            .type(CharsetName.ASCII, CollationName.ASCII_GENERAL_CI);
        sql("select concat(v_ascii, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_ascii, v_utf16_bin) from collation_test").type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_ascii, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_ascii, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_ascii, v_utf8) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_ascii, v_utf8_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_ascii, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_ascii, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_ascii, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_ascii, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_ascii, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_ascii, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_ascii, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf8mb4) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf8mb4_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf8mb4_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf8mb4_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf16, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_ascii) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf8) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf8_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf8_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf8_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_gbk) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_big5) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16, v_big5_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_bin, v_utf8mb4) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf8mb4_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf8mb4_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf8mb4_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf16_bin, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_ascii) from collation_test").type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf8) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf8_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf8_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf8_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_bin, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16_bin, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16_bin, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16_bin, v_latin1) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_gbk) from collation_test").type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_big5) from collation_test").type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_bin, v_big5_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_utf16_general_ci, v_utf8mb4) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf8mb4_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf8mb4_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf8mb4_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf16_general_ci, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_ascii) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf8) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf8_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf8_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf8_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_general_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16_general_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_gbk) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_big5) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_general_ci, v_big5_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_utf16_unicode_ci, v_utf8mb4) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf8mb4_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf8mb4_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf8mb4_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf16_unicode_ci, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_ascii) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_utf8) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf8_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf8_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf8_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16_unicode_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16_unicode_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16_unicode_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_gbk) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_big5) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf16_unicode_ci, v_big5_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_utf8, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf8, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_ascii) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf8, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8, v_utf8) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_utf8_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_utf8_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf8, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_gbk) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_big5) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8, v_big5_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_bin, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8_bin, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8_bin, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8_bin, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8_bin, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf8_bin, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_ascii) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf8_bin, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8_bin, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_bin, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_bin, v_utf8) from collation_test").throwValidateError();
        sql("select concat(v_utf8_bin, v_utf8_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_utf8_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_bin, v_utf8_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_bin, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8_bin, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf8_bin, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8_bin, v_latin1) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_gbk) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_gbk_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_big5) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_bin, v_big5_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_utf8_general_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8_general_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8_general_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf8_general_ci, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_ascii) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf8_general_ci, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8_general_ci, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_general_ci, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_general_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_utf8_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8_general_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_utf8_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_general_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf8_general_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_gbk) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_big5) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_general_ci, v_big5_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_utf8_unicode_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8_unicode_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_utf8_unicode_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_utf8_unicode_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf8_unicode_ci, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_ascii) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_utf16) from collation_test").throwValidateError();
        sql("select concat(v_utf8_unicode_ci, v_utf16_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8_unicode_ci, v_utf16_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_unicode_ci, v_utf16_unicode_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_unicode_ci, v_utf8) from collation_test").throwValidateError();
        sql("select concat(v_utf8_unicode_ci, v_utf8_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf8_unicode_ci, v_utf8_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf8_unicode_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8_unicode_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf8_unicode_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_gbk) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_big5) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf8_unicode_ci, v_big5_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_utf16le, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf16le, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_ascii) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf16) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf8) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_utf16le_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16le, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_gbk) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_big5) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le, v_big5_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_bin, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf16le_bin, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_ascii) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf16) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf8) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf16le) from collation_test").throwValidateError();
        sql("select concat(v_utf16le_bin, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_utf16le_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_utf16le_bin, v_latin1) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_gbk) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_big5) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_bin, v_big5_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_utf16le_general_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_utf16le_general_ci, v_ascii_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_ascii_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_ascii) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_utf16le_bin) from collation_test").throwValidateError();
        sql("select concat(v_utf16le_general_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1_general_cs) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_gbk) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_gbk_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_big5) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_utf16le_general_ci, v_big5_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1, v_utf8_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1, v_latin1) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_SWEDISH_CI);
        sql("select concat(v_latin1, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_SWEDISH_CI);
        sql("select concat(v_latin1, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1_swedish_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1_swedish_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1_swedish_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1_swedish_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_swedish_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1_swedish_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_swedish_ci, v_latin1) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_SWEDISH_CI);
        sql("select concat(v_latin1_swedish_ci, v_latin1_swedish_ci) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_SWEDISH_CI);
        sql("select concat(v_latin1_swedish_ci, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_swedish_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_german1_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_german1_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1_german1_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1_german1_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1_german1_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_german1_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1_german1_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_german1_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1_german1_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_german1_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1_german1_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_german1_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1_german1_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_german1_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1_german1_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_german1_ci, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_latin1_german1_ci) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_GERMAN1_CI);
        sql("select concat(v_latin1_german1_ci, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german1_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_danish_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_danish_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1_danish_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1_danish_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1_danish_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_danish_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1_danish_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_danish_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1_danish_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_danish_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1_danish_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_danish_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1_danish_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_danish_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1_danish_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_danish_ci, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_latin1_danish_ci) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_DANISH_CI);
        sql("select concat(v_latin1_danish_ci, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_danish_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_german2_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_german2_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1_german2_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1_german2_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1_german2_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_german2_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1_german2_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_german2_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1_german2_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_german2_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1_german2_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_german2_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1_german2_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_german2_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1_german2_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_german2_ci, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_latin1_german2_ci) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_GERMAN2_CI);
        sql("select concat(v_latin1_german2_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_german2_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_bin, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_bin, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1_bin, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1_bin, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1_bin, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_bin, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1_bin, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_bin, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1_bin, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_bin, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1_bin, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_bin, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1_bin, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_bin, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1_bin, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_bin, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_latin1_bin) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_BIN);
        sql("select concat(v_latin1_bin, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_bin, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1_general_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1_general_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1_general_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1_general_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1_general_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1_general_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1_general_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1_general_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_latin1_general_ci) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_GENERAL_CI);
        sql("select concat(v_latin1_general_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_general_cs, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_general_cs, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1_general_cs, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1_general_cs, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1_general_cs, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_general_cs, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1_general_cs, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_general_cs, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1_general_cs, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_general_cs, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1_general_cs, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_general_cs, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1_general_cs, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_general_cs, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1_general_cs, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_general_cs, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_latin1_general_cs) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_GENERAL_CS);
        sql("select concat(v_latin1_general_cs, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_general_cs, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_latin1_spanish_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_latin1_spanish_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_latin1_spanish_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_latin1_spanish_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_spanish_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_latin1_spanish_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_latin1_spanish_ci, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_latin1_spanish_ci) from collation_test")
            .type(CharsetName.LATIN1, CollationName.LATIN1_SPANISH_CI);
        sql("select concat(v_latin1_spanish_ci, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_latin1_spanish_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_gbk, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_gbk, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_gbk, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_gbk, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_gbk, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_gbk, v_utf16_bin) from collation_test").type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_gbk, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_gbk, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_gbk, v_utf8) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_gbk, v_utf8_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_gbk, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_gbk, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_gbk, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_gbk, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_gbk, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_gbk, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_gbk) from collation_test").type(CharsetName.GBK, CollationName.GBK_CHINESE_CI);
        sql("select concat(v_gbk, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.GBK, CollationName.GBK_CHINESE_CI);
        sql("select concat(v_gbk, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_gbk_chinese_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_gbk_chinese_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_gbk_chinese_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_gbk_chinese_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_gbk_chinese_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_gbk_chinese_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_gbk_chinese_ci, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_gbk) from collation_test")
            .type(CharsetName.GBK, CollationName.GBK_CHINESE_CI);
        sql("select concat(v_gbk_chinese_ci, v_gbk_chinese_ci) from collation_test")
            .type(CharsetName.GBK, CollationName.GBK_CHINESE_CI);
        sql("select concat(v_gbk_chinese_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_chinese_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_gbk_bin, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_gbk_bin, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_gbk_bin, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_gbk_bin, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_gbk_bin, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_gbk_bin, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_gbk_bin, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_gbk_bin, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_gbk_bin, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_gbk_bin, v_utf8_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_gbk_bin, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_gbk_bin, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_gbk_bin, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_gbk_bin, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_gbk_bin, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_gbk_bin, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_gbk_bin) from collation_test").type(CharsetName.GBK, CollationName.GBK_BIN);
        sql("select concat(v_gbk_bin, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_gbk_bin, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_big5, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_big5, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_big5, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_big5, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_big5, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_big5, v_utf16_bin) from collation_test").type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_big5, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_big5, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_big5, v_utf8) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_big5, v_utf8_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_big5, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_big5, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_big5, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_big5, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_big5, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_big5, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5, v_big5) from collation_test").type(CharsetName.BIG5, CollationName.BIG5_CHINESE_CI);
        sql("select concat(v_big5, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.BIG5, CollationName.BIG5_CHINESE_CI);
        sql("select concat(v_big5, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_big5_chinese_ci, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_big5_chinese_ci, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_big5_chinese_ci, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_big5_chinese_ci, v_binary) from collation_test")
            .type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_big5_chinese_ci, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_big5_chinese_ci, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_big5_chinese_ci, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_big5_chinese_ci, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_big5_chinese_ci, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_big5_chinese_ci, v_utf8_bin) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_big5_chinese_ci, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_big5_chinese_ci, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_big5_chinese_ci, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_big5_chinese_ci, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_big5_chinese_ci, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_big5_chinese_ci, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5_chinese_ci, v_big5) from collation_test")
            .type(CharsetName.BIG5, CollationName.BIG5_CHINESE_CI);
        sql("select concat(v_big5_chinese_ci, v_big5_chinese_ci) from collation_test")
            .type(CharsetName.BIG5, CollationName.BIG5_CHINESE_CI);
        sql("select concat(v_big5_chinese_ci, v_big5_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_utf8mb4) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_big5_bin, v_utf8mb4_general_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        sql("select concat(v_big5_bin, v_utf8mb4_unicode_ci) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
        sql("select concat(v_big5_bin, v_utf8mb4_bin) from collation_test")
            .type(CharsetName.UTF8MB4, CollationName.UTF8MB4_BIN);
        sql("select concat(v_big5_bin, v_binary) from collation_test").type(CharsetName.BINARY, CollationName.BINARY);
        sql("select concat(v_big5_bin, v_ascii_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_ascii_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_ascii) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_utf16) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_big5_bin, v_utf16_bin) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_BIN);
        sql("select concat(v_big5_bin, v_utf16_general_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_GENERAL_CI);
        sql("select concat(v_big5_bin, v_utf16_unicode_ci) from collation_test")
            .type(CharsetName.UTF16, CollationName.UTF16_UNICODE_CI);
        sql("select concat(v_big5_bin, v_utf8) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_big5_bin, v_utf8_bin) from collation_test").type(CharsetName.UTF8, CollationName.UTF8_BIN);
        sql("select concat(v_big5_bin, v_utf8_general_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_GENERAL_CI);
        sql("select concat(v_big5_bin, v_utf8_unicode_ci) from collation_test")
            .type(CharsetName.UTF8, CollationName.UTF8_UNICODE_CI);
        sql("select concat(v_big5_bin, v_utf16le) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_big5_bin, v_utf16le_bin) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_BIN);
        sql("select concat(v_big5_bin, v_utf16le_general_ci) from collation_test")
            .type(CharsetName.UTF16LE, CollationName.UTF16LE_GENERAL_CI);
        sql("select concat(v_big5_bin, v_latin1) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_latin1_swedish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_latin1_german1_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_latin1_danish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_latin1_german2_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_latin1_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_latin1_general_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_latin1_general_cs) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_latin1_spanish_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_gbk) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_gbk_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_gbk_bin) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_big5) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_big5_chinese_ci) from collation_test").throwValidateError();
        sql("select concat(v_big5_bin, v_big5_bin) from collation_test").type(CharsetName.BIG5, CollationName.BIG5_BIN);
    }
}

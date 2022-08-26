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

package com.alibaba.polardbx.qatest.dql.sharding.charset;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * insert into collation_test (v_utf8mb4_general_ci) values (?)
 * select distinct(v_utf8mb4_general_ci) from collation_test;
 */

public class CollationDistinctTest extends CharsetTestBase {
    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return PARAMETERS;
    }

    public CollationDistinctTest(String table, String suffix) {
        this.table = randomTableName(table, 4);
        this.suffix = suffix;
    }

    // gbk_chinese_ci
    @Test
    public void testGBK_CHINESE_CI() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGBKUnicode(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(gbkStrings, COL_GBK_CHINESE_CI);
    }

    @Test
    public void testGBK_BIN() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGBKUnicode(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(gbkStrings, COL_GBK_BIN);
    }

    // utf8mb4_general_ci
    @Test
    public void testUTF8MB4_GENERAL_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(utf8Strings, COL_UTF8MB4_GENERAL_CI);
    }

    // utf8mb4_bin
    @Test
    public void testUTF8MB4_BIN() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(utf8Strings, COL_UTF8MB4_BIN);
    }

    // utf8_general_ci
    @Ignore
    @Test
    public void testUTF8_GENERAL_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB3(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(utf8Strings, COL_UTF8_GENERAL_CI);
    }

    // utf16_general_ci
    @Ignore
    @Test
    public void testUTF16_GENERAL_CI() {
        List<byte[]> utf16Strings = CharsetTestUtils.generateUTF16(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(utf16Strings, COL_UTF16_GENERAL_CI);
    }

    // utf16_bin
    @Ignore
    @Test
    public void testUTF16_BIN() {
        List<byte[]> utf16Strings = CharsetTestUtils.generateUTF16(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(utf16Strings, COL_UTF16_BIN);
    }

    // utf8_bin
    @Ignore
    @Test
    public void testUTF8_BIN() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB3(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(utf8Strings, COL_UTF8_BIN);
    }

    @Test
    public void testLATIN1_GENERAL_CI() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(latin1Strings, COL_LATIN1_GENERAL_CI);
    }

    @Test
    public void testLATIN1_GENERAL_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(latin1Strings, COL_LATIN1_GENERAL_CS);
    }

    @Test
    public void testLATIN1_BIN() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(latin1Strings, COL_LATIN1_BIN);
    }

    @Test
    public void testLATIN1_SWEDISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(latin1Strings, COL_LATIN1_SWEDISH_CI);
    }

    @Test
    public void testLATIN1_SPANISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(latin1Strings, COL_LATIN1_SPANISH_CI);
    }

    @Test
    public void testLATIN1_GERMAN1_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(latin1Strings, COL_LATIN1_GERMAN1_CI);
    }

    @Test
    public void testLATIN1_DANISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(latin1Strings, COL_LATIN1_DANISH_CI);
    }

    @Test
    public void testASCII_BIN() {
        List<byte[]> asciiStrings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(asciiStrings, COL_ASCII_BIN);
    }

    @Test
    public void testASCII_GENERAL_CI() {
        List<byte[]> asciiStrings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testDistinct(asciiStrings, COL_ASCII_GENERAL_CI);
    }

}

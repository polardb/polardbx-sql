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
 * select v_utf8mb4_general_ci from collation_test order by v_utf8mb4_general_ci, hex(v_utf8mb4_general_ci);
 */

public class CollationOrderByTest extends CharsetTestBase {
    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return PARAMETERS;
    }

    public CollationOrderByTest(String table, String suffix) {
        this.table = randomTableName(table, 4);
        this.suffix = suffix;
    }

    // gbk_bin
    @Test
    public void testGBK_BIN() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGBKUnicode(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(gbkStrings, COL_GBK_BIN);
    }

    // gbk_chinese_ci
    @Test
    public void testGBK_CHINESE_CI() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGBKUnicode(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(gbkStrings, COL_GBK_CHINESE_CI);
    }

    // Ignore because jdk uses GB18030-2000 while Mysql uses GB18030-2005
    // gb18030_chinese_ci
    @Ignore
    @Test
    public void testGB18030_CHINESE_CI() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGB18030Unicode(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(gbkStrings, COL_GB18030_CHINESE_CI);
    }

    // gb18030_bin
    @Ignore
    @Test
    public void testGB18030_BIN() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGB18030Unicode(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(gbkStrings, COL_GB18030_BIN);
    }

    // gb18030_unicode_520_ci
    @Ignore
    @Test
    public void testGB18030_UNICODE_520_CI() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGB18030Unicode(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(gbkStrings, COL_GB18030_UNICODE_520_CI);
    }

    // utf8mb4_general_ci
    @Test
    public void testUTF8MB4_GENERAL_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(utf8Strings, COL_UTF8MB4_GENERAL_CI);
    }

    // utf8mb4_bin
    @Test
    public void testUTF8MB4_BIN() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(utf8Strings, COL_UTF8MB4_BIN);
    }

    // utf8mb4_unicode_ci
    @Test
    public void testUTF8MB4_UNICODE_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(utf8Strings, COL_UTF8MB4_UNICODE_CI);
    }

    // utf8mb4_unicode_520_ci
    @Test
    public void testUTF8MB4_UNICODE_520_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(utf8Strings, COL_UTF8MB4_UNICODE_520_CI);
    }

    // utf8_general_ci
    @Ignore
    @Test
    public void testUTF8_GENERAL_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB3(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(utf8Strings, COL_UTF8_GENERAL_CI);
    }

    // utf8_bin
    @Ignore
    @Test
    public void testUTF8_BIN() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB3(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(utf8Strings, COL_UTF8_BIN);
    }

    // utf8_unicode_ci
    @Ignore
    @Test
    public void testUTF8_UNICODE_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB3(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(utf8Strings, COL_UTF8_UNICODE_CI);
    }

    @Test
    public void testLATIN1_GENERAL_CI() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(latin1Strings, COL_LATIN1_GENERAL_CI);
    }

    @Test
    public void testLATIN1_GENERAL_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(latin1Strings, COL_LATIN1_GENERAL_CS);
    }

    @Test
    public void testLATIN1_BIN() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(latin1Strings, COL_LATIN1_BIN);
    }

    @Test
    public void testLATIN1_SWEDISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(latin1Strings, COL_LATIN1_SWEDISH_CI);
    }

    @Test
    public void testLATIN1_SPANISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(latin1Strings, COL_LATIN1_SPANISH_CI);
    }

    @Test
    public void testLATIN1_GERMAN1_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(latin1Strings, COL_LATIN1_GERMAN1_CI);
    }

    @Test
    public void testLATIN1_DANISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(latin1Strings, COL_LATIN1_DANISH_CI);
    }

    @Test
    public void testASCII_BIN() {
        List<byte[]> asciiStrings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(asciiStrings, COL_ASCII_BIN);
    }

    @Test
    public void testASCII_GENERAL_CI() {
        List<byte[]> asciiStrings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        testOrderBy(asciiStrings, COL_ASCII_GENERAL_CI);
    }
}

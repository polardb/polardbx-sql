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

@Ignore

public class CollationInstrTest extends CharsetTestBase {
    private static final int INSTR_SIZE = 100;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return PARAMETERS;
    }

    public CollationInstrTest(String table, String suffix) {
        this.table = randomTableName(table, 4);
        this.suffix = suffix;
    }

    // gbk_chinese_ci
    @Test
    public void testGBK_CHINESE_CI() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGBKCode(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateGBKCode(INSTR_SIZE, 1, false);

        testInstr(gbkStrings, subStrs, COL_GBK_CHINESE_CI, "gbk", "gbk_chinese_ci");
    }

    @Test
    public void testGBK_BIN() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGBKCode(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateGBKCode(INSTR_SIZE, 1, false);

        testInstr(gbkStrings, subStrs, COL_GBK_BIN, "gbk", "gbk_bin");
    }

    // utf8mb4_general_ci
    @Test
    public void testUTF8MB4_GENERAL_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateUTF8MB4(INSTR_SIZE, 1, false);

        testInstr(utf8Strings, subStrs, COL_UTF8MB4_GENERAL_CI, "utf8mb4", "utf8mb4_general_ci");
    }

    // utf8mb4_bin
    @Test
    public void testUTF8MB4_BIN() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateUTF8MB4(INSTR_SIZE, 1, false);

        testInstr(utf8Strings, subStrs, COL_UTF8MB4_BIN, "utf8mb4", "utf8mb4_bin");
    }

    // utf8_general_ci
    @Ignore
    @Test
    public void testUTF8_GENERAL_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB3(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateUTF8MB3(INSTR_SIZE, 1, false);

        testInstr(utf8Strings, subStrs, COL_UTF8_GENERAL_CI, "utf8", "utf8_general_ci");
    }

    // utf8_bin
    @Ignore
    @Test
    public void testUTF8_BIN() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB3(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateUTF8MB3(INSTR_SIZE, 1, false);

        testInstr(utf8Strings, subStrs, COL_UTF8_BIN, "utf8", "utf8_bin");
    }

    @Test
    public void testLATIN1_GENERAL_CI() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_GENERAL_CI, "latin1", "latin1_general_ci");
    }

    @Test
    public void testLATIN1_GENERAL_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_GENERAL_CS, "latin1", "latin1_general_cs");
    }

    @Test
    public void testLATIN1_BIN() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_BIN, "latin1", "latin1_bin");
    }

    @Test
    public void testLATIN1_SWEDISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_SWEDISH_CI, "latin1", "latin1_swedish_ci");
    }

    @Test
    public void testLATIN1_SPANISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_SPANISH_CI, "latin1", "latin1_spanish_ci");
    }

    @Test
    public void testLATIN1_GERMAN1_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_GERMAN1_CI, "latin1", "latin1_german1_ci");
    }

    @Test
    public void testLATIN1_DANISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_DANISH_CI, "latin1", "latin1_danish_ci");

    }

    @Test
    public void testASCII_BIN() {
        List<byte[]> asciiStrings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(asciiStrings, subStrs, COL_ASCII_BIN, "ascii", "ascii_bin");
    }

    @Test
    public void testASCII_GENERAL_CI() {
        List<byte[]> asciiStrings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(asciiStrings, subStrs, COL_ASCII_GENERAL_CI, "ascii", "ascii_general_ci");
    }
}

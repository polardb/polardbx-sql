package com.alibaba.polardbx.qatest.dql.auto.charset;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

public class CollationInstrTest extends CharsetTestBase {
    private static final int INSTR_SIZE = 100;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return PARAMETERS_FOR_PART_TBL;
    }

    public CollationInstrTest(String table, String suffix) {
        this.table = randomTableName(table, 4);
        this.suffix = suffix;
    }

    // gbk_chinese_ci
    @Test
    public void testGBK_CHINESE_CI() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGBKUnicode(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateGBKUnicode(INSTR_SIZE, 1, false);

        testInstrRaw(gbkStrings, subStrs, COL_GBK_CHINESE_CI);
    }

    // gbk_bin
    @Test
    public void testGBK_BIN() {
        List<byte[]> gbkStrings = CharsetTestUtils.generateGBKUnicode(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateGBKUnicode(INSTR_SIZE, 1, false);

        testInstrRaw(gbkStrings, subStrs, COL_GBK_BIN);
    }

    // gb18030_chinese_ci
    @Test
    public void testGB18030_CHINESE_CI() {
        List<byte[]> gb18030Strings = CharsetTestUtils.generateGB18030Unicode(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateGB18030Unicode(INSTR_SIZE, 1, false);

        testInstrRaw(gb18030Strings, subStrs, COL_GB18030_CHINESE_CI);
    }

    // gb18030_bin
    @Test
    public void testGB18030_BIN() {
        List<byte[]> gb18030Strings = CharsetTestUtils.generateGB18030Unicode(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateGB18030Unicode(INSTR_SIZE, 1, false);

        testInstrRaw(gb18030Strings, subStrs, COL_GB18030_BIN);
    }

    // gb18030_unicode_520_ci
    @Ignore
    @Test
    public void testGB18030_UNICODE_520_CI() {
        List<byte[]> gb18030Strings = CharsetTestUtils.generateGB18030Unicode(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateGB18030Unicode(INSTR_SIZE, 1, false);

        testInstrRaw(gb18030Strings, subStrs, COL_GB18030_UNICODE_520_CI);
    }

    // utf8mb4_general_ci
    @Ignore
    @Test
    public void testUTF8MB4_GENERAL_CI() {
        List<byte[]> utf8Strings = CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);
        List<byte[]> subStrs = CharsetTestUtils.generateUTF8MB4(INSTR_SIZE, 1, false);

        testInstr(utf8Strings, subStrs, COL_UTF8MB4_GENERAL_CI, "utf8mb4", "utf8mb4_general_ci");
    }

    // utf8mb4_bin
    @Ignore
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

    @Ignore
    @Test
    public void testLATIN1_GENERAL_CI() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_GENERAL_CI, "latin1", "latin1_general_ci");
    }

    @Ignore
    @Test
    public void testLATIN1_GENERAL_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_GENERAL_CS, "latin1", "latin1_general_cs");
    }

    @Ignore
    @Test
    public void testLATIN1_BIN() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_BIN, "latin1", "latin1_bin");
    }

    @Ignore
    @Test
    public void testLATIN1_SWEDISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_SWEDISH_CI, "latin1", "latin1_swedish_ci");
    }

    @Ignore
    @Test
    public void testLATIN1_SPANISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_SPANISH_CI, "latin1", "latin1_spanish_ci");
    }

    @Ignore
    @Test
    public void testLATIN1_GERMAN1_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_GERMAN1_CI, "latin1", "latin1_german1_ci");
    }

    @Ignore
    @Test
    public void testLATIN1_DANISH_CS() {
        List<byte[]> latin1Strings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(latin1Strings, subStrs, COL_LATIN1_DANISH_CI, "latin1", "latin1_danish_ci");

    }

    @Ignore
    @Test
    public void testASCII_BIN() {
        List<byte[]> asciiStrings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(asciiStrings, subStrs, COL_ASCII_BIN, "ascii", "ascii_bin");
    }

    @Ignore
    @Test
    public void testASCII_GENERAL_CI() {
        List<byte[]> asciiStrings = CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);

        List<byte[]> subStrs = CharsetTestUtils.generateSimple(INSTR_SIZE, 1, false);
        testInstr(asciiStrings, subStrs, COL_ASCII_GENERAL_CI, "ascii", "ascii_general_ci");
    }
}

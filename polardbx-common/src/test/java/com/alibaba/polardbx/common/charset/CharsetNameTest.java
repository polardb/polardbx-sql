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

package com.alibaba.polardbx.common.charset;

import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.polardbx.common.charset.CharsetName.*;
import static com.alibaba.polardbx.common.charset.CharsetName.BINARY;
import static com.alibaba.polardbx.common.charset.CollationName.*;

public class CharsetNameTest {
    @Test
    public void testMatch() {
        doTestMatch("big5_chinese_ci", BIG5, true);
        doTestMatch("latin2_czech_cs", LATIN2, true);
        doTestMatch("dec8_swedish_ci", DEC8, true);
        doTestMatch("cp850_general_ci", CP850, true);
        doTestMatch("latin1_german1_ci", LATIN1, true);
        doTestMatch("hp8_english_ci", HP8, true);
        doTestMatch("koi8r_general_ci", KOI8R, true);
        doTestMatch("latin1_swedish_ci", LATIN1, true);
        doTestMatch("latin2_general_ci", LATIN2, true);
        doTestMatch("swe7_swedish_ci", SWE7, true);
        doTestMatch("utf8mb4_et_0900_ai_ci", UTF8MB4, true);
        doTestMatch("utf32_romanian_ci", UTF32, true);
        doTestMatch("utf32_czech_ci", UTF32, true);
        doTestMatch("ucs2_hungarian_ci", UCS2, true);
        doTestMatch("utf8mb4_hr_0900_as_cs", UTF8MB4, true);
        doTestMatch("utf32_hungarian_ci", UTF32, true);
        doTestMatch("ucs2_spanish_ci", UCS2, true);
        doTestMatch("utf32_latvian_ci", UTF32, true);
        doTestMatch("utf32_lithuanian_ci", UTF32, true);
        doTestMatch("eucjpms_bin", EUCJPMS, true);
        doTestMatch(UCS2_CROATIAN_CI, UCS2, true);
        doTestMatch(UTF32_SLOVENIAN_CI, UTF32, true);
        doTestMatch(UCS2_ROMAN_CI, UCS2, true);
        doTestMatch(UCS2_UNICODE_CI, UCS2, true);
        doTestMatch(GB18030_UNICODE_520_CI, GB18030, true);
        doTestMatch(UTF32_TURKISH_CI, UTF32, true);
        doTestMatch(UTF8MB4_PERSIAN_CI, UTF8MB4, true);
        doTestMatch(KEYBCS2_BIN, KEYBCS2, true);
        doTestMatch(UTF32_VIETNAMESE_CI, UTF32, true);
        doTestMatch(UTF8MB4_RO_0900_AI_CI, UTF8MB4, true);
        doTestMatch("binary", GB18030, false);
        doTestMatch("utf8mb4_0900_as_cs", LATIN2, false);
        doTestMatch("utf8_slovak_ci", UTF8MB4, false);
        doTestMatch("latin5_bin", UTF8MB4, false);
        doTestMatch("latin2_general_ci", ARMSCII8, false);
        doTestMatch("latin1_swedish_ci", UTF8MB4, false);
        doTestMatch("latin1_swedish_ci", UCS2, false);
        doTestMatch("utf8mb4_et_0900_as_cs", UCS2, false);
        doTestMatch("utf8_croatian_ci", UTF32, false);
        doTestMatch("utf8mb4_spanish_ci", UCS2, false);
        doTestMatch(UTF32_CZECH_CI, LATIN1, false);
        doTestMatch(CP852_GENERAL_CI, UTF8MB4, false);
        doTestMatch(UTF32_LITHUANIAN_CI, UCS2, false);
        doTestMatch(CP932_BIN, UCS2, false);
        doTestMatch(MACROMAN_BIN, UCS2, false);
        doTestMatch(UTF8_HUNGARIAN_CI, GREEK, false);
        doTestMatch(UCS2_CZECH_CI, MACROMAN, false);
        doTestMatch(UTF8MB4_CS_0900_AI_CI, EUCKR, false);
        doTestMatch(UTF16_ICELANDIC_CI, DEC8, false);
        doTestMatch(UTF8MB4_SLOVAK_CI, DEC8, false);
    }

    private void doTestMatch(CollationName collationName, CharsetName charsetName, boolean isMatched) {
        Assert.assertEquals(isMatched, charsetName.match(collationName));
    }

    private void doTestMatch(String collationNameStr, CharsetName charsetName, boolean isMatched) {
        Assert.assertEquals(isMatched, charsetName.match(collationNameStr));
    }

    @Test
    public void testOf() {
        doTestOf("armscii8", ARMSCII8);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("hebrew", HEBREW);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf16", UTF16);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("ucs2", UCS2);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("ucs2", UCS2);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("ucs2", UCS2);
        doTestOf("utf16", UTF16);
        doTestOf("gb18030", GB18030);
        doTestOf("armscii8", ARMSCII8);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("hebrew", HEBREW);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf16", UTF16);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("ucs2", UCS2);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("ucs2", UCS2);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("utf8mb4", UTF8MB4);
        doTestOf("ucs2", UCS2);
        doTestOf("utf16", UTF16);
        doTestOf("gb18030", GB18030);
    }

    private void doTestOf(String charsetNameStr, CharsetName expected) {
        Assert.assertEquals(expected, CharsetName.of(charsetNameStr));
    }

    @Test
    public void testDefaultCollationMySQL57() {
        InstanceVersion.setMYSQL80(false);
        Assert.assertTrue(ARMSCII8.getDefaultCollationName() == ARMSCII8_GENERAL_CI);
        Assert.assertTrue(ASCII.getDefaultCollationName() == ASCII_GENERAL_CI);
        Assert.assertTrue(BIG5.getDefaultCollationName() == BIG5_CHINESE_CI);
        Assert.assertTrue(BINARY.getDefaultCollationName() == CollationName.BINARY);
        Assert.assertTrue(CP1250.getDefaultCollationName() == CP1250_GENERAL_CI);
        Assert.assertTrue(CP1251.getDefaultCollationName() == CP1251_GENERAL_CI);
        Assert.assertTrue(CP1256.getDefaultCollationName() == CP1256_GENERAL_CI);
        Assert.assertTrue(CP1257.getDefaultCollationName() == CP1257_GENERAL_CI);
        Assert.assertTrue(CP850.getDefaultCollationName() == CP850_GENERAL_CI);
        Assert.assertTrue(CP852.getDefaultCollationName() == CP852_GENERAL_CI);
        Assert.assertTrue(CP866.getDefaultCollationName() == CP866_GENERAL_CI);
        Assert.assertTrue(CP932.getDefaultCollationName() == CP932_JAPANESE_CI);
        Assert.assertTrue(DEC8.getDefaultCollationName() == DEC8_SWEDISH_CI);
        Assert.assertTrue(EUCJPMS.getDefaultCollationName() == EUCJPMS_JAPANESE_CI);
        Assert.assertTrue(EUCKR.getDefaultCollationName() == EUCKR_KOREAN_CI);
        Assert.assertTrue(GB18030.getDefaultCollationName() == GB18030_CHINESE_CI);
        Assert.assertTrue(GB2312.getDefaultCollationName() == GB2312_CHINESE_CI);
        Assert.assertTrue(GBK.getDefaultCollationName() == GBK_CHINESE_CI);
        Assert.assertTrue(GEOSTD8.getDefaultCollationName() == GEOSTD8_GENERAL_CI);
        Assert.assertTrue(GREEK.getDefaultCollationName() == GREEK_GENERAL_CI);
        Assert.assertTrue(HEBREW.getDefaultCollationName() == HEBREW_GENERAL_CI);
        Assert.assertTrue(HP8.getDefaultCollationName() == HP8_ENGLISH_CI);
        Assert.assertTrue(KEYBCS2.getDefaultCollationName() == KEYBCS2_GENERAL_CI);
        Assert.assertTrue(KOI8R.getDefaultCollationName() == KOI8R_GENERAL_CI);
        Assert.assertTrue(KOI8U.getDefaultCollationName() == KOI8U_GENERAL_CI);
        Assert.assertTrue(LATIN1.getDefaultCollationName() == LATIN1_SWEDISH_CI);
        Assert.assertTrue(LATIN2.getDefaultCollationName() == LATIN2_GENERAL_CI);
        Assert.assertTrue(LATIN5.getDefaultCollationName() == LATIN5_TURKISH_CI);
        Assert.assertTrue(LATIN7.getDefaultCollationName() == LATIN7_GENERAL_CI);
        Assert.assertTrue(MACCE.getDefaultCollationName() == MACCE_GENERAL_CI);
        Assert.assertTrue(MACROMAN.getDefaultCollationName() == MACROMAN_GENERAL_CI);
        Assert.assertTrue(SJIS.getDefaultCollationName() == SJIS_JAPANESE_CI);
        Assert.assertTrue(SWE7.getDefaultCollationName() == SWE7_SWEDISH_CI);
        Assert.assertTrue(TIS620.getDefaultCollationName() == TIS620_THAI_CI);
        Assert.assertTrue(UCS2.getDefaultCollationName() == UCS2_GENERAL_CI);
        Assert.assertTrue(UJIS.getDefaultCollationName() == UJIS_JAPANESE_CI);
        Assert.assertTrue(UTF16.getDefaultCollationName() == UTF16_GENERAL_CI);
        Assert.assertTrue(UTF16LE.getDefaultCollationName() == UTF16LE_GENERAL_CI);
        Assert.assertTrue(UTF32.getDefaultCollationName() == UTF32_GENERAL_CI);
        Assert.assertTrue(UTF8MB3.getDefaultCollationName() == UTF8_GENERAL_CI);
        Assert.assertTrue(UTF8MB4.getDefaultCollationName() == UTF8MB4_GENERAL_CI);
    }

    @Test
    public void testDefaultCollationMySQL80() {
        InstanceVersion.setMYSQL80(true);
        Assert.assertTrue(ARMSCII8.getDefaultCollationName() == ARMSCII8_GENERAL_CI);
        Assert.assertTrue(ASCII.getDefaultCollationName() == ASCII_GENERAL_CI);
        Assert.assertTrue(BIG5.getDefaultCollationName() == BIG5_CHINESE_CI);
        Assert.assertTrue(BINARY.getDefaultCollationName() == CollationName.BINARY);
        Assert.assertTrue(CP1250.getDefaultCollationName() == CP1250_GENERAL_CI);
        Assert.assertTrue(CP1251.getDefaultCollationName() == CP1251_GENERAL_CI);
        Assert.assertTrue(CP1256.getDefaultCollationName() == CP1256_GENERAL_CI);
        Assert.assertTrue(CP1257.getDefaultCollationName() == CP1257_GENERAL_CI);
        Assert.assertTrue(CP850.getDefaultCollationName() == CP850_GENERAL_CI);
        Assert.assertTrue(CP852.getDefaultCollationName() == CP852_GENERAL_CI);
        Assert.assertTrue(CP866.getDefaultCollationName() == CP866_GENERAL_CI);
        Assert.assertTrue(CP932.getDefaultCollationName() == CP932_JAPANESE_CI);
        Assert.assertTrue(DEC8.getDefaultCollationName() == DEC8_SWEDISH_CI);
        Assert.assertTrue(EUCJPMS.getDefaultCollationName() == EUCJPMS_JAPANESE_CI);
        Assert.assertTrue(EUCKR.getDefaultCollationName() == EUCKR_KOREAN_CI);
        Assert.assertTrue(GB18030.getDefaultCollationName() == GB18030_CHINESE_CI);
        Assert.assertTrue(GB2312.getDefaultCollationName() == GB2312_CHINESE_CI);
        Assert.assertTrue(GBK.getDefaultCollationName() == GBK_CHINESE_CI);
        Assert.assertTrue(GEOSTD8.getDefaultCollationName() == GEOSTD8_GENERAL_CI);
        Assert.assertTrue(GREEK.getDefaultCollationName() == GREEK_GENERAL_CI);
        Assert.assertTrue(HEBREW.getDefaultCollationName() == HEBREW_GENERAL_CI);
        Assert.assertTrue(HP8.getDefaultCollationName() == HP8_ENGLISH_CI);
        Assert.assertTrue(KEYBCS2.getDefaultCollationName() == KEYBCS2_GENERAL_CI);
        Assert.assertTrue(KOI8R.getDefaultCollationName() == KOI8R_GENERAL_CI);
        Assert.assertTrue(KOI8U.getDefaultCollationName() == KOI8U_GENERAL_CI);
        Assert.assertTrue(LATIN1.getDefaultCollationName() == LATIN1_SWEDISH_CI);
        Assert.assertTrue(LATIN2.getDefaultCollationName() == LATIN2_GENERAL_CI);
        Assert.assertTrue(LATIN5.getDefaultCollationName() == LATIN5_TURKISH_CI);
        Assert.assertTrue(LATIN7.getDefaultCollationName() == LATIN7_GENERAL_CI);
        Assert.assertTrue(MACCE.getDefaultCollationName() == MACCE_GENERAL_CI);
        Assert.assertTrue(MACROMAN.getDefaultCollationName() == MACROMAN_GENERAL_CI);
        Assert.assertTrue(SJIS.getDefaultCollationName() == SJIS_JAPANESE_CI);
        Assert.assertTrue(SWE7.getDefaultCollationName() == SWE7_SWEDISH_CI);
        Assert.assertTrue(TIS620.getDefaultCollationName() == TIS620_THAI_CI);
        Assert.assertTrue(UCS2.getDefaultCollationName() == UCS2_GENERAL_CI);
        Assert.assertTrue(UJIS.getDefaultCollationName() == UJIS_JAPANESE_CI);
        Assert.assertTrue(UTF16.getDefaultCollationName() == UTF16_GENERAL_CI);
        Assert.assertTrue(UTF16LE.getDefaultCollationName() == UTF16LE_GENERAL_CI);
        Assert.assertTrue(UTF32.getDefaultCollationName() == UTF32_GENERAL_CI);
        Assert.assertTrue(UTF8MB3.getDefaultCollationName() == UTF8_GENERAL_CI);
        Assert.assertTrue(UTF8MB4.getDefaultCollationName() == UTF8MB4_0900_AI_CI);
    }
}

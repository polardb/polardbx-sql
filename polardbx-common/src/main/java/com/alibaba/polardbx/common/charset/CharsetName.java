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

import com.google.common.collect.ImmutableList;

import com.google.common.collect.ImmutableMap;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.charset.CollationName.*;

public enum CharsetName {
    /**
     * UTF-8 Unicode
     */
    UTF8(UTF8_GENERAL_CI,
        ImmutableList.of(UTF8_GENERAL_CI, UTF8_BIN, UTF8_UNICODE_CI),
        ImmutableList.of(
            UTF8_ICELANDIC_CI,
            UTF8_LATVIAN_CI,
            UTF8_ROMANIAN_CI,
            UTF8_SLOVENIAN_CI,
            UTF8_POLISH_CI,
            UTF8_ESTONIAN_CI,
            UTF8_SPANISH_CI,
            UTF8_SWEDISH_CI,
            UTF8_TURKISH_CI,
            UTF8_CZECH_CI,
            UTF8_DANISH_CI,
            UTF8_LITHUANIAN_CI,
            UTF8_SLOVAK_CI,
            UTF8_SPANISH2_CI,
            UTF8_ROMAN_CI,
            UTF8_PERSIAN_CI,
            UTF8_ESPERANTO_CI,
            UTF8_HUNGARIAN_CI,
            UTF8_SINHALA_CI,
            UTF8_GERMAN2_CI,
            UTF8_CROATIAN_CI,
            UTF8_UNICODE_520_CI,
            UTF8_VIETNAMESE_CI,
            UTF8_GENERAL_MYSQL500_CI
        ),
        "UTF8", "UTF-8", 3),

    /**
     * UTF-8 Unicode (UTF8MB3)
     */
    UTF8MB3(UTF8_GENERAL_CI,
        ImmutableList.of(UTF8_GENERAL_CI, UTF8_BIN, UTF8_UNICODE_CI),
        ImmutableList.of(
            UTF8_ICELANDIC_CI,
            UTF8_LATVIAN_CI,
            UTF8_ROMANIAN_CI,
            UTF8_SLOVENIAN_CI,
            UTF8_POLISH_CI,
            UTF8_ESTONIAN_CI,
            UTF8_SPANISH_CI,
            UTF8_SWEDISH_CI,
            UTF8_TURKISH_CI,
            UTF8_CZECH_CI,
            UTF8_DANISH_CI,
            UTF8_LITHUANIAN_CI,
            UTF8_SLOVAK_CI,
            UTF8_SPANISH2_CI,
            UTF8_ROMAN_CI,
            UTF8_PERSIAN_CI,
            UTF8_ESPERANTO_CI,
            UTF8_HUNGARIAN_CI,
            UTF8_SINHALA_CI,
            UTF8_GERMAN2_CI,
            UTF8_CROATIAN_CI,
            UTF8_UNICODE_520_CI,
            UTF8_VIETNAMESE_CI,
            UTF8_GENERAL_MYSQL500_CI
        ),
        "UTF8", "UTF-8", 3),

    /**
     * UTF-8 Unicode (MySQL style)
     */
    UTF8MB4(UTF8MB4_0900_AI_CI,
        ImmutableList
            .of(UTF8MB4_GENERAL_CI, UTF8MB4_BIN, UTF8MB4_UNICODE_CI, UTF8MB4_0900_AI_CI, UTF8MB4_UNICODE_520_CI),
        ImmutableList.of(
            UTF8MB4_ICELANDIC_CI,
            UTF8MB4_LATVIAN_CI,
            UTF8MB4_ROMANIAN_CI,
            UTF8MB4_SLOVENIAN_CI,
            UTF8MB4_POLISH_CI,
            UTF8MB4_ESTONIAN_CI,
            UTF8MB4_SPANISH_CI,
            UTF8MB4_SWEDISH_CI,
            UTF8MB4_TURKISH_CI,
            UTF8MB4_CZECH_CI,
            UTF8MB4_DANISH_CI,
            UTF8MB4_LITHUANIAN_CI,
            UTF8MB4_SLOVAK_CI,
            UTF8MB4_SPANISH2_CI,
            UTF8MB4_ROMAN_CI,
            UTF8MB4_PERSIAN_CI,
            UTF8MB4_ESPERANTO_CI,
            UTF8MB4_HUNGARIAN_CI,
            UTF8MB4_SINHALA_CI,
            UTF8MB4_GERMAN2_CI,
            UTF8MB4_CROATIAN_CI,
            UTF8MB4_VIETNAMESE_CI,
            UTF8MB4_0900_AS_CI,
            UTF8MB4_0900_AS_CS,
            UTF8MB4_BIN,
            UTF8MB4_CROATIAN_CI,
            UTF8MB4_CS_0900_AI_CI,
            UTF8MB4_CS_0900_AS_CS,
            UTF8MB4_CZECH_CI,
            UTF8MB4_DANISH_CI,
            UTF8MB4_DA_0900_AI_CI,
            UTF8MB4_DA_0900_AS_CS,
            UTF8MB4_DE_PB_0900_AI_CI,
            UTF8MB4_DE_PB_0900_AS_CS,
            UTF8MB4_EO_0900_AI_CI,
            UTF8MB4_EO_0900_AS_CS,
            UTF8MB4_ESPERANTO_CI,
            UTF8MB4_ESTONIAN_CI,
            UTF8MB4_ES_0900_AI_CI,
            UTF8MB4_ES_0900_AS_CS,
            UTF8MB4_ES_TRAD_0900_AI_CI,
            UTF8MB4_ES_TRAD_0900_AS_CS,
            UTF8MB4_ET_0900_AI_CI,
            UTF8MB4_ET_0900_AS_CS,
            UTF8MB4_GENERAL_CI,
            UTF8MB4_GERMAN2_CI,
            UTF8MB4_HR_0900_AI_CI,
            UTF8MB4_HR_0900_AS_CS,
            UTF8MB4_HUNGARIAN_CI,
            UTF8MB4_HU_0900_AI_CI,
            UTF8MB4_HU_0900_AS_CS,
            UTF8MB4_ICELANDIC_CI,
            UTF8MB4_IS_0900_AI_CI,
            UTF8MB4_IS_0900_AS_CS,
            UTF8MB4_JA_0900_AS_CS,
            UTF8MB4_JA_0900_AS_CS_KS,
            UTF8MB4_LATVIAN_CI,
            UTF8MB4_LA_0900_AI_CI,
            UTF8MB4_LA_0900_AS_CS,
            UTF8MB4_LITHUANIAN_CI,
            UTF8MB4_LT_0900_AI_CI,
            UTF8MB4_LT_0900_AS_CS,
            UTF8MB4_LV_0900_AI_CI,
            UTF8MB4_LV_0900_AS_CS,
            UTF8MB4_PERSIAN_CI,
            UTF8MB4_PL_0900_AI_CI,
            UTF8MB4_PL_0900_AS_CS,
            UTF8MB4_POLISH_CI,
            UTF8MB4_ROMANIAN_CI,
            UTF8MB4_ROMAN_CI,
            UTF8MB4_RO_0900_AI_CI,
            UTF8MB4_RO_0900_AS_CS,
            UTF8MB4_RU_0900_AI_CI,
            UTF8MB4_RU_0900_AS_CS,
            UTF8MB4_SINHALA_CI,
            UTF8MB4_SK_0900_AI_CI,
            UTF8MB4_SK_0900_AS_CS,
            UTF8MB4_SLOVAK_CI,
            UTF8MB4_SLOVENIAN_CI,
            UTF8MB4_SL_0900_AI_CI,
            UTF8MB4_SL_0900_AS_CS,
            UTF8MB4_SPANISH2_CI,
            UTF8MB4_SPANISH_CI,
            UTF8MB4_SV_0900_AI_CI,
            UTF8MB4_SV_0900_AS_CS,
            UTF8MB4_SWEDISH_CI,
            UTF8MB4_TR_0900_AI_CI,
            UTF8MB4_TR_0900_AS_CS,
            UTF8MB4_TURKISH_CI,
            UTF8MB4_UNICODE_520_CI,
            UTF8MB4_UNICODE_CI,
            UTF8MB4_VIETNAMESE_CI,
            UTF8MB4_VI_0900_AI_CI,
            UTF8MB4_VI_0900_AS_CS,
            UTF8MB4_ZH_0900_AS_CS
        ),
        "UTF8MB4", "UTF-8", 4),

    /**
     * UTF-16 Unicode
     */
    UTF16(UTF16_GENERAL_CI,
        ImmutableList.of(UTF16_GENERAL_CI, UTF16_BIN, UTF16_UNICODE_CI),
        ImmutableList.of(
            UTF16_ICELANDIC_CI,
            UTF16_LATVIAN_CI,
            UTF16_ROMANIAN_CI,
            UTF16_SLOVENIAN_CI,
            UTF16_POLISH_CI,
            UTF16_ESTONIAN_CI,
            UTF16_SPANISH_CI,
            UTF16_SWEDISH_CI,
            UTF16_TURKISH_CI,
            UTF16_CZECH_CI,
            UTF16_DANISH_CI,
            UTF16_LITHUANIAN_CI,
            UTF16_SLOVAK_CI,
            UTF16_SPANISH2_CI,
            UTF16_ROMAN_CI,
            UTF16_PERSIAN_CI,
            UTF16_ESPERANTO_CI,
            UTF16_HUNGARIAN_CI,
            UTF16_SINHALA_CI,
            UTF16_GERMAN2_CI,
            UTF16_CROATIAN_CI,
            UTF16_UNICODE_520_CI,
            UTF16_VIETNAMESE_CI
        ),
        "UTF-16", "UTF-16", 4),

    /**
     * UTF-16LE Unicode
     */
    UTF16LE(UTF16LE_GENERAL_CI,
        ImmutableList.of(UTF16LE_GENERAL_CI, UTF16LE_BIN),
        ImmutableList.of(),
        "UTF-16LE", "UTF-16LE", 4),

    /**
     * UTF-32 Unicode
     */
    UTF32(UTF32_GENERAL_CI,
        ImmutableList.of(UTF32_GENERAL_CI, UTF32_BIN, UTF32_UNICODE_CI),
        ImmutableList.of(
            UTF32_ICELANDIC_CI,
            UTF32_LATVIAN_CI,
            UTF32_ROMANIAN_CI,
            UTF32_SLOVENIAN_CI,
            UTF32_POLISH_CI,
            UTF32_ESTONIAN_CI,
            UTF32_SPANISH_CI,
            UTF32_SWEDISH_CI,
            UTF32_TURKISH_CI,
            UTF32_CZECH_CI,
            UTF32_DANISH_CI,
            UTF32_LITHUANIAN_CI,
            UTF32_SLOVAK_CI,
            UTF32_SPANISH2_CI,
            UTF32_ROMAN_CI,
            UTF32_PERSIAN_CI,
            UTF32_ESPERANTO_CI,
            UTF32_HUNGARIAN_CI,
            UTF32_SINHALA_CI,
            UTF32_GERMAN2_CI,
            UTF32_CROATIAN_CI,
            UTF32_UNICODE_520_CI,
            UTF32_VIETNAMESE_CI
        ),
        "UTF-32", "UTF-32", 4),

    /**
     * US ASCII
     */
    ASCII(ASCII_GENERAL_CI,
        ImmutableList.of(ASCII_GENERAL_CI, ASCII_BIN),
        ImmutableList.of(),
        "ASCII", "ASCII", 1),

    /**
     * Binary pseudo charset
     */
    BINARY(CollationName.BINARY,
        ImmutableList.of(CollationName.BINARY),
        ImmutableList.of(),
        "BINARY", "ISO-8859-1", 1),

    /**
     * GBK Simplified Chinese
     */
    GBK(GBK_CHINESE_CI,
        ImmutableList.of(GBK_CHINESE_CI, GBK_BIN),
        ImmutableList.of(),
        "GBK", "GBK", 2),
    /*
     * China National Standard GB18030
     */
    GB18030(GB18030_CHINESE_CI,
        ImmutableList.of(GB18030_CHINESE_CI, GB18030_BIN, GB18030_UNICODE_520_CI),
        ImmutableList.of(),
        "GB18030", "GB18030", 4),

    /**
     * BIG5 Traditional Chinese
     */
    BIG5(BIG5_CHINESE_CI,
        ImmutableList.of(BIG5_CHINESE_CI, BIG5_BIN),
        ImmutableList.of(),
        "BIG5", "BIG5", 2),

    LATIN1(LATIN1_SWEDISH_CI,
        ImmutableList.of(LATIN1_SWEDISH_CI, LATIN1_GENERAL_CI, LATIN1_GENERAL_CS, LATIN1_BIN, LATIN1_GERMAN1_CI,
            LATIN1_GERMAN2_CI, LATIN1_DANISH_CI, LATIN1_SPANISH_CI),
        ImmutableList.of(),
        "LATIN1", "LATIN1", 1),

    /*
     * unsupported mysql character sets.
     */
    //ARMSCII-8_Armenian
    ARMSCII8(ARMSCII8_GENERAL_CI, ImmutableList.of(ARMSCII8_BIN, ARMSCII8_GENERAL_CI), 1),
    //Windows_Central_European
    CP1250(CP1250_GENERAL_CI,
        ImmutableList.of(CP1250_CROATIAN_CI, CP1250_BIN, CP1250_POLISH_CI, CP1250_GENERAL_CI, CP1250_CZECH_CS), 1),
    //Windows_Cyrillic
    CP1251(CP1251_GENERAL_CI,
        ImmutableList.of(CP1251_BULGARIAN_CI, CP1251_UKRAINIAN_CI, CP1251_BIN, CP1251_GENERAL_CI, CP1251_GENERAL_CS),
        1),
    //Windows_Arabic
    CP1256(CP1256_GENERAL_CI, ImmutableList.of(CP1256_GENERAL_CI, CP1256_BIN), 1),
    //Windows_Baltic
    CP1257(CP1257_GENERAL_CI, ImmutableList.of(CP1257_LITHUANIAN_CI, CP1257_BIN, CP1257_GENERAL_CI), 1),
    //DOS_West_European
    CP850(CP850_GENERAL_CI, ImmutableList.of(CP850_GENERAL_CI, CP850_BIN), 1),
    //DOS_Central_European
    CP852(CP852_GENERAL_CI, ImmutableList.of(CP852_BIN, CP852_GENERAL_CI), 1),
    //DOS_Russian
    CP866(CP866_GENERAL_CI, ImmutableList.of(CP866_GENERAL_CI, CP866_BIN), 1),
    //SJIS_for_Windows_Japanese
    CP932(CP932_JAPANESE_CI, ImmutableList.of(CP932_JAPANESE_CI, CP932_BIN), 2),
    //DEC_West_European
    DEC8(DEC8_SWEDISH_CI, ImmutableList.of(DEC8_SWEDISH_CI, DEC8_BIN), 1),
    //UJIS_for_Windows_Japanese
    EUCJPMS(EUCJPMS_JAPANESE_CI, ImmutableList.of(EUCJPMS_BIN, EUCJPMS_JAPANESE_CI), 3),
    //EUC-KR_Korean
    EUCKR(EUCKR_KOREAN_CI, ImmutableList.of(EUCKR_KOREAN_CI, EUCKR_BIN), 2),
    //GB2312_Simplified_Chinese
    GB2312(GB2312_CHINESE_CI, ImmutableList.of(GB2312_CHINESE_CI, GB2312_BIN), 2),
    //GEOSTD8_Georgian
    GEOSTD8(GEOSTD8_GENERAL_CI, ImmutableList.of(GEOSTD8_GENERAL_CI, GEOSTD8_BIN), 1),
    //ISO_8859-7_Greek
    GREEK(GREEK_GENERAL_CI, ImmutableList.of(GREEK_GENERAL_CI, GREEK_BIN), 1),
    //ISO_8859-8_Hebrew
    HEBREW(HEBREW_GENERAL_CI, ImmutableList.of(HEBREW_GENERAL_CI, HEBREW_BIN), 1),
    //HP_West_European
    HP8(HP8_ENGLISH_CI, ImmutableList.of(HP8_ENGLISH_CI, HP8_BIN), 1),
    //DOS_Kamenicky_Czech-Slovak
    KEYBCS2(KEYBCS2_GENERAL_CI, ImmutableList.of(KEYBCS2_GENERAL_CI, KEYBCS2_BIN), 1),
    //KOI8-R_Relcom_Russian
    KOI8R(KOI8R_GENERAL_CI, ImmutableList.of(KOI8R_GENERAL_CI, KOI8R_BIN), 1),
    //KOI8-U_Ukrainian
    KOI8U(KOI8U_GENERAL_CI, ImmutableList.of(KOI8U_GENERAL_CI, KOI8U_BIN), 1),
    //ISO_8859-2_Central_European
    LATIN2(LATIN2_GENERAL_CI,
        ImmutableList.of(LATIN2_CZECH_CS, LATIN2_GENERAL_CI, LATIN2_HUNGARIAN_CI, LATIN2_CROATIAN_CI, LATIN2_BIN), 1),
    //ISO_8859-9_Turkish
    LATIN5(LATIN5_TURKISH_CI, ImmutableList.of(LATIN5_TURKISH_CI, LATIN5_BIN), 1),
    //ISO_8859-13_Baltic
    LATIN7(LATIN7_GENERAL_CI, ImmutableList.of(LATIN7_GENERAL_CI, LATIN7_GENERAL_CS, LATIN7_BIN, LATIN7_ESTONIAN_CS),
        1),
    //Mac_Central_European
    MACCE(MACCE_GENERAL_CI, ImmutableList.of(MACCE_GENERAL_CI, MACCE_BIN), 1),
    //Mac_West_European
    MACROMAN(MACROMAN_GENERAL_CI, ImmutableList.of(MACROMAN_GENERAL_CI, MACROMAN_BIN), 1),
    //Shift-JIS_Japanese
    SJIS(SJIS_JAPANESE_CI, ImmutableList.of(SJIS_BIN, SJIS_JAPANESE_CI), 2),
    //7bit_Swedish
    SWE7(SWE7_SWEDISH_CI, ImmutableList.of(SWE7_SWEDISH_CI, SWE7_BIN), 1),
    //TIS620_Thai
    TIS620(TIS620_THAI_CI, ImmutableList.of(TIS620_THAI_CI, TIS620_BIN), 1),
    //UCS-2_Unicode
    UCS2(UCS2_GENERAL_CI, ImmutableList
        .of(UCS2_ICELANDIC_CI, UCS2_TURKISH_CI, UCS2_ESPERANTO_CI, UCS2_LATVIAN_CI, UCS2_CZECH_CI, UCS2_HUNGARIAN_CI,
            UCS2_ROMANIAN_CI, UCS2_DANISH_CI, UCS2_SINHALA_CI, UCS2_SLOVENIAN_CI, UCS2_LITHUANIAN_CI, UCS2_GERMAN2_CI,
            UCS2_POLISH_CI, UCS2_SLOVAK_CI, UCS2_CROATIAN_CI, UCS2_GENERAL_CI, UCS2_ESTONIAN_CI, UCS2_SPANISH2_CI,
            UCS2_UNICODE_520_CI, UCS2_BIN, UCS2_SPANISH_CI, UCS2_ROMAN_CI, UCS2_VIETNAMESE_CI, UCS2_UNICODE_CI,
            UCS2_SWEDISH_CI, UCS2_PERSIAN_CI, UCS2_GENERAL_MYSQL500_CI), 2),
    //EUC-JP_Japanese
    UJIS(UJIS_JAPANESE_CI, ImmutableList.of(UJIS_JAPANESE_CI, UJIS_BIN), 3);

    /**
     * Collect all collation names to map so we can check them in O(1)
     */
    public static Map<String, CharsetName> CHARSET_NAME_MAP = Arrays.stream(values())
        .collect(Collectors.toMap(Enum::name, Function.identity()));

    public static ImmutableList<CharsetName> POLAR_DB_X_IMPLEMENTED_CHARSET_NAMES = ImmutableList.of(
        UTF8, UTF8MB3, UTF8MB4, UTF16, UTF16LE, UTF32, LATIN1, GBK, GB18030, BIG5, BINARY, ASCII
    );

    static Set<String> POLAR_DB_X_IMPLEMENTED_CHARSET_NAME_STRINGS = new HashSet<>();

    public static final Map<String, CharsetName> CHARSET_NAME_MATCHES = ImmutableMap.<String, CharsetName>builder()

        .put("utf-8", UTF8)
        .put("UTF-8", UTF8)
        .put("utf8", UTF8)
        .put("UTF8", UTF8)
        .put("utf8mb3", UTF8)
        .put("UTF8MB3", UTF8)

        .put("utf8mb4", UTF8MB4)
        .put("UTF8MB4", UTF8MB4)

        .put("utf-16", UTF16)
        .put("UTF-16", UTF16)
        .put("utf16", UTF16)
        .put("UTF16", UTF16)

        .put("utf-16le", UTF16LE)
        .put("UTF-16LE", UTF16LE)
        .put("utf16le", UTF16LE)
        .put("UTF16LE", UTF16LE)

        .put("utf-32", UTF32)
        .put("UTF-32", UTF32)
        .put("utf32", UTF32)
        .put("UTF32", UTF32)

        .put("ascii", ASCII)
        .put("ASCII", ASCII)

        .put("binary", BINARY)
        .put("BINARY", BINARY)

        .put("ISO-8859-1", LATIN1)
        .put("iso-8859-1", LATIN1)
        .put("ISO8859-1", LATIN1)
        .put("iso8859-1", LATIN1)
        .put("latin1", LATIN1)
        .put("latin-1", LATIN1)
        .put("LATIN1", LATIN1)
        .put("LATIN-1", LATIN1)

        .put("gbk", GBK)
        .put("GBK", GBK)

        .put("GB18030", GB18030)
        .put("gb18030", GB18030)

        .put("BIG5", BIG5)
        .put("big5", BIG5)
        .build();

    /**
     * Mapping from collation id to charset name.
     */
    public static final CharsetName[] CHARSET_NAMES_OF_COLLATION = new CharsetName[512];

    static {
        // Initialize both upper case & lower case to check implementation in O(1).
        for (CharsetName charsetName : POLAR_DB_X_IMPLEMENTED_CHARSET_NAMES) {
            POLAR_DB_X_IMPLEMENTED_CHARSET_NAME_STRINGS.add(charsetName.name().toUpperCase());
            POLAR_DB_X_IMPLEMENTED_CHARSET_NAME_STRINGS.add(charsetName.name().toLowerCase());
        }

        CHARSET_NAMES_OF_COLLATION[1] = BIG5;
        CHARSET_NAMES_OF_COLLATION[2] = LATIN2;
        CHARSET_NAMES_OF_COLLATION[3] = DEC8;
        CHARSET_NAMES_OF_COLLATION[4] = CP850;
        CHARSET_NAMES_OF_COLLATION[5] = LATIN1;
        CHARSET_NAMES_OF_COLLATION[6] = HP8;
        CHARSET_NAMES_OF_COLLATION[7] = KOI8R;
        CHARSET_NAMES_OF_COLLATION[8] = LATIN1;
        CHARSET_NAMES_OF_COLLATION[9] = LATIN2;
        CHARSET_NAMES_OF_COLLATION[10] = SWE7;
        CHARSET_NAMES_OF_COLLATION[11] = ASCII;
        CHARSET_NAMES_OF_COLLATION[12] = UJIS;
        CHARSET_NAMES_OF_COLLATION[13] = SJIS;
        CHARSET_NAMES_OF_COLLATION[14] = CP1251;
        CHARSET_NAMES_OF_COLLATION[15] = LATIN1;
        CHARSET_NAMES_OF_COLLATION[16] = HEBREW;
        CHARSET_NAMES_OF_COLLATION[18] = TIS620;
        CHARSET_NAMES_OF_COLLATION[19] = EUCKR;
        CHARSET_NAMES_OF_COLLATION[20] = LATIN7;
        CHARSET_NAMES_OF_COLLATION[21] = LATIN2;
        CHARSET_NAMES_OF_COLLATION[22] = KOI8U;
        CHARSET_NAMES_OF_COLLATION[23] = CP1251;
        CHARSET_NAMES_OF_COLLATION[24] = GB2312;
        CHARSET_NAMES_OF_COLLATION[25] = GREEK;
        CHARSET_NAMES_OF_COLLATION[26] = CP1250;
        CHARSET_NAMES_OF_COLLATION[27] = LATIN2;
        CHARSET_NAMES_OF_COLLATION[28] = GBK;
        CHARSET_NAMES_OF_COLLATION[29] = CP1257;
        CHARSET_NAMES_OF_COLLATION[30] = LATIN5;
        CHARSET_NAMES_OF_COLLATION[31] = LATIN1;
        CHARSET_NAMES_OF_COLLATION[32] = ARMSCII8;
        CHARSET_NAMES_OF_COLLATION[33] = UTF8;
        CHARSET_NAMES_OF_COLLATION[34] = CP1250;
        CHARSET_NAMES_OF_COLLATION[35] = UCS2;
        CHARSET_NAMES_OF_COLLATION[36] = CP866;
        CHARSET_NAMES_OF_COLLATION[37] = KEYBCS2;
        CHARSET_NAMES_OF_COLLATION[38] = MACCE;
        CHARSET_NAMES_OF_COLLATION[39] = MACROMAN;
        CHARSET_NAMES_OF_COLLATION[40] = CP852;
        CHARSET_NAMES_OF_COLLATION[41] = LATIN7;
        CHARSET_NAMES_OF_COLLATION[42] = LATIN7;
        CHARSET_NAMES_OF_COLLATION[43] = MACCE;
        CHARSET_NAMES_OF_COLLATION[44] = CP1250;
        CHARSET_NAMES_OF_COLLATION[45] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[46] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[47] = LATIN1;
        CHARSET_NAMES_OF_COLLATION[48] = LATIN1;
        CHARSET_NAMES_OF_COLLATION[49] = LATIN1;
        CHARSET_NAMES_OF_COLLATION[50] = CP1251;
        CHARSET_NAMES_OF_COLLATION[51] = CP1251;
        CHARSET_NAMES_OF_COLLATION[52] = CP1251;
        CHARSET_NAMES_OF_COLLATION[53] = MACROMAN;
        CHARSET_NAMES_OF_COLLATION[54] = UTF16;
        CHARSET_NAMES_OF_COLLATION[55] = UTF16;
        CHARSET_NAMES_OF_COLLATION[56] = UTF16LE;
        CHARSET_NAMES_OF_COLLATION[57] = CP1256;
        CHARSET_NAMES_OF_COLLATION[58] = CP1257;
        CHARSET_NAMES_OF_COLLATION[59] = CP1257;
        CHARSET_NAMES_OF_COLLATION[60] = UTF32;
        CHARSET_NAMES_OF_COLLATION[61] = UTF32;
        CHARSET_NAMES_OF_COLLATION[62] = UTF16LE;
        CHARSET_NAMES_OF_COLLATION[63] = BINARY;
        CHARSET_NAMES_OF_COLLATION[64] = ARMSCII8;
        CHARSET_NAMES_OF_COLLATION[65] = ASCII;
        CHARSET_NAMES_OF_COLLATION[66] = CP1250;
        CHARSET_NAMES_OF_COLLATION[67] = CP1256;
        CHARSET_NAMES_OF_COLLATION[68] = CP866;
        CHARSET_NAMES_OF_COLLATION[69] = DEC8;
        CHARSET_NAMES_OF_COLLATION[70] = GREEK;
        CHARSET_NAMES_OF_COLLATION[71] = HEBREW;
        CHARSET_NAMES_OF_COLLATION[72] = HP8;
        CHARSET_NAMES_OF_COLLATION[73] = KEYBCS2;
        CHARSET_NAMES_OF_COLLATION[74] = KOI8R;
        CHARSET_NAMES_OF_COLLATION[75] = KOI8U;
        CHARSET_NAMES_OF_COLLATION[76] = UTF8;
        CHARSET_NAMES_OF_COLLATION[77] = LATIN2;
        CHARSET_NAMES_OF_COLLATION[78] = LATIN5;
        CHARSET_NAMES_OF_COLLATION[79] = LATIN7;
        CHARSET_NAMES_OF_COLLATION[80] = CP850;
        CHARSET_NAMES_OF_COLLATION[81] = CP852;
        CHARSET_NAMES_OF_COLLATION[82] = SWE7;
        CHARSET_NAMES_OF_COLLATION[83] = UTF8;
        CHARSET_NAMES_OF_COLLATION[84] = BIG5;
        CHARSET_NAMES_OF_COLLATION[85] = EUCKR;
        CHARSET_NAMES_OF_COLLATION[86] = GB2312;
        CHARSET_NAMES_OF_COLLATION[87] = GBK;
        CHARSET_NAMES_OF_COLLATION[88] = SJIS;
        CHARSET_NAMES_OF_COLLATION[89] = TIS620;
        CHARSET_NAMES_OF_COLLATION[90] = UCS2;
        CHARSET_NAMES_OF_COLLATION[91] = UJIS;
        CHARSET_NAMES_OF_COLLATION[92] = GEOSTD8;
        CHARSET_NAMES_OF_COLLATION[93] = GEOSTD8;
        CHARSET_NAMES_OF_COLLATION[94] = LATIN1;
        CHARSET_NAMES_OF_COLLATION[95] = CP932;
        CHARSET_NAMES_OF_COLLATION[96] = CP932;
        CHARSET_NAMES_OF_COLLATION[97] = EUCJPMS;
        CHARSET_NAMES_OF_COLLATION[98] = EUCJPMS;
        CHARSET_NAMES_OF_COLLATION[99] = CP1250;
        CHARSET_NAMES_OF_COLLATION[101] = UTF16;
        CHARSET_NAMES_OF_COLLATION[102] = UTF16;
        CHARSET_NAMES_OF_COLLATION[103] = UTF16;
        CHARSET_NAMES_OF_COLLATION[104] = UTF16;
        CHARSET_NAMES_OF_COLLATION[105] = UTF16;
        CHARSET_NAMES_OF_COLLATION[106] = UTF16;
        CHARSET_NAMES_OF_COLLATION[107] = UTF16;
        CHARSET_NAMES_OF_COLLATION[108] = UTF16;
        CHARSET_NAMES_OF_COLLATION[109] = UTF16;
        CHARSET_NAMES_OF_COLLATION[110] = UTF16;
        CHARSET_NAMES_OF_COLLATION[111] = UTF16;
        CHARSET_NAMES_OF_COLLATION[112] = UTF16;
        CHARSET_NAMES_OF_COLLATION[113] = UTF16;
        CHARSET_NAMES_OF_COLLATION[114] = UTF16;
        CHARSET_NAMES_OF_COLLATION[115] = UTF16;
        CHARSET_NAMES_OF_COLLATION[116] = UTF16;
        CHARSET_NAMES_OF_COLLATION[117] = UTF16;
        CHARSET_NAMES_OF_COLLATION[118] = UTF16;
        CHARSET_NAMES_OF_COLLATION[119] = UTF16;
        CHARSET_NAMES_OF_COLLATION[120] = UTF16;
        CHARSET_NAMES_OF_COLLATION[121] = UTF16;
        CHARSET_NAMES_OF_COLLATION[122] = UTF16;
        CHARSET_NAMES_OF_COLLATION[123] = UTF16;
        CHARSET_NAMES_OF_COLLATION[124] = UTF16;
        CHARSET_NAMES_OF_COLLATION[128] = UCS2;
        CHARSET_NAMES_OF_COLLATION[129] = UCS2;
        CHARSET_NAMES_OF_COLLATION[130] = UCS2;
        CHARSET_NAMES_OF_COLLATION[131] = UCS2;
        CHARSET_NAMES_OF_COLLATION[132] = UCS2;
        CHARSET_NAMES_OF_COLLATION[133] = UCS2;
        CHARSET_NAMES_OF_COLLATION[134] = UCS2;
        CHARSET_NAMES_OF_COLLATION[135] = UCS2;
        CHARSET_NAMES_OF_COLLATION[136] = UCS2;
        CHARSET_NAMES_OF_COLLATION[137] = UCS2;
        CHARSET_NAMES_OF_COLLATION[138] = UCS2;
        CHARSET_NAMES_OF_COLLATION[139] = UCS2;
        CHARSET_NAMES_OF_COLLATION[140] = UCS2;
        CHARSET_NAMES_OF_COLLATION[141] = UCS2;
        CHARSET_NAMES_OF_COLLATION[142] = UCS2;
        CHARSET_NAMES_OF_COLLATION[143] = UCS2;
        CHARSET_NAMES_OF_COLLATION[144] = UCS2;
        CHARSET_NAMES_OF_COLLATION[145] = UCS2;
        CHARSET_NAMES_OF_COLLATION[146] = UCS2;
        CHARSET_NAMES_OF_COLLATION[147] = UCS2;
        CHARSET_NAMES_OF_COLLATION[148] = UCS2;
        CHARSET_NAMES_OF_COLLATION[149] = UCS2;
        CHARSET_NAMES_OF_COLLATION[150] = UCS2;
        CHARSET_NAMES_OF_COLLATION[151] = UCS2;
        CHARSET_NAMES_OF_COLLATION[159] = UCS2;
        CHARSET_NAMES_OF_COLLATION[160] = UTF32;
        CHARSET_NAMES_OF_COLLATION[161] = UTF32;
        CHARSET_NAMES_OF_COLLATION[162] = UTF32;
        CHARSET_NAMES_OF_COLLATION[163] = UTF32;
        CHARSET_NAMES_OF_COLLATION[164] = UTF32;
        CHARSET_NAMES_OF_COLLATION[165] = UTF32;
        CHARSET_NAMES_OF_COLLATION[166] = UTF32;
        CHARSET_NAMES_OF_COLLATION[167] = UTF32;
        CHARSET_NAMES_OF_COLLATION[168] = UTF32;
        CHARSET_NAMES_OF_COLLATION[169] = UTF32;
        CHARSET_NAMES_OF_COLLATION[170] = UTF32;
        CHARSET_NAMES_OF_COLLATION[171] = UTF32;
        CHARSET_NAMES_OF_COLLATION[172] = UTF32;
        CHARSET_NAMES_OF_COLLATION[173] = UTF32;
        CHARSET_NAMES_OF_COLLATION[174] = UTF32;
        CHARSET_NAMES_OF_COLLATION[175] = UTF32;
        CHARSET_NAMES_OF_COLLATION[176] = UTF32;
        CHARSET_NAMES_OF_COLLATION[177] = UTF32;
        CHARSET_NAMES_OF_COLLATION[178] = UTF32;
        CHARSET_NAMES_OF_COLLATION[179] = UTF32;
        CHARSET_NAMES_OF_COLLATION[180] = UTF32;
        CHARSET_NAMES_OF_COLLATION[181] = UTF32;
        CHARSET_NAMES_OF_COLLATION[182] = UTF32;
        CHARSET_NAMES_OF_COLLATION[183] = UTF32;
        CHARSET_NAMES_OF_COLLATION[192] = UTF8;
        CHARSET_NAMES_OF_COLLATION[193] = UTF8;
        CHARSET_NAMES_OF_COLLATION[194] = UTF8;
        CHARSET_NAMES_OF_COLLATION[195] = UTF8;
        CHARSET_NAMES_OF_COLLATION[196] = UTF8;
        CHARSET_NAMES_OF_COLLATION[197] = UTF8;
        CHARSET_NAMES_OF_COLLATION[198] = UTF8;
        CHARSET_NAMES_OF_COLLATION[199] = UTF8;
        CHARSET_NAMES_OF_COLLATION[200] = UTF8;
        CHARSET_NAMES_OF_COLLATION[201] = UTF8;
        CHARSET_NAMES_OF_COLLATION[202] = UTF8;
        CHARSET_NAMES_OF_COLLATION[203] = UTF8;
        CHARSET_NAMES_OF_COLLATION[204] = UTF8;
        CHARSET_NAMES_OF_COLLATION[205] = UTF8;
        CHARSET_NAMES_OF_COLLATION[206] = UTF8;
        CHARSET_NAMES_OF_COLLATION[207] = UTF8;
        CHARSET_NAMES_OF_COLLATION[208] = UTF8;
        CHARSET_NAMES_OF_COLLATION[209] = UTF8;
        CHARSET_NAMES_OF_COLLATION[210] = UTF8;
        CHARSET_NAMES_OF_COLLATION[211] = UTF8;
        CHARSET_NAMES_OF_COLLATION[212] = UTF8;
        CHARSET_NAMES_OF_COLLATION[213] = UTF8;
        CHARSET_NAMES_OF_COLLATION[214] = UTF8;
        CHARSET_NAMES_OF_COLLATION[215] = UTF8;
        CHARSET_NAMES_OF_COLLATION[223] = UTF8;
        CHARSET_NAMES_OF_COLLATION[224] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[225] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[226] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[227] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[228] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[229] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[230] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[231] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[232] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[233] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[234] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[235] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[236] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[237] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[238] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[239] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[240] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[241] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[242] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[243] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[244] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[245] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[246] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[247] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[248] = GB18030;
        CHARSET_NAMES_OF_COLLATION[249] = GB18030;
        CHARSET_NAMES_OF_COLLATION[250] = GB18030;
        CHARSET_NAMES_OF_COLLATION[255] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[256] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[257] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[258] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[259] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[260] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[261] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[262] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[263] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[264] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[265] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[266] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[267] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[268] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[269] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[270] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[271] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[273] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[274] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[275] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[277] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[278] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[279] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[280] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[281] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[282] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[283] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[284] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[285] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[286] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[287] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[288] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[289] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[290] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[291] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[292] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[293] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[294] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[296] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[297] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[298] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[300] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[303] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[304] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[305] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[306] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[307] = UTF8MB4;
        CHARSET_NAMES_OF_COLLATION[308] = UTF8MB4;
    }

    public static String DEFAULT_CHARACTER_SET = defaultCharset().name();
    public static String DEFAULT_COLLATION = defaultCollation().name();
    public static final Charset DEFAULT_STORAGE_CHARSET_IN_CHUNK = Charset.forName("UTF-8");
    public static final String POLAR_DB_X_STANDARD_UTF8_CHARSET_NAME = "UTF-8";

    private CollationName defaultCollationName;
    private List<CollationName> implementedCollationNames;
    private List<CollationName> unimplementedCollationNames;
    private String javaCharset;
    private String originalCharset;

    private int maxLen;

    private Charset javaCharsetImpl;

    CharsetName(CollationName defaultCollationName,
                List<CollationName> unsupportedCollationNames,
                int maxLen) {
        this(
            defaultCollationName,
            ImmutableList.of(),
            unsupportedCollationNames,
            "UTF8",
            "UTF-8",
            maxLen
        );
    }

    CharsetName(CollationName defaultCollationName,
                List<CollationName> supportedCollationNames,
                List<CollationName> unsupportedCollationNames,
                String javaCharset,
                String originalCharset,
                int maxLen) {
        this.defaultCollationName = defaultCollationName;
        this.implementedCollationNames = supportedCollationNames;
        this.unimplementedCollationNames = unsupportedCollationNames;
        this.javaCharset = javaCharset;
        this.originalCharset = originalCharset;
        this.maxLen = maxLen;
    }

    /**
     * Check if the charset name match the collation name.
     */
    public boolean match(CollationName collationName) {
        if (collationName == null) {
            return false;
        }
        if (CHARSET_NAMES_OF_COLLATION[collationName.getMysqlCollationId()] == this) {
            return true;
        }
        return Optional.ofNullable(collationName)
            .map(c -> implementedCollationNames.contains(c) || unimplementedCollationNames.contains(c))
            .orElse(false);
    }

    /**
     * Check if the charset name match the collation name.
     */
    public boolean match(String collation) {
        CollationName collationName = CollationName.of(collation);
        if (collationName == null) {
            return false;
        }
        return match(collationName);
    }

    public int getMaxLen() {
        return maxLen;
    }

    public CollationName getDefaultCollationName() {
        return defaultCollationName;
    }

    public static String getDefaultCollationName(String mysqlCharset) {
        CharsetName charsetName = CharsetName.of(mysqlCharset);
        if (charsetName == null) {
            return "";
        }
        return charsetName.getDefaultCollationName().name();
    }

    public List<CollationName> getImplementedCollationNames() {
        return implementedCollationNames;
    }

    public List<CollationName> getUnimplementedCollationNames() {
        return unimplementedCollationNames;
    }

    public String getJavaCharset() {
        return javaCharset;
    }

    public String getOriginalCharset() {
        return originalCharset;
    }

    public static CharsetName defaultCharset() {
        return UTF8;
    }

    public static CharsetName of(String mysqlCharset) {
        return of(mysqlCharset, true);
    }

    public static CharsetName of(String mysqlCharset, boolean useDefault) {
        if (TStringUtil.isEmpty(mysqlCharset)) {
            return null;
        }

        CharsetName result = CHARSET_NAME_MATCHES.get(mysqlCharset);
        if (result != null) {
            return result;
        }

        String upper = normalizeCharsetName(mysqlCharset);

        for (CharsetName charsetName : values()) {
            if (charsetName.name().equals(upper)) {
                return charsetName;
            }
        }

        return useDefault ? defaultCharset() : null;
    }

    /**
     * Mapping: Java Charset - MySQL Charset
     */
    public static CharsetName of(Charset charset) {
        return of(charset, true);
    }

    public static CharsetName of(Charset charset, boolean useDefault) {
        String name = charset.name().toUpperCase();

        // Find in O(1)
        CharsetName result = CHARSET_NAME_MATCHES.get(name);
        if (result != null) {
            return result;
        }

        Set<String> aliases = charset.aliases();

        return POLAR_DB_X_IMPLEMENTED_CHARSET_NAMES.stream()
            .filter(c -> c.name().equals(name) || aliases.stream().anyMatch(c.name()::equalsIgnoreCase))
            .findFirst()
            .orElseGet(
                () -> CharsetName.of(normalizeCharsetName(charset.name()), useDefault)
            );
    }

    public Charset toJavaCharset() {
        return lookupCharsetIfAbsent(Charset::forName);
    }

    public String toUTF16String(String hexString) {
        return Optional.ofNullable(hexString)
            .map(CharsetName::hexStringToByteArray)
            .map(b -> new String(b, toJavaCharset()))
            .orElse(null);
    }

    public static Charset convertStrToJavaCharset(String mysqlCharset) {
        return Optional.ofNullable(mysqlCharset)
            .filter(c -> !TStringUtil.isEmpty(c))
            .map(CharsetName::of)
            .map(CharsetName::toJavaCharset)
            .orElseGet(() -> defaultCharset().toJavaCharset());
    }

    public static boolean isUnicode(CharsetName charsetName) {
        return Optional.ofNullable(charsetName)
            .map(Enum::name)
            .map(s -> s.startsWith("UTF"))
            .orElse(false);
    }

    private static String normalizeCharsetName(String str) {
        if (TStringUtil.isEmpty(str)) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch != '_' && ch != '-') {
                stringBuilder.append(Character.toUpperCase(ch));
            }
        }
        return stringBuilder.toString();
    }

    public static byte[] hexStringToByteArray(String s) {
        if (TStringUtil.isEmpty(s)) {
            return null;
        }

        if (s.startsWith("0x") || s.startsWith("0X")) {
            s = s.substring(2);
        }

        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public static boolean isUTF8(String encoding) {
        if (encoding == null) {
            return true;
        }
        int i = 0;
        int length = encoding.length();
        if (length < 4) {
            return false;
        }
        while (i < length && encoding.charAt(i) == ' ') {
            i++;
        }
        if (i > length - 4) {
            return false;
        }
        if ((encoding.charAt(i) == 'U' || encoding.charAt(i) == 'u')
            && (encoding.charAt(i + 1) == 'T' || encoding.charAt(i + 1) == 't')
            && (encoding.charAt(i + 2) == 'F' || encoding.charAt(i + 2) == 'f')) {
            i += 3;
        }
        while (i < length && encoding.charAt(i) == '-') {
            i++;
        }
        if (i > length - 1) {
            return false;
        }
        return encoding.charAt(i) == '8';
    }

    private Charset lookupCharsetIfAbsent(Function<? super String, ? extends Charset> mappingFunction) {
        Objects.requireNonNull(mappingFunction);
        Charset charset = this.javaCharsetImpl;
        if (charset == null) {
            Charset newValue;
            if ((newValue = mappingFunction.apply(this.javaCharset)) != null) {
                this.javaCharsetImpl = newValue;
                return newValue;
            }
        }

        return charset;
    }
}

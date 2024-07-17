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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MySQLCharsetDDLValidatorTest {

    private static final String[] LOWER_VALID_COLLATIONS = {
        "armscii8_bin", "armscii8_general_ci", "ascii_bin", "ascii_general_ci", "big5_bin", "big5_chinese_ci", "binary",
        "cp1250_bin", "cp1250_croatian_ci", "cp1250_czech_cs", "cp1250_general_ci", "cp1250_polish_ci",
        "cp1251_bin", "cp1251_bulgarian_ci", "cp1251_general_ci", "cp1251_general_cs", "cp1251_ukrainian_ci",
        "cp1256_bin", "cp1256_general_ci", "cp1257_bin", "cp1257_general_ci", "cp1257_lithuanian_ci", "cp850_bin",
        "cp850_general_ci",
        "cp852_bin", "cp852_general_ci", "cp866_bin", "cp866_general_ci", "cp932_bin", "cp932_japanese_ci", "dec8_bin",
        "dec8_swedish_ci", "eucjpms_bin", "eucjpms_japanese_ci", "euckr_bin", "euckr_korean_ci",
        "gb18030_bin", "gb18030_chinese_ci", "gb18030_unicode_520_ci", "gb2312_bin", "gb2312_chinese_ci", "gbk_bin",
        "gbk_chinese_ci", "geostd8_bin", "geostd8_general_ci", "greek_bin", "greek_general_ci", "hebrew_bin",
        "hebrew_general_ci", "hp8_bin", "hp8_english_ci", "keybcs2_bin", "keybcs2_general_ci", "koi8r_bin",
        "koi8r_general_ci", "koi8u_bin", "koi8u_general_ci", "latin1_bin", "latin1_danish_ci", "latin1_general_ci",
        "latin1_general_cs", "latin1_german1_ci", "latin1_german2_ci", "latin1_spanish_ci", "latin1_swedish_ci",
        "latin2_bin", "latin2_croatian_ci", "latin2_czech_cs", "latin2_general_ci", "latin2_hungarian_ci", "latin5_bin",
        "latin5_turkish_ci",
        "latin7_bin", "latin7_estonian_cs", "latin7_general_ci", "latin7_general_cs", "macce_bin", "macce_general_ci",
        "macroman_bin", "macroman_general_ci", "sjis_bin", "sjis_japanese_ci", "swe7_bin", "swe7_swedish_ci",
        "tis620_bin", "tis620_thai_ci", "ucs2_bin", "ucs2_croatian_ci", "ucs2_czech_ci", "ucs2_danish_ci",
        "ucs2_esperanto_ci", "ucs2_estonian_ci", "ucs2_general_ci", "ucs2_general_mysql500_ci", "ucs2_german2_ci",
        "ucs2_hungarian_ci",
        "ucs2_icelandic_ci", "ucs2_latvian_ci", "ucs2_lithuanian_ci", "ucs2_persian_ci", "ucs2_polish_ci",
        "ucs2_romanian_ci", "ucs2_roman_ci", "ucs2_sinhala_ci", "ucs2_slovak_ci", "ucs2_slovenian_ci",
        "ucs2_spanish2_ci", "ucs2_spanish_ci",
        "ucs2_swedish_ci", "ucs2_turkish_ci", "ucs2_unicode_520_ci", "ucs2_unicode_ci", "ucs2_vietnamese_ci",
        "ujis_bin", "ujis_japanese_ci", "utf16le_bin", "utf16le_general_ci", "utf16_bin", "utf16_croatian_ci",
        "utf16_czech_ci",
        "utf16_danish_ci", "utf16_esperanto_ci", "utf16_estonian_ci", "utf16_general_ci", "utf16_german2_ci",
        "utf16_hungarian_ci", "utf16_icelandic_ci", "utf16_latvian_ci", "utf16_lithuanian_ci", "utf16_persian_ci",
        "utf16_polish_ci", "utf16_romanian_ci",
        "utf16_roman_ci", "utf16_sinhala_ci", "utf16_slovak_ci", "utf16_slovenian_ci", "utf16_spanish2_ci",
        "utf16_spanish_ci", "utf16_swedish_ci", "utf16_turkish_ci", "utf16_unicode_520_ci", "utf16_unicode_ci",
        "utf16_vietnamese_ci", "utf32_bin",
        "utf32_croatian_ci", "utf32_czech_ci", "utf32_danish_ci", "utf32_esperanto_ci", "utf32_estonian_ci",
        "utf32_general_ci", "utf32_german2_ci", "utf32_hungarian_ci", "utf32_icelandic_ci", "utf32_latvian_ci",
        "utf32_lithuanian_ci", "utf32_persian_ci",
        "utf32_polish_ci", "utf32_romanian_ci", "utf32_roman_ci", "utf32_sinhala_ci", "utf32_slovak_ci",
        "utf32_slovenian_ci", "utf32_spanish2_ci", "utf32_spanish_ci", "utf32_swedish_ci", "utf32_turkish_ci",
        "utf32_unicode_520_ci", "utf32_unicode_ci",
        "utf32_vietnamese_ci", "utf8mb4_0900_ai_ci", "utf8mb4_0900_as_ci", "utf8mb4_0900_as_cs", "utf8mb4_bin",
        "utf8mb4_croatian_ci", "utf8mb4_cs_0900_ai_ci", "utf8mb4_cs_0900_as_cs", "utf8mb4_czech_ci",
        "utf8mb4_danish_ci", "utf8mb4_da_0900_ai_ci", "utf8mb4_da_0900_as_cs",
        "utf8mb4_de_pb_0900_ai_ci", "utf8mb4_de_pb_0900_as_cs", "utf8mb4_eo_0900_ai_ci", "utf8mb4_eo_0900_as_cs",
        "utf8mb4_esperanto_ci", "utf8mb4_estonian_ci", "utf8mb4_es_0900_ai_ci", "utf8mb4_es_0900_as_cs",
        "utf8mb4_es_trad_0900_ai_ci", "utf8mb4_es_trad_0900_as_cs", "utf8mb4_et_0900_ai_ci", "utf8mb4_et_0900_as_cs",
        "utf8mb4_general_ci", "utf8mb4_german2_ci", "utf8mb4_hr_0900_ai_ci", "utf8mb4_hr_0900_as_cs",
        "utf8mb4_hungarian_ci", "utf8mb4_hu_0900_ai_ci", "utf8mb4_hu_0900_as_cs", "utf8mb4_icelandic_ci",
        "utf8mb4_is_0900_ai_ci", "utf8mb4_is_0900_as_cs", "utf8mb4_ja_0900_as_cs", "utf8mb4_ja_0900_as_cs_ks",
        "utf8mb4_latvian_ci", "utf8mb4_la_0900_ai_ci", "utf8mb4_la_0900_as_cs", "utf8mb4_lithuanian_ci",
        "utf8mb4_lt_0900_ai_ci", "utf8mb4_lt_0900_as_cs", "utf8mb4_lv_0900_ai_ci", "utf8mb4_lv_0900_as_cs",
        "utf8mb4_persian_ci", "utf8mb4_pl_0900_ai_ci", "utf8mb4_pl_0900_as_cs", "utf8mb4_polish_ci",
        "utf8mb4_romanian_ci", "utf8mb4_roman_ci", "utf8mb4_ro_0900_ai_ci", "utf8mb4_ro_0900_as_cs",
        "utf8mb4_ru_0900_ai_ci", "utf8mb4_ru_0900_as_cs", "utf8mb4_sinhala_ci", "utf8mb4_sk_0900_ai_ci",
        "utf8mb4_sk_0900_as_cs", "utf8mb4_slovak_ci", "utf8mb4_slovenian_ci", "utf8mb4_sl_0900_ai_ci",
        "utf8mb4_sl_0900_as_cs", "utf8mb4_spanish2_ci", "utf8mb4_spanish_ci", "utf8mb4_sv_0900_ai_ci",
        "utf8mb4_sv_0900_as_cs", "utf8mb4_swedish_ci", "utf8mb4_tr_0900_ai_ci", "utf8mb4_tr_0900_as_cs",
        "utf8mb4_turkish_ci", "utf8mb4_unicode_520_ci", "utf8mb4_unicode_ci", "utf8mb4_vietnamese_ci",
        "utf8mb4_vi_0900_ai_ci", "utf8mb4_vi_0900_as_cs", "utf8mb4_zh_0900_as_cs", "utf8_bin", "utf8_croatian_ci",
        "utf8_czech_ci", "utf8_danish_ci", "utf8_esperanto_ci", "utf8_estonian_ci", "utf8_general_ci",
        "utf8_general_mysql500_ci", "utf8_german2_ci",
        "utf8_hungarian_ci", "utf8_icelandic_ci", "utf8_latvian_ci", "utf8_lithuanian_ci", "utf8_persian_ci",
        "utf8_polish_ci", "utf8_romanian_ci", "utf8_roman_ci", "utf8_sinhala_ci", "utf8_slovak_ci", "utf8_slovenian_ci",
        "utf8_spanish2_ci",
        "utf8_spanish_ci", "utf8_swedish_ci", "utf8_turkish_ci", "utf8_unicode_520_ci",
        "utf8_unicode_ci", "utf8_vietnamese_ci"};

    private static final String[] UPPER_VALID_COLLATIONS = {
        "ARMSCII8_BIN", "ARMSCII8_GENERAL_CI", "ASCII_BIN", "ASCII_GENERAL_CI", "BIG5_BIN", "BIG5_CHINESE_CI", "BINARY",
        "CP1250_BIN", "CP1250_CROATIAN_CI", "CP1250_CZECH_CS", "CP1250_GENERAL_CI", "CP1250_POLISH_CI",
        "CP1251_BIN", "CP1251_BULGARIAN_CI", "CP1251_GENERAL_CI", "CP1251_GENERAL_CS", "CP1251_UKRAINIAN_CI",
        "CP1256_BIN", "CP1256_GENERAL_CI", "CP1257_BIN", "CP1257_GENERAL_CI", "CP1257_LITHUANIAN_CI", "CP850_BIN",
        "CP850_GENERAL_CI",
        "CP852_BIN", "CP852_GENERAL_CI", "CP866_BIN", "CP866_GENERAL_CI", "CP932_BIN", "CP932_JAPANESE_CI", "DEC8_BIN",
        "DEC8_SWEDISH_CI", "EUCJPMS_BIN", "EUCJPMS_JAPANESE_CI", "EUCKR_BIN", "EUCKR_KOREAN_CI",
        "GB18030_BIN", "GB18030_CHINESE_CI", "GB18030_UNICODE_520_CI", "GB2312_BIN", "GB2312_CHINESE_CI", "GBK_BIN",
        "GBK_CHINESE_CI", "GEOSTD8_BIN", "GEOSTD8_GENERAL_CI", "GREEK_BIN", "GREEK_GENERAL_CI", "HEBREW_BIN",
        "HEBREW_GENERAL_CI", "HP8_BIN", "HP8_ENGLISH_CI", "KEYBCS2_BIN", "KEYBCS2_GENERAL_CI", "KOI8R_BIN",
        "KOI8R_GENERAL_CI", "KOI8U_BIN", "KOI8U_GENERAL_CI", "LATIN1_BIN", "LATIN1_DANISH_CI", "LATIN1_GENERAL_CI",
        "LATIN1_GENERAL_CS", "LATIN1_GERMAN1_CI", "LATIN1_GERMAN2_CI", "LATIN1_SPANISH_CI", "LATIN1_SWEDISH_CI",
        "LATIN2_BIN", "LATIN2_CROATIAN_CI", "LATIN2_CZECH_CS", "LATIN2_GENERAL_CI", "LATIN2_HUNGARIAN_CI", "LATIN5_BIN",
        "LATIN5_TURKISH_CI",
        "LATIN7_BIN", "LATIN7_ESTONIAN_CS", "LATIN7_GENERAL_CI", "LATIN7_GENERAL_CS", "MACCE_BIN", "MACCE_GENERAL_CI",
        "MACROMAN_BIN", "MACROMAN_GENERAL_CI", "SJIS_BIN", "SJIS_JAPANESE_CI", "SWE7_BIN", "SWE7_SWEDISH_CI",
        "TIS620_BIN", "TIS620_THAI_CI", "UCS2_BIN", "UCS2_CROATIAN_CI", "UCS2_CZECH_CI", "UCS2_DANISH_CI",
        "UCS2_ESPERANTO_CI", "UCS2_ESTONIAN_CI", "UCS2_GENERAL_CI", "UCS2_GENERAL_MYSQL500_CI", "UCS2_GERMAN2_CI",
        "UCS2_HUNGARIAN_CI",
        "UCS2_ICELANDIC_CI", "UCS2_LATVIAN_CI", "UCS2_LITHUANIAN_CI", "UCS2_PERSIAN_CI", "UCS2_POLISH_CI",
        "UCS2_ROMANIAN_CI", "UCS2_ROMAN_CI", "UCS2_SINHALA_CI", "UCS2_SLOVAK_CI", "UCS2_SLOVENIAN_CI",
        "UCS2_SPANISH2_CI", "UCS2_SPANISH_CI",
        "UCS2_SWEDISH_CI", "UCS2_TURKISH_CI", "UCS2_UNICODE_520_CI", "UCS2_UNICODE_CI", "UCS2_VIETNAMESE_CI",
        "UJIS_BIN", "UJIS_JAPANESE_CI", "UTF16LE_BIN", "UTF16LE_GENERAL_CI", "UTF16_BIN", "UTF16_CROATIAN_CI",
        "UTF16_CZECH_CI",
        "UTF16_DANISH_CI", "UTF16_ESPERANTO_CI", "UTF16_ESTONIAN_CI", "UTF16_GENERAL_CI", "UTF16_GERMAN2_CI",
        "UTF16_HUNGARIAN_CI", "UTF16_ICELANDIC_CI", "UTF16_LATVIAN_CI", "UTF16_LITHUANIAN_CI", "UTF16_PERSIAN_CI",
        "UTF16_POLISH_CI", "UTF16_ROMANIAN_CI",
        "UTF16_ROMAN_CI", "UTF16_SINHALA_CI", "UTF16_SLOVAK_CI", "UTF16_SLOVENIAN_CI", "UTF16_SPANISH2_CI",
        "UTF16_SPANISH_CI", "UTF16_SWEDISH_CI", "UTF16_TURKISH_CI", "UTF16_UNICODE_520_CI", "UTF16_UNICODE_CI",
        "UTF16_VIETNAMESE_CI", "UTF32_BIN",
        "UTF32_CROATIAN_CI", "UTF32_CZECH_CI", "UTF32_DANISH_CI", "UTF32_ESPERANTO_CI", "UTF32_ESTONIAN_CI",
        "UTF32_GENERAL_CI", "UTF32_GERMAN2_CI", "UTF32_HUNGARIAN_CI", "UTF32_ICELANDIC_CI", "UTF32_LATVIAN_CI",
        "UTF32_LITHUANIAN_CI", "UTF32_PERSIAN_CI",
        "UTF32_POLISH_CI", "UTF32_ROMANIAN_CI", "UTF32_ROMAN_CI", "UTF32_SINHALA_CI", "UTF32_SLOVAK_CI",
        "UTF32_SLOVENIAN_CI", "UTF32_SPANISH2_CI", "UTF32_SPANISH_CI", "UTF32_SWEDISH_CI", "UTF32_TURKISH_CI",
        "UTF32_UNICODE_520_CI", "UTF32_UNICODE_CI",
        "UTF32_VIETNAMESE_CI", "UTF8MB4_0900_AI_CI", "UTF8MB4_0900_AS_CI", "UTF8MB4_0900_AS_CS", "UTF8MB4_BIN",
        "UTF8MB4_CROATIAN_CI", "UTF8MB4_CS_0900_AI_CI", "UTF8MB4_CS_0900_AS_CS", "UTF8MB4_CZECH_CI",
        "UTF8MB4_DANISH_CI", "UTF8MB4_DA_0900_AI_CI", "UTF8MB4_DA_0900_AS_CS",
        "UTF8MB4_DE_PB_0900_AI_CI", "UTF8MB4_DE_PB_0900_AS_CS", "UTF8MB4_EO_0900_AI_CI", "UTF8MB4_EO_0900_AS_CS",
        "UTF8MB4_ESPERANTO_CI", "UTF8MB4_ESTONIAN_CI", "UTF8MB4_ES_0900_AI_CI", "UTF8MB4_ES_0900_AS_CS",
        "UTF8MB4_ES_TRAD_0900_AI_CI", "UTF8MB4_ES_TRAD_0900_AS_CS", "UTF8MB4_ET_0900_AI_CI", "UTF8MB4_ET_0900_AS_CS",
        "UTF8MB4_GENERAL_CI", "UTF8MB4_GERMAN2_CI", "UTF8MB4_HR_0900_AI_CI", "UTF8MB4_HR_0900_AS_CS",
        "UTF8MB4_HUNGARIAN_CI", "UTF8MB4_HU_0900_AI_CI", "UTF8MB4_HU_0900_AS_CS", "UTF8MB4_ICELANDIC_CI",
        "UTF8MB4_IS_0900_AI_CI", "UTF8MB4_IS_0900_AS_CS", "UTF8MB4_JA_0900_AS_CS", "UTF8MB4_JA_0900_AS_CS_KS",
        "UTF8MB4_LATVIAN_CI", "UTF8MB4_LA_0900_AI_CI", "UTF8MB4_LA_0900_AS_CS", "UTF8MB4_LITHUANIAN_CI",
        "UTF8MB4_LT_0900_AI_CI", "UTF8MB4_LT_0900_AS_CS", "UTF8MB4_LV_0900_AI_CI", "UTF8MB4_LV_0900_AS_CS",
        "UTF8MB4_PERSIAN_CI", "UTF8MB4_PL_0900_AI_CI", "UTF8MB4_PL_0900_AS_CS", "UTF8MB4_POLISH_CI",
        "UTF8MB4_ROMANIAN_CI", "UTF8MB4_ROMAN_CI", "UTF8MB4_RO_0900_AI_CI", "UTF8MB4_RO_0900_AS_CS",
        "UTF8MB4_RU_0900_AI_CI", "UTF8MB4_RU_0900_AS_CS", "UTF8MB4_SINHALA_CI", "UTF8MB4_SK_0900_AI_CI",
        "UTF8MB4_SK_0900_AS_CS", "UTF8MB4_SLOVAK_CI", "UTF8MB4_SLOVENIAN_CI", "UTF8MB4_SL_0900_AI_CI",
        "UTF8MB4_SL_0900_AS_CS", "UTF8MB4_SPANISH2_CI", "UTF8MB4_SPANISH_CI", "UTF8MB4_SV_0900_AI_CI",
        "UTF8MB4_SV_0900_AS_CS", "UTF8MB4_SWEDISH_CI", "UTF8MB4_TR_0900_AI_CI", "UTF8MB4_TR_0900_AS_CS",
        "UTF8MB4_TURKISH_CI", "UTF8MB4_UNICODE_520_CI", "UTF8MB4_UNICODE_CI", "UTF8MB4_VIETNAMESE_CI",
        "UTF8MB4_VI_0900_AI_CI", "UTF8MB4_VI_0900_AS_CS", "UTF8MB4_ZH_0900_AS_CS", "UTF8_BIN", "UTF8_CROATIAN_CI",
        "UTF8_CZECH_CI", "UTF8_DANISH_CI", "UTF8_ESPERANTO_CI", "UTF8_ESTONIAN_CI", "UTF8_GENERAL_CI",
        "UTF8_GENERAL_MYSQL500_CI", "UTF8_GERMAN2_CI",
        "UTF8_HUNGARIAN_CI", "UTF8_ICELANDIC_CI", "UTF8_LATVIAN_CI", "UTF8_LITHUANIAN_CI", "UTF8_PERSIAN_CI",
        "UTF8_POLISH_CI", "UTF8_ROMANIAN_CI", "UTF8_ROMAN_CI", "UTF8_SINHALA_CI", "UTF8_SLOVAK_CI", "UTF8_SLOVENIAN_CI",
        "UTF8_SPANISH2_CI",
        "UTF8_SPANISH_CI", "UTF8_SWEDISH_CI", "UTF8_TURKISH_CI", "UTF8_UNICODE_520_CI",
        "UTF8_UNICODE_CI", "UTF8_VIETNAMESE_CI"
    };

    private static final String[] LOWER_INVALID_COLLATIONS = {
        "ii8_bin", "ii8_general_ci", "_bin", "_general_ci", "bin", "chinese_ci", "y", "0_bin", "0_croatian_ci",
        "0_czech_cs", "0_general_ci",
        "0_polish_ci", "1_bin", "1_bulgarian_ci", "1_general_ci", "1_general_cs", "1_ukrainian_ci", "6_bin",
        "6_general_ci", "7_bin", "7_general_ci", "7_lithuanian_ci",
        "_bin", "_general_ci", "_bin", "_general_ci", "_bin", "_general_ci", "_bin", "_japanese_ci", "bin",
        "swedish_ci", "ms_bin",
        "ms_japanese_ci", "_bin", "_korean_ci", "30_bin", "30_chinese_ci", "30_unicode_520_ci", "2_bin", "2_chinese_ci",
        "in", "hinese_ci", "d8_bin",
        "d8_general_ci", "_bin", "_general_ci", "w_bin", "w_general_ci", "in", "nglish_ci", "s2_bin", "s2_general_ci",
        "_bin", "_general_ci",
        "_bin", "_general_ci", "1_bin", "1_danish_ci", "1_general_ci", "1_general_cs", "1_german1_ci", "1_german2_ci",
        "1_spanish_ci", "1_swedish_ci", "2_bin",
        "2_croatian_ci", "2_czech_cs", "2_general_ci", "2_hungarian_ci", "5_bin", "5_turkish_ci", "7_bin",
        "7_estonian_cs", "7_general_ci", "7_general_cs", "_bin",
        "_general_ci", "man_bin", "man_general_ci", "bin", "japanese_ci", "bin", "swedish_ci", "0_bin", "0_thai_ci",
        "bin", "croatian_ci",
        "czech_ci", "danish_ci", "esperanto_ci", "estonian_ci", "general_ci", "general_mysql500_ci", "german2_ci",
        "hungarian_ci", "icelandic_ci", "latvian_ci", "lithuanian_ci",
        "persian_ci", "polish_ci", "romanian_ci", "roman_ci", "sinhala_ci", "slovak_ci", "slovenian_ci", "spanish2_ci",
        "spanish_ci", "swedish_ci", "turkish_ci",
        "unicode_520_ci", "unicode_ci", "vietnamese_ci", "bin", "japanese_ci", "le_bin", "le_general_ci", "_bin",
        "_croatian_ci", "_czech_ci", "_danish_ci",
        "_esperanto_ci", "_estonian_ci", "_general_ci", "_german2_ci", "_hungarian_ci", "_icelandic_ci", "_latvian_ci",
        "_lithuanian_ci", "_persian_ci", "_polish_ci", "_romanian_ci",
        "_roman_ci", "_sinhala_ci", "_slovak_ci", "_slovenian_ci", "_spanish2_ci", "_spanish_ci", "_swedish_ci",
        "_turkish_ci", "_unicode_520_ci", "_unicode_ci", "_vietnamese_ci",
        "_bin", "_croatian_ci", "_czech_ci", "_danish_ci", "_esperanto_ci", "_estonian_ci", "_general_ci",
        "_german2_ci", "_hungarian_ci", "_icelandic_ci", "_latvian_ci",
        "_lithuanian_ci", "_persian_ci", "_polish_ci", "_romanian_ci", "_roman_ci", "_sinhala_ci", "_slovak_ci",
        "_slovenian_ci", "_spanish2_ci", "_spanish_ci", "_swedish_ci",
        "_turkish_ci", "_unicode_520_ci", "_unicode_ci", "_vietnamese_ci", "b4_0900_ai_ci", "b4_0900_as_ci",
        "b4_0900_as_cs", "b4_bin", "b4_croatian_ci", "b4_cs_0900_ai_ci", "b4_cs_0900_as_cs",
        "b4_czech_ci", "b4_danish_ci", "b4_da_0900_ai_ci", "b4_da_0900_as_cs", "b4_de_pb_0900_ai_ci",
        "b4_de_pb_0900_as_cs", "b4_eo_0900_ai_ci", "b4_eo_0900_as_cs", "b4_esperanto_ci", "b4_estonian_ci",
        "b4_es_0900_ai_ci",
        "b4_es_0900_as_cs", "b4_es_trad_0900_ai_ci", "b4_es_trad_0900_as_cs", "b4_et_0900_ai_ci", "b4_et_0900_as_cs",
        "b4_general_ci", "b4_german2_ci", "b4_hr_0900_ai_ci", "b4_hr_0900_as_cs", "b4_hungarian_ci", "b4_hu_0900_ai_ci",
        "b4_hu_0900_as_cs", "b4_icelandic_ci", "b4_is_0900_ai_ci", "b4_is_0900_as_cs", "b4_ja_0900_as_cs",
        "b4_ja_0900_as_cs_ks", "b4_latvian_ci", "b4_la_0900_ai_ci", "b4_la_0900_as_cs", "b4_lithuanian_ci",
        "b4_lt_0900_ai_ci",
        "b4_lt_0900_as_cs", "b4_lv_0900_ai_ci", "b4_lv_0900_as_cs", "b4_persian_ci", "b4_pl_0900_ai_ci",
        "b4_pl_0900_as_cs", "b4_polish_ci", "b4_romanian_ci", "b4_roman_ci", "b4_ro_0900_ai_ci", "b4_ro_0900_as_cs",
        "b4_ru_0900_ai_ci", "b4_ru_0900_as_cs", "b4_sinhala_ci", "b4_sk_0900_ai_ci", "b4_sk_0900_as_cs", "b4_slovak_ci",
        "b4_slovenian_ci", "b4_sl_0900_ai_ci", "b4_sl_0900_as_cs", "b4_spanish2_ci", "b4_spanish_ci",
        "b4_sv_0900_ai_ci", "b4_sv_0900_as_cs", "b4_swedish_ci", "b4_tr_0900_ai_ci", "b4_tr_0900_as_cs",
        "b4_turkish_ci", "b4_unicode_520_ci", "b4_unicode_ci", "b4_vietnamese_ci", "b4_vi_0900_ai_ci",
        "b4_vi_0900_as_cs",
        "b4_zh_0900_as_cs", "bin", "croatian_ci", "czech_ci", "danish_ci", "esperanto_ci", "estonian_ci", "general_ci",
        "general_mysql500_ci", "german2_ci", "hungarian_ci",
        "icelandic_ci", "latvian_ci", "lithuanian_ci", "persian_ci", "polish_ci", "romanian_ci", "roman_ci",
        "sinhala_ci", "slovak_ci", "slovenian_ci", "spanish2_ci",
        "spanish_ci", "swedish_ci", "tolower_ci", "turkish_ci", "unicode_520_ci", "unicode_ci", "vietnamese_ci"
    };

    @Test
    public void testCheckCollation() {
        for (String lowerValidCollations : LOWER_VALID_COLLATIONS) {
            Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation(lowerValidCollations));
        }

        for (String upperValidCollations : UPPER_VALID_COLLATIONS) {
            Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation(upperValidCollations));
        }

        for (String invalidCollations : LOWER_INVALID_COLLATIONS) {
            Assert.assertFalse(MySQLCharsetDDLValidator.checkCollation(invalidCollations));
        }
    }

    private static final String[] LOWER_VALID_CHARSETS = {
        "big5", "dec8", "cp850", "hp8", "koi8r", "latin1", "latin2", "swe7", "ascii", "ujis", "sjis", "hebrew",
        "tis620", "euckr", "koi8u", "gb2312", "greek", "cp1250", "gbk", "latin5", "armscii8", "utf8", "ucs2", "cp866",
        "keybcs2", "macce", "macroman", "cp852", "latin7", "utf8mb4", "cp1251", "utf16", "utf16le", "cp1256", "cp1257",
        "utf32", "binary", "geostd8", "cp932", "eucjpms", "gb18030"
    };

    private static final String[] UPPER_VALID_CHARSETS = {
        "BIG5", "DEC8", "CP850", "HP8", "KOI8R", "LATIN1", "LATIN2", "SWE7", "ASCII", "UJIS", "SJIS", "HEBREW",
        "TIS620", "EUCKR", "KOI8U", "GB2312", "GREEK", "CP1250", "GBK", "LATIN5", "ARMSCII8", "UTF8", "UCS2", "CP866",
        "KEYBCS2", "MACCE", "MACROMAN", "CP852", "LATIN7", "UTF8MB4", "CP1251", "UTF16", "UTF16LE", "CP1256", "CP1257",
        "UTF32", "BINARY", "GEOSTD8", "CP932", "EUCJPMS", "GB18030"
    };

    private static final String[] LOWER_INVALID_CHARSETS = {
        "g5", "c8", "850", "8", "i8r", "tin1", "tin2", "e7", "cii", "is", "is", "brew",
        "s620", "ckr", "i8u", "2312", "eek", "1250", "k", "tin5", "mscii8", "f8", "s2", "866",
        "ybcs2", "cce", "croman", "852", "tin7", "f8mb4", "1251", "f16", "f16le", "1256", "1257", "f32",
        "nary", "ostd8", "932", "cjpms", "18030"
    };

    @Test
    public void testCheckCharset() {
        for (String lowerValidCharset : LOWER_VALID_CHARSETS) {
            Assert.assertTrue(MySQLCharsetDDLValidator.checkCharset(lowerValidCharset));
        }

        for (String upperInvalidCharset : UPPER_VALID_CHARSETS) {
            Assert.assertTrue(MySQLCharsetDDLValidator.checkCharset(upperInvalidCharset));
        }

        for (String lowerInvalidCharset : LOWER_INVALID_CHARSETS) {
            Assert.assertFalse(MySQLCharsetDDLValidator.checkCharset(lowerInvalidCharset));
        }
    }

    @Test
    public void testMatch() {
        for (CharsetName charsetName : CharsetName.values()) {
            // Find matched & unmatched collation set for specific charset name.
            Set<CollationName> matched = Stream
                .concat(charsetName.getImplementedCollationNames().stream(),
                    charsetName.getUnimplementedCollationNames().stream())
                .collect(Collectors.toSet());
            Set<CollationName> unmatched = Arrays
                .stream(CollationName.values())
                .collect(Collectors.toSet());
            unmatched.removeAll(matched);

            for (CollationName m : matched) {
                Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetCollation(charsetName.name(), m.name()));
            }

            for (CollationName um : unmatched) {
                Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetCollation(charsetName.name(), um.name()));
            }
        }
    }

    @Test
    public void testImplement() {
        // test all implemented & unimplemented charset names.
        Set<CharsetName> implementedCharsets = CharsetName.POLAR_DB_X_IMPLEMENTED_CHARSET_NAMES.stream().collect(
            Collectors.toSet());
        Set<CharsetName> unimplementedCharsets = Arrays.stream(CharsetName.values()).collect(
            Collectors.toSet());
        unimplementedCharsets.removeAll(implementedCharsets);
        for (CharsetName implemented : implementedCharsets) {
            Assert.assertTrue(MySQLCharsetDDLValidator.isCharsetImplemented(implemented.name()));
        }
        for (CharsetName unimplemented : unimplementedCharsets) {
            Assert.assertFalse(MySQLCharsetDDLValidator.isCharsetImplemented(unimplemented.name()));
        }

        // test all implemented & unimplemented collation names.
        Set<CollationName> implementedCollations =
            CollationName.POLAR_DB_X_IMPLEMENTED_COLLATION_NAMES.stream().collect(
                Collectors.toSet());
        Set<CollationName> unimplementedCollations = Arrays.stream(CollationName.values()).collect(
            Collectors.toSet());
        unimplementedCollations.removeAll(implementedCollations);
        for (CollationName implemented : implementedCollations) {
            Assert.assertTrue(MySQLCharsetDDLValidator.isCollationImplemented(implemented.name()));
        }
        for (CollationName unimplemented : unimplementedCollations) {
            Assert.assertFalse(MySQLCharsetDDLValidator.isCollationImplemented(unimplemented.name()));
        }

    }

    @Test
    public void testCheckCharsetSupported() {
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4", null));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported(null, "utf8mb4_general_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4", "utf8mb4_general_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported(null, null));

        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4__", null));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported(null, "utf8mb4___general_ci"));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4", "utf16_general_ci"));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4_", "utf8mb4_general_ci"));

        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4", null, true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported(null, "utf8mb4_unicode_ci", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4", "utf8mb4_general_ci", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported(null, null, true));

        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4__", null, true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported(null, "utf8mb4___general_ci", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4", "utf16_general_ci", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4_", "utf8mb4_general_ci", true));

        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("latin2", null, true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb4", "utf8mb4_estonian_ci", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported(null, "utf8mb4_estonian_ci", true));
    }

    @Test
    public void testCheckCharsetSupportedMySQL80() {
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8", "utf8mb3_general_ci", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8", "utf8mb3_bin", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8", "utf8mb3_unicode_ci", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8", "utf8mb3_general_mysql500_ci", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8", "utf8mb3_bi", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8", "utf8mb3_icelandic_ci", true));

        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8mb3_general_ci", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8mb3_bin", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8mb3_unicode_ci", true));
        Assert.assertTrue(
            MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8mb3_general_mysql500_ci", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8mb3_bi", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8mb3_icelandic_ci", true));

        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8_general_ci", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8_bin", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8_unicode_ci", true));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8_general_mysql500_ci", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8_bi", true));
        Assert.assertFalse(MySQLCharsetDDLValidator.checkCharsetSupported("utf8mb3", "utf8_icelandic_ci", true));

    }

    @Test
    public void testCheckCollationMySQL80() {
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_general_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_bin"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_unicode_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_icelandic_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_latvian_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_romanian_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_slovenian_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_polish_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_estonian_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_spanish_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_swedish_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_turkish_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_czech_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_danish_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_lithuanian_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_slovak_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_spanish2_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_roman_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_persian_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_esperanto_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_hungarian_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_sinhala_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_german2_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_croatian_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_unicode_520_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_vietnamese_ci"));
        Assert.assertTrue(MySQLCharsetDDLValidator.checkCollation("utf8mb3_general_mysql500_ci"));
    }
}
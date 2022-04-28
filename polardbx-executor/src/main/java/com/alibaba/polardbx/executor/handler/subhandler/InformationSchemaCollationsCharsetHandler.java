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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaCollationCharacterSetApplicability;
import com.alibaba.polardbx.optimizer.view.VirtualView;

/**
 * @author shengyu
 */
public class InformationSchemaCollationsCharsetHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaCollationsCharsetHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaCollationCharacterSetApplicability;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        InformationSchemaCollationCharacterSetApplicability informationSchemaCollations =
            (InformationSchemaCollationCharacterSetApplicability) virtualView;
        cursor.addRow(new Object[] {"armscii8_general_ci", "armscii8"});
        cursor.addRow(new Object[] {"armscii8_bin", "armscii8"});
        cursor.addRow(new Object[] {"ascii_general_ci", "ascii"});
        cursor.addRow(new Object[] {"ascii_bin", "ascii"});
        cursor.addRow(new Object[] {"big5_chinese_ci", "big5"});
        cursor.addRow(new Object[] {"big5_bin", "big5"});
        cursor.addRow(new Object[] {"binary", "binary"});
        cursor.addRow(new Object[] {"cp1250_general_ci", "cp1250"});
        cursor.addRow(new Object[] {"cp1250_czech_cs", "cp1250"});
        cursor.addRow(new Object[] {"cp1250_croatian_ci", "cp1250"});
        cursor.addRow(new Object[] {"cp1250_bin", "cp1250"});
        cursor.addRow(new Object[] {"cp1250_polish_ci", "cp1250"});
        cursor.addRow(new Object[] {"cp1251_bulgarian_ci", "cp1251"});
        cursor.addRow(new Object[] {"cp1251_ukrainian_ci", "cp1251"});
        cursor.addRow(new Object[] {"cp1251_bin", "cp1251"});
        cursor.addRow(new Object[] {"cp1251_general_ci", "cp1251"});
        cursor.addRow(new Object[] {"cp1251_general_cs", "cp1251"});
        cursor.addRow(new Object[] {"cp1256_general_ci", "cp1256"});
        cursor.addRow(new Object[] {"cp1256_bin", "cp1256"});
        cursor.addRow(new Object[] {"cp1257_lithuanian_ci", "cp1257"});
        cursor.addRow(new Object[] {"cp1257_bin", "cp1257"});
        cursor.addRow(new Object[] {"cp1257_general_ci", "cp1257"});
        cursor.addRow(new Object[] {"cp850_general_ci", "cp850"});
        cursor.addRow(new Object[] {"cp850_bin", "cp850"});
        cursor.addRow(new Object[] {"cp852_general_ci", "cp852"});
        cursor.addRow(new Object[] {"cp852_bin", "cp852"});
        cursor.addRow(new Object[] {"cp866_general_ci", "cp866"});
        cursor.addRow(new Object[] {"cp866_bin", "cp866"});
        cursor.addRow(new Object[] {"cp932_japanese_ci", "cp932"});
        cursor.addRow(new Object[] {"cp932_bin", "cp932"});
        cursor.addRow(new Object[] {"dec8_swedish_ci", "dec8"});
        cursor.addRow(new Object[] {"dec8_bin", "dec8"});
        cursor.addRow(new Object[] {"eucjpms_japanese_ci", "eucjpms"});
        cursor.addRow(new Object[] {"eucjpms_bin", "eucjpms"});
        cursor.addRow(new Object[] {"euckr_korean_ci", "euckr"});
        cursor.addRow(new Object[] {"euckr_bin", "euckr"});
        cursor.addRow(new Object[] {"gb18030_chinese_ci", "gb18030"});
        cursor.addRow(new Object[] {"gb18030_bin", "gb18030"});
        cursor.addRow(new Object[] {"gb18030_unicode_520_ci", "gb18030"});
        cursor.addRow(new Object[] {"gb2312_chinese_ci", "gb2312"});
        cursor.addRow(new Object[] {"gb2312_bin", "gb2312"});
        cursor.addRow(new Object[] {"gbk_chinese_ci", "gbk"});
        cursor.addRow(new Object[] {"gbk_bin", "gbk"});
        cursor.addRow(new Object[] {"geostd8_general_ci", "geostd8"});
        cursor.addRow(new Object[] {"geostd8_bin", "geostd8"});
        cursor.addRow(new Object[] {"greek_general_ci", "greek"});
        cursor.addRow(new Object[] {"greek_bin", "greek"});
        cursor.addRow(new Object[] {"hebrew_general_ci", "hebrew"});
        cursor.addRow(new Object[] {"hebrew_bin", "hebrew"});
        cursor.addRow(new Object[] {"hp8_english_ci", "hp8"});
        cursor.addRow(new Object[] {"hp8_bin", "hp8"});
        cursor.addRow(new Object[] {"keybcs2_general_ci", "keybcs2"});
        cursor.addRow(new Object[] {"keybcs2_bin", "keybcs2"});
        cursor.addRow(new Object[] {"koi8r_general_ci", "koi8r"});
        cursor.addRow(new Object[] {"koi8r_bin", "koi8r"});
        cursor.addRow(new Object[] {"koi8u_general_ci", "koi8u"});
        cursor.addRow(new Object[] {"koi8u_bin", "koi8u"});
        cursor.addRow(new Object[] {"latin1_german1_ci", "latin1"});
        cursor.addRow(new Object[] {"latin1_swedish_ci", "latin1"});
        cursor.addRow(new Object[] {"latin1_danish_ci", "latin1"});
        cursor.addRow(new Object[] {"latin1_german2_ci", "latin1"});
        cursor.addRow(new Object[] {"latin1_bin", "latin1"});
        cursor.addRow(new Object[] {"latin1_general_ci", "latin1"});
        cursor.addRow(new Object[] {"latin1_general_cs", "latin1"});
        cursor.addRow(new Object[] {"latin1_spanish_ci", "latin1"});
        cursor.addRow(new Object[] {"latin2_czech_cs", "latin2"});
        cursor.addRow(new Object[] {"latin2_general_ci", "latin2"});
        cursor.addRow(new Object[] {"latin2_hungarian_ci", "latin2"});
        cursor.addRow(new Object[] {"latin2_croatian_ci", "latin2"});
        cursor.addRow(new Object[] {"latin2_bin", "latin2"});
        cursor.addRow(new Object[] {"latin5_turkish_ci", "latin5"});
        cursor.addRow(new Object[] {"latin5_bin", "latin5"});
        cursor.addRow(new Object[] {"latin7_estonian_cs", "latin7"});
        cursor.addRow(new Object[] {"latin7_general_ci", "latin7"});
        cursor.addRow(new Object[] {"latin7_general_cs", "latin7"});
        cursor.addRow(new Object[] {"latin7_bin", "latin7"});
        cursor.addRow(new Object[] {"macce_general_ci", "macce"});
        cursor.addRow(new Object[] {"macce_bin", "macce"});
        cursor.addRow(new Object[] {"macroman_general_ci", "macroman"});
        cursor.addRow(new Object[] {"macroman_bin", "macroman"});
        cursor.addRow(new Object[] {"sjis_japanese_ci", "sjis"});
        cursor.addRow(new Object[] {"sjis_bin", "sjis"});
        cursor.addRow(new Object[] {"swe7_swedish_ci", "swe7"});
        cursor.addRow(new Object[] {"swe7_bin", "swe7"});
        cursor.addRow(new Object[] {"tis620_thai_ci", "tis620"});
        cursor.addRow(new Object[] {"tis620_bin", "tis620"});
        cursor.addRow(new Object[] {"ucs2_general_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_bin", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_unicode_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_icelandic_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_latvian_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_romanian_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_slovenian_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_polish_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_estonian_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_spanish_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_swedish_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_turkish_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_czech_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_danish_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_lithuanian_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_slovak_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_spanish2_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_roman_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_persian_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_esperanto_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_hungarian_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_sinhala_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_german2_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_croatian_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_unicode_520_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_vietnamese_ci", "ucs2"});
        cursor.addRow(new Object[] {"ucs2_general_mysql500_ci", "ucs2"});
        cursor.addRow(new Object[] {"ujis_japanese_ci", "ujis"});
        cursor.addRow(new Object[] {"ujis_bin", "ujis"});
        cursor.addRow(new Object[] {"utf16_general_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_bin", "utf16"});
        cursor.addRow(new Object[] {"utf16_unicode_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_icelandic_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_latvian_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_romanian_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_slovenian_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_polish_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_estonian_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_spanish_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_swedish_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_turkish_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_czech_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_danish_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_lithuanian_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_slovak_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_spanish2_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_roman_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_persian_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_esperanto_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_hungarian_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_sinhala_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_german2_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_croatian_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_unicode_520_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16_vietnamese_ci", "utf16"});
        cursor.addRow(new Object[] {"utf16le_general_ci", "utf16le"});
        cursor.addRow(new Object[] {"utf16le_bin", "utf16le"});
        cursor.addRow(new Object[] {"utf32_general_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_bin", "utf32"});
        cursor.addRow(new Object[] {"utf32_unicode_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_icelandic_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_latvian_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_romanian_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_slovenian_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_polish_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_estonian_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_spanish_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_swedish_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_turkish_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_czech_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_danish_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_lithuanian_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_slovak_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_spanish2_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_roman_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_persian_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_esperanto_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_hungarian_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_sinhala_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_german2_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_croatian_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_unicode_520_ci", "utf32"});
        cursor.addRow(new Object[] {"utf32_vietnamese_ci", "utf32"});
        cursor.addRow(new Object[] {"utf8_general_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_tolower_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_bin", "utf8"});
        cursor.addRow(new Object[] {"utf8_unicode_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_icelandic_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_latvian_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_romanian_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_slovenian_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_polish_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_estonian_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_spanish_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_swedish_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_turkish_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_czech_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_danish_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_lithuanian_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_slovak_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_spanish2_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_roman_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_persian_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_esperanto_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_hungarian_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_sinhala_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_german2_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_croatian_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_unicode_520_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_vietnamese_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8_general_mysql500_ci", "utf8"});
        cursor.addRow(new Object[] {"utf8mb4_general_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_bin", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_unicode_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_icelandic_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_latvian_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_romanian_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_slovenian_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_polish_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_estonian_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_spanish_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_swedish_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_turkish_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_czech_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_danish_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_lithuanian_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_slovak_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_spanish2_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_roman_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_persian_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_esperanto_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_hungarian_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_sinhala_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_german2_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_croatian_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_unicode_520_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_vietnamese_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_de_pb_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_is_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_lv_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_ro_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_sl_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_pl_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_et_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_es_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_sv_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_tr_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_cs_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_da_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_lt_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_sk_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_es_trad_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_la_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_eo_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_hu_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_hr_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_vi_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_de_pb_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_is_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_lv_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_ro_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_sl_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_pl_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_et_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_es_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_sv_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_tr_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_cs_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_da_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_lt_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_sk_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_es_trad_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_la_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_eo_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_hu_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_hr_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_vi_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_ja_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_ja_0900_as_cs_ks", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_0900_as_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_ru_0900_ai_ci", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_ru_0900_as_cs", "utf8mb4"});
        cursor.addRow(new Object[] {"utf8mb4_zh_0900_as_cs", "utf8mb4"});
        return cursor;
    }
}

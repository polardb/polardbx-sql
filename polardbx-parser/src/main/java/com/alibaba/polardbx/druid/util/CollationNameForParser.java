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

package com.alibaba.polardbx.druid.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Standard collation names of MySQL Collation.
 */
public enum CollationNameForParser {
    /*
     * All default collations
     */
    BIG5_CHINESE_CI(1, false, true),
    DEC8_SWEDISH_CI(3, false, true),
    CP850_GENERAL_CI(4, false, true),
    HP8_ENGLISH_CI(6, false, true),
    KOI8R_GENERAL_CI(7, false, true),
    LATIN1_SWEDISH_CI(8, false, true),
    LATIN2_GENERAL_CI(9, false, true),
    SWE7_SWEDISH_CI(10, false, true),
    ASCII_GENERAL_CI(11, false, true),
    UJIS_JAPANESE_CI(12, false, true),
    SJIS_JAPANESE_CI(13, false, true),
    HEBREW_GENERAL_CI(16, false, true),
    TIS620_THAI_CI(18, false, true),
    EUCKR_KOREAN_CI(19, false, true),
    KOI8U_GENERAL_CI(22, false, true),
    GB2312_CHINESE_CI(24, false, true),
    GREEK_GENERAL_CI(25, false, true),
    CP1250_GENERAL_CI(26, false, true),
    GBK_CHINESE_CI(28, false, true),
    LATIN5_TURKISH_CI(30, false, true),
    ARMSCII8_GENERAL_CI(32, false, true),
    UTF8_GENERAL_CI(33, false, true),
    UCS2_GENERAL_CI(35, false, true),
    CP866_GENERAL_CI(36, false, true),
    KEYBCS2_GENERAL_CI(37, false, true),
    MACCE_GENERAL_CI(38, false, true),
    MACROMAN_GENERAL_CI(39, false, true),
    CP852_GENERAL_CI(40, false, true),
    LATIN7_GENERAL_CI(41, false, true),
    UTF8MB4_GENERAL_CI(45, false, true),
    CP1251_GENERAL_CI(51, false, true),
    UTF16_GENERAL_CI(54, false, true),
    UTF16LE_GENERAL_CI(56, false, true),
    CP1256_GENERAL_CI(57, false, true),
    CP1257_GENERAL_CI(59, false, true),
    UTF32_GENERAL_CI(60, false, true),
    BINARY(63, true, true),
    GEOSTD8_GENERAL_CI(92, false, true),
    CP932_JAPANESE_CI(95, false, true),
    EUCJPMS_JAPANESE_CI(97, false, true),
    GB18030_CHINESE_CI(248, false, true),

    /*
     * All non-default collations
     */
    BIG5_BIN(84, true, false),
    DEC8_BIN(69, true, false),
    CP850_BIN(80, true, false),
    HP8_BIN(72, true, false),
    KOI8R_BIN(74, true, false),
    LATIN1_GERMAN1_CI(5, false, false),
    LATIN1_DANISH_CI(15, false, false),
    LATIN1_GERMAN2_CI(31, false, false),
    LATIN1_BIN(47, true, false),
    LATIN1_GENERAL_CI(48, false, false),
    LATIN1_GENERAL_CS(49, true, false),
    LATIN1_SPANISH_CI(94, false, false),
    LATIN2_CZECH_CS(2, true, false),
    LATIN2_HUNGARIAN_CI(21, false, false),
    LATIN2_CROATIAN_CI(27, false, false),
    LATIN2_BIN(77, true, false),
    SWE7_BIN(82, true, false),
    ASCII_BIN(65, true, false),
    UJIS_BIN(91, true, false),
    SJIS_BIN(88, true, false),
    HEBREW_BIN(71, true, false),
    TIS620_BIN(89, true, false),
    EUCKR_BIN(85, true, false),
    KOI8U_BIN(75, true, false),
    GB2312_BIN(86, true, false),
    GREEK_BIN(70, true, false),
    CP1250_CZECH_CS(34, true, false),
    CP1250_CROATIAN_CI(44, false, false),
    CP1250_BIN(66, true, false),
    CP1250_POLISH_CI(99, false, false),
    GBK_BIN(87, true, false),
    LATIN5_BIN(78, true, false),
    ARMSCII8_BIN(64, true, false),
    UTF8_BIN(83, true, false),
    UTF8_UNICODE_CI(192, false, false),
    UTF8_ICELANDIC_CI(193, false, false),
    UTF8_LATVIAN_CI(194, false, false),
    UTF8_ROMANIAN_CI(195, false, false),
    UTF8_SLOVENIAN_CI(196, false, false),
    UTF8_POLISH_CI(197, false, false),
    UTF8_ESTONIAN_CI(198, false, false),
    UTF8_SPANISH_CI(199, false, false),
    UTF8_SWEDISH_CI(200, false, false),
    UTF8_TURKISH_CI(201, false, false),
    UTF8_CZECH_CI(202, false, false),
    UTF8_DANISH_CI(203, false, false),
    UTF8_LITHUANIAN_CI(204, false, false),
    UTF8_SLOVAK_CI(205, false, false),
    UTF8_SPANISH2_CI(206, false, false),
    UTF8_ROMAN_CI(207, false, false),
    UTF8_PERSIAN_CI(208, false, false),
    UTF8_ESPERANTO_CI(209, false, false),
    UTF8_HUNGARIAN_CI(210, false, false),
    UTF8_SINHALA_CI(211, false, false),
    UTF8_GERMAN2_CI(212, false, false),
    UTF8_CROATIAN_CI(213, false, false),
    UTF8_UNICODE_520_CI(214, false, false),
    UTF8_VIETNAMESE_CI(215, false, false),
    UTF8_GENERAL_MYSQL500_CI(223, false, false),
    UCS2_BIN(90, true, false),
    UCS2_UNICODE_CI(128, false, false),
    UCS2_ICELANDIC_CI(129, false, false),
    UCS2_LATVIAN_CI(130, false, false),
    UCS2_ROMANIAN_CI(131, false, false),
    UCS2_SLOVENIAN_CI(132, false, false),
    UCS2_POLISH_CI(133, false, false),
    UCS2_ESTONIAN_CI(134, false, false),
    UCS2_SPANISH_CI(135, false, false),
    UCS2_SWEDISH_CI(136, false, false),
    UCS2_TURKISH_CI(137, false, false),
    UCS2_CZECH_CI(138, false, false),
    UCS2_DANISH_CI(139, false, false),
    UCS2_LITHUANIAN_CI(140, false, false),
    UCS2_SLOVAK_CI(141, false, false),
    UCS2_SPANISH2_CI(142, false, false),
    UCS2_ROMAN_CI(143, false, false),
    UCS2_PERSIAN_CI(144, false, false),
    UCS2_ESPERANTO_CI(145, false, false),
    UCS2_HUNGARIAN_CI(146, false, false),
    UCS2_SINHALA_CI(147, false, false),
    UCS2_GERMAN2_CI(148, false, false),
    UCS2_CROATIAN_CI(149, false, false),
    UCS2_UNICODE_520_CI(150, false, false),
    UCS2_VIETNAMESE_CI(151, false, false),
    UCS2_GENERAL_MYSQL500_CI(159, false, false),
    CP866_BIN(68, true, false),
    KEYBCS2_BIN(73, true, false),
    MACCE_BIN(43, true, false),
    MACROMAN_BIN(53, true, false),
    CP852_BIN(81, true, false),
    LATIN7_ESTONIAN_CS(20, true, false),
    LATIN7_GENERAL_CS(42, true, false),
    LATIN7_BIN(79, true, false),
    UTF8MB4_BIN(46, true, false),
    UTF8MB4_UNICODE_CI(224, false, false),
    UTF8MB4_ICELANDIC_CI(225, false, false),
    UTF8MB4_LATVIAN_CI(226, false, false),
    UTF8MB4_ROMANIAN_CI(227, false, false),
    UTF8MB4_SLOVENIAN_CI(228, false, false),
    UTF8MB4_POLISH_CI(229, false, false),
    UTF8MB4_ESTONIAN_CI(230, false, false),
    UTF8MB4_SPANISH_CI(231, false, false),
    UTF8MB4_SWEDISH_CI(232, false, false),
    UTF8MB4_TURKISH_CI(233, false, false),
    UTF8MB4_CZECH_CI(234, false, false),
    UTF8MB4_DANISH_CI(235, false, false),
    UTF8MB4_LITHUANIAN_CI(236, false, false),
    UTF8MB4_SLOVAK_CI(237, false, false),
    UTF8MB4_SPANISH2_CI(238, false, false),
    UTF8MB4_ROMAN_CI(239, false, false),
    UTF8MB4_PERSIAN_CI(240, false, false),
    UTF8MB4_ESPERANTO_CI(241, false, false),
    UTF8MB4_HUNGARIAN_CI(242, false, false),
    UTF8MB4_SINHALA_CI(243, false, false),
    UTF8MB4_GERMAN2_CI(244, false, false),
    UTF8MB4_CROATIAN_CI(245, false, false),
    UTF8MB4_UNICODE_520_CI(246, false, false),
    UTF8MB4_VIETNAMESE_CI(247, false, false),
    CP1251_BULGARIAN_CI(14, false, false),
    CP1251_UKRAINIAN_CI(23, false, false),
    CP1251_BIN(50, true, false),
    CP1251_GENERAL_CS(52, true, false),
    UTF16_BIN(55, true, false),
    UTF16_UNICODE_CI(101, false, false),
    UTF16_ICELANDIC_CI(102, false, false),
    UTF16_LATVIAN_CI(103, false, false),
    UTF16_ROMANIAN_CI(104, false, false),
    UTF16_SLOVENIAN_CI(105, false, false),
    UTF16_POLISH_CI(106, false, false),
    UTF16_ESTONIAN_CI(107, false, false),
    UTF16_SPANISH_CI(108, false, false),
    UTF16_SWEDISH_CI(109, false, false),
    UTF16_TURKISH_CI(110, false, false),
    UTF16_CZECH_CI(111, false, false),
    UTF16_DANISH_CI(112, false, false),
    UTF16_LITHUANIAN_CI(113, false, false),
    UTF16_SLOVAK_CI(114, false, false),
    UTF16_SPANISH2_CI(115, false, false),
    UTF16_ROMAN_CI(116, false, false),
    UTF16_PERSIAN_CI(117, false, false),
    UTF16_ESPERANTO_CI(118, false, false),
    UTF16_HUNGARIAN_CI(119, false, false),
    UTF16_SINHALA_CI(120, false, false),
    UTF16_GERMAN2_CI(121, false, false),
    UTF16_CROATIAN_CI(122, false, false),
    UTF16_UNICODE_520_CI(123, false, false),
    UTF16_VIETNAMESE_CI(124, false, false),
    UTF16LE_BIN(62, true, false),
    CP1256_BIN(67, true, false),
    CP1257_LITHUANIAN_CI(29, false, false),
    CP1257_BIN(58, true, false),
    UTF32_BIN(61, true, false),
    UTF32_UNICODE_CI(160, false, false),
    UTF32_ICELANDIC_CI(161, false, false),
    UTF32_LATVIAN_CI(162, false, false),
    UTF32_ROMANIAN_CI(163, false, false),
    UTF32_SLOVENIAN_CI(164, false, false),
    UTF32_POLISH_CI(165, false, false),
    UTF32_ESTONIAN_CI(166, false, false),
    UTF32_SPANISH_CI(167, false, false),
    UTF32_SWEDISH_CI(168, false, false),
    UTF32_TURKISH_CI(169, false, false),
    UTF32_CZECH_CI(170, false, false),
    UTF32_DANISH_CI(171, false, false),
    UTF32_LITHUANIAN_CI(172, false, false),
    UTF32_SLOVAK_CI(173, false, false),
    UTF32_SPANISH2_CI(174, false, false),
    UTF32_ROMAN_CI(175, false, false),
    UTF32_PERSIAN_CI(176, false, false),
    UTF32_ESPERANTO_CI(177, false, false),
    UTF32_HUNGARIAN_CI(178, false, false),
    UTF32_SINHALA_CI(179, false, false),
    UTF32_GERMAN2_CI(180, false, false),
    UTF32_CROATIAN_CI(181, false, false),
    UTF32_UNICODE_520_CI(182, false, false),
    UTF32_VIETNAMESE_CI(183, false, false),
    GEOSTD8_BIN(93, true, false),
    CP932_BIN(96, true, false),
    EUCJPMS_BIN(98, true, false),
    GB18030_BIN(249, true, false),
    GB18030_UNICODE_520_CI(250, false, false),

    /*
     * All mysql 8.0 new supported collations.
     */
    UTF8MB4_0900_AI_CI(255, false, true, true),
    UTF8MB4_0900_AS_CI(305, false, false, true),
    UTF8MB4_0900_AS_CS(278, true, false, true),
    UTF8MB4_CS_0900_AI_CI(266, false, false, true),
    UTF8MB4_CS_0900_AS_CS(289, true, false, true),
    UTF8MB4_DA_0900_AI_CI(267, false, false, true),
    UTF8MB4_DA_0900_AS_CS(290, true, false, true),
    UTF8MB4_DE_PB_0900_AI_CI(256, false, false, true),
    UTF8MB4_DE_PB_0900_AS_CS(279, true, false, true),
    UTF8MB4_EO_0900_AI_CI(273, false, false, true),
    UTF8MB4_EO_0900_AS_CS(296, true, false, true),
    UTF8MB4_ES_0900_AI_CI(263, false, false, true),
    UTF8MB4_ES_0900_AS_CS(286, true, false, true),
    UTF8MB4_ES_TRAD_0900_AI_CI(270, false, false, true),
    UTF8MB4_ES_TRAD_0900_AS_CS(293, true, false, true),
    UTF8MB4_ET_0900_AI_CI(262, false, false, true),
    UTF8MB4_ET_0900_AS_CS(285, true, false, true),
    UTF8MB4_HR_0900_AI_CI(275, false, false, true),
    UTF8MB4_HR_0900_AS_CS(298, true, false, true),
    UTF8MB4_HU_0900_AI_CI(274, false, false, true),
    UTF8MB4_HU_0900_AS_CS(297, true, false, true),
    UTF8MB4_IS_0900_AI_CI(257, false, false, true),
    UTF8MB4_IS_0900_AS_CS(280, true, false, true),
    UTF8MB4_JA_0900_AS_CS(303, true, false, true),
    UTF8MB4_JA_0900_AS_CS_KS(304, true, false, true),
    UTF8MB4_LA_0900_AI_CI(271, false, false, true),
    UTF8MB4_LA_0900_AS_CS(294, true, false, true),
    UTF8MB4_LT_0900_AI_CI(268, false, false, true),
    UTF8MB4_LT_0900_AS_CS(291, true, false, true),
    UTF8MB4_LV_0900_AI_CI(258, false, false, true),
    UTF8MB4_LV_0900_AS_CS(281, true, false, true),
    UTF8MB4_PL_0900_AI_CI(261, false, false, true),
    UTF8MB4_PL_0900_AS_CS(284, true, false, true),
    UTF8MB4_RO_0900_AI_CI(259, false, false, true),
    UTF8MB4_RO_0900_AS_CS(282, true, false, true),
    UTF8MB4_RU_0900_AI_CI(306, false, false, true),
    UTF8MB4_RU_0900_AS_CS(307, true, false, true),
    UTF8MB4_SK_0900_AI_CI(269, false, false, true),
    UTF8MB4_SK_0900_AS_CS(292, true, false, true),
    UTF8MB4_SL_0900_AI_CI(260, false, false, true),
    UTF8MB4_SL_0900_AS_CS(283, true, false, true),
    UTF8MB4_SV_0900_AI_CI(264, false, false, true),
    UTF8MB4_SV_0900_AS_CS(287, true, false, true),
    UTF8MB4_TR_0900_AI_CI(265, false, false, true),
    UTF8MB4_TR_0900_AS_CS(288, true, false, true),
    UTF8MB4_VI_0900_AI_CI(277, false, false, true),
    UTF8MB4_VI_0900_AS_CS(300, true, false, true),
    UTF8MB4_ZH_0900_AS_CS(308, true, false, true);

    /**
     * Collect all collation names to map so we can check them in O(1)
     */
    public static Map<String, CollationNameForParser> COLLATION_NAME_MAP = Arrays.stream(values())
        .collect(Collectors.toMap(Enum::name, Function.identity()));

    public static ImmutableList<CollationNameForParser> POLAR_DB_X_IMPLEMENTED_COLLATION_NAMES = ImmutableList.of(
        // for utf8
        UTF8_GENERAL_CI, UTF8_BIN, UTF8_UNICODE_CI, UTF8_GENERAL_MYSQL500_CI,

        // for utf8mb
        UTF8MB4_GENERAL_CI, UTF8MB4_BIN, UTF8MB4_UNICODE_CI,

        // for utf16
        UTF16_GENERAL_CI, UTF16_BIN, UTF16_UNICODE_CI,

        // for utf16le
        UTF16LE_GENERAL_CI, UTF16LE_BIN,

        // for utf32
        UTF32_GENERAL_CI, UTF32_BIN, UTF32_UNICODE_CI,

        // for ascii
        ASCII_GENERAL_CI, ASCII_BIN,

        // for binary
        BINARY,

        // for gbk
        GBK_CHINESE_CI, GBK_BIN,

        // for gb18030
        GB18030_CHINESE_CI, GB18030_BIN, GB18030_UNICODE_520_CI,

        // for latin1
        LATIN1_SWEDISH_CI, LATIN1_GERMAN1_CI, LATIN1_DANISH_CI, LATIN1_GERMAN2_CI, LATIN1_BIN, LATIN1_GENERAL_CI,
        LATIN1_GENERAL_CS, LATIN1_SPANISH_CI,

        // for traditional chinese
        BIG5_BIN, BIG5_CHINESE_CI,

        // MySQL 8.0
        UTF8MB4_UNICODE_520_CI, UTF8MB4_0900_AI_CI
    );

    static Set<String> POLAR_DB_X_IMPLEMENTED_COLLATION_NAME_STRINGS = new HashSet<>();

    /**
     * Mapping from upper case collation string to collation name.
     */
    static Map<String, CollationNameForParser> STRING_TO_COLLATION_MAP_UPPER =
        Arrays.stream(CollationNameForParser.values())
            .collect(Collectors.toMap(k -> k.name(), k -> k));

    /**
     * Mapping from lower case collation string to collation name.
     */
    static Map<String, CollationNameForParser> STRING_TO_COLLATION_MAP_LOWER =
        Arrays.stream(CollationNameForParser.values())
            .collect(Collectors.toMap(k -> k.name(), k -> k));

    static {
        // Initialize both upper case & lower case to check implementation in O(1).
        for (CollationNameForParser collationName : POLAR_DB_X_IMPLEMENTED_COLLATION_NAMES) {
            POLAR_DB_X_IMPLEMENTED_COLLATION_NAME_STRINGS.add(collationName.name().toUpperCase());
            POLAR_DB_X_IMPLEMENTED_COLLATION_NAME_STRINGS.add(collationName.name().toLowerCase());
        }
    }

    /**
     * MySQL standard collation id
     */
    private final int mysqlCollationId;

    /**
     * If the collation is case-sensitive or not.
     */
    private final boolean isCaseSensitive;

    /**
     * If the collation is new supported in mysql 8.0 or not.
     */
    private final boolean isMySQL80NewSupported;

    CollationNameForParser(int mysqlCollationId, boolean isCaseSensitive, boolean isDefaultCollation) {
        this(mysqlCollationId, isCaseSensitive, isDefaultCollation, false);
    }

    CollationNameForParser(int mysqlCollationId, boolean isCaseSensitive, boolean isDefaultCollation,
                           boolean isMySQL80NewSupported) {
        this.mysqlCollationId = mysqlCollationId;
        this.isCaseSensitive = isCaseSensitive;
        this.isMySQL80NewSupported = isMySQL80NewSupported;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }

    public boolean isMySQL80NewSupported() {
        return isMySQL80NewSupported;
    }

    public int getMysqlCollationId() {
        return mysqlCollationId;
    }

    /**
     * Find the collation name enum from collation string.
     *
     * @param collation collation string.
     * @return default collation if not found.
     */
    public static CollationNameForParser of(String collation) {
        return of(collation, true);
    }

    public static CollationNameForParser of(String collation, boolean useDefault) {
        if (StringUtils.isEmpty(collation)) {
            return null;
        }
        CollationNameForParser res;

        // Find in O(1), without case conversion.
        if ((res = STRING_TO_COLLATION_MAP_UPPER.get(collation)) != null) {
            return res;
        }

        // Find in O(1), without case conversion.
        if ((res = STRING_TO_COLLATION_MAP_LOWER.get(collation)) != null) {
            return res;
        }

        // Find in O(N)
        return Arrays.stream(values())
            .filter(c -> c.name().equalsIgnoreCase(collation))
            .findFirst()
            .orElse(useDefault ? defaultCollation() : null);
    }

    public static CharsetNameForParser getCharsetOf(String collation) {
        return getCharsetOf(collation, true);
    }

    public static CharsetNameForParser getCharsetOf(String collation, boolean useDefault) {
        if (StringUtils.isEmpty(collation)) {
            return null;
        }

        CollationNameForParser collationName = CollationNameForParser.of(collation);
        if (collationName == null) {
            return null;
        }
        // Find by enum.
        return getCharsetOf(collationName, useDefault);
    }

    public static CharsetNameForParser getCharsetOf(CollationNameForParser collationName) {
        return getCharsetOf(collationName, true);
    }

    public static CharsetNameForParser getCharsetOf(CollationNameForParser collationName, boolean useDefault) {
        if (collationName == null) {
            return null;
        }

        // Find in O(1)
        CharsetNameForParser res = CharsetNameForParser.CHARSET_NAMES_OF_COLLATION[collationName.mysqlCollationId];
        if (res != null) {
            return res;
        }

        // Find in O(N)
        return Arrays.stream(CharsetNameForParser.values())
            .filter(charsetName -> charsetName.getImplementedCollationNames().contains(collationName)
                || charsetName.getUnimplementedCollationNames().contains(collationName))
            .findFirst()
            .orElse(useDefault ? CharsetNameForParser.defaultCharset() : null);
    }

    public static boolean isBinary(CollationNameForParser collationName) {
        return Optional.ofNullable(collationName)
            .map(c -> c.name().endsWith("BIN"))
            .orElse(false);
    }

    public static CollationNameForParser defaultCollation() {
        return UTF8_GENERAL_CI;
    }

    public static CollationNameForParser defaultNumericCollation() {
        return LATIN1_SWEDISH_CI;
    }

    private static final ImmutableMap<MixCollationKey, CollationNameForParser> MIX_OF_COLLATION_MAP =
        ImmutableMap.<MixCollationKey, CollationNameForParser>builder()
            .put(new MixCollationKey(UTF8_GENERAL_CI, UTF8MB4_GENERAL_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, UTF8MB4_BIN), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8_GENERAL_CI, UTF8MB4_UNICODE_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF8_GENERAL_CI, ASCII_GENERAL_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, ASCII_BIN), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF8_GENERAL_CI, GBK_CHINESE_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, GBK_BIN), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, GB18030_CHINESE_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, GB18030_BIN), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, GB18030_UNICODE_520_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, LATIN1_SWEDISH_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, LATIN1_GERMAN1_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, LATIN1_DANISH_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, LATIN1_GERMAN2_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, LATIN1_BIN), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, LATIN1_GENERAL_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, LATIN1_GENERAL_CS), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, LATIN1_SPANISH_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, BIG5_BIN), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_GENERAL_CI, BIG5_CHINESE_CI), UTF8_GENERAL_CI)
            .put(new MixCollationKey(UTF8_BIN, UTF8MB4_GENERAL_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8_BIN, UTF8MB4_BIN), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8_BIN, UTF8MB4_UNICODE_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8_BIN, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF8_BIN, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF8_BIN, ASCII_GENERAL_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, ASCII_BIN), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, BINARY), BINARY)
            .put(new MixCollationKey(UTF8_BIN, GBK_CHINESE_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, GBK_BIN), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, GB18030_CHINESE_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, GB18030_BIN), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, GB18030_UNICODE_520_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, LATIN1_SWEDISH_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, LATIN1_GERMAN1_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, LATIN1_DANISH_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, LATIN1_GERMAN2_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, LATIN1_BIN), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, LATIN1_GENERAL_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, LATIN1_GENERAL_CS), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, LATIN1_SPANISH_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, BIG5_BIN), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, BIG5_CHINESE_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, UTF8_GENERAL_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, UTF8_UNICODE_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_BIN, UTF8_GENERAL_MYSQL500_CI), UTF8_BIN)
            .put(new MixCollationKey(UTF8_UNICODE_CI, UTF8MB4_GENERAL_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, UTF8MB4_BIN), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8_UNICODE_CI, UTF8MB4_UNICODE_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF8_UNICODE_CI, ASCII_GENERAL_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, ASCII_BIN), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF8_UNICODE_CI, GBK_CHINESE_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, GBK_BIN), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, GB18030_CHINESE_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, GB18030_BIN), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, GB18030_UNICODE_520_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, LATIN1_SWEDISH_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, LATIN1_GERMAN1_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, LATIN1_DANISH_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, LATIN1_GERMAN2_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, LATIN1_BIN), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, LATIN1_GENERAL_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, LATIN1_GENERAL_CS), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, LATIN1_SPANISH_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, BIG5_BIN), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8_UNICODE_CI, BIG5_CHINESE_CI), UTF8_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, ASCII_GENERAL_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, ASCII_BIN), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, GBK_CHINESE_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, GBK_BIN), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, GB18030_CHINESE_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, GB18030_BIN), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, GB18030_UNICODE_520_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, LATIN1_SWEDISH_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, LATIN1_GERMAN1_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, LATIN1_DANISH_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, LATIN1_GERMAN2_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, LATIN1_BIN), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, LATIN1_GENERAL_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, LATIN1_GENERAL_CS), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, LATIN1_SPANISH_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, BIG5_BIN), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_GENERAL_CI, BIG5_CHINESE_CI), UTF8MB4_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_BIN, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_BIN, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, ASCII_GENERAL_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, ASCII_BIN), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, BINARY), BINARY)
            .put(new MixCollationKey(UTF8MB4_BIN, GBK_CHINESE_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, GBK_BIN), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, GB18030_CHINESE_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, GB18030_BIN), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, GB18030_UNICODE_520_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, LATIN1_SWEDISH_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, LATIN1_GERMAN1_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, LATIN1_DANISH_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, LATIN1_GERMAN2_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, LATIN1_BIN), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, LATIN1_GENERAL_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, LATIN1_GENERAL_CS), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, LATIN1_SPANISH_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, BIG5_BIN), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, BIG5_CHINESE_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, UTF8MB4_GENERAL_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, UTF8MB4_UNICODE_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, UTF8MB4_UNICODE_520_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, UTF8MB4_0900_AI_CI), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_BIN, UTF8MB4_ZH_0900_AS_CS), UTF8MB4_BIN)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, ASCII_GENERAL_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, ASCII_BIN), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, GBK_CHINESE_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, GBK_BIN), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, GB18030_CHINESE_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, GB18030_BIN), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, GB18030_UNICODE_520_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, LATIN1_SWEDISH_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, LATIN1_GERMAN1_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, LATIN1_DANISH_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, LATIN1_GERMAN2_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, LATIN1_BIN), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, LATIN1_GENERAL_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, LATIN1_GENERAL_CS), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, LATIN1_SPANISH_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, BIG5_BIN), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, BIG5_CHINESE_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16_GENERAL_CI, ASCII_GENERAL_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, ASCII_BIN), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF16_GENERAL_CI, GBK_CHINESE_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, GBK_BIN), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, GB18030_CHINESE_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, GB18030_BIN), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, GB18030_UNICODE_520_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, LATIN1_SWEDISH_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, LATIN1_GERMAN1_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, LATIN1_DANISH_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, LATIN1_GERMAN2_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, LATIN1_BIN), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, LATIN1_GENERAL_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, LATIN1_GENERAL_CS), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, LATIN1_SPANISH_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, BIG5_BIN), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_GENERAL_CI, BIG5_CHINESE_CI), UTF16_GENERAL_CI)
            .put(new MixCollationKey(UTF16_BIN, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16_BIN, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16_BIN, ASCII_GENERAL_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, ASCII_BIN), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, BINARY), BINARY)
            .put(new MixCollationKey(UTF16_BIN, GBK_CHINESE_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, GBK_BIN), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, GB18030_CHINESE_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, GB18030_BIN), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, GB18030_UNICODE_520_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, LATIN1_SWEDISH_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, LATIN1_GERMAN1_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, LATIN1_DANISH_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, LATIN1_GERMAN2_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, LATIN1_BIN), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, LATIN1_GENERAL_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, LATIN1_GENERAL_CS), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, LATIN1_SPANISH_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, BIG5_BIN), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, BIG5_CHINESE_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, UTF16_GENERAL_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_BIN, UTF16_UNICODE_CI), UTF16_BIN)
            .put(new MixCollationKey(UTF16_UNICODE_CI, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16_UNICODE_CI, ASCII_GENERAL_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, ASCII_BIN), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF16_UNICODE_CI, GBK_CHINESE_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, GBK_BIN), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, GB18030_CHINESE_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, GB18030_BIN), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, GB18030_UNICODE_520_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, LATIN1_SWEDISH_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, LATIN1_GERMAN1_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, LATIN1_DANISH_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, LATIN1_GERMAN2_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, LATIN1_BIN), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, LATIN1_GENERAL_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, LATIN1_GENERAL_CS), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, LATIN1_SPANISH_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, BIG5_BIN), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, BIG5_CHINESE_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, UTF32_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, UTF32_BIN), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, UTF32_UNICODE_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, ASCII_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, ASCII_BIN), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, GBK_CHINESE_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, GBK_BIN), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, GB18030_CHINESE_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, GB18030_BIN), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, GB18030_UNICODE_520_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, LATIN1_SWEDISH_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, LATIN1_GERMAN1_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, LATIN1_DANISH_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, LATIN1_GERMAN2_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, LATIN1_BIN), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, LATIN1_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, LATIN1_GENERAL_CS), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, LATIN1_SPANISH_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, BIG5_BIN), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_GENERAL_CI, BIG5_CHINESE_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16LE_BIN, UTF32_GENERAL_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, UTF32_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, UTF32_UNICODE_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, ASCII_GENERAL_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, ASCII_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, BINARY), BINARY)
            .put(new MixCollationKey(UTF16LE_BIN, GBK_CHINESE_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, GBK_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, GB18030_CHINESE_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, GB18030_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, GB18030_UNICODE_520_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, LATIN1_SWEDISH_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, LATIN1_GERMAN1_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, LATIN1_DANISH_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, LATIN1_GERMAN2_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, LATIN1_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, LATIN1_GENERAL_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, LATIN1_GENERAL_CS), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, LATIN1_SPANISH_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, BIG5_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, BIG5_CHINESE_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16LE_BIN, UTF16LE_GENERAL_CI), UTF16LE_BIN)
            .put(new MixCollationKey(UTF32_GENERAL_CI, ASCII_GENERAL_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, ASCII_BIN), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF32_GENERAL_CI, GBK_CHINESE_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, GBK_BIN), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, GB18030_CHINESE_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, GB18030_BIN), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, GB18030_UNICODE_520_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, LATIN1_SWEDISH_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, LATIN1_GERMAN1_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, LATIN1_DANISH_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, LATIN1_GERMAN2_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, LATIN1_BIN), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, LATIN1_GENERAL_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, LATIN1_GENERAL_CS), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, LATIN1_SPANISH_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, BIG5_BIN), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, BIG5_CHINESE_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_BIN, ASCII_GENERAL_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, ASCII_BIN), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, BINARY), BINARY)
            .put(new MixCollationKey(UTF32_BIN, GBK_CHINESE_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, GBK_BIN), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, GB18030_CHINESE_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, GB18030_BIN), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, GB18030_UNICODE_520_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, LATIN1_SWEDISH_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, LATIN1_GERMAN1_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, LATIN1_DANISH_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, LATIN1_GERMAN2_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, LATIN1_BIN), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, LATIN1_GENERAL_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, LATIN1_GENERAL_CS), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, LATIN1_SPANISH_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, BIG5_BIN), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, BIG5_CHINESE_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, UTF32_GENERAL_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_BIN, UTF32_UNICODE_CI), UTF32_BIN)
            .put(new MixCollationKey(UTF32_UNICODE_CI, ASCII_GENERAL_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, ASCII_BIN), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF32_UNICODE_CI, GBK_CHINESE_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, GBK_BIN), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, GB18030_CHINESE_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, GB18030_BIN), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, GB18030_UNICODE_520_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, LATIN1_SWEDISH_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, LATIN1_GERMAN1_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, LATIN1_DANISH_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, LATIN1_GERMAN2_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, LATIN1_BIN), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, LATIN1_GENERAL_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, LATIN1_GENERAL_CS), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, LATIN1_SPANISH_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, BIG5_BIN), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, BIG5_CHINESE_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(ASCII_GENERAL_CI, BINARY), BINARY)
            .put(new MixCollationKey(ASCII_BIN, BINARY), BINARY)
            .put(new MixCollationKey(BINARY, GBK_CHINESE_CI), BINARY)
            .put(new MixCollationKey(BINARY, GBK_BIN), BINARY)
            .put(new MixCollationKey(BINARY, GB18030_CHINESE_CI), BINARY)
            .put(new MixCollationKey(BINARY, GB18030_BIN), BINARY)
            .put(new MixCollationKey(BINARY, GB18030_UNICODE_520_CI), BINARY)
            .put(new MixCollationKey(BINARY, LATIN1_SWEDISH_CI), BINARY)
            .put(new MixCollationKey(BINARY, LATIN1_GERMAN1_CI), BINARY)
            .put(new MixCollationKey(BINARY, LATIN1_DANISH_CI), BINARY)
            .put(new MixCollationKey(BINARY, LATIN1_GERMAN2_CI), BINARY)
            .put(new MixCollationKey(BINARY, LATIN1_BIN), BINARY)
            .put(new MixCollationKey(BINARY, LATIN1_GENERAL_CI), BINARY)
            .put(new MixCollationKey(BINARY, LATIN1_GENERAL_CS), BINARY)
            .put(new MixCollationKey(BINARY, LATIN1_SPANISH_CI), BINARY)
            .put(new MixCollationKey(BINARY, BIG5_BIN), BINARY)
            .put(new MixCollationKey(BINARY, BIG5_CHINESE_CI), BINARY)
            // If UTF8MB4_0900_AI_CI exists, it means the server is in MySQL 80 mode and unicode_ci and general_ci
            // should be compatible with it.
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF8MB4_GENERAL_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF8MB4_UNICODE_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF8_GENERAL_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF8_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF8_UNICODE_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF8MB4_0900_AI_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF16_GENERAL_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF16_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF16_UNICODE_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF16LE_GENERAL_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF16LE_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF32_GENERAL_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF32_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF32_UNICODE_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, ASCII_GENERAL_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, ASCII_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, GBK_CHINESE_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, GBK_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, GB18030_CHINESE_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, GB18030_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, GB18030_UNICODE_520_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, LATIN1_SWEDISH_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, LATIN1_GERMAN1_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, LATIN1_DANISH_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, LATIN1_GERMAN2_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, LATIN1_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, LATIN1_GENERAL_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, LATIN1_GENERAL_CS), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, LATIN1_SPANISH_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, BIG5_BIN), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, BIG5_CHINESE_CI), UTF8MB4_0900_AI_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF8_UNICODE_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF8MB4_UNICODE_520_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF16_GENERAL_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF16_BIN), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF16_UNICODE_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF16LE_GENERAL_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF16LE_BIN), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF32_GENERAL_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF32_BIN), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, UTF32_UNICODE_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, ASCII_GENERAL_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, ASCII_BIN), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, GBK_CHINESE_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, GBK_BIN), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, GB18030_CHINESE_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, GB18030_BIN), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, GB18030_UNICODE_520_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, LATIN1_SWEDISH_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, LATIN1_GERMAN1_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, LATIN1_DANISH_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, LATIN1_GERMAN2_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, LATIN1_BIN), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, LATIN1_GENERAL_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, LATIN1_GENERAL_CS), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, LATIN1_SPANISH_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, BIG5_BIN), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_520_CI, BIG5_CHINESE_CI), UTF8MB4_UNICODE_520_CI)
            .put(new MixCollationKey(LATIN1_BIN, LATIN1_SWEDISH_CI), LATIN1_BIN)
            .put(new MixCollationKey(LATIN1_BIN, LATIN1_GERMAN1_CI), LATIN1_BIN)
            .put(new MixCollationKey(LATIN1_BIN, LATIN1_DANISH_CI), LATIN1_BIN)
            .put(new MixCollationKey(LATIN1_BIN, LATIN1_GERMAN2_CI), LATIN1_BIN)
            .put(new MixCollationKey(LATIN1_BIN, LATIN1_GENERAL_CI), LATIN1_BIN)
            .put(new MixCollationKey(LATIN1_BIN, LATIN1_GENERAL_CS), LATIN1_BIN)
            .put(new MixCollationKey(LATIN1_BIN, LATIN1_SPANISH_CI), LATIN1_BIN)
            .put(new MixCollationKey(GB18030_BIN, GB18030_UNICODE_520_CI), GB18030_BIN)
            .put(new MixCollationKey(GB18030_BIN, GB18030_CHINESE_CI), GB18030_BIN)
            .put(new MixCollationKey(ASCII_BIN, ASCII_GENERAL_CI), ASCII_BIN)
            .put(new MixCollationKey(GBK_BIN, GBK_CHINESE_CI), GBK_BIN)
            .put(new MixCollationKey(BIG5_BIN, BIG5_CHINESE_CI), BIG5_BIN)
            .build();

    private static class MixCollationKey {
        final CollationNameForParser collation1;
        final CollationNameForParser collation2;

        private MixCollationKey(CollationNameForParser collation1, CollationNameForParser collation2) {
            this.collation1 = collation1;
            this.collation2 = collation2;
        }

        @Override
        public int hashCode() {
            return collation1.name().hashCode() ^ collation2.name().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof MixCollationKey)) {
                return false;
            }
            MixCollationKey that = (MixCollationKey) obj;
            return ((collation1 == that.collation1) && (collation2 == that.collation2))
                || ((collation1 == that.collation2) && (collation2 == that.collation1));
        }
    }

    public static CollationNameForParser getMixOfCollation0(CollationNameForParser collation1,
                                                            CollationNameForParser collation2) {
        Preconditions.checkNotNull(collation1);
        Preconditions.checkNotNull(collation2);
        if (collation1 == collation2) {
            return collation1;
        } else {
            return MIX_OF_COLLATION_MAP.get(new MixCollationKey(collation1, collation2));
        }
    }

    public static CollationNameForParser getMixOfCollation(CollationNameForParser... collationNames) {
        CollationNameForParser res = collationNames[0];
        for (int i = 1; i < collationNames.length; i++) {
            res = getMixOfCollation0(res, collationNames[i]);
            if (res == null) {
                break;
            }
        }
        return res;
    }

    public static CollationNameForParser findCollationName(String collation) {
        if (StringUtils.isEmpty(collation)) {
            return null;
        }
        return Arrays.stream(values())
            .filter(c -> c.name().equalsIgnoreCase(collation))
            .findFirst().orElse(null);
    }
}

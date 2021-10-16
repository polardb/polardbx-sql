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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.Arrays;
import java.util.Optional;

public enum CollationName {

    UTF8_GENERAL_CI(false),

    UTF8_BIN(true),

    UTF8_UNICODE_CI(false),

    UTF8MB4_GENERAL_CI(false),

    UTF8MB4_BIN(true),

    UTF8MB4_UNICODE_CI(false),

    UTF16_GENERAL_CI(false),

    UTF16_BIN(true),

    UTF16_UNICODE_CI(false),

    UTF16LE_GENERAL_CI(false),

    UTF16LE_BIN(true),

    UTF32_GENERAL_CI(false),

    UTF32_BIN(true),

    UTF32_UNICODE_CI(false),

    ASCII_GENERAL_CI(false),

    ASCII_BIN(true),

    BINARY(true),

    GBK_CHINESE_CI(false),

    GBK_BIN(true),

    LATIN1_SWEDISH_CI(false),

    LATIN1_GERMAN1_CI(false),

    LATIN1_DANISH_CI(false),

    LATIN1_GERMAN2_CI(false),

    LATIN1_BIN(true),

    LATIN1_GENERAL_CI(false),

    LATIN1_GENERAL_CS(true),

    LATIN1_SPANISH_CI(false),

    BIG5_BIN(true),

    BIG5_CHINESE_CI(false),

    UTF8MB4_UNICODE_520_CI(true),

    UTF8MB4_0900_AI_CI(true);

    private final boolean isCaseSensitive;

    CollationName(boolean isCaseSensitive) {
        this.isCaseSensitive = isCaseSensitive;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }

    public static CollationName of(String collation) {
        if (TStringUtil.isEmpty(collation)) {
            return null;
        }
        return Arrays.stream(values())
            .filter(c -> c.name().equalsIgnoreCase(collation))
            .findFirst()
            .orElse(defaultCollation());
    }

    public static CharsetName getCharsetOf(String collation) {
        CollationName collationName = CollationName.of(collation);
        return Arrays.stream(CharsetName.values())
            .filter(charsetName -> charsetName.getSupportedCollationName().contains(collationName))
            .findFirst()
            .orElse(CharsetName.defaultCharset());
    }

    public static CharsetName getCharsetOf(CollationName collationName) {
        return Arrays.stream(CharsetName.values())
            .filter(charsetName -> charsetName.getSupportedCollationName().contains(collationName))
            .findFirst()
            .orElse(CharsetName.defaultCharset());
    }

    public static boolean isBinary(CollationName collationName) {
        return Optional.ofNullable(collationName)
            .map(c -> c.name().endsWith("BIN"))
            .orElse(false);
    }

    public static CollationName defaultCollation() {
        return UTF8_GENERAL_CI;
    }

    public static CollationName defaultNumericCollation() {
        return LATIN1_SWEDISH_CI;
    }

    private static final ImmutableMap<MixCollationKey, CollationName> MIX_OF_COLLATION_MAP =
        ImmutableMap.<MixCollationKey, CollationName>builder()
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
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, ASCII_GENERAL_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, ASCII_BIN), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, GBK_CHINESE_CI), UTF8MB4_UNICODE_CI)
            .put(new MixCollationKey(UTF8MB4_UNICODE_CI, GBK_BIN), UTF8MB4_UNICODE_CI)
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
            .put(new MixCollationKey(UTF16_UNICODE_CI, UTF16LE_GENERAL_CI), UTF16LE_GENERAL_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, UTF16LE_BIN), UTF16LE_BIN)
            .put(new MixCollationKey(UTF16_UNICODE_CI, ASCII_GENERAL_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, ASCII_BIN), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF16_UNICODE_CI, GBK_CHINESE_CI), UTF16_UNICODE_CI)
            .put(new MixCollationKey(UTF16_UNICODE_CI, GBK_BIN), UTF16_UNICODE_CI)
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
            .put(new MixCollationKey(UTF32_GENERAL_CI, ASCII_GENERAL_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, ASCII_BIN), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF32_GENERAL_CI, GBK_CHINESE_CI), UTF32_GENERAL_CI)
            .put(new MixCollationKey(UTF32_GENERAL_CI, GBK_BIN), UTF32_GENERAL_CI)
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
            .put(new MixCollationKey(UTF32_UNICODE_CI, ASCII_GENERAL_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, ASCII_BIN), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, BINARY), BINARY)
            .put(new MixCollationKey(UTF32_UNICODE_CI, GBK_CHINESE_CI), UTF32_UNICODE_CI)
            .put(new MixCollationKey(UTF32_UNICODE_CI, GBK_BIN), UTF32_UNICODE_CI)
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
            .put(new MixCollationKey(UTF8MB4_0900_AI_CI, UTF8_GENERAL_CI), UTF8_GENERAL_CI)
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
            .build();

    private static class MixCollationKey {
        final CollationName collation1;
        final CollationName collation2;

        private MixCollationKey(CollationName collation1, CollationName collation2) {
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

    public static CollationName getMixOfCollation0(CollationName collation1, CollationName collation2) {
        Preconditions.checkNotNull(collation1);
        Preconditions.checkNotNull(collation2);
        if (collation1 == collation2) {
            return collation1;
        } else {
            return MIX_OF_COLLATION_MAP.get(new MixCollationKey(collation1, collation2));
        }
    }

    public static CollationName getMixOfCollation(CollationName... collationNames) {
        CollationName res = collationNames[0];
        for (int i = 1; i < collationNames.length; i++) {
            res = getMixOfCollation0(res, collationNames[i]);
            if (res == null) {
                break;
            }
        }
        return res;
    }
}

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.alibaba.polardbx.common.charset.CollationName.*;

public enum CharsetName {

    UTF8(UTF8_GENERAL_CI, ImmutableList.of(UTF8_GENERAL_CI, UTF8_BIN, UTF8_UNICODE_CI), "UTF8", "UTF-8"),

    UTF8MB4(UTF8MB4_GENERAL_CI, ImmutableList
        .of(UTF8MB4_GENERAL_CI, UTF8MB4_BIN, UTF8MB4_UNICODE_CI, UTF8MB4_0900_AI_CI, UTF8MB4_UNICODE_520_CI), "UTF8MB4",
        "UTF-8"),

    UTF16(UTF16_GENERAL_CI, ImmutableList.of(UTF16_GENERAL_CI, UTF16_BIN, UTF16_UNICODE_CI), "UTF-16", "UTF-16"),

    UTF16LE(UTF16LE_GENERAL_CI, ImmutableList.of(UTF16LE_GENERAL_CI, UTF16LE_BIN), "UTF-16LE", "UTF-16LE"),

    UTF32(UTF32_GENERAL_CI, ImmutableList.of(UTF32_GENERAL_CI, UTF32_BIN, UTF32_UNICODE_CI), "UTF-32", "UTF-32"),

    ASCII(ASCII_GENERAL_CI, ImmutableList.of(ASCII_GENERAL_CI, ASCII_BIN), "ASCII", "ASCII"),

    BINARY(CollationName.BINARY, ImmutableList.of(CollationName.BINARY), "BINARY", "ISO-8859-1"),

    GBK(GBK_CHINESE_CI, ImmutableList.of(GBK_CHINESE_CI, GBK_BIN), "GBK", "GBK"),

    BIG5(BIG5_CHINESE_CI, ImmutableList.of(BIG5_CHINESE_CI, BIG5_BIN), "BIG5", "BIG5"),

    LATIN1(LATIN1_SWEDISH_CI,
        ImmutableList.of(LATIN1_SWEDISH_CI, LATIN1_GENERAL_CI, LATIN1_GENERAL_CS, LATIN1_BIN, LATIN1_GERMAN1_CI,
            LATIN1_GERMAN2_CI, LATIN1_DANISH_CI, LATIN1_SPANISH_CI), "LATIN1", "LATIN1");

    public static String DEFAULT_CHARACTER_SET = defaultCharset().name();
    public static String DEFAULT_COLLATION = defaultCollation().name();
    public static final Charset DEFAULT_STORAGE_CHARSET_IN_CHUNK = Charset.forName("UTF-8");
    public static final String POLAR_DB_X_STANDARD_UTF8_CHARSET_NAME = "UTF-8";

    public static final Map<String, CharsetName> CHARSET_NAME_MATCHES = ImmutableMap.<String, CharsetName>builder()

        .put("utf-8", UTF8)
        .put("UTF-8", UTF8)
        .put("utf8", UTF8)
        .put("UTF8", UTF8)

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

        .put("BIG5", BIG5)
        .put("big5", BIG5)
        .build();

    private CollationName defaultCollationName;
    private List<CollationName> supportedCollationName;
    private String javaCharset;
    private String originalCharset;

    private Charset javaCharsetImpl;

    CharsetName(CollationName defaultCollationName,
                List<CollationName> supportedCollationNames,
                String javaCharset,
                String originalCharset) {
        this.defaultCollationName = defaultCollationName;
        this.supportedCollationName = supportedCollationNames;
        this.javaCharset = javaCharset;
        this.originalCharset = originalCharset;
    }

    public boolean isSupported(CollationName collationName) {
        return Optional.ofNullable(collationName)
            .map(supportedCollationName::contains)
            .orElse(false);
    }

    public boolean isSupported(String collation) {
        return Optional.ofNullable(collation)
            .map(String::toUpperCase)
            .map(CollationName::of)
            .map(supportedCollationName::contains)
            .orElse(false);
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

    public List<CollationName> getSupportedCollationName() {
        return supportedCollationName;
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

        return defaultCharset();
    }

    public static CharsetName of(Charset charset) {
        String name = charset.name().toUpperCase();

        for (CharsetName charsetName : values()) {
            if (charsetName.toJavaCharset() == charset) {
                return charsetName;
            }
        }

        Set<String> aliases = charset.aliases();
        return Arrays.stream(values())
            .filter(c -> c.name().equals(name) || aliases.stream().anyMatch(c.name()::equalsIgnoreCase))
            .findFirst()
            .orElseGet(
                () -> CharsetName.of(charset.name())
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

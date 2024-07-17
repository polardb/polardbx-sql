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

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * charset string - mysql charset name - java charset implementation
 */
public class CharsetTest {
    @Test
    public void test() {
        final String[] gbk = {"gbk", "_gbk"};
        final String[] big5 = {"big5", "_big5"};
        final String[] utf8 = {"utf8", "utf-8", "_UTF8", "_utf8"};
        final String[] utf8mb4 = {"utf8mb4", "_utf8mb4", "utf-8mb4", "UTF8MB4"};
        final String[] utf16 = {"UTF-16", "utf16", "_utf16"};
        final String[] utf16le = {"UTF-16LE", "utf16le", "_utf16le"};
        final String[] utf32 = {"UTF-32", "utf32", "_utf32"};
        final String[] latin1 = {"latin1", "_latin1", "LATIN1"};
        final String[] binary = {"BINARY", "_binary", "binary"};
        final String[] ascii = {"ascii", "ASCII", "_ascii"};

        Arrays.stream(gbk)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("gbk")));
        Arrays.stream(big5)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("big5")));
        Arrays.stream(utf8)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("utf-8")));
        Arrays.stream(utf8mb4)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("utf8mb4")));
        Arrays.stream(utf16)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("utf16")));
        Arrays.stream(utf16le)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("utf-16le")));
        Arrays.stream(utf32)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("utf32")));
        Arrays.stream(latin1)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("latin1")));
        Arrays.stream(binary)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("binary")));
        Arrays.stream(ascii)
            .forEach(str -> Assert.assertEquals(CharsetName.convertStrToJavaCharset(str), Charset.forName("ascii")));

        Assert.assertEquals(CharsetName.of(Charset.forName("gbk")), CharsetName.GBK);
        Assert.assertEquals(CharsetName.of(Charset.forName("big5")), CharsetName.BIG5);
        Assert.assertEquals(CharsetName.of(Charset.forName("utf-8")), CharsetName.UTF8);
        Assert.assertEquals(CharsetName.of(Charset.forName("utf8mb4")), CharsetName.UTF8MB4);
        Assert.assertEquals(CharsetName.of(Charset.forName("utf16")), CharsetName.UTF16);
        Assert.assertEquals(CharsetName.of(Charset.forName("utf-16le")), CharsetName.UTF16LE);
        Assert.assertEquals(CharsetName.of(Charset.forName("utf32")), CharsetName.UTF32);
        Assert.assertEquals(CharsetName.of(Charset.forName("latin1")), CharsetName.LATIN1);
        Assert.assertEquals(CharsetName.of(Charset.forName("BINARY")), CharsetName.BINARY);
        Assert.assertEquals(CharsetName.of(Charset.forName("ASCII")), CharsetName.ASCII);
    }

    @Test
    public void testMixedCollation() {
        Assert.assertSame(CollationName.getMixOfCollation0(CollationName.UTF8MB4_BIN, CollationName.UTF8MB4_GENERAL_CI),
            CollationName.UTF8MB4_BIN);
        Assert.assertSame(CollationName.getMixOfCollation0(CollationName.UTF8MB4_GENERAL_CI, CollationName.UTF8MB4_BIN),
            CollationName.UTF8MB4_BIN);

        Assert.assertSame(
            CollationName.getMixOfCollation0(CollationName.UTF8MB4_0900_AI_CI, CollationName.UTF8MB4_GENERAL_CI),
            CollationName.UTF8MB4_0900_AI_CI);
        Assert.assertSame(
            CollationName.getMixOfCollation0(CollationName.UTF8MB4_0900_AI_CI, CollationName.UTF8MB4_UNICODE_CI),
            CollationName.UTF8MB4_0900_AI_CI);
        Assert.assertSame(CollationName.getMixOfCollation0(CollationName.UTF8MB4_0900_AI_CI, CollationName.UTF8MB4_BIN),
            CollationName.UTF8MB4_BIN);
    }
}

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

import com.alibaba.polardbx.common.collation.CollationHandler;

public class CollationHandlers {
    public final static CollationHandler COLLATION_HANDLER_UTF8_GENERAL_CI =
        new Utf8CharsetHandler(CollationName.UTF8_GENERAL_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF8_UNICODE_CI =
        new Utf8CharsetHandler(CollationName.UTF8_UNICODE_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF8_BIN =
        new Utf8CharsetHandler(CollationName.UTF8_BIN).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF8MB4_GENERAL_CI =
        new Utf8mb4CharsetHandler(CollationName.UTF8MB4_GENERAL_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF8MB4_UNICODE_CI =
        new Utf8mb4CharsetHandler(CollationName.UTF8MB4_UNICODE_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF8MB4_0900_AI_CI =
        new Utf8mb4CharsetHandler(CollationName.UTF8MB4_0900_AI_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF8MB4_UNICODE_520_CI =
        new Utf8mb4CharsetHandler(CollationName.UTF8MB4_UNICODE_520_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF8MB4_BIN =
        new Utf8mb4CharsetHandler(CollationName.UTF8MB4_BIN).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF16_GENERAL_CI =
        new Utf16CharsetHandler(CollationName.UTF16_GENERAL_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF16_UNICODE_CI =
        new Utf16CharsetHandler(CollationName.UTF16_UNICODE_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF16_BIN =
        new Utf16CharsetHandler(CollationName.UTF16_BIN).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF16LE_GENERAL_CI =
        new Utf16leCharsetHandler(CollationName.UTF16LE_GENERAL_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF16LE_BIN =
        new Utf16leCharsetHandler(CollationName.UTF16LE_BIN).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF32_GENERAL_CI =
        new Utf32CharsetHandler(CollationName.UTF32_GENERAL_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF32_UNICODE_CI =
        new Utf32CharsetHandler(CollationName.UTF32_UNICODE_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_UTF32_BIN =
        new Utf32CharsetHandler(CollationName.UTF32_BIN).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_LATIN1_SWEDISH_CI =
        new Latin1CharsetHandler(CollationName.LATIN1_SWEDISH_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_LATIN1_DANISH_CI =
        new Latin1CharsetHandler(CollationName.LATIN1_DANISH_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_LATIN1_GENERAL_CI =
        new Latin1CharsetHandler(CollationName.LATIN1_GENERAL_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_LATIN1_GENERAL_CS =
        new Latin1CharsetHandler(CollationName.LATIN1_GENERAL_CS).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_LATIN1_GERMAN1_CI =
        new Latin1CharsetHandler(CollationName.LATIN1_GERMAN1_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_LATIN1_SPANISH_CI =
        new Latin1CharsetHandler(CollationName.LATIN1_SPANISH_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_LATIN1_BIN =
        new Latin1CharsetHandler(CollationName.LATIN1_BIN).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_LATIN1_GERMAN2_CI =
        new Latin1CharsetHandler(CollationName.LATIN1_GERMAN2_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_BIG5_CHINESE_CI =
        new Big5CharsetHandler(CollationName.BIG5_CHINESE_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_BIG5_BIN =
        new Big5CharsetHandler(CollationName.BIG5_BIN).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_ASCII_GENERAL_CI =
        new AsciiCharsetHandler(CollationName.ASCII_GENERAL_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_ASCII_BIN =
        new AsciiCharsetHandler(CollationName.ASCII_BIN).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_BINARY =
        new BinaryCharsetHandler(CollationName.BINARY).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_GBK_CHINESE_CI =
        new GbkCharsetHandler(CollationName.GBK_CHINESE_CI).getCollationHandler();
    public final static CollationHandler COLLATION_HANDLER_GBK_BIN =
        new GbkCharsetHandler(CollationName.GBK_BIN).getCollationHandler();
}

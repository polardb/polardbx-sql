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

package com.alibaba.polardbx.optimizer.config.table.charset;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.google.common.base.Preconditions;

public class CharsetFactoryImpl implements CharsetFactory {

    @Override
    public CharsetHandler createCharsetHandler() {
        return DEFAULT_CHARSET_HANDLER;
    }

    @Override
    public CharsetHandler createCharsetHandler(CharsetName charset) {
        return createCharsetHandler(charset, charset.getDefaultCollationName());
    }

    @Override
    public CharsetHandler createCharsetHandler(CharsetName charset, CollationName collation) {
        Preconditions.checkNotNull(charset);
        Preconditions.checkNotNull(collation);
        Preconditions.checkArgument(charset.match(collation),
            "collation " + collation + " is not supported in charset " + charset);
        switch (charset) {
        case UTF8MB4:
            switch (collation) {
            case UTF8MB4_GENERAL_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF8MB4_GENERAL_CI.getCharsetHandler();
            case UTF8MB4_UNICODE_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF8MB4_UNICODE_CI.getCharsetHandler();
            case UTF8MB4_BIN:
                return CollationHandlers.COLLATION_HANDLER_UTF8MB4_BIN.getCharsetHandler();
            case UTF8MB4_0900_AI_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF8MB4_0900_AI_CI.getCharsetHandler();
            case UTF8MB4_UNICODE_520_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF8MB4_UNICODE_520_CI.getCharsetHandler();
            }
        case UTF8:
            switch (collation) {
            case UTF8_GENERAL_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF8_GENERAL_CI.getCharsetHandler();
            case UTF8_UNICODE_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF8_UNICODE_CI.getCharsetHandler();
            case UTF8_BIN:
                return CollationHandlers.COLLATION_HANDLER_UTF8_BIN.getCharsetHandler();
            case UTF8_GENERAL_MYSQL500_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF8_GENERAL_MYSQL500_CI.getCharsetHandler();
            }
        case BINARY:
            switch (collation) {
            case BINARY:
                return CollationHandlers.COLLATION_HANDLER_BINARY.getCharsetHandler();
            }
        case UTF16:
            switch (collation) {
            case UTF16_GENERAL_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF16_GENERAL_CI.getCharsetHandler();
            case UTF16_UNICODE_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF16_UNICODE_CI.getCharsetHandler();
            case UTF16_BIN:
                return CollationHandlers.COLLATION_HANDLER_UTF16_BIN.getCharsetHandler();
            }
        case UTF16LE:
            switch (collation) {
            case UTF16LE_GENERAL_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF16LE_GENERAL_CI.getCharsetHandler();
            case UTF16LE_BIN:
                return CollationHandlers.COLLATION_HANDLER_UTF16LE_BIN.getCharsetHandler();
            }
        case UTF32:
            switch (collation) {
            case UTF32_GENERAL_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF32_GENERAL_CI.getCharsetHandler();
            case UTF32_UNICODE_CI:
                return CollationHandlers.COLLATION_HANDLER_UTF32_UNICODE_CI.getCharsetHandler();
            case UTF32_BIN:
                return CollationHandlers.COLLATION_HANDLER_UTF32_BIN.getCharsetHandler();
            }
        case LATIN1:
            switch (collation) {
            case LATIN1_SWEDISH_CI:
                return CollationHandlers.COLLATION_HANDLER_LATIN1_SWEDISH_CI.getCharsetHandler();
            case LATIN1_DANISH_CI:
                return CollationHandlers.COLLATION_HANDLER_LATIN1_DANISH_CI.getCharsetHandler();
            case LATIN1_GENERAL_CI:
                return CollationHandlers.COLLATION_HANDLER_LATIN1_GENERAL_CI.getCharsetHandler();
            case LATIN1_GENERAL_CS:
                return CollationHandlers.COLLATION_HANDLER_LATIN1_GENERAL_CS.getCharsetHandler();
            case LATIN1_GERMAN1_CI:
                return CollationHandlers.COLLATION_HANDLER_LATIN1_GERMAN1_CI.getCharsetHandler();
            case LATIN1_SPANISH_CI:
                return CollationHandlers.COLLATION_HANDLER_LATIN1_SPANISH_CI.getCharsetHandler();
            case LATIN1_BIN:
                return CollationHandlers.COLLATION_HANDLER_LATIN1_BIN.getCharsetHandler();
            case LATIN1_GERMAN2_CI:
                return CollationHandlers.COLLATION_HANDLER_LATIN1_GERMAN2_CI.getCharsetHandler();
            }
        case BIG5:
            switch (collation) {
            case BIG5_CHINESE_CI:
                return CollationHandlers.COLLATION_HANDLER_BIG5_CHINESE_CI.getCharsetHandler();
            case BIG5_BIN:
                return CollationHandlers.COLLATION_HANDLER_BIG5_BIN.getCharsetHandler();
            }
        case GBK:
            switch (collation) {
            case GBK_CHINESE_CI:
                return CollationHandlers.COLLATION_HANDLER_GBK_CHINESE_CI.getCharsetHandler();
            case GBK_BIN:
                return CollationHandlers.COLLATION_HANDLER_GBK_BIN.getCharsetHandler();
            }
        case ASCII:
            switch (collation) {
            case ASCII_GENERAL_CI:
                return CollationHandlers.COLLATION_HANDLER_ASCII_GENERAL_CI.getCharsetHandler();
            case ASCII_BIN:
                return CollationHandlers.COLLATION_HANDLER_ASCII_BIN.getCharsetHandler();
            }
        case GB18030:
            switch (collation) {
            case GB18030_CHINESE_CI:
                return CollationHandlers.COLLATION_HANDLER_GB18030_CHINESE_CI.getCharsetHandler();
            case GB18030_BIN:
                return CollationHandlers.COLLATION_HANDLER_GB18030_BIN.getCharsetHandler();
            case GB18030_UNICODE_520_CI:
                return CollationHandlers.COLLATION_HANDLER_GB18030_UNICODE_520_CI.getCharsetHandler();
            }
        default:
            return DEFAULT_CHARSET_HANDLER;
        }
    }

    @Override
    public CollationHandler createCollationHandler(CollationName collation) {
        Preconditions.checkNotNull(collation);

        switch (collation) {
        case UTF8MB4_GENERAL_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF8MB4_GENERAL_CI;
        case UTF8MB4_UNICODE_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF8MB4_UNICODE_CI;
        case UTF8MB4_BIN:
            return CollationHandlers.COLLATION_HANDLER_UTF8MB4_BIN;
        case UTF8MB4_0900_AI_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF8MB4_0900_AI_CI;
        case UTF8MB4_UNICODE_520_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF8MB4_UNICODE_520_CI;
        case UTF8_GENERAL_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF8_GENERAL_CI;
        case UTF8_UNICODE_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF8_UNICODE_CI;
        case UTF8_BIN:
            return CollationHandlers.COLLATION_HANDLER_UTF8_BIN;
        case UTF8_GENERAL_MYSQL500_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF8_GENERAL_MYSQL500_CI;
        case BINARY:
            return CollationHandlers.COLLATION_HANDLER_BINARY;
        case UTF16_GENERAL_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF16_GENERAL_CI;
        case UTF16_UNICODE_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF16_UNICODE_CI;
        case UTF16_BIN:
            return CollationHandlers.COLLATION_HANDLER_UTF16_BIN;
        case UTF16LE_GENERAL_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF16LE_GENERAL_CI;
        case UTF16LE_BIN:
            return CollationHandlers.COLLATION_HANDLER_UTF16LE_BIN;
        case UTF32_GENERAL_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF32_GENERAL_CI;
        case UTF32_UNICODE_CI:
            return CollationHandlers.COLLATION_HANDLER_UTF32_UNICODE_CI;
        case UTF32_BIN:
            return CollationHandlers.COLLATION_HANDLER_UTF32_BIN;
        case LATIN1_SWEDISH_CI:
            return CollationHandlers.COLLATION_HANDLER_LATIN1_SWEDISH_CI;
        case LATIN1_DANISH_CI:
            return CollationHandlers.COLLATION_HANDLER_LATIN1_DANISH_CI;
        case LATIN1_GENERAL_CI:
            return CollationHandlers.COLLATION_HANDLER_LATIN1_GENERAL_CI;
        case LATIN1_GENERAL_CS:
            return CollationHandlers.COLLATION_HANDLER_LATIN1_GENERAL_CS;
        case LATIN1_GERMAN1_CI:
            return CollationHandlers.COLLATION_HANDLER_LATIN1_GERMAN1_CI;
        case LATIN1_SPANISH_CI:
            return CollationHandlers.COLLATION_HANDLER_LATIN1_SPANISH_CI;
        case LATIN1_BIN:
            return CollationHandlers.COLLATION_HANDLER_LATIN1_BIN;
        case LATIN1_GERMAN2_CI:
            return CollationHandlers.COLLATION_HANDLER_LATIN1_GERMAN2_CI;
        case BIG5_CHINESE_CI:
            return CollationHandlers.COLLATION_HANDLER_BIG5_CHINESE_CI;
        case BIG5_BIN:
            return CollationHandlers.COLLATION_HANDLER_BIG5_BIN;
        case GBK_CHINESE_CI:
            return CollationHandlers.COLLATION_HANDLER_GBK_CHINESE_CI;
        case GBK_BIN:
            return CollationHandlers.COLLATION_HANDLER_GBK_BIN;
        case GB18030_CHINESE_CI:
            return CollationHandlers.COLLATION_HANDLER_GB18030_CHINESE_CI;
        case GB18030_BIN:
            return CollationHandlers.COLLATION_HANDLER_GB18030_BIN;
        case GB18030_UNICODE_520_CI:
            return CollationHandlers.COLLATION_HANDLER_GB18030_UNICODE_520_CI;
        case ASCII_GENERAL_CI:
            return CollationHandlers.COLLATION_HANDLER_ASCII_GENERAL_CI;
        case ASCII_BIN:
            return CollationHandlers.COLLATION_HANDLER_ASCII_BIN;
        default:
            return CollationHandlers.COLLATION_HANDLER_UTF8MB4_GENERAL_CI;
        }
    }

}

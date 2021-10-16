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

package com.alibaba.polardbx.common.utils.time.parser;

public class MySQLTimeParserBase {
    protected static final long MAX_UNSIGNED_INTEGER_VALUE = (long) Integer.MAX_VALUE - (long) Integer.MIN_VALUE;

    public static final boolean TIME_NO_ZERO_IN_DATE = false;

    public static final boolean TIME_NO_ZERO_DATE = false;

    public static final boolean TIME_FUZZY_DATE = true;

    public static final boolean TIME_INVALID_DATES = false;

    public static final boolean TIME_DATETIME_ONLY = false;

    public static final boolean TIME_NO_NANO_ROUNDING = false;

    public static long[] LOG_10 =
        {
            1, 10, 100, 1000, 10000, 100000, 1000000, 10000000,
            100000000L, 1000000000L, 10000000000L, 100000000000L,
            1000000000000L, 10000000000000L, 100000000000000L,
            1000000000000000L, 10000000000000000L, 100000000000000000L,
            1000000000000000000L
        };

    protected static boolean isSpace(byte b) {
        return b == ' ';
    }

    protected static boolean isDigit(byte b) {
        return b >= '0' && b <= '9';
    }

    protected static boolean isPunctuation(byte b) {
        return b == '.' || b == ',' || b == '-' || b == ':';
    }

    protected static boolean isAlpha(byte b) {
        return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z');
    }

    protected static int toAlphaUpper(byte b) {
        int diff = (b >= 'a' && b <= 'z') ? 32 : 0;
        return Byte.toUnsignedInt(b) - diff;
    }
}

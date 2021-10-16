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

public class TimeParserFlags {

    public static final int FLAG_TIME_FUZZY_DATE = 1;

    public static final int FLAG_TIME_DATETIME_ONLY = 2;

    public static final int FLAG_TIME_NO_NANO_ROUNDING = 4;
    public static final int FLAG_TIME_NO_DATE_FRAC_WARN = 8;

    public static final int FLAG_TIME_NO_ZERO_IN_DATE = 16;

    public static final int FLAG_TIME_NO_ZERO_DATE = 32;

    public static final int FLAG_TIME_INVALID_DATES = 64;

    public static final int FLAG_WEEK_MONDAY_FIRST = 1;
    public static final int FLAG_WEEK_YEAR = 2;
    public static final int FLAG_WEEK_FIRST_WEEKDAY = 4;

    public static final int FLAG_TIME_WARN_TRUNCATED = 1;
    public static final int FLAG_TIME_WARN_OUT_OF_RANGE = 2;
    public static final int FLAG_TIME_WARN_INVALID_TIMESTAMP = 4;
    public static final int FLAG_TIME_WARN_ZERO_DATE = 8;
    public static final int FLAG_TIME_NOTE_TRUNCATED = 16;
    public static final int FLAG_TIME_WARN_ZERO_IN_DATE = 32;

    public static boolean check(int flags, int toCheck) {
        return (flags & toCheck) != 0;
    }
}

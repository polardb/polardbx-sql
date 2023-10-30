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

package com.alibaba.polardbx.executor.archive.reader;

public enum TypeComparison {
    /**
     * new column, equal type with the latest type
     */
    MISSING_EQUAL,

    /**
     * new column, not equal type with the latest type
     */
    MISSING_NO_EQUAL,

    /**
     * Absolutely equal.
     */
    IS_EQUAL_YES,

    /**
     * Absolutely not equal.
     */
    IS_EQUAL_NO,

    /**
     * The same datatype with longer variable length.
     */
    IS_EQUAL_PACK_LENGTH,

    /**
     * unexpected state
     */
    INVALID;

    public static boolean isMissing(TypeComparison type) {
        return (type == MISSING_EQUAL || type == MISSING_NO_EQUAL);
    }
}

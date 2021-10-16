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

package com.alibaba.polardbx.matrix.jdbc.utils;

import com.alibaba.polardbx.druid.sql.parser.ByteString;

public class ByteStringUtil {

    public static ByteString substringBefore(ByteString str, String separator) {
        if (!isEmpty(str) && separator != null) {
            if (separator.length() == 0) {
                return ByteString.EMPTY;
            } else {
                int pos = str.indexOf(separator);
                return pos == -1 ? str : str.slice(0, pos);
            }
        } else {
            return str;
        }
    }

    public static ByteString substringAfter(ByteString str, String separator) {
        if (isEmpty(str)) {
            return str;
        } else if (separator == null) {
            return ByteString.EMPTY;
        } else {
            int pos = str.indexOf(separator);
            return pos == -1 ? ByteString.EMPTY : str.slice(pos + separator.length());
        }
    }

    public static boolean isEmpty(ByteString str) {
        return str == null || str.length() == 0;
    }

    public static boolean isNotEmpty(ByteString str) {
        return !isEmpty(str);
    }
}

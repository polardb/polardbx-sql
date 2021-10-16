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

package com.alibaba.polardbx.druid.sql.repository.function;

import java.lang.reflect.Type;

public class SignatureUtils {
    public static Type getJavaType(String signature) {
        if (signature == null || signature.length() == 0) {
            return null;
        }

        char c0 = signature.charAt(0);
        if (signature.length() == 1) {
            switch (c0) {
                case 'b':
                    return byte.class;
                case 's':
                    return short.class;
                case 'i':
                    return int.class;
                case 'j':
                    return long.class;
                case 'f':
                    return float.class;
                case 'd':
                    return double.class;
                case 'c':
                    return char.class;

                case 'g':
                    return String.class; // not null
                case 'a':
                    return java.sql.Date.class; // not null
                case 't':
                    return java.sql.Time.class; // not null
                case 'p':
                    return java.sql.Timestamp.class; // not null

                case 'B':
                    return Byte.class;
                case 'S':
                    return Short.class;
                case 'I':
                    return Integer.class;
                case 'J':
                    return Long.class;
                case 'F':
                    return Float.class;
                case 'D':
                    return Double.class;
                case 'C':
                    return Character.class;

                case 'G':
                    return String.class;
                case 'A':
                    return java.sql.Date.class;
                case 'T':
                    return java.sql.Time.class;
                case 'P':
                    return java.sql.Timestamp.class;


                default:
                     break;
            }
        }

        throw new UnsupportedOperationException("type : " + signature + " is not support.");
    }
}

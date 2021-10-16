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

package com.alibaba.polardbx.server.parser;

import com.alibaba.polardbx.server.util.ParseUtil;
import com.alibaba.polardbx.druid.sql.parser.ByteString;

public class ServerParseBaseline {
    public static final int OTHER = -1;
    public static final int LIST = 1;
    public static final int PERSIST = 2;
    public static final int CLEAR = 3;
    public static final int LOAD = 4;
    public static final int VALIDATE = 5;

    public static int parse(ByteString stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
            case ' ':
                continue;
            case '/':
            case '#':
                i = ParseUtil.comment(stmt, i);
                continue;
            case 'L':
            case 'l':
                if (stmt.charAt(i + 1) == 'I' || stmt.charAt(i + 1) == 'i') {
                    return listCheck(stmt, i);
                } else if (stmt.charAt(i + 1) == 'O' || stmt.charAt(i + 1) == 'o') {
                    return loadCheck(stmt, i);
                } else {
                    return OTHER;
                }
            case 'P':
            case 'p':
                return persistCheck(stmt, i);
            case 'C':
            case 'c':
                return clearCheck(stmt, i);
            case 'V':
            case 'v':
                return validateCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int persistCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ersist".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);

            if ((c2 == 'E' || c2 == 'e') &&
                (c3 == 'R' || c3 == 'r') &&
                (c4 == 'S' || c4 == 's') &&
                (c5 == 'I' || c5 == 'i') &&
                (c6 == 'S' || c6 == 's') &&
                (c7 == 'T' || c7 == 't') &&
                (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return PERSIST;
            }
        }
        return OTHER;
    }

    private static int clearCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "lear".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);

            if ((c2 == 'L' || c2 == 'l') &&
                (c3 == 'E' || c3 == 'e') &&
                (c4 == 'A' || c4 == 'a') &&
                (c5 == 'R' || c5 == 'r') &&
                (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return CLEAR;
            }
        }
        return OTHER;
    }

    private static int validateCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "alidate".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);

            if ((c2 == 'A' || c2 == 'a') &&
                (c3 == 'L' || c3 == 'l') &&
                (c4 == 'I' || c4 == 'i') &&
                (c5 == 'D' || c5 == 'd') &&
                (c6 == 'A' || c6 == 'a') &&
                (c7 == 'T' || c7 == 't') &&
                (c8 == 'E' || c8 == 'e') &&
                (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return VALIDATE;
            }
        }
        return OTHER;
    }

    private static int loadCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "oad".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);

            if ((c2 == 'O' || c2 == 'o') &&
                (c3 == 'A' || c3 == 'a') &&
                (c4 == 'D' || c4 == 'd') &&
                (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return LOAD;
            }
        }
        return OTHER;
    }

    private static int listCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ist".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);

            if ((c2 == 'I' || c2 == 'i') &&
                (c3 == 'S' || c3 == 's') &&
                (c4 == 'T' || c4 == 't') &&
                (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return LIST;
            }
        }
        return OTHER;
    }

}

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

public class ServerParseCollect {
    public static final int OTHER = -1;
    public static final int STATISTIC = 1;

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
            case 'S':
            case 's':
                return statisticCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int statisticCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "tatistic".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 't' || c1 == 'T') &&
                (c2 == 'a' || c2 == 'A') &&
                (c3 == 't' || c3 == 'T') &&
                (c4 == 'i' || c4 == 'I') &&
                (c5 == 's' || c5 == 'S') &&
                (c6 == 't' || c6 == 'T') &&
                (c7 == 'i' || c7 == 'I') &&
                (c8 == 'c' || c8 == 'C') &&
                (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return STATISTIC;
            }
        }
        return OTHER;
    }

}

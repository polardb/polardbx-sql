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

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.util.ParseUtil;

public final class ServerParseResize {

    public static final int OTHER = -1;
    public static final int PLANCACHE = 1;

    public static int parse(String stmt, int offset) {
        return parse(ByteString.from(stmt), offset);
    }

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
            case 'P':
            case 'p':
                return planCacheCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int planCacheCheck(ByteString stmt, int offset) {
        final String expect = "PLANCACHE";
        if (stmt.length() >= offset + expect.length()) {
            int endOffset = offset + expect.length();
            if (stmt.substring(offset, endOffset).equalsIgnoreCase(expect)) {
                return (endOffset << 8) | PLANCACHE;
            }
        }
        return OTHER;
    }
}

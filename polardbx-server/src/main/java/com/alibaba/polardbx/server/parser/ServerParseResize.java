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

import java.util.HashSet;
import java.util.Set;

public final class ServerParseResize {

    public static final int OTHER = -1;
    public static final int PLANCACHE = 1;
    public static final int PROCEDURE_CACHE = 2;
    public static final int FUNCTION_CACHE = 3;

    public static final Set<Integer> PREPARE_UNSUPPORTED_RESIZE_TYPE;

    static {
        PREPARE_UNSUPPORTED_RESIZE_TYPE = new HashSet<>();
        PREPARE_UNSUPPORTED_RESIZE_TYPE.add(ServerParseResize.PLANCACHE);
        PREPARE_UNSUPPORTED_RESIZE_TYPE.add(ServerParseResize.PROCEDURE_CACHE);
        PREPARE_UNSUPPORTED_RESIZE_TYPE.add(ServerParseResize.FUNCTION_CACHE);
    }

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
                return pCheck(stmt, i);
            case 'F':
            case 'f':
                return functionCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int pCheck(ByteString stmt, int offset) {
        final String expect = "PROCEDURE CACHE";
        if (stmt.length() >= offset + expect.length()) {
            int endOffset = offset + expect.length();
            if (stmt.substring(offset, endOffset).equalsIgnoreCase(expect)) {
                return (endOffset << 8) | PROCEDURE_CACHE;
            }
        }
        return planCacheCheck(stmt, offset);
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

    private static int functionCheck(ByteString stmt, int offset) {
        final String expect = "FUNCTION CACHE";
        if (stmt.length() >= offset + expect.length()) {
            int endOffset = offset + expect.length();
            if (stmt.substring(offset, endOffset).equalsIgnoreCase(expect)) {
                return (endOffset << 8) | FUNCTION_CACHE;
            }
        }
        return OTHER;
    }
}

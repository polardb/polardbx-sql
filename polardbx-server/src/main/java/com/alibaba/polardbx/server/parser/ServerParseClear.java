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

/**
 * @author xianmao.hexm 2011-5-7 下午01:23:06
 */
public final class ServerParseClear {

    public static final int OTHER = -1;
    public static final int SLOW = 1;
    public static final int PLANCACHE = 2;
    public static final int OSSCACHE = 3;

    public static final int HEATMAP_CACHE = 4;
    public static final int PROCEDURE_CACHE = 5;
    public static final int FUNCTION_CACHE = 6;

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
                return slowCheck(stmt, i);
            case 'P':
            case 'p':
                return pCheck(stmt, i);
            case 'H':
            case 'h':
                return partitionsHeatmapCacheCheck(stmt, i);
            case 'O':
            case 'o':
                return ossCacheCheck(stmt, i);
            case 'F':
            case 'f':
                return functionCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int functionCheck(ByteString stmt, int offset) {
        final String expect = "FUNCTION CACHE";
        if (stmt.length() >= offset + expect.length()) {
            if (stmt.substring(offset, offset + expect.length()).equalsIgnoreCase(expect)) {
                return FUNCTION_CACHE;
            }
        }
        return OTHER;
    }

    // CLEAR SLOW
    private static int slowCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "low".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'O' || c2 == 'o') && (c3 == 'W' || c3 == 'w')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return SLOW;
            }
        }
        return OTHER;
    }

    private static int pCheck(ByteString stmt, int offset) {
        final String expect = "PROCEDURE CACHE";
        if (stmt.length() >= offset + expect.length()) {
            if (stmt.substring(offset, offset + expect.length()).equalsIgnoreCase(expect)) {
                return PROCEDURE_CACHE;
            }
        }
        return planCacheCheck(stmt, offset);
    }

    // CLEAR PLANCACHE
    private static int planCacheCheck(ByteString stmt, int offset) {
        final String expect = "PLANCACHE";
        if (stmt.length() >= offset + expect.length()) {
            if (stmt.substring(offset, offset + expect.length()).equalsIgnoreCase(expect)) {
                return PLANCACHE;
            }
        }
        return OTHER;
    }


    private static int ossCacheCheck(ByteString stmt, int offset) {
        final String expect = "OSS CACHE";
        if (stmt.length() >= offset + expect.length()) {
            if (stmt.substring(offset, offset + expect.length()).equalsIgnoreCase(expect)) {
                return OSSCACHE;
            }
        }
        return OTHER;
    }

    private static int partitionsHeatmapCacheCheck(ByteString stmt, int offset) {
        final String expect = "HEATMAP_CACHE";
        if (stmt.length() >= offset + expect.length()) {
            if (stmt.substring(offset, offset + expect.length()).equalsIgnoreCase(expect)) {
                return HEATMAP_CACHE;
            }
        }
        return OTHER;
    }
}

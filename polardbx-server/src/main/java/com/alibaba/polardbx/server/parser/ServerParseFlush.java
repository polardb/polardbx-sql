package com.alibaba.polardbx.server.parser;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.util.ParseUtil;

public final class ServerParseFlush {

    public static final int OTHER = -1;
    /**
     * only support `FLUSH TABLES WITH READ LOCK` currently
     */
    public static final int FLUSH_TABLES = 1;

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
            case 't':
            case 'T':
                return tablesCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int tablesCheck(ByteString stmt, int offset) {
        final String expect = "TABLES";
        if (stmt.length() >= offset + expect.length()) {
            int endOffset = offset + expect.length();
            if (stmt.substring(offset, endOffset).equalsIgnoreCase(expect)) {
                return (endOffset << 8) | FLUSH_TABLES;
            }
        }
        return OTHER;
    }
}

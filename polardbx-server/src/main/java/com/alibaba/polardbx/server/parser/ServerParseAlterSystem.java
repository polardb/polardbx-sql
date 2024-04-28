package com.alibaba.polardbx.server.parser;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.util.ParseUtil;

public class ServerParseAlterSystem {
    public static final int OTHER = -1;
    public static final int COMPATIBILITY_LEVEL = 1;

    public static final char[] _COMPATIBILITY_LEVEL = "COMPATIBILITY_LEVEL ".toCharArray();

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
            case 'C':
            case 'c':
                return cCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int cCheck(ByteString stmt, int offset) {
        if (ParseUtil.compare(stmt, offset, _COMPATIBILITY_LEVEL)) {
            return COMPATIBILITY_LEVEL;
        }
        return OTHER;
    }
}

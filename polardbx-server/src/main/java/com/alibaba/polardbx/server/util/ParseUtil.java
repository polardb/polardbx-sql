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

package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.parse.util.CharTypes;

/**
 * @author xianmao.hexm 2011-5-9 下午02:40:29
 */
public final class ParseUtil {

    public static boolean isEOF(char c) {
        return (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';');
    }

    public static boolean isEOF(ByteString stmt, int offset) {
        for (; offset < stmt.length(); ++offset) {
            if (!isEOF(stmt.charAt(offset))) {
                return false;
            }
        }
        return true;
    }

    public static long getSQLId(ByteString stmt) {
        int offset = stmt.indexOf('=');
        if (offset != -1 && stmt.length() > ++offset) {
            String id = stmt.substring(offset).trim();
            try {
                return Long.parseLong(id);
            } catch (NumberFormatException e) {
            }
        }
        return 0L;
    }

    /**
     * <code>'abc'</code>
     *
     * @param offset stmt.charAt(offset) == first <code>'</code>
     */
    private static String parseString(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop:
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '\\') {
                switch (c = stmt.charAt(++offset)) {
                case '0':
                    sb.append('\0');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'Z':
                    sb.append((char) 26);
                    break;
                default:
                    sb.append(c);
                }
            } else if (c == '\'') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '\'') {
                    ++offset;
                    sb.append('\'');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * <code>"abc"</code>
     *
     * @param offset stmt.charAt(offset) == first <code>"</code>
     */
    private static String parseString2(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop:
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '\\') {
                switch (c = stmt.charAt(++offset)) {
                case '0':
                    sb.append('\0');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'Z':
                    sb.append((char) 26);
                    break;
                default:
                    sb.append(c);
                }
            } else if (c == '"') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '"') {
                    ++offset;
                    sb.append('"');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * <code>AS `abc`</code>
     *
     * @param offset stmt.charAt(offset) == first <code>`</code>
     */
    private static String parseIdentifierEscape(String stmt, int offset) {
        StringBuilder sb = new StringBuilder();
        loop:
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            if (c == '`') {
                if (offset + 1 < stmt.length() && stmt.charAt(offset + 1) == '`') {
                    ++offset;
                    sb.append('`');
                } else {
                    break loop;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * @param aliasIndex for <code>AS id</code>, index of 'i'
     */
    public static String parseAlias(String stmt, final int aliasIndex) {
        if (aliasIndex < 0 || aliasIndex >= stmt.length()) {
            return null;
        }
        switch (stmt.charAt(aliasIndex)) {
        case '\'':
            return parseString(stmt, aliasIndex);
        case '"':
            return parseString2(stmt, aliasIndex);
        case '`':
            return parseIdentifierEscape(stmt, aliasIndex);
        default:
            int offset = aliasIndex;
            for (; offset < stmt.length() && CharTypes.isIdentifierChar(stmt.charAt(offset)); ++offset)
                ;
            return stmt.substring(aliasIndex, offset);
        }
    }

    public static int comment(String stmt, int offset) {
        int len = stmt.length();
        int n = offset;
        switch (stmt.charAt(n)) {
        case '/':
            if (len > ++n && stmt.charAt(n++) == '*' && len > n + 1) {
                if (stmt.charAt(n) != '!') {
                    for (int i = n; i < len; ++i) {
                        if (stmt.charAt(i) == '*') {
                            int m = i + 1;
                            if (len > m && stmt.charAt(m) == '/') {
                                return m;
                            }
                        }
                    }
                } else if (len > n + 6) {
                    // MySQL use 5 digits to indicate version. 50508
                    // means MySQL 5.5.8
                    if (CharTypes.isDigit(stmt.charAt(n + 1))
                        && CharTypes.isDigit(stmt.charAt(n + 2)) && CharTypes.isDigit(stmt.charAt(n + 3))
                        && CharTypes.isDigit(stmt.charAt(n + 4)) && CharTypes.isDigit(stmt.charAt(n + 5))) {
                        return n + 5;
                    }
                }

            }
            break;
        case '#':
            for (int i = n + 1; i < len; ++i) {
                if (stmt.charAt(i) == '\n') {
                    return i;
                }
            }
            break;
        }
        return offset;
    }

    public static int comment(ByteString stmt, int offset) {
        int len = stmt.length();
        int n = offset;
        switch (stmt.charAt(n)) {
        case '/':
            if (len > ++n && stmt.charAt(n++) == '*' && len > n + 1) {
                if (stmt.charAt(n) != '!') {
                    for (int i = n; i < len; ++i) {
                        if (stmt.charAt(i) == '*') {
                            int m = i + 1;
                            if (len > m && stmt.charAt(m) == '/') {
                                return m;
                            }
                        }
                    }
                } else if (len > n + 6) {
                    // MySQL use 5 digits to indicate version. 50508
                    // means MySQL 5.5.8
                    if (CharTypes.isDigit(stmt.charAt(n + 1))
                        && CharTypes.isDigit(stmt.charAt(n + 2)) && CharTypes.isDigit(stmt.charAt(n + 3)) && CharTypes
                        .isDigit(stmt.charAt(n + 4)) && CharTypes.isDigit(stmt.charAt(n + 5))) {
                        return n + 5;
                    }
                }

            }
            break;
        case '#':
            for (int i = n + 1; i < len; ++i) {
                if (stmt.charAt(i) == '\n') {
                    return i;
                }
            }
            break;
        }
        return offset;
    }

    public static int move(ByteString stmt, int offset, int length) {
        int i = offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            case '/':
            case '#':
                i = comment(stmt, i);
                continue;
            default:
                return i + length;
            }
        }
        return i;
    }

    public static boolean compare(ByteString s, int offset, char[] keyword) {
        if (s.length() >= offset + keyword.length) {
            for (int i = 0; i < keyword.length; ++i, ++offset) {
                if (Character.toUpperCase(s.charAt(offset)) != keyword[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean compare(String s, int offset, char[] keyword) {
        if (s.length() >= offset + keyword.length) {
            for (int i = 0; i < keyword.length; ++i, ++offset) {
                if (Character.toUpperCase(s.charAt(offset)) != keyword[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static String removeQuotation(String s) {
        if ((s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') ||
            (s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') ||
            (s.charAt(0) == '`' && s.charAt(s.length() - 1) == '`')) {
            return s.substring(1, s.length() - 2);
        }
        return s;
    }
}

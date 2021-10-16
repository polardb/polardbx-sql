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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xianmao.hexm
 */
public final class ServerParse {

    public static final int OTHER = -1;
    public static final int BEGIN = 1;
    public static final int COMMIT = 2;
    public static final int DELETE = 3;
    public static final int INSERT = 4;
    public static final int REPLACE = 5;
    public static final int ROLLBACK = 6;
    public static final int SELECT = 7;
    public static final int SET = 8;
    public static final int SHOW = 9;
    public static final int START = 10;
    public static final int UPDATE = 11;
    public static final int KILL = 12;
    public static final int SAVEPOINT = 13;
    public static final int USE = 14;
    public static final int KILL_QUERY = 16;
    public static final int PREPARE = 18;
    public static final int EXECUTE = 19;
    public static final int DEALLOCATE = 20;
    public static final int CLEAR = 21;
    public static final int HELP = 22;
    public static final int GRANT = 23;
    public static final int REVOKE = 24;
    public static final int CREATE_USER = 25;
    public static final int DROP_USER = 26;
    public static final int SET_PASSWORD = 27;
    public static final int PURGE_TRANS = 30;
    public static final int BALANCE = 31;
    public static final int COLLECT = 32;
    public static final int RELOAD = 33;
    public static final int CREATE_ROLE = 34;
    public static final int DROP_ROLE = 35;
    public static final int RESIZE = 36;
    public static final int LOAD_DATA_INFILE_SQL = 99;
    public static final int TABLE = 100;

    private static final Pattern CREATE_USER_PATTERN = Pattern.compile("^\\s*create\\s+user\\s+.*$",
        Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_ROLE_PATTERN = Pattern.compile("^\\s*create\\s+role\\s+.*$",
        Pattern.CASE_INSENSITIVE);
    private static final Pattern DROP_USER_PATTERN = Pattern.compile("^\\s*drop\\s+user\\s+.*$",
        Pattern.CASE_INSENSITIVE);
    private static final Pattern DROP_ROLE_PATTERN = Pattern.compile("^\\s*drop\\s+role\\s+.*$",
        Pattern.CASE_INSENSITIVE);
    private static final Pattern SET_PASSWORD_PATTERN = Pattern.compile("^\\s*set\\s+password\\s+.*$",
        Pattern.CASE_INSENSITIVE);
    private static final Pattern SET_DEFAULT_ROLE_PATTERN = Pattern.compile("^\\s*set\\s+default\\s+role.*$",
        Pattern.CASE_INSENSITIVE);

    public static int parse(String stmt) {
        return parse(ByteString.from(stmt));
    }

    public static int parse(ByteString stmt) {
        for (int i = 0; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            case '/':
            case '#':
                i = ParseUtil.comment(stmt, i);
                continue;
            case 'B':
            case 'b':
                return bCheck(stmt, i);
            case 'C':
            case 'c':
                return cCheck(stmt, i);
            case 'D':
            case 'd': {
                int cmd = deleteCheck(stmt, i);
                if (cmd == OTHER) {
                    cmd = deallocateCheck(stmt, i);
                }
                if (cmd == OTHER) {
                    return dCheck(stmt, i);
                }
                return cmd;
            }
            case 'E':
            case 'e': {
                return executeCheck(stmt, i);
            }
            case 'G':
            case 'g':
                return gCheck(stmt, i);
            case 'I':
            case 'i':
                return insertCheck(stmt, i);
            case 'P':
            case 'p':
                return pCheck(stmt, i);
            case 'R':
            case 'r':
                return rCheck(stmt, i);
            case 'S':
            case 's':
                return sCheck(stmt, i);
            case 'U':
            case 'u':
                return uCheck(stmt, i);
            case 'K':
            case 'k':
                return killCheck(stmt, i);
            case 'H':
            case 'h':
                return hCheck(stmt, i);
            case 'L':
            case 'l':
                return lCheck(stmt, i);
            case 'T':
            case 't':
                return tableCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static boolean isDDL(ByteString stmt, int offset) {
        for (int i = offset; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;

            case 'D':
            case 'd':
                // DDL' '
                if (i + 3 < stmt.length() &&
                    ('D' == stmt.charAt(i + 1) || 'd' == stmt.charAt(i + 1)) &&
                    ('L' == stmt.charAt(i + 2) || 'l' == stmt.charAt(i + 2)) &&
                    Character.isSpaceChar(stmt.charAt(i + 3))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static int dCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'R':
            case 'r':
                return drCheck(stmt, offset);
            default:
                return OTHER;
            }
        }

        return OTHER;
    }

    private static int drCheck(ByteString stmt, int offset) {
        String stmtStr = stmt.toString().substring(offset - 1);
        if (stmt.length() > ++offset) {
            if (DROP_USER_PATTERN.matcher(stmtStr).matches()) {
                return DROP_USER;
            } else if (DROP_ROLE_PATTERN.matcher(stmtStr).matches()) {
                return DROP_ROLE;
            }
        }

        return OTHER;
    }

    private static int pCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'R':
            case 'r':
                return prepareCheck(stmt, offset);
            case 'U':
            case 'u':
                return purgeCheck(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // PURGE
    private static int purgeCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "RGE ".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c2 == 'R' || c2 == 'r') && (c3 == 'G' || c3 == 'g') && (c4 == 'E' || c4 == 'e')
                && (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    case 'T':
                    case 't':
                        return purgeTransCheck(stmt, offset);
                    default:
                        return OTHER;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }

    // PURGE TRANS
    private static int purgeTransCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "RANS".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'A' || c2 == 'a') && (c3 == 'N' || c3 == 'n')
                && (c4 == 'S' || c4 == 's') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    case ';':
                        break;
                    case 'B':
                    case 'b':
                        return purgeTransBeforeCheck(stmt, offset);
                    default:
                        return OTHER;
                    }
                }
                return PURGE_TRANS;
            }
        }
        return OTHER;
    }

    // PURGE TRANS BEFORE
    private static int purgeTransBeforeCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "EFORE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'F' || c2 == 'f') && (c3 == 'O' || c3 == 'o')
                && (c4 == 'R' || c4 == 'r') && (c5 == 'E' || c5 == 'e')) {
                if (stmt.length() == ++offset) {
                    return OTHER;
                }
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    case ';':
                        break;
                    default:
                        return (offset << 8) | PURGE_TRANS;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }

    // PREPARE
    private static int prepareCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "EPARE ".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c2 == 'E' || c2 == 'e') && (c3 == 'P' || c3 == 'p') && (c4 == 'A' || c4 == 'a')
                && (c5 == 'R' || c5 == 'r') && (c6 == 'E' || c6 == 'e')
                && (c7 == ' ' || c7 == '\t' || c7 == '\r' || c7 == '\n')) {
                return (offset << 8) | PREPARE;
            }
        }
        return OTHER;
    }

    // EXECUTE
    private static int executeCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "XECUTE ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'X' || c1 == 'x') && (c2 == 'E' || c2 == 'e') && (c3 == 'C' || c3 == 'c')
                && (c4 == 'U' || c4 == 'u') && (c5 == 'T' || c5 == 't') && (c6 == 'E' || c6 == 'e')
                && (c7 == ' ' || c7 == '\t' || c7 == '\r' || c7 == '\n')) {
                return (offset << 8) | EXECUTE;
            }
        }
        return OTHER;
    }

    // DEALLOCATE
    private static int deallocateCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "EALLOCATE ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            char c10 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a') && (c3 == 'L' || c3 == 'l')
                && (c4 == 'L' || c4 == 'l') && (c5 == 'O' || c5 == 'o') && (c6 == 'C' || c6 == 'c')
                && (c7 == 'A' || c7 == 'a') && (c8 == 'T' || c8 == 't') && (c9 == 'E' || c9 == 'e')
                && (c10 == ' ' || c10 == '\t' || c10 == '\r' || c10 == '\n')) {
                return (offset << 8) | DEALLOCATE;
            }
        }
        return OTHER;
    }

    // KILL' '
    private static int killCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ILL ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'L' || c2 == 'l') && (c3 == 'L' || c3 == 'l')
                && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    case 'Q':
                    case 'q':
                        return killQueryCheck(stmt, offset);
                    case '\'':
                    case '\"':
                        return OTHER;
                    default:
                        return (offset << 8) | KILL;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }

    // KILL QUERY' '
    private static int killQueryCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "UERY ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r')
                && (c4 == 'Y' || c4 == 'y') && (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    default:
                        return (offset << 8) | KILL_QUERY;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int bCheck(ByteString stmt, int offset) {
        switch (stmt.charAt(offset + 1)) {
        case 'a':
        case 'A':
            return balanceCheck(stmt, offset);
        case 'e':
        case 'E':
            return beginCheck(stmt, offset);
        default:
            return OTHER;
        }
    }

    // BALANCE
    private static int balanceCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'L' || c2 == 'l') && (c3 == 'A' || c3 == 'a')
                && (c4 == 'N' || c4 == 'n') && (c5 == 'C' || c5 == 'c') && (c6 == 'E' || c6 == 'e')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return BALANCE;
            }
        }
        return OTHER;
    }

    // BEGIN
    private static int beginCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'G' || c2 == 'g') && (c3 == 'I' || c3 == 'i')
                && (c4 == 'N' || c4 == 'n') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return BEGIN;
            }
        }
        return OTHER;
    }

    // COMMIT
    private static int commitCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'M' || c1 == 'm') && (c2 == 'M' || c2 == 'm') && (c3 == 'I' || c3 == 'i')
                && (c4 == 'T' || c4 == 't') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return COMMIT;
            }
        }
        return OTHER;
    }

    // DELETE' '
    private static int deleteCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'L' || c2 == 'l') && (c3 == 'E' || c3 == 'e')
                && (c4 == 'T' || c4 == 't') && (c5 == 'E' || c5 == 'e')
                && (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
                return DELETE;
            }
        }
        return OTHER;
    }

    // INSERT' '
    private static int insertCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'S' || c2 == 's') && (c3 == 'E' || c3 == 'e')
                && (c4 == 'R' || c4 == 'r') && (c5 == 'T' || c5 == 't')
                && (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
                return INSERT;
            }
        }
        return OTHER;
    }

    private static int cCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'L':
            case 'l':
                return clearCheck(stmt, offset);
            case 'O':
            case 'o':
                return coCheck(stmt, offset);
            case 'R':
            case 'r':
                return crCheck(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int coCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(offset + 1)) {
            case 'm':
            case 'M':
                return commitCheck(stmt, offset);
            case 'l':
            case 'L':
                return collectCheck(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // COLLECT' '
    private static int collectCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'L' || c2 == 'l') && (c3 == 'E' || c3 == 'e')
                && (c4 == 'C' || c4 == 'c') && (c5 == 'T' || c5 == 't')
                && (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
                return (offset << 8) | COLLECT;
            }
        }
        return OTHER;
    }

    private static int crCheck(ByteString stmt, int offset) {
        String str = stmt.toString().substring(offset - 1);
        if (stmt.length() > ++offset) {
            if (CREATE_USER_PATTERN.matcher(str).matches()) {
                return CREATE_USER;
            } else if (CREATE_ROLE_PATTERN.matcher(str).matches()) {
                return CREATE_ROLE;
            }
        }

        return OTHER;
    }

    private static int gCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'R':
            case 'r':
                return grCheck(stmt, offset);
            default:
                return OTHER;
            }
        }

        return OTHER;
    }

    private static int grCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            char c3 = stmt.charAt(offset++);
            char c4 = stmt.charAt(offset++);
            char c5 = stmt.charAt(offset++);

            if ((c3 == 'A' || c3 == 'a') && (c4 == 'N' || c4 == 'n') && (c5 == 'T' || c5 == 't')) {
                return GRANT;
            } else {
                return OTHER;
            }
        }

        return OTHER;
    }

    private static int rCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'E':
            case 'e':
                return reCheck(stmt, offset);
            case 'O':
            case 'o':
                return rollbackCheck(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int reCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'V':
            case 'v':
                return revokeCheck(stmt, offset);
            case 'P':
            case 'p':
                return replaceCheck(stmt, offset);
            case 'L':
            case 'l':
                return reloadCheck(stmt, offset);
            case 'S':
            case 's':
                return resizeCheck(stmt, offset);
            default:
                return OTHER;
            }
        }

        return OTHER;
    }

    private static int revokeCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            char c4 = stmt.charAt(offset++);
            char c5 = stmt.charAt(offset++);
            char c6 = stmt.charAt(offset++);

            if ((c4 == 'O' || c4 == 'o') && (c5 == 'K' || c5 == 'k') && (c6 == 'E' || c6 == 'e')) {
                return REVOKE;
            } else {
                return OTHER;
            }
        }

        return OTHER;
    }

    // REPLACE' '
    private static int replaceCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'A' || c2 == 'a')
                && (c3 == 'C' || c3 == 'c') && (c4 == 'E' || c4 == 'e')
                && (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                return REPLACE;
            }
        }
        return OTHER;
    }

    // RELOAD' '
    private static int reloadCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'A' || c2 == 'a') && (c3 == 'D' || c3 == 'd')
                && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    default:
                        if (!isDDL(stmt, offset)) {
                            return (offset << 8) | RELOAD;
                        } else {
                            return OTHER;
                        }
                    }
                }
            }
        }
        return OTHER;
    }

    // RESIZE
    private static int resizeCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'Z' || c2 == 'z')
                && (c3 == 'E' || c3 == 'e') && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return (offset << 8) | RESIZE;
            }
        }
        return OTHER;
    }

    // ROLLBACK
    private static int rollbackCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'L' || c2 == 'l') && (c3 == 'B' || c3 == 'b')
                && (c4 == 'A' || c4 == 'a') && (c5 == 'C' || c5 == 'c') && (c6 == 'K' || c6 == 'k')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return ROLLBACK;
            }
        }
        return OTHER;
    }

    private static int sCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'A':
            case 'a':
                return savepointCheck(stmt, offset);
            case 'E':
            case 'e':
                return seCheck(stmt, offset);
            case 'H':
            case 'h':
                return showCheck(stmt, offset);
            case 'T':
            case 't':
                return startCheck(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // SAVEPOINT
    private static int savepointCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'V' || c1 == 'v') && (c2 == 'E' || c2 == 'e') && (c3 == 'P' || c3 == 'p')
                && (c4 == 'O' || c4 == 'o') && (c5 == 'I' || c5 == 'i') && (c6 == 'N' || c6 == 'n')
                && (c7 == 'T' || c7 == 't') && (c8 == ' ' || c8 == '\t' || c8 == '\r' || c8 == '\n')) {
                return SAVEPOINT;
            }
        }
        return OTHER;
    }

    private static int seCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'L':
            case 'l':
                return selectCheck(stmt, offset);
            case 'T':
            case 't':
                // 先匹配看看是不是set password语句
                Matcher m = SET_PASSWORD_PATTERN.matcher(stmt.toString().substring(offset - 2));
                if (m.matches()) {
                    return SET_PASSWORD;
                }

                // set default role 走plan
                if (SET_DEFAULT_ROLE_PATTERN.matcher(stmt.toString().substring(offset - 2)).matches()) {
                    return OTHER;
                }

                if (stmt.length() > ++offset) {
                    char c = stmt.charAt(offset);
                    if (c == ' ' || c == '\r' || c == '\n' || c == '\t' || c == '/' || c == '#') {
                        return (offset << 8) | SET;
                    }
                }
                return OTHER;
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // SELECT' '
    private static int selectCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'C' || c2 == 'c') && (c3 == 'T' || c3 == 't')
                && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n' || c4 == '/' || c4 == '#')) {
                return (offset << 8) | SELECT;
            }
        }
        return OTHER;
    }

    // SHOW' '
    private static int showCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'W' || c2 == 'w')
                && (c3 == ' ' || c3 == '\t' || c3 == '\r' || c3 == '\n')) {
                return (offset << 8) | SHOW;
            }
        }
        return OTHER;
    }

    // CLEAR' '
    private static int clearCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);

            if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a' && (c3 == 'R' || c3 == 'r'))
                && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return (offset << 8) | CLEAR;
            }
        }
        return OTHER;
    }

    // START' '
    private static int startCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r') && (c3 == 'T' || c3 == 't')
                && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return (offset << 8) | START;
            }
        }
        return OTHER;
    }

    // UPDATE' ' | USE' '
    private static int uCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'P':
            case 'p':
                if (stmt.length() > offset + 5) {
                    char c1 = stmt.charAt(++offset);
                    char c2 = stmt.charAt(++offset);
                    char c3 = stmt.charAt(++offset);
                    char c4 = stmt.charAt(++offset);
                    char c5 = stmt.charAt(++offset);
                    if ((c1 == 'D' || c1 == 'd') && (c2 == 'A' || c2 == 'a') && (c3 == 'T' || c3 == 't')
                        && (c4 == 'E' || c4 == 'e') && (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                        return UPDATE;
                    }
                }
                break;
            case 'S':
            case 's':
                if (stmt.length() > offset + 2) {
                    char c1 = stmt.charAt(++offset);
                    char c2 = stmt.charAt(++offset);
                    if ((c1 == 'E' || c1 == 'e') && (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
                        return (offset << 8) | USE;
                    }
                }
                break;
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int loadCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if ((c1 == 'D' || c1 == 'd') && (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
                return loadParse(stmt, offset);
            }
        }
        return OTHER;
    }

    private static int loadParse(ByteString stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'D' || c1 == 'd') && (c2 == 'A' || c2 == 'a') && (c3 == 'T' || c3 == 't') && (c4 == 'A'
                || c4 == 'a') &&
                (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                return LOAD_DATA_INFILE_SQL;
            } else if ((c1 == 'I' || c1 == 'i') && (c2 == 'N' || c2 == 'n') && (c3 == 'D' || c3 == 'd') && (c4 == 'E'
                || c4 == 'e') &&
                (c5 == 'X' || c5 == 'x')) {
                if (stmt.length() > offset + 1) {
                    char c6 = stmt.charAt(++offset);
                    if ((c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
                        return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    private static int lockCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if ((c1 == 'K' || c1 == 'k') && (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int tableCheck(ByteString stmt, int offset) {
        if (stmt.length() <= offset + 4) {
            return OTHER;
        }
        char c2 = stmt.charAt(++offset);
        char c3 = stmt.charAt(++offset);
        char c4 = stmt.charAt(++offset);
        char c5 = stmt.charAt(++offset);
        char c6 = stmt.charAt(++offset);
        if ((c2 == 'A' || c2 == 'a') && (c3 == 'B' || c3 == 'b') && (c4 == 'L'
            || c4 == 'l') && (c5 == 'E' || c5 == 'e') &&
            (c6 == ' ' || c6 == '\t' || c6 == '\r' || c6 == '\n')) {
            return TABLE;
        }
        return OTHER;
    }

    private static int lCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            if (c1 == 'o' || c1 == 'O') {
                switch (stmt.charAt(++offset)) {
                case 'A':
                case 'a':
                    return loadCheck(stmt, offset);
                case 'C':
                case 'c':
                    return lockCheck(stmt, offset);
                default:
                    return OTHER;
                }
            }
        }

        return OTHER;
    }

    // HELP DRDS
    private static int hCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ELP ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'L' || c2 == 'l' && (c3 == 'P' || c3 == 'p'))
                && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                if (stmt.length() > offset + "'DRDS'".length()) {
                    char c5 = stmt.charAt(++offset);
                    char c6 = stmt.charAt(++offset);
                    char c7 = stmt.charAt(++offset);
                    char c8 = stmt.charAt(++offset);
                    char c9 = stmt.charAt(++offset);
                    char c10 = stmt.charAt(++offset);

                    if ((c5 == '\'') && (c6 == 'D' || c6 == 'd')
                        && (c7 == 'R' || c7 == 'r' && (c8 == 'D' || c8 == 'd') && (c9 == 'S' || c9 == 's'))
                        && (c10 == '\'')) {
                        return (offset << 8) | HELP;
                    }
                } else if (stmt.length() > offset + "DRDS".length()) {
                    char c5 = stmt.charAt(++offset);
                    char c6 = stmt.charAt(++offset);
                    char c7 = stmt.charAt(++offset);
                    char c8 = stmt.charAt(++offset);

                    if ((c5 == 'D' || c5 == 'd')
                        && (c6 == 'R' || c6 == 'r' && (c7 == 'D' || c7 == 'd') && (c8 == 'S' || c8 == 's'))) {
                        return (offset << 8) | HELP;
                    }
                }
            }
        }
        return OTHER;
    }

    public static ByteString rewriteTableIntoSql(ByteString sql) {
        if (null == sql) {
            return null;
        }
        if (sql.startsWith("table ") || sql.startsWith("TABLE ")) {
            int spaceIndex = sql.indexOf(" ");
            return ByteString.from("select * from" + sql.substring(spaceIndex));
        }
        return sql;
    }

}

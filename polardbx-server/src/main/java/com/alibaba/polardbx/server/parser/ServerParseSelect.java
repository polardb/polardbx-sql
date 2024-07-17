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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.parse.util.CharTypes;
import com.alibaba.polardbx.server.util.ParseUtil;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * @author xianmao.hexm 2011-5-7 下午01:23:18
 */
public final class ServerParseSelect {

    public static final int OTHER = -1;
    public static final int VERSION_COMMENT = 1;
    public static final int DATABASE = 2;
    public static final int USER = 3;
    public static final int IDENTITY = 5;
    public static final int VERSION = 6;
    public static final int LASTTXCXID = 7;
    public static final int SESSION_TX_READ_ONLY = 8;
    public static final int CURRENT_TRANS_ID = 9;
    public static final int CURRENT_TRANS_POLICY = 10;
    public static final int LITERAL_NUMBER = 11;
    public static final int TSO_TIMESTAMP = 12;
    public static final int EXTRACT_TRACE_ID = 13;
    public static final int SEQ_NEXTVAL_BENCHMARK = 14;
    public static final int SEQ_SKIP_BENCHMARK = 15;
    public static final int POLARDB_VERSION = 16;
    public static final int COMPATIBILITY_LEVEL = 17;
    public static final int ENCDB_PROCESS_MESSAGE = 18;
    public static final int SESSION_TRANSACTION_READ_ONLY = 19;

    public static final Set<Integer> PREPARE_UNSUPPORTED_SELECT_TYPE;
    private static final char[] _VERSION_COMMENT = "VERSION_COMMENT".toCharArray();
    private static final char[] _IDENTITY = "IDENTITY".toCharArray();
    private static final char[] _LAST_INSERT_ID = "LAST_INSERT_ID".toCharArray();
    private static final char[] _DATABASE = "DATABASE()".toCharArray();
    private static final char[] _SESSION_TX_READ_ONLY = "SESSION.TX_READ_ONLY".toCharArray();
    private static final char[] _CURRENT_TRANS_ID = "CURRENT_TRANS_ID".toCharArray();
    private static final char[] _CURRENT_TRANS_POLICY = "CURRENT_TRANS_POLICY".toCharArray();
    private static final char[] _EXTRACT_TRACE_ID = "EXTRACT_TRACE_ID".toCharArray();
    private static final char[] _SEQ_NEXTVAL_BENCHMARK = "SEQ_NEXTVAL_BENCHMARK".toCharArray();
    private static final char[] _SEQ_SKIP_BENCHMARK = "SEQ_SKIP_BENCHMARK".toCharArray();
    private static final char[] _POLARDB_VERSION = "POLARDB_VERSION".toCharArray();
    private static final char[] _SESSION_TRANSACTION_READ_ONLY = "SESSION.TRANSACTION_READ_ONLY".toCharArray();
    private static final char[] _COMPATIBILITY_LEVEL = "COMPATIBILITY_LEVEL".toCharArray();
    private static final char[] _ENCDB_PROCESS_MESSAGE = "ENCDB_PROCESS_MESSAGE".toCharArray();

    static {
        PREPARE_UNSUPPORTED_SELECT_TYPE = new HashSet<>();

        PREPARE_UNSUPPORTED_SELECT_TYPE.add(VERSION_COMMENT);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(DATABASE);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(USER);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(VERSION);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(LASTTXCXID);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(SESSION_TX_READ_ONLY);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(CURRENT_TRANS_ID);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(CURRENT_TRANS_POLICY);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(LITERAL_NUMBER);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(TSO_TIMESTAMP);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(EXTRACT_TRACE_ID);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(SEQ_NEXTVAL_BENCHMARK);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(SEQ_SKIP_BENCHMARK);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(COMPATIBILITY_LEVEL);
        PREPARE_UNSUPPORTED_SELECT_TYPE.add(ENCDB_PROCESS_MESSAGE);
    }

    public static int parse(String stmt, int offset, Object[] exData) {
        return parse(ByteString.from(stmt), offset, exData);
    }

    public static int parse(ByteString stmt, int offset, Object[] exData) {
        int i = offset;
        for (; i < stmt.length(); ++i) {
            switch (stmt.charAt(i)) {
            case ' ':
                continue;
            case '/':
            case '#':
                i = ParseUtil.comment(stmt, i);
                continue;
            case '@':
                return select2Check(stmt, i);
            case 'D':
            case 'd':
                return databaseCheck(stmt, i);
            case 'U':
            case 'u':
                return userCheck(stmt, i);
            case 'P':
            case 'p':
                return polardbVersionsCheck(stmt, i);
            case 'V':
            case 'v':
                return versionCheck(stmt, i);
            case 'L':
            case 'l':
                return lastTxcXid(stmt, i, exData);
            case 'C':
            case 'c':
                return cCheck(stmt, i);
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return selectLiteralNumberCheck(stmt, i, exData);
            case 'T':
            case 't':
                return tsoTimestampCheck(stmt, i);
            case 'e':
            case 'E':
                int kind = extractTraceIdCheck(stmt, i, exData);
                if (kind == OTHER) {
                    return extractEncdbProcessMessage(stmt, i, exData);
                }
                return kind;
            case 's':
            case 'S':
                return parseSeqBenchmark(stmt, i, exData);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // SELECT POLARDB_VERSION/POLARDB_VERSIONS
    private static int polardbVersionsCheck(ByteString stmt, int offset) {
        if (ParseUtil.compare(stmt, offset, _POLARDB_VERSION)) {
            offset = ParseUtil.move(stmt, offset + _POLARDB_VERSION.length, 0);
            if (offset + 1 < stmt.length() && stmt.charAt(offset) == '(') {
                offset = ParseUtil.move(stmt, offset + 1, 0);
                if (offset < stmt.length() && stmt.charAt(offset) == ')') {
                    return POLARDB_VERSION;
                }
            }
        }
        return OTHER;
    }

    // SELECT VERSION
    private static int versionCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ERSION".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'R' || c2 == 'r') && (c3 == 'S' || c3 == 's')
                && (c4 == 'I' || c4 == 'i') && (c5 == 'O' || c5 == 'o') && (c6 == 'N' || c6 == 'n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    case '(':
                        return versionParenthesisCheck(stmt, offset);
                    default:
                        return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SELECT VERSION (
    private static int versionParenthesisCheck(ByteString stmt, int offset) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            case ')':
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        /**
                         * navicat会发过来;，而对于version会返回OTHER然后下推到mysql,
                         * 所以显示的是 mysql的version而不是drds version.
                         */
                    case ';':
                        continue;
                    default:
                        return OTHER;
                    }
                }
                return VERSION;
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    /**
     * <code>SELECT LAST_INSERT_ID() AS id, </code>
     *
     * @param offset index of 'i', offset == stmt.length() is possible
     * @return index of ','. return stmt.length() is possible. -1 if not alias
     */
    private static int skipAlias(ByteString stmt, int offset) {
        offset = ParseUtil.move(stmt, offset, 0);
        if (offset >= stmt.length()) {
            return offset;
        }
        switch (stmt.charAt(offset)) {
        case '\'':
            return skipString(stmt, offset);
        case '"':
            return skipString2(stmt, offset);
        case '`':
            return skipIdentifierEscape(stmt, offset);
        default:
            if (CharTypes.isIdentifierChar(stmt.charAt(offset))) {
                for (; offset < stmt.length() && CharTypes.isIdentifierChar(stmt.charAt(offset)); ++offset) {
                    ;
                }
                return offset;
            }
        }
        return -1;
    }

    /**
     * <code>`abc`d</code>
     *
     * @param offset index of first <code>`</code>
     * @return index of 'd'. return stmt.length() is possible. -1 if string
     * invalid
     */
    private static int skipIdentifierEscape(ByteString stmt, int offset) {
        for (++offset; offset < stmt.length(); ++offset) {
            if (stmt.charAt(offset) == '`') {
                if (++offset >= stmt.length() || stmt.charAt(offset) != '`') {
                    return offset;
                }
            }
        }
        return -1;
    }

    /**
     * <code>"abc"d</code>
     *
     * @param offset index of first <code>"</code>
     * @return index of 'd'. return stmt.length() is possible. -1 if string
     * invalid
     */
    private static int skipString2(ByteString stmt, int offset) {
        int state = 0;
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            switch (state) {
            case 0:
                switch (c) {
                case '\\':
                    state = 1;
                    break;
                case '"':
                    state = 2;
                    break;
                }
                break;
            case 1:
                state = 0;
                break;
            case 2:
                switch (c) {
                case '"':
                    state = 0;
                    break;
                default:
                    return offset;
                }
                break;
            }
        }
        if (offset == stmt.length() && state == 2) {
            return stmt.length();
        }
        return -1;
    }

    /**
     * <code>'abc'd</code>
     *
     * @param offset index of first <code>'</code>
     * @return index of 'd'. return stmt.length() is possible. -1 if string
     * invalid
     */
    private static int skipString(ByteString stmt, int offset) {
        int state = 0;
        for (++offset; offset < stmt.length(); ++offset) {
            char c = stmt.charAt(offset);
            switch (state) {
            case 0:
                switch (c) {
                case '\\':
                    state = 1;
                    break;
                case '\'':
                    state = 2;
                    break;
                }
                break;
            case 1:
                state = 0;
                break;
            case 2:
                switch (c) {
                case '\'':
                    state = 0;
                    break;
                default:
                    return offset;
                }
                break;
            }
        }
        if (offset == stmt.length() && state == 2) {
            return stmt.length();
        }
        return -1;
    }

    /**
     * <code>SELECT LAST_INSERT_ID() AS id</code>
     *
     * @param offset index of first ' ' after LAST_INSERT_ID(), offset ==
     * stmt.length() is possible
     * @return index of 'i'. return stmt.length() is possible
     */
    public static int skipAs(ByteString stmt, int offset) {
        offset = ParseUtil.move(stmt, offset, 0);
        if (stmt.length() > offset + "AS".length() && (stmt.charAt(offset) == 'A' || stmt.charAt(offset) == 'a')
            && (stmt.charAt(offset + 1) == 'S' || stmt.charAt(offset + 1) == 's')
            && (stmt.charAt(offset + 2) == ' ' || stmt.charAt(offset + 2) == '\r' || stmt.charAt(offset + 2) == '\n'
            || stmt.charAt(offset + 2) == '\t' || stmt.charAt(offset + 2) == '/'
            || stmt.charAt(offset + 2) == '#')) {
            offset = ParseUtil.move(stmt, offset + 2, 0);
        }
        return offset;
    }

    /**
     * @param offset <code>stmt.charAt(offset) == first 'L' OR 'l'</code>
     * @return index after LAST_INSERT_ID(), might equals to length. -1 if not
     * LAST_INSERT_ID
     */
    public static int indexAfterLastInsertIdFunc(ByteString stmt, int offset) {
        if (stmt.length() >= offset + "LAST_INSERT_ID()".length()) {
            if (ParseUtil.compare(stmt, offset, _LAST_INSERT_ID)) {
                offset = ParseUtil.move(stmt, offset + _LAST_INSERT_ID.length, 0);
                if (offset + 1 < stmt.length() && stmt.charAt(offset) == '(') {
                    offset = ParseUtil.move(stmt, offset + 1, 0);
                    if (offset < stmt.length() && stmt.charAt(offset) == ')') {
                        return ++offset;
                    }
                }
            }
        }
        return -1;
    }

    /**
     * @param offset <code>stmt.charAt(offset) == first '`' OR 'i' OR 'I' OR '\'' OR '"'</code>
     * @return index after identity or `identity` or "identity" or 'identity',
     * might equals to length. -1 if not identity or `identity` or "identity" or
     * 'identity'
     */
    public static int indexAfterIdentity(ByteString stmt, int offset) {
        char first = stmt.charAt(offset);
        switch (first) {
        case '`':
        case '\'':
        case '"':
            if (stmt.length() < offset + "identity".length() + 2) {
                return -1;
            }
            if (stmt.charAt(offset + "identity".length() + 1) != first) {
                return -1;
            }
            ++offset;
            break;
        case 'i':
        case 'I':
            if (stmt.length() < offset + "identity".length()) {
                return -1;
            }
            break;
        default:
            return -1;
        }
        if (ParseUtil.compare(stmt, offset, _IDENTITY)) {
            offset += _IDENTITY.length;
            switch (first) {
            case '`':
            case '\'':
            case '"':
                return ++offset;
            }
            return offset;
        }
        return -1;
    }

    /**
     * select @@identity<br/>
     * select @@identiTy aS iD
     */
    static int identityCheck(ByteString stmt, int offset) {
        offset = indexAfterIdentity(stmt, offset);
        if (offset < 0) {
            return OTHER;
        }
        offset = skipAs(stmt, offset);
        offset = skipAlias(stmt, offset);
        if (offset < 0) {
            return OTHER;
        }
        offset = ParseUtil.move(stmt, offset, 0);
        if (offset < stmt.length()) {
            return OTHER;
        }
        return IDENTITY;
    }

    static int select2Check(ByteString stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@') {
            if (stmt.length() > ++offset) {
                switch (stmt.charAt(offset)) {
                case 'V':
                case 'v':
                    return versionCommentCheck(stmt, offset);
                case 'i':
                case 'I':
                    return identityCheck(stmt, offset);
                case 'S':
                case 's':
                    return select2SCheck(stmt, offset);
                default:
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    /**
     * SELECT DATABASE()
     */
    static int databaseCheck(ByteString stmt, int offset) {
        int length = offset + _DATABASE.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, _DATABASE)) {
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            } else {
                return DATABASE;
            }
        }
        return OTHER;
    }

    /**
     * SELECT USER()
     */
    static int userCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r') && (c4 == '(')
                && (c5 == ')') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return USER;
            }
        }
        return OTHER;
    }

    /**
     * SELECT @@VERSION_COMMENT
     */
    static int versionCommentCheck(ByteString stmt, int offset) {
        int length = offset + _VERSION_COMMENT.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, _VERSION_COMMENT)) {
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            } else {
                return VERSION_COMMENT;
            }
        }
        return OTHER;
    }

    private static int lastTxcXid(ByteString stmt, int offset, Object[] exData) {

        if (stmt.length() > offset + "AST_TXC_XID".length()) {
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
            char c11 = stmt.charAt(++offset);

            if ((c1 == 'A' || c1 == 'a') && (c2 == 'S' || c2 == 's') && (c3 == 'T' || c3 == 't') && (c4 == '_')
                && (c5 == 'T' || c5 == 't') && (c6 == 'X' || c6 == 'x') && (c7 == 'C' || c7 == 'c') && (c8 == '_')
                && (c9 == 'X' || c9 == 'x') && (c10 == 'I' || c10 == 'i' && (c11 == 'D' || c11 == 'd'))) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    case '(':
                        return lastTxcXidParenthesisCheck(stmt, offset, exData);
                    default:
                        return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    private static int cCheck(ByteString stmt, int offset) {
        if (ParseUtil.compare(stmt, offset, _CURRENT_TRANS_ID)) {
            offset = ParseUtil.move(stmt, offset + _CURRENT_TRANS_ID.length, 0);
            if (offset + 1 < stmt.length() && stmt.charAt(offset) == '(') {
                offset = ParseUtil.move(stmt, offset + 1, 0);
                if (offset < stmt.length() && stmt.charAt(offset) == ')') {
                    return CURRENT_TRANS_ID;
                }
            }
        } else if (ParseUtil.compare(stmt, offset, _CURRENT_TRANS_POLICY)) {
            offset = ParseUtil.move(stmt, offset + _CURRENT_TRANS_POLICY.length, 0);
            if (offset + 1 < stmt.length() && stmt.charAt(offset) == '(') {
                offset = ParseUtil.move(stmt, offset + 1, 0);
                if (offset < stmt.length() && stmt.charAt(offset) == ')') {
                    return CURRENT_TRANS_POLICY;
                }
            }
        } else if (ParseUtil.compare(stmt, offset, _COMPATIBILITY_LEVEL)) {
            return COMPATIBILITY_LEVEL;
        }
        return OTHER;
    }

    private static int lastTxcXidParenthesisCheck(ByteString stmt, int offset, Object[] exData) {
        char[] timeout = new char[16];
        int idx = 0;

        while (stmt.length() > ++offset) {
            char c = stmt.charAt(offset);
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                continue;
            } else if (c >= '0' && c <= '9') {
                if (idx < timeout.length) {
                    timeout[idx++] = c;
                }
                continue;
            } else if (c == ')') {
                while (stmt.length() > ++offset) {
                    char cc = stmt.charAt(offset);
                    if (cc == ' ' || cc == '\t' || cc == '\r' || cc == '\n'
                        || cc == ';' /* navicat会发过来;必须处理 */) {
                        continue;
                    } else {
                        return OTHER;
                    }
                }
                if (exData != null) {
                    String val = new String(timeout).trim();
                    if (!StringUtils.isEmpty(val)) {
                        exData[0] = new Long(val);
                        if ((Long) exData[0] > 999999999999999L) {
                            exData[0] = 999999999999999L;
                        }
                    }
                }
                return LASTTXCXID;
            } else {
                return OTHER;
            }
        }
        return OTHER;
    }

    // SESSION.TX_READ_ONLY, SESSION.TRANSACTION_READ_ONLY
    static int select2SCheck(ByteString stmt, int offset) {
        if (ParseUtil.compare(stmt, offset, _SESSION_TX_READ_ONLY)) {
            int length = offset + _SESSION_TX_READ_ONLY.length;
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            }
            return SESSION_TX_READ_ONLY;
        } else if (ParseUtil.compare(stmt, offset, _SESSION_TRANSACTION_READ_ONLY)) {
            int length = offset + _SESSION_TRANSACTION_READ_ONLY.length;
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            }
            return SESSION_TX_READ_ONLY;
        } else if (ParseUtil.compare(stmt, offset, _SESSION_TRANSACTION_READ_ONLY)) {
            int length = offset + _SESSION_TRANSACTION_READ_ONLY.length;
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            }
            return SESSION_TRANSACTION_READ_ONLY;
        }
        return OTHER;
    }

    // SELECT 123
    private static int selectLiteralNumberCheck(ByteString stmt, int offset, Object[] exData) {
        StringBuilder num = new StringBuilder();
        while (offset < stmt.length()) {
            char c = stmt.charAt(offset++);
            if (c >= '0' && c <= '9') {
                num.append(c);
            } else {
                break;
            }
        }

        while (offset < stmt.length()) {
            char c = stmt.charAt(offset++);
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == ';') {
                continue;
            } else {
                return OTHER;
            }
        }

        try {
            exData[0] = Long.parseLong(num.toString());
            return LITERAL_NUMBER;
        } catch (NumberFormatException e) {
            return OTHER;
        }
    }

    /**
     * SELECT TSO_TIMESTAMP()
     */
    static int tsoTimestampCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "SO_TIMESTAMP()".length()) {
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
            char c11 = stmt.charAt(++offset);
            char c12 = stmt.charAt(++offset);
            char c13 = stmt.charAt(++offset);
            char c14 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'O' || c2 == 'o') && (c3 == '_') && (c4 == 'T' || c4 == 't')
                && (c5 == 'I' || c5 == 'i') && (c6 == 'M' || c6 == 'm') && (c7 == 'E' || c7 == 'e') && (c8 == 'S'
                || c8 == 's')
                && (c9 == 'T' || c9 == 't') && (c10 == 'A' || c10 == 'a') && (c11 == 'M' || c11 == 'm') && (c12 == 'P'
                || c12 == 'p')
                && (c13 == '(') && (c14 == ')')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return TSO_TIMESTAMP;
            }
        }
        return OTHER;
    }

    private static int extractTraceIdCheck(ByteString stmt, int offset, Object[] exData) {
        if (ParseUtil.compare(stmt, offset, _EXTRACT_TRACE_ID)) {
            int length = offset + _EXTRACT_TRACE_ID.length;
            if (length == stmt.length() || stmt.charAt(length) != '(' || stmt.charAt(stmt.length() - 1) != ')') {
                return OTHER;
            }
            exData[0] = Long.parseLong(stmt.substring(length + 1, stmt.length() - 1), 16);
            return EXTRACT_TRACE_ID;
        }
        return OTHER;
    }

    private static int extractEncdbProcessMessage(ByteString stmt, int offset, Object[] exData) {
        if (ParseUtil.compare(stmt, offset, _ENCDB_PROCESS_MESSAGE)) {
            int length = offset + _ENCDB_PROCESS_MESSAGE.length;
            if (length == stmt.length() || stmt.charAt(length) != '(' || stmt.charAt(stmt.length() - 1) != ')') {
                return OTHER;
            }
            String str = TStringUtil.trim(stmt.substring(length + 1, stmt.length() - 1));
            if (TStringUtil.isBlank(str)) {
                return OTHER;
            }
            if ((str.startsWith("\"") && str.endsWith("\""))
                || (str.startsWith("'")) && str.endsWith("'")) {
                str = str.substring(1, str.length() - 1);
            }
            exData[0] = str;
            return ENCDB_PROCESS_MESSAGE;
        }
        return OTHER;
    }

    private static int parseSeqBenchmark(ByteString stmt, int offset, Object[] exData) {
        boolean isNextval;
        int length;

        if (ParseUtil.compare(stmt, offset, _SEQ_NEXTVAL_BENCHMARK)) {
            length = offset + _SEQ_NEXTVAL_BENCHMARK.length;
            isNextval = true;
        } else if (ParseUtil.compare(stmt, offset, _SEQ_SKIP_BENCHMARK)) {
            length = offset + _SEQ_SKIP_BENCHMARK.length;
            isNextval = false;
        } else {
            return OTHER;
        }

        if (length == stmt.length() || stmt.charAt(length) != '(' || stmt.charAt(stmt.length() - 1) != ')') {
            return OTHER;
        }

        String paramList = stmt.substring(length + 1, stmt.length() - 1);
        if (TStringUtil.isBlank(paramList)) {
            return OTHER;
        }

        String[] params = paramList.split(",");
        if (params.length < 2) {
            return OTHER;
        }

        exData[0] = TStringUtil.trim(params[0]);
        exData[1] = Integer.parseInt(TStringUtil.trim(params[1]));

        return isNextval ? SEQ_NEXTVAL_BENCHMARK : SEQ_SKIP_BENCHMARK;
    }
}

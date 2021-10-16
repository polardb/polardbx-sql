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
 * @author xianmao.hexm 2011-5-7 下午01:22:57
 */
public final class ServerParseSet {

    public static final int OTHER = -1;
    public static final int AUTOCOMMIT_ON = 1;
    public static final int AUTOCOMMIT_OFF = 2;
    public static final int TX_READ_UNCOMMITTED = 3;
    public static final int TX_READ_COMMITTED = 4;
    public static final int TX_REPEATABLE_READ = 5;
    public static final int TX_SERIALIZABLE = 6;
    public static final int NAMES = 7;
    public static final int CHARACTER_SET_CLIENT = 8;
    public static final int CHARACTER_SET_CONNECTION = 9;
    public static final int CHARACTER_SET_RESULTS = 10;
    public static final int SQL_MODE = 11;

    /**
     * 完全禁止跨库事务
     */
    public static final int TX_POLICY_1 = 12;

    /**
     * autocommit=true时跨库写在多个库上commit
     */
    public static final int TX_POLICY_2 = 13;

    /**
     * 禁止跨库写，允许跨库读
     */
    public static final int TX_POLICY_3 = 14;

    /**
     * policy_2的基础上允许跨库读
     */
    public static final int TX_POLICY_4 = 15;

    /**
     * 完全允许跨库事务
     */
    public static final int TX_POLICY_5 = 16;

    public static final int AT_VAR = 17;

    public static int parse(String stmt, int offset) {
        return parse(ByteString.from(stmt), offset);
    }

    public static int parse(ByteString stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
            case ' ':
            case '\r':
            case '\n':
            case '\t':
                continue;
            case '/':
            case '#':
                i = ParseUtil.comment(stmt, i);
                continue;
            case 'A':
            case 'a':
                return autocommit(stmt, i);
            case 'C':
            case 'c':
                return characterSet(stmt, i, 0);
            case 'N':
            case 'n':
                return names(stmt, i);
            case 'S':
            case 's':
                return sCheck(stmt, i);
            case 'T':
            case 't':
                return transaction(stmt, i);
            case '@':
                /* // SET @PARAM = */
                return atVar(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // SET AUTOCOMMIT(' '=)
    private static int autocommit(ByteString stmt, int offset) {
        if (stmt.length() > offset + 9) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'T' || c2 == 't') && (c3 == 'O' || c3 == 'o')
                && (c4 == 'C' || c4 == 'c') && (c5 == 'O' || c5 == 'o') && (c6 == 'M' || c6 == 'm')
                && (c7 == 'M' || c7 == 'm') && (c8 == 'I' || c8 == 'i') && (c9 == 'T' || c9 == 't')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\t':
                        continue;
                    case '=':
                        return autocommitValue(stmt, offset);
                    default:
                        return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    private static int autocommitValue(ByteString stmt, int offset) {
        for (; ; ) {
            offset++;
            if (stmt.length() <= offset) {
                return OTHER;
            }
            switch (stmt.charAt(offset)) {
            case ' ':
            case '\r':
            case '\n':
            case '\t':
                continue;
            case '1':
                if (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset))) {
                    return AUTOCOMMIT_ON;
                } else {
                    return OTHER;
                }
            case '0':
                if (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset))) {
                    return AUTOCOMMIT_OFF;
                } else {
                    return OTHER;
                }
            case 'O':
            case 'o':
                return autocommitOn(stmt, offset);
            case 'T':
            case 't':
                return autocommitTrue(stmt, offset);
            case 'F':
            case 'f':
                return autocommitFalse(stmt, offset);
            default:
                return OTHER;
            }
        }
    }

    private static int autocommitOn(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'N':
            case 'n':
                if (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset))) {
                    return AUTOCOMMIT_ON;
                } else {
                    return OTHER;
                }
            case 'F':
            case 'f':
                return autocommitOff(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // SET AUTOCOMMIT = TRUE
    private static int autocommitTrue(ByteString stmt, int offset) {
        if (stmt.length() > offset + "RUE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'U' || c2 == 'u') && (c3 == 'E' || c3 == 'e')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return AUTOCOMMIT_ON;
            }
        }

        return OTHER;
    }

    // SET AUTOCOMMIT = OFF
    private static int autocommitOff(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'F':
            case 'f':
                if (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset))) {
                    return AUTOCOMMIT_OFF;
                } else {
                    return OTHER;
                }
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // SET AUTOCOMMIT = FALSE
    private static int autocommitFalse(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ALSE".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'L' || c2 == 'l') && (c3 == 'S' || c3 == 's')
                && (c4 == 'E' || c4 == 'e') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return AUTOCOMMIT_OFF;
            }
        }

        return OTHER;
    }

    // SET NAMES' '
    private static int names(ByteString stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'M' || c2 == 'm') && (c3 == 'E' || c3 == 'e')
                && (c4 == 'S' || c4 == 's') && stmt.charAt(++offset) == ' ') {
                return (offset << 8) | NAMES;
            }
        }
        return OTHER;
    }

    // SET @PARAM =
    private static int atVar(ByteString stmt, int offset) {
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            if (c1 != '@' && !Character.isDigit(c1)) {
                return (offset << 8) | AT_VAR;
            }
        }

        return OTHER;
    }

    // SET CHARACTER_SET_
    private static int characterSet(ByteString stmt, int offset, int depth) {
        if (stmt.length() > offset + 14) {
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
            if ((c1 == 'H' || c1 == 'h') && (c2 == 'A' || c2 == 'a') && (c3 == 'R' || c3 == 'r')
                && (c4 == 'A' || c4 == 'a') && (c5 == 'C' || c5 == 'c') && (c6 == 'T' || c6 == 't')
                && (c7 == 'E' || c7 == 'e') && (c8 == 'R' || c8 == 'r') && (c9 == '_') && (c10 == 'S' || c10 == 's')
                && (c11 == 'E' || c11 == 'e') && (c12 == 'T' || c12 == 't') && (c13 == '_')) {
                switch (c14) {
                case 'R':
                case 'r':
                    return characterSetResults(stmt, offset);
                case 'C':
                case 'c':
                    return characterSetC(stmt, offset);
                default:
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SET CHARACTER_SET_RESULTS =
    private static int characterSetResults(ByteString stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's') && (c3 == 'U' || c3 == 'u')
                && (c4 == 'L' || c4 == 'l') && (c5 == 'T' || c5 == 't') && (c6 == 'S' || c6 == 's')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\t':
                        continue;
                    case '=':
                        while (stmt.length() > ++offset) {
                            switch (stmt.charAt(offset)) {
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                                continue;
                            default:
                                return (offset << 8) | CHARACTER_SET_RESULTS;
                            }
                        }
                        return OTHER;
                    default:
                        return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SET CHARACTER_SET_C
    private static int characterSetC(ByteString stmt, int offset) {
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            switch (c1) {
            case 'o':
            case 'O':
                return characterSetConnection(stmt, offset);
            case 'l':
            case 'L':
                return characterSetClient(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // SET CHARACTER_SET_CONNECTION =
    private static int characterSetConnection(ByteString stmt, int offset) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'N' || c2 == 'n') && (c3 == 'E' || c3 == 'e')
                && (c4 == 'C' || c4 == 'c') && (c5 == 'T' || c5 == 't') && (c6 == 'I' || c6 == 'i')
                && (c7 == 'O' || c7 == 'o') && (c8 == 'N' || c8 == 'n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\t':
                        continue;
                    case '=':
                        while (stmt.length() > ++offset) {
                            switch (stmt.charAt(offset)) {
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                                continue;
                            default:
                                return (offset << 8) | CHARACTER_SET_CONNECTION;
                            }
                        }
                        return OTHER;
                    default:
                        return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SET CHARACTER_SET_CLIENT =
    private static int characterSetClient(ByteString stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'E' || c2 == 'e') && (c3 == 'N' || c3 == 'n')
                && (c4 == 'T' || c4 == 't')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\t':
                        continue;
                    case '=':
                        while (stmt.length() > ++offset) {
                            switch (stmt.charAt(offset)) {
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                                continue;
                            default:
                                return (offset << 8) | CHARACTER_SET_CLIENT;
                            }
                        }
                        return OTHER;
                    default:
                        return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SET SESSION' '
    // SET SQL_MODE
    private static int sCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 1) {
            int nextOffset = offset + 1;
            char c = stmt.charAt(nextOffset);
            if (c == 'E' || c == 'e') {
                return session(stmt, offset);
            } else if (c == 'Q' || c == 'q') {
                return sqlMode(stmt, offset);
            }
        }
        return OTHER;
    }

    // SET [SESSION] SQL_MODE = "mode"
    private static int sqlMode(ByteString stmt, int offset) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);

            if ((c1 == 'Q' || c1 == 'q') && (c2 == 'L' || c2 == 'l') && (c3 == '_') && (c4 == 'M' || c4 == 'm')
                && (c5 == 'O' || c5 == 'o') && (c6 == 'D' || c6 == 'd') && (c7 == 'E' || c7 == 'e')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\t':
                        continue;
                    case '=':
                        while (stmt.length() > ++offset) {
                            switch (stmt.charAt(offset)) {
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                                continue;
                            default:
                                return (offset << 8) | SQL_MODE;
                            }
                        }
                        return OTHER;
                    default:
                        return OTHER;
                    }
                }
            }
            return OTHER;
        }
        return OTHER;
    }

    private static int session(ByteString stmt, int offset) {
        if (stmt.length() > offset + 7) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's') && (c3 == 'S' || c3 == 's')
                && (c4 == 'I' || c4 == 'i') && (c5 == 'O' || c5 == 'o') && (c6 == 'N' || c6 == 'n')
                && stmt.charAt(++offset) == ' ') {
                for (; ; ) {
                    if (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'T':
                        case 't':
                            return transaction(stmt, offset);
                        case 'S':
                        case 's':
                            return sqlMode(stmt, offset);
                        default:
                            return OTHER;
                        }
                    }
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SET [SESSION] TRANSACTION ISOLATION LEVEL
    private static int transaction(ByteString stmt, int offset) {
        if (stmt.length() > offset + 11) {
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
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'A' || c2 == 'a') && (c3 == 'N' || c3 == 'n')
                && (c4 == 'S' || c4 == 's') && (c5 == 'A' || c5 == 'a') && (c6 == 'C' || c6 == 'c')
                && (c7 == 'T' || c7 == 't') && (c8 == 'I' || c8 == 'i') && (c9 == 'O' || c9 == 'o')
                && (c10 == 'N' || c10 == 'n') && stmt.charAt(++offset) == ' ') {
                for (; ; ) {
                    if (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;

                        case 'P':
                        case 'p':
                            return policy(stmt, offset);
                        case 'I':
                        case 'i':
                            return isolation(stmt, offset);
                        default:
                            return OTHER;
                        }
                    }
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // set transaction policy 3
    private static int policy(ByteString stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);

            if ((c1 == 'O' || c1 == 'o') && (c2 == 'L' || c2 == 'l') && (c3 == 'I' || c3 == 'i')
                && (c4 == 'C' || c4 == 'c') && (c5 == 'Y' || c5 == 'y') && stmt.charAt(++offset) == ' ') {
                for (; ; ) {
                    if (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case '1':
                            return TX_POLICY_1;
                        case '2':
                            return TX_POLICY_2;
                        case '3':
                            return TX_POLICY_3;
                        case '4':
                            return TX_POLICY_4;
                        case '5':
                            return TX_POLICY_5;
                        default:
                            return OTHER;
                        }
                    }
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SET [SESSION] TRANSACTION ISOLATION LEVEL
    private static int isolation(ByteString stmt, int offset) {
        if (stmt.length() > offset + 9) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'O' || c2 == 'o') && (c3 == 'L' || c3 == 'l')
                && (c4 == 'A' || c4 == 'a') && (c5 == 'T' || c5 == 't') && (c6 == 'I' || c6 == 'i')
                && (c7 == 'O' || c7 == 'o') && (c8 == 'N' || c8 == 'n') && stmt.charAt(++offset) == ' ') {
                for (; ; ) {
                    if (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'L':
                        case 'l':
                            return level(stmt, offset);
                        default:
                            return OTHER;
                        }
                    }
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SET [SESSION] TRANSACTION ISOLATION LEVEL' '
    private static int level(ByteString stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'V' || c2 == 'v') && (c3 == 'E' || c3 == 'e')
                && (c4 == 'L' || c4 == 'l') && stmt.charAt(++offset) == ' ') {
                for (; ; ) {
                    if (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'R':
                        case 'r':
                            return rCheck(stmt, offset);
                        case 'S':
                        case 's':
                            return serializable(stmt, offset);
                        default:
                            return OTHER;
                        }
                    }
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SET [SESSION] TRANSACTION ISOLATION LEVEL SERIALIZABLE
    private static int serializable(ByteString stmt, int offset) {
        if (stmt.length() > offset + 11) {
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
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'R' || c2 == 'r') && (c3 == 'I' || c3 == 'i')
                && (c4 == 'A' || c4 == 'a') && (c5 == 'L' || c5 == 'l') && (c6 == 'I' || c6 == 'i')
                && (c7 == 'Z' || c7 == 'z') && (c8 == 'A' || c8 == 'a') && (c9 == 'B' || c9 == 'b')
                && (c10 == 'L' || c10 == 'l') && (c11 == 'E' || c11 == 'e')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return TX_SERIALIZABLE;
            }
        }
        return OTHER;
    }

    // READ' '|REPEATABLE
    private static int rCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'E':
            case 'e':
                return eCheck(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // READ' '|REPEATABLE
    private static int eCheck(ByteString stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case 'A':
            case 'a':
                return aCheck(stmt, offset);
            case 'P':
            case 'p':
                return pCheck(stmt, offset);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    // READ' '
    private static int aCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            if ((c1 == 'D' || c1 == 'd') && stmt.charAt(++offset) == ' ') {
                for (; ; ) {
                    if (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'C':
                        case 'c':
                            return committed(stmt, offset);
                        case 'U':
                        case 'u':
                            return uncommitted(stmt, offset);
                        default:
                            return OTHER;
                        }
                    }
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // COMMITTED
    private static int committed(ByteString stmt, int offset) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'M' || c2 == 'm') && (c3 == 'M' || c3 == 'm')
                && (c4 == 'I' || c4 == 'i') && (c5 == 'T' || c5 == 't') && (c6 == 'T' || c6 == 't')
                && (c7 == 'E' || c7 == 'e') && (c8 == 'D' || c8 == 'd')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return TX_READ_COMMITTED;
            }
        }
        return OTHER;
    }

    // UNCOMMITTED
    private static int uncommitted(ByteString stmt, int offset) {
        if (stmt.length() > offset + 10) {
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
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'C' || c2 == 'c') && (c3 == 'O' || c3 == 'o')
                && (c4 == 'M' || c4 == 'm') && (c5 == 'M' || c5 == 'm') && (c6 == 'I' || c6 == 'i')
                && (c7 == 'T' || c7 == 't') && (c8 == 'T' || c8 == 't') && (c9 == 'E' || c9 == 'e')
                && (c10 == 'D' || c10 == 'd') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return TX_READ_UNCOMMITTED;
            }
        }
        return OTHER;
    }

    // REPEATABLE
    private static int pCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a') && (c3 == 'T' || c3 == 't')
                && (c4 == 'A' || c4 == 'a') && (c5 == 'B' || c5 == 'b') && (c6 == 'L' || c6 == 'l')
                && (c7 == 'E' || c7 == 'e') && stmt.charAt(++offset) == ' ') {
                for (; ; ) {
                    if (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'R':
                        case 'r':
                            return prCheck(stmt, offset);
                        default:
                            return OTHER;
                        }
                    }
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // READ
    private static int prCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a') && (c3 == 'D' || c3 == 'd')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return TX_REPEATABLE_READ;
            }
        }
        return OTHER;
    }

}

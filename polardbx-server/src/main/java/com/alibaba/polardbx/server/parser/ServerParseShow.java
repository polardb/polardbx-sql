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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author xianmao.hexm 2011-5-7 下午01:23:06
 */
public final class ServerParseShow {

    public static final int OTHER = -1;
    public static final int DATABASES = 1;
    public static final int DATASOURCES = 2;
    public static final int COBAR_STATUS = 3;
    public static final int COBAR_CLUSTER = 4;
    public static final int SLOW = 5;
    public static final int PHYSICAL_SLOW = 6;
    public static final int CONNECTION = 7;
    public static final int NODE = 8;
    public static final int WARNINGS = 9;
    public static final int ERRORS = 10;
    public static final int HELP = 11;
    public static final int GIT_COMMIT = 12;
    public static final int INTERNAL_HELP = 17;
    public static final int STATISTIC = 17;
    public static final int MEMORYPOOL = 18;
    public static final int STORAGE = 19;
    public static final int MPP = 20;
    public static final int STORAGE_REPLICAS = 21;
    public static final int MDL_DEADLOCK_DETECTION = 22;
    public static final int WORKLOAD = 23;
    public static final int PARAMETRIC = 24;
    public static final int CACHE_STATS = 25;
    public static final int ARCHIVE = 26;
    public static final int FILE_STORAGE = 27;
    public static final int FULL_DATABASES = 28;
    public static final int PROCEDURE_CACHE = 29;

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
            case 'A':
            case 'a':
                return aCheck(stmt, i);
            case 'C':
            case 'c':
                return cCheck(stmt, i);
            case 'D':
            case 'd':
                return dataCheck(stmt, i);
            case 'N':
            case 'n':
                return nodeCheck(stmt, i);
            case 'S':
            case 's':
                return sCheck(stmt, i);
            case 'p':
            case 'P':
                return pCheck(stmt, i);
            case 'W':
            case 'w':
                return warningsCheck(stmt, i);
            case 'E':
            case 'e':
                return errorsCheck(stmt, i);
            case 'H':
            case 'h':
                return helpCheck(stmt, i);
            case 'G':
            case 'g':
                return gitCheck(stmt, i);
            case 'F':
            case 'f':
                return fCheck(stmt, i);
            case 'M':
            case 'm':
                return memoryPoolCheck(stmt, i);
            case 'I':
            case 'i':
                return internalHelpCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int gitCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "IT_COMMIT".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            if ((c1 == 'i' || c1 == 'I') &&
                (c2 == 't' || c2 == 'T') &&
                c3 == '_' &&
                (c4 == 'c' || c4 == 'C') &&
                (c5 == 'o' || c5 == 'O') &&
                (c6 == 'm' || c6 == 'M') &&
                (c7 == 'm' || c7 == 'M') &&
                (c8 == 'i' || c8 == 'I') &&
                (c9 == 't' || c9 == 'T') &&
                (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return GIT_COMMIT;
            }
        }
        return OTHER;
    }

    private static int aCheck(ByteString stmt, int offset) {
        final String expect = "archive";
        if (stmt.length() >= offset + expect.length()) {
            if (stmt.substring(offset, offset + expect.length()).equalsIgnoreCase(expect)
                && (stmt.length() == offset + expect.length() ||
                ParseUtil.isEOF(stmt.charAt(offset + expect.length())))) {
                return ARCHIVE;
            }
        }
        return OTHER;
    }

    static int cCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ACHE_STATS".length()) {
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
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'C' || c2 == 'c') && (c3 == 'H' || c3 == 'h')
                && (c4 == 'E' || c4 == 'e') && (c5 == '_') && (c6 == 'S' || c6 == 's') && (c7 == 'T' || c7 == 't')
                && (c8 == 'A' || c8 == 'a') && (c9 == 'T' || c9 == 't') && (c10 == 'S' || c10 == 's')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {

                return CACHE_STATS;

            }
        } else if (stmt.length() > offset + "ONNECTION".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'N' || c2 == 'n') && (c3 == 'N' || c3 == 'n')
                && (c4 == 'E' || c4 == 'e') && (c5 == 'C' || c5 == 'c') && (c6 == 'T' || c6 == 't')
                && (c7 == 'I' || c7 == 'i') && (c8 == 'O' || c8 == 'o') && (c9 == 'N' || c9 == 'n')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {

                return CONNECTION;

            }
        }
        return OTHER;
    }

    private static int sCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(offset + 1)) {
            case 'l':
            case 'L':
                return slowCheck(stmt, offset);
            case 't':
            case 'T':
                if (stmt.length() > offset + 2) {
                    switch (stmt.charAt(offset + 2)) {
                    case 'a':
                    case 'A':
                        return statisticCheck(stmt, offset);
                    case 'o':
                    case 'O':
                        return storageCheck(stmt, offset);
                    }
                } else {
                    return OTHER;
                }
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int fCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(offset + 1)) {
            case 'u':
            case 'U':
                return fullCheck(stmt, offset);
            case 'i':
            case 'I':
                return fileStorageCheck(stmt, offset);
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

    private static int nodeCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ode".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'D' || c2 == 'd') && (c3 == 'E' || c3 == 'e')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return NODE;
            }
        }
        return OTHER;
    }

    private static int pCheck(ByteString stmt, int offset) {
        if (stmt.charAt(offset) == 'h' || stmt.charAt(offset) == 'H') {
            return physicalCheck(stmt, offset);
        } else {
            return parametricsCheck(stmt, offset);
        }
    }

    private static int parametricsCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "arametrics".length()) {
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
            if ((c1 == 'a' || c1 == 'A') && (c2 == 'r' || c2 == 'R') && (c3 == 'a' || c3 == 'A') && (c4 == 'm'
                || c4 == 'M') && (c5 == 'e' || c5 == 'E') && (c6 == 't' || c6 == 'T') && (c7 == 'r' || c7 == 'R') && (
                c8 == 'i' || c8 == 'I') && (c9 == 'c' || c9 == 'C') && (c10 == 's' || c10 == 'S')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return PARAMETRIC;
            }
        }
        return OTHER;
    }

    private static int physicalCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "hysical_slow".length()) {
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
            if ((c1 == 'h' || c1 == 'H') && (c2 == 'y' || c2 == 'Y') && (c3 == 's' || c3 == 'S')

                && (c4 == 'i' || c4 == 'I') && (c5 == 'c' || c5 == 'C') && (c6 == 'a' || c6 == 'A')
                && (c7 == 'l' || c7 == 'L') && (c8 == '_') && (c9 == 's' || c9 == 'S') && (c10 == 'l' || c10 == 'L')
                && (c11 == 'o' || c11 == 'O') && (c12 == 'w' || c12 == 'W')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return PHYSICAL_SLOW;

            }
        }
        return OTHER;
    }

    // SHOW COBAR_
    static int cobarCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "obar_?".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'B' || c2 == 'b') && (c3 == 'A' || c3 == 'a')
                && (c4 == 'R' || c4 == 'r') && (c5 == '_')) {
                switch (stmt.charAt(++offset)) {
                case 'S':
                case 's':
                    return showCobarStatus(stmt, offset);
                case 'C':
                case 'c':
                    return showCobarCluster(stmt, offset);
                default:
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SHOW COBAR_STATUS
    static int showCobarStatus(ByteString stmt, int offset) {
        if (stmt.length() > offset + "tatus".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 't' || c1 == 'T') && (c2 == 'a' || c2 == 'A') && (c3 == 't' || c3 == 'T')
                && (c4 == 'u' || c4 == 'U') && (c5 == 's' || c5 == 'S')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return COBAR_STATUS;
            }
        }
        return OTHER;
    }

    // SHOW COBAR_CLUSTER
    static int showCobarCluster(ByteString stmt, int offset) {
        if (stmt.length() > offset + "luster".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'U' || c2 == 'u') && (c3 == 'S' || c3 == 's')
                && (c4 == 'T' || c4 == 't') && (c5 == 'E' || c5 == 'e') && (c6 == 'R' || c6 == 'r')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return COBAR_CLUSTER;
            }
        }
        return OTHER;
    }

    // SHOW DATA
    static int dataCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ata?".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a')) {
                switch (stmt.charAt(++offset)) {
                case 'B':
                case 'b':
                    return showDatabases(stmt, offset);
                case 'S':
                case 's':
                    return showDataSources(stmt, offset);
                default:
                    return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SHOW DATABASES
    static int showDatabases(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ases".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'S' || c2 == 's') && (c3 == 'E' || c3 == 'e')
                && (c4 == 'S' || c4 == 's') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return DATABASES;
            }
        }
        return OTHER;
    }

    // SHOW DATASOURCES
    static int showDataSources(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ources".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'U' || c2 == 'u') && (c3 == 'R' || c3 == 'r')
                && (c4 == 'C' || c4 == 'c') && (c5 == 'E' || c5 == 'e') && (c6 == 'S' || c6 == 's')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return DATASOURCES;
            }
        }
        return OTHER;
    }

    // SHOW WARNINGS
    static int warningsCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ARNINGS".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r') && (c3 == 'N' || c3 == 'n')
                && (c4 == 'I' || c4 == 'i') && (c5 == 'N' || c5 == 'n') && (c6 == 'G' || c6 == 'g')
                && (c7 == 'S' || c7 == 's') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return WARNINGS;
            }

            if ((c1 == 'O' || c1 == 'o') && (c2 == 'R' || c2 == 'r') && (c3 == 'K' || c3 == 'k')
                && (c4 == 'L' || c4 == 'l') && (c5 == 'O' || c5 == 'o') && (c6 == 'A' || c6 == 'a')
                && (c7 == 'D' || c7 == 'd') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return WORKLOAD;
            }
        }
        return OTHER;
    }

    // SHOW ERRORS
    static int errorsCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "RRORS".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'R' || c2 == 'r') && (c3 == 'O' || c3 == 'o')
                && (c4 == 'R' || c4 == 'r') && (c5 == 'S' || c5 == 's')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return ERRORS;

            }
        }
        return OTHER;
    }

    // SHOW HELP
    static int helpCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ELP".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'e' || c1 == 'E') && (c2 == 'l' || c2 == 'L') && (c3 == 'p' || c3 == 'P')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return HELP;

            }
        }
        return OTHER;
    }

    // SHOW FULL
    private static int fullCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ULL ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'L' || c2 == 'l') && (c3 == 'L' || c3 == 'l')
                && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        continue;
                    case 'D':
                    case 'd':
                    {
                        int res = dataCheck(stmt, offset);
                        if (res == DATABASES) {
                            return FULL_DATABASES;
                        } else {
                            return OTHER;
                        }
                    }

                    default:
                        return OTHER;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }

    // SHOW FILE STORAGE
    private static int fileStorageCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "ILE ".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'L' || c2 == 'l')
                && (c3 == 'E' || c3 == 'e') && (stmt.length() > ++offset)) {
                while (ParseUtil.isEOF(stmt.charAt(offset)) && stmt.charAt(offset) != ';') {
                    offset++;
                }
                if (stmt.length() > offset + "TORAGE".length()) {
                    char c11 = stmt.charAt(offset++);
                    char c12 = stmt.charAt(offset++);
                    char c13 = stmt.charAt(offset++);
                    char c14 = stmt.charAt(offset++);
                    char c15 = stmt.charAt(offset++);
                    char c16 = stmt.charAt(offset++);
                    char c17 = stmt.charAt(offset++);
                    if ((c11 == 's' || c11 == 'S') && (c12 == 't' || c12 == 'T')
                        && (c13 == 'o' || c13 == 'O') && (c14 == 'r' || c14 == 'R')
                        && (c15 == 'A' || c15 == 'a') && (c16 == 'g' || c16 == 'G')
                        && (c17 == 'e' || c17 == 'E')
                        && (stmt.length() == offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                        return FILE_STORAGE;
                    }
                }
            }
        }
        return OTHER;
    }

    // SHOW INTERNAL HELP
    private static int internalHelpCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "NTERNAL".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'n' || c1 == 'N') && (c2 == 't' || c2 == 'T') && (c3 == 'e' || c3 == 'E')
                && (c4 == 'r' || c4 == 'R') && (c5 == 'n' || c5 == 'N') && (c6 == 'a' || c6 == 'A')
                && (c7 == 'l' || c7 == 'L') && (stmt.length() > ++offset)) {
                while (ParseUtil.isEOF(stmt.charAt(offset)) && stmt.charAt(offset) != ';') {
                    offset++;
                }
                if (stmt.length() > offset + "ELP".length()) {
                    char c11 = stmt.charAt(offset++);
                    char c12 = stmt.charAt(offset++);
                    char c13 = stmt.charAt(offset++);
                    char c14 = stmt.charAt(offset++);
                    if ((c11 == 'h' || c11 == 'H') && (c12 == 'e' || c12 == 'E')
                        && (c13 == 'l' || c13 == 'L') && (c14 == 'p' || c14 == 'P')
                        && (stmt.length() == offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                        return INTERNAL_HELP;
                    }
                }
            }
        }
        return OTHER;
    }

    private static int memoryPoolCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "dl_deadlock_detection".length()) {
            // SHOW STORAGE REPLICAS
            final String identifier = "dl_deadlock_detection";
            ++offset;
            byte[] bs = stmt.getBytes(offset, Math.min(offset + identifier.length(), stmt.length()));
            for (int i = 0; i < bs.length; i++) {
                bs[i] = (byte) Character.toLowerCase((char) bs[i]);
            }
            if (Arrays.equals(identifier.getBytes(StandardCharsets.UTF_8), bs)) {
                return MDL_DEADLOCK_DETECTION;
            }
        }
        if (stmt.length() > offset + "emorypool".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            if ((c1 == 'e' || c1 == 'E') && (c2 == 'm' || c2 == 'M') && (c3 == 'o' || c3 == 'O')
                && (c4 == 'r' || c4 == 'R') && (c5 == 'y' || c5 == 'Y') && (c6 == 'p' || c6 == 'P')
                && (c7 == 'o' || c7 == 'O') && (c8 == 'o' || c8 == 'O') && (c9 == 'l' || c8 == 'L')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return MEMORYPOOL;
            }
        } else if (stmt.length() > offset + "pp".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if ((c1 == 'p' || c1 == 'P') && (c2 == 'p' || c2 == 'P')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return MPP;
            }
        }
        return OTHER;
    }

    private static int storageCheck(ByteString stmt, int offset) {
        if (stmt.length() > offset + "torage".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 't' || c1 == 'T') && (c2 == 'o' || c2 == 'O') && (c3 == 'r' || c3 == 'R')
                && (c4 == 'a' || c4 == 'A') && (c5 == 'g' || c5 == 'G') && (c6 == 'e' || c6 == 'E')) {
                while (++offset < stmt.length() && stmt.charAt(offset) == ' ') {
                }
                if ((stmt.length() == offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                    // SHOW STORAGE
                    return STORAGE;
                } else {
                    // SHOW STORAGE REPLICAS
                    final String identifier = "replicas";
                    byte[] bs = stmt.getBytes(offset, Math.min(offset + identifier.length(), stmt.length()));
                    for (int i = 0; i < bs.length; i++) {
                        bs[i] = (byte) Character.toLowerCase((char) bs[i]);
                    }
                    if (Arrays.equals(identifier.getBytes(StandardCharsets.UTF_8), bs)) {
                        return STORAGE_REPLICAS;
                    }
                }
            }
        }
        return OTHER;
    }
}

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

package com.alibaba.polardbx.common;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.common.SQLModeFlags.*;


public enum SQLMode {

    ALLOW_INVALID_DATES(MODE_INVALID_DATES),
    ANSI_QUOTES(MODE_ANSI_QUOTES),
    ERROR_FOR_DIVISION_BY_ZERO(MODE_ERROR_FOR_DIVISION_BY_ZERO),
    HIGH_NOT_PRECEDENCE(MODE_HIGH_NOT_PRECEDENCE),
    IGNORE_SPACE(MODE_IGNORE_SPACE),
    NO_AUTO_CREATE_USER(MODE_NO_AUTO_CREATE_USER),
    NO_AUTO_VALUE_ON_ZERO(MODE_NO_AUTO_VALUE_ON_ZERO),
    NO_BACKSLASH_ESCAPES(MODE_NO_BACKSLASH_ESCAPES),
    NO_DIR_IN_CREATE(MODE_NO_DIR_IN_CREATE),
    NO_ENGINE_SUBSTITUTION(MODE_NO_ENGINE_SUBSTITUTION),
    NO_FIELD_OPTIONS(MODE_NO_FIELD_OPTIONS),
    NO_KEY_OPTIONS(MODE_NO_KEY_OPTIONS),
    NO_TABLE_OPTIONS(MODE_NO_TABLE_OPTIONS),
    NO_UNSIGNED_SUBTRACTION(MODE_NO_UNSIGNED_SUBTRACTION),
    NO_ZERO_DATE(MODE_NO_ZERO_DATE),
    NO_ZERO_IN_DATE(MODE_NO_ZERO_IN_DATE),
    ONLY_FULL_GROUP_BY(MODE_ONLY_FULL_GROUP_BY),
    PAD_CHAR_TO_FULL_LENGTH(MODE_PAD_CHAR_TO_FULL_LENGTH),
    PIPES_AS_CONCAT(MODE_PIPES_AS_CONCAT),
    REAL_AS_FLOAT(MODE_REAL_AS_FLOAT),
    STRICT_ALL_TABLES(MODE_STRICT_ALL_TABLES),
    STRICT_TRANS_TABLES(MODE_STRICT_TRANS_TABLES);

    private static Multimap<String, SQLMode> combSQLModeMap = HashMultimap.create();

    private static final Map<String, Long> sqlModeFlagMap = new ConcurrentHashMap<>();

    public static final String ANSI = "ANSI";
    public static final String DB_2 = "DB2";
    public static final String MAXDB = "MAXDB";
    public static final String MSSQL = "MSSQL";
    public static final String ORACLE = "ORACLE";
    public static final String POSTGRESQL = "POSTGRESQL";
    public static final String TRADITIONAL = "TRADITIONAL";

    static {

        combSQLModeMap.put(ANSI, SQLMode.REAL_AS_FLOAT);
        combSQLModeMap.put(ANSI, SQLMode.PIPES_AS_CONCAT);
        combSQLModeMap.put(ANSI, SQLMode.ANSI_QUOTES);
        combSQLModeMap.put(ANSI, SQLMode.IGNORE_SPACE);
        combSQLModeMap.put(ANSI, SQLMode.ONLY_FULL_GROUP_BY);

        combSQLModeMap.put(DB_2, SQLMode.PIPES_AS_CONCAT);
        combSQLModeMap.put(DB_2, SQLMode.ANSI_QUOTES);
        combSQLModeMap.put(DB_2, SQLMode.IGNORE_SPACE);
        combSQLModeMap.put(DB_2, SQLMode.NO_KEY_OPTIONS);
        combSQLModeMap.put(DB_2, SQLMode.NO_TABLE_OPTIONS);
        combSQLModeMap.put(DB_2, SQLMode.NO_FIELD_OPTIONS);

        combSQLModeMap.put(MAXDB, SQLMode.PIPES_AS_CONCAT);
        combSQLModeMap.put(MAXDB, SQLMode.ANSI_QUOTES);
        combSQLModeMap.put(MAXDB, SQLMode.IGNORE_SPACE);
        combSQLModeMap.put(MAXDB, SQLMode.NO_KEY_OPTIONS);
        combSQLModeMap.put(MAXDB, SQLMode.NO_TABLE_OPTIONS);
        combSQLModeMap.put(MAXDB, SQLMode.NO_FIELD_OPTIONS);
        combSQLModeMap.put(MAXDB, SQLMode.NO_AUTO_CREATE_USER);

        combSQLModeMap.put(MSSQL, SQLMode.PIPES_AS_CONCAT);
        combSQLModeMap.put(MSSQL, SQLMode.ANSI_QUOTES);
        combSQLModeMap.put(MSSQL, SQLMode.IGNORE_SPACE);
        combSQLModeMap.put(MSSQL, SQLMode.NO_KEY_OPTIONS);
        combSQLModeMap.put(MSSQL, SQLMode.NO_TABLE_OPTIONS);
        combSQLModeMap.put(MSSQL, SQLMode.NO_FIELD_OPTIONS);

        combSQLModeMap.put(ORACLE, SQLMode.PIPES_AS_CONCAT);
        combSQLModeMap.put(ORACLE, SQLMode.ANSI_QUOTES);
        combSQLModeMap.put(ORACLE, SQLMode.IGNORE_SPACE);
        combSQLModeMap.put(ORACLE, SQLMode.NO_KEY_OPTIONS);
        combSQLModeMap.put(ORACLE, SQLMode.NO_TABLE_OPTIONS);
        combSQLModeMap.put(ORACLE, SQLMode.NO_FIELD_OPTIONS);
        combSQLModeMap.put(ORACLE, SQLMode.NO_AUTO_CREATE_USER);

        combSQLModeMap.put(POSTGRESQL, SQLMode.PIPES_AS_CONCAT);
        combSQLModeMap.put(POSTGRESQL, SQLMode.ANSI_QUOTES);
        combSQLModeMap.put(POSTGRESQL, SQLMode.IGNORE_SPACE);
        combSQLModeMap.put(POSTGRESQL, SQLMode.NO_KEY_OPTIONS);
        combSQLModeMap.put(POSTGRESQL, SQLMode.NO_TABLE_OPTIONS);
        combSQLModeMap.put(POSTGRESQL, SQLMode.NO_FIELD_OPTIONS);

        combSQLModeMap.put(TRADITIONAL, SQLMode.STRICT_TRANS_TABLES);
        combSQLModeMap.put(TRADITIONAL, SQLMode.STRICT_ALL_TABLES);
        combSQLModeMap.put(TRADITIONAL, SQLMode.NO_ZERO_IN_DATE);
        combSQLModeMap.put(TRADITIONAL, SQLMode.NO_ZERO_DATE);
        combSQLModeMap.put(TRADITIONAL, SQLMode.ERROR_FOR_DIVISION_BY_ZERO);
        combSQLModeMap.put(TRADITIONAL, SQLMode.NO_AUTO_CREATE_USER);
        combSQLModeMap.put(TRADITIONAL, SQLMode.NO_ENGINE_SUBSTITUTION);
    }

    public static boolean contains(String combKey, SQLMode sqlMode) {
        return combSQLModeMap.containsEntry(combKey, sqlMode);
    }

    public static boolean isCombSQLMode(String val) {
        return combSQLModeMap.keySet().contains(val);
    }

    public static boolean isBasicSQLMode(String val) {
        for (SQLMode sqlMode : values()) {
            if (valueOf(val) == sqlMode) {
                return true;
            }
        }

        return false;
    }

    public static long getCachedFlag(String sqlModeStr) {
        if (sqlModeStr == null) {
            return 0;
        }
        return sqlModeFlagMap.computeIfAbsent(sqlModeStr, SQLMode::convertToFlag);
    }

    public static long convertToFlag(String sqlModeStr) {
        if (sqlModeStr == null) {
            return 0;
        }
        long flag = 0L;

        for (SQLMode sqlMode : SQLMode.values()) {
            if (sqlModeStr.contains(sqlMode.name())) {
                flag |= sqlMode.sqlModeFlag;
            }
        }

        for (String combinedSqlMode : combSQLModeMap.keySet()) {
            if (sqlModeStr.contains(combinedSqlMode)) {
                for (SQLMode sqlMode : combSQLModeMap.get(combinedSqlMode)) {
                    flag |= sqlMode.sqlModeFlag;
                }
            }
        }

        return flag;
    }

    public static Set<SQLMode> convertFromFlag(long sqlModeFlag) {
        Set<SQLMode> sqlModes = new HashSet<>();
        for (SQLMode sqlMode : values()) {
            if ((sqlModeFlag & sqlMode.sqlModeFlag) != 0) {
                sqlModes.add(sqlMode);
            }
        }
        return sqlModes;
    }

    public static boolean isStrictMode(long sqlModeFlags) {
        return SQLModeFlags.check(sqlModeFlags,
            MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES);
    }

    private final long sqlModeFlag;

    SQLMode(long sqlModeFlag) {
        this.sqlModeFlag = sqlModeFlag;
    }

    public long getModeFlag() {
        return sqlModeFlag;
    }
}

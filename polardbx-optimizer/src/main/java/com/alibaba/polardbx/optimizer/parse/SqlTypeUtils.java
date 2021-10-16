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

package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.common.model.SqlType;

import java.util.EnumSet;

/**
 * @author chenghui.lch
 */
public class SqlTypeUtils {

    public static EnumSet<SqlType> INSERT_STATEMENT = EnumSet.of(SqlType.INSERT,
        SqlType.INSERT_INTO_SELECT,
        SqlType.MULTI_STATEMENT_INSERT);

    public static EnumSet<SqlType> UPDATE_STATEMENT = EnumSet.of(SqlType.UPDATE,
        SqlType.MULTI_UPDATE,
        SqlType.MULTI_STATEMENT_UPDATE);

    public static EnumSet<SqlType> DELETE_STATEMENT = EnumSet.of(SqlType.DELETE,
        SqlType.MULTI_DELETE,
        SqlType.MULTI_STATEMENT_DELETE);

    public static EnumSet<SqlType> REPLACE_STATEMENT = EnumSet.of(SqlType.REPLACE,
        SqlType.REPLACE_INTO_SELECT);

    public static EnumSet<SqlType> LOAD_STATEMENT = EnumSet.of(SqlType.LOAD);

    public static EnumSet<SqlType> DML_STATEMENT = combine(INSERT_STATEMENT,
        UPDATE_STATEMENT,
        DELETE_STATEMENT,
        REPLACE_STATEMENT,
        LOAD_STATEMENT);

    public static EnumSet<SqlType> SHOW_STATEMENT = EnumSet.of(SqlType.SHOW,
        SqlType.SHOW_SEQUENCES,
        SqlType.TDDL_SHOW,
        SqlType.SHOW_CHARSET,
        SqlType.DESC);

    /**
     * DQL
     */
    public static EnumSet<SqlType> SELECT_STATEMENT = EnumSet.of(SqlType.SELECT,
        SqlType.SELECT_WITHOUT_TABLE,
        SqlType.SELECT_FOR_UPDATE,
        SqlType.SELECT_FROM_UPDATE,
        SqlType.SELECT_UNION,
        SqlType.MULTI_STATEMENT);

    public static EnumSet<SqlType> combine(EnumSet<SqlType>... enumSets) {
        EnumSet<SqlType> result = EnumSet.noneOf(SqlType.class);
        for (int i = 0; i < enumSets.length; i++) {
            result.addAll(enumSets[i]);
        }
        return result;
    }

    public static boolean isDmlSqlType(SqlType sqlType) {
        if (DML_STATEMENT.contains(sqlType)) {
            return true;
        }
        return false;
    }

    public static boolean isSelectSqlType(SqlType sqlType) {
        if (SELECT_STATEMENT.contains(sqlType)) {
            return true;
        }
        return false;
    }

    public static boolean isDmlAndDqlSqlType(SqlType sqlType) {
        if (DML_STATEMENT.contains(sqlType) || SELECT_STATEMENT.contains(sqlType)) {
            return true;
        }
        return false;
    }

    public static boolean isShowSqlType(SqlType sqlType) {
        if (SHOW_STATEMENT.contains(sqlType)) {
            return true;
        }
        return false;
    }
}

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

package com.alibaba.polardbx.druid.sql.ast;

public enum SqlType {

    SELECT(0),
    INSERT(1),
    UPDATE(2),
    DELETE(3),
    SELECT_FOR_UPDATE(4),
    REPLACE(5),
    SHOW(11),
    EXPLAIN(16),
    DESC(18),
    SHOW_CONVERT_TABLE_MODE(19),

    /**
     * insert select
     */
    INSERT_INTO_SELECT(45),

    COMMIT(55),
    ROLLBACK(56),

    /**
     * ddl
     */

    CREATE(80),
    DROP(81),
    TRUNCATE(82),
    PURGE(83),
    ALTER(84),
    GENERIC_DDL(85),
    UNARCHIVE(86),
    ARCHIVE(86);

    private int i;

    SqlType(int i) {
        this.i = i;
    }

    public int getI() {
        return i;
    }

    public static SqlType getValueFromI(int i) {
        for (SqlType sqlType : SqlType.values()) {
            if (sqlType.getI() == i) {
                return sqlType;
            }
        }
        return null;
    }

    public static boolean isDML(SqlType sqlType) {
        return sqlType != null
            && (sqlType.equals(INSERT)
            || sqlType.equals(UPDATE)
            || sqlType.equals(DELETE)
            || sqlType.equals(REPLACE)
            || sqlType.equals(INSERT_INTO_SELECT));
    }

    public static boolean isDDL(SqlType sqlType) {
        return sqlType != null
            && (sqlType.equals(DROP)
            || sqlType.equals(CREATE)
            || sqlType.equals(TRUNCATE)
            || sqlType.equals(PURGE)
            || sqlType.equals(ALTER)
            || sqlType.equals(GENERIC_DDL)
            || sqlType.equals(UNARCHIVE));
    }

}

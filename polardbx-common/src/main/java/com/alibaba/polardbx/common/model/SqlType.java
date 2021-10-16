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

package com.alibaba.polardbx.common.model;

public enum SqlType {

    COMMENT(-1),

    SELECT(0),

    INSERT(1),

    UPDATE(2),

    DELETE(3),

    SELECT_FOR_UPDATE(4),

    REPLACE(5),

    TRUNCATE(6),

    CREATE(7),

    DROP(8),

    LOAD(9),

    MERGE(10),

    SHOW(11),

    ALTER(12),

    RENAME(13),

    DUMP(14),

    DEBUG(15),

    EXPLAIN(16),

    PROCEDURE(17),

    DESC(18),

    SELECT_WITHOUT_TABLE(20),

    CREATE_SEQUENCE(21), SHOW_SEQUENCES(22), GET_SEQUENCE(23), ALTER_SEQUENCE(24), DROP_SEQUENCE(25),

    TDDL_SHOW(26),

    SET(27), RELOAD(28),

    SELECT_UNION(29),

    CREATE_TABLE(30), DROP_TABLE(31), ALTER_TABLE(32), SAVE_POINT(33), SELECT_FROM_UPDATE(34),

    MULTI_DELETE(35), MULTI_UPDATE(36),

    CREATE_INDEX(37), DROP_INDEX(38), KILL(39), LOCK_TABLES(41), UNLOCK_TABLES(42), CHECK_TABLE(43),

    INSERT_INTO_SELECT(45),

    MULTI_STATEMENT(46),

    MULTI_STATEMENT_UPDATE(47), MULTI_STATEMENT_INSERT(48), MULTI_STATEMENT_DELETE(49),

    REPLACE_INTO_SELECT(50),

    RENAME_SEQUENCE(51),

    CHANGE_RULE_VERSION(52),

    INSPECT_RULE_VERSION(53),

    RESYNC_LOCAL_RULES(54),

    COMMIT(55),

    ROLLBACK(56),

    CLEAR_SEQ_CACHE(57), OPTIMIZE_TABLE(58),

    INSPECT_GROUP_SEQ_RANGE(59),

    ANALYZE_TABLE(60),

    GET_INFORMATION_SCHEMA(66), GET_SYSTEM_VARIABLE(67),

    XA(68),
    PURGE(69),
    SHOW_CHARSET(70),
    SHOW_INSTANCE_TYPE(71),
    CREATE_VIEW(80),

    GRANT(81),

    ASYNC_DDL(82),

    BASELINE(83),

    CHECK_GLOBAL_INDEX(84),

    MOVE_DATABASE(85),

    CREATE_CCL_RULE(86), DROP_CCL_RULE(87), SHOW_CCL_RULE(88), CLEAR_CCL_RULES(89);

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

}

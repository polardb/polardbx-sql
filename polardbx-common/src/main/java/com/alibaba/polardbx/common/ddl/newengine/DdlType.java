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

package com.alibaba.polardbx.common.ddl.newengine;

public enum DdlType {

    UNSUPPORTED,

    CREATE_TABLE,
    ALTER_TABLE,
    DROP_TABLE,
    RENAME_TABLE,
    TRUNCATE_TABLE,

    CREATE_INDEX,
    DROP_INDEX,

    CREATE_FUNCTION,
    DROP_FUNCTION,
    CREATE_PROCEDURE,
    DROP_PROCEDURE,
    ALTER_PROCEDURE,
    ALTER_FUNCTION,
    PUSH_DOWN_UDF,

    CREATE_VIEW,
    ALTER_VIEW,
    DROP_VIEW,
    CREATE_JAVA_FUNCTION,
    DROP_JAVA_FUNCTION,

    CREATE_GLOBAL_INDEX,
    ALTER_GLOBAL_INDEX,
    DROP_GLOBAL_INDEX,
    RENAME_GLOBAL_INDEX,
    CHECK_GLOBAL_INDEX,
    CHECK_COLUMNAR_INDEX,

    MOVE_DATABASE,
    REBALANCE,
    UNARCHIVE,

    ALTER_TABLEGROUP,
    REFRESH_TOPOLOGY,

    CREATE_DATABASE_LIKE_AS,

    ALTER_TABLE_SET_TABLEGROUP,
    ALTER_TABLEGROUP_ADD_TABLE,
    ALTER_TABLE_RENAME_PARTITION,

    MERGE_TABLEGROUP,

    /**
     * 忽略的ddl类型，实际不是ddl
     */
    IGNORED;

    public enum AlterColumnSpecification {
        AlterColumnName,
        AlterColumnType,
        AlterColumnDefault,
        AlterColumnComment,
        AlterColumnOrder
    }

    public static boolean needDefaultDdlShareLock(DdlType type) {
        if (type == null) {
            return true;
        }
        switch (type) {
        case REFRESH_TOPOLOGY:
        case REBALANCE:
        case CREATE_FUNCTION:
        case DROP_FUNCTION:
        case PUSH_DOWN_UDF:
            return false;
        default:
            return true;
        }
    }

}

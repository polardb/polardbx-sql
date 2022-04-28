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

    CREATE_GLOBAL_INDEX,
    ALTER_GLOBAL_INDEX,
    DROP_GLOBAL_INDEX,
    RENAME_GLOBAL_INDEX,
    CHECK_GLOBAL_INDEX,

    MOVE_DATABASE,
    REBALANCE,
    UNARCHIVE,

    ALTER_TABLEGROUP,
    REFRESH_TOPOLOGY,
    ;

    public static boolean needDefaultDdlShareLock(DdlType type){
        if(type == null){
            return true;
        }
        switch (type){
        case REFRESH_TOPOLOGY:
        case REBALANCE:
            return false;
        default:
            return true;
        }
    }

}

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

package com.alibaba.polardbx.executor.ddl.job.task.basic.spec;

import java.util.EnumSet;

public enum AlterTableOperation {

    ADD_COLUMN,
    ADD_INDEX,
    ADD_PRIMARY_KEY,
    RENAME_COLUMN,
    RENAME_INDEX,
    ALTER_COLUMN_SET_DEFAULT,
    ALTER_COLUMN_DROP_DEFAULT,
    ALGORITHM,

    OTHERS;

    public static final EnumSet<AlterTableOperation> ROLLBACKABLE = EnumSet.of(
        ADD_COLUMN,
        ADD_INDEX,
        ADD_PRIMARY_KEY,
        RENAME_COLUMN,
        RENAME_INDEX,
        ALTER_COLUMN_SET_DEFAULT,
        ALTER_COLUMN_DROP_DEFAULT,
        ALGORITHM
    );

    public static boolean areAllOperationsRollbackable(EnumSet<AlterTableOperation> operations) {
        return ROLLBACKABLE.containsAll(operations);
    }

}

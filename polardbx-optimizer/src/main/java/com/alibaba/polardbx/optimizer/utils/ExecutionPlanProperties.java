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

package com.alibaba.polardbx.optimizer.utils;

import org.apache.calcite.util.BitSets;

import java.util.BitSet;

/**
 * @author chenmo.cm
 */
public final class ExecutionPlanProperties {

    public static final int MODIFY_BROADCAST_TABLE = 0;

    public static final int MODIFY_GSI_TABLE = 1;

    public static final int WITH_FORCE_INDEX = 2;

    public static final int MODIFY_TABLE = 3;

    public static final int MODIFY_SHARDING_COLUMN = 4;

    public static final int MODIFY_CROSS_DB = 5;

    public static final int MODIFY_SCALE_OUT_GROUP = 6;

    public static final int SELECT_WITH_LOCK = 7;

    public static final int REPLICATE_TABLE = 8;
    /**
     * Simple query which only select single broadcast table
     */
    public static final int ONLY_BROADCAST_TABLE = 9;

    public static final int QUERY = 10;

    public static final int DML = 11;

    public static final int DDL = 12;

    public static final BitSet MDL_REQUIRED = BitSets
        .of(MODIFY_TABLE, MODIFY_GSI_TABLE, MODIFY_BROADCAST_TABLE, MODIFY_SHARDING_COLUMN, MODIFY_CROSS_DB,
            MODIFY_SCALE_OUT_GROUP, REPLICATE_TABLE);

    public static final BitSet MDL_REQUIRED_POLARDBX = BitSets
        .of(QUERY, DML);

    public static final BitSet XA_REQUIRED = BitSets
        .of(MODIFY_BROADCAST_TABLE, MODIFY_GSI_TABLE, MODIFY_SHARDING_COLUMN, MODIFY_CROSS_DB, MODIFY_SCALE_OUT_GROUP,
            REPLICATE_TABLE);

    public static final BitSet DDL_STATEMENT = BitSets
        .of(DDL);
}

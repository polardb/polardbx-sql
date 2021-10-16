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

package com.alibaba.polardbx.gms.metadb.limit;

import com.alibaba.polardbx.gms.topology.DbTopologyManager;

public class Limits {

    /**
     * General Limits
     */
    public static final String METADATA_CHAR_SET = "UTF-8";
    public static final int MAX_LENGTH_OF_IDENTIFIER_NAME = 64;

    /**
     * Logical Table Related
     */
    public static final int MAX_LENGTH_OF_LOGICAL_TABLE_NAME = MAX_LENGTH_OF_IDENTIFIER_NAME;
    public static final int MAX_NUM_OF_LOGICAL_TABLES_PER_LOGICAL_DB = 8192;
    public static final int MAX_LENGTH_OF_LOGICAL_TABLE_COMMENT = 2048;

    public static final int MAX_NUM_OF_TOTAL_LOGICAL_TABLES =
        MAX_NUM_OF_LOGICAL_TABLES_PER_LOGICAL_DB * DbTopologyManager.maxLogicalDbCount;

    /**
     * Column Related
     */
    public static final int MAX_LENGTH_OF_COLUMN_NAME = MAX_LENGTH_OF_IDENTIFIER_NAME;
    public static final int MAX_NUM_OF_COLUMNS_PER_LOGICAL_TABLE = 1017;
    public static final int MAX_LENGTH_OF_COLUMN_COMMENT = 1024;

    /**
     * Sequence Related
     */
    public static final int MAX_LENGTH_OF_SEQUENCE_NAME = 128;
    public static final int MAX_NUM_OF_SEPARATE_SEQUENCES_PER_DB = MAX_NUM_OF_LOGICAL_TABLES_PER_LOGICAL_DB;

    /**
     * User-Defined Variable Related
     */
    public static final int MAX_LENGTH_OF_USER_DEFINED_VARIABLE = MAX_LENGTH_OF_IDENTIFIER_NAME;

    /**
     * Constraint Related
     */
    public static final int MAX_LENGTH_OF_CONSTRAINT_NAME = MAX_LENGTH_OF_IDENTIFIER_NAME;

    /**
     * Index Related
     */
    public static final int MAX_LENGTH_OF_INDEX_NAME = MAX_LENGTH_OF_IDENTIFIER_NAME;

    /**
     * Table group related
     */

    public static final int MAX_LENGTH_OF_TABLE_GROUP = MAX_LENGTH_OF_IDENTIFIER_NAME;
}

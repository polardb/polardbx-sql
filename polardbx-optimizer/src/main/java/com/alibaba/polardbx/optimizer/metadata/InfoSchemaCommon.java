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

package com.alibaba.polardbx.optimizer.metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlShowTableStatus;

public class InfoSchemaCommon {

    public static final String                INFO_SCHEMA_QUERY_TITLE        = "INFORMATION_SCHEMA_QUERY";

    public static final String                INFO_SCHEMA_TABLES             = "TABLES";
    public static final String                INFO_SCHEMA_COLUMNS            = "COLUMNS";
    public static final String                INFO_SCHEMA_STATISTICS         = "STATISTICS";
    public static final String                INFO_SCHEMA_SCHEMATA           = "SCHEMATA";

    public static final String                COLUMN_TABLE_SCHEMA            = "TABLE_SCHEMA";
    public static final String                COLUMN_TABLE_NAME              = "TABLE_NAME";
    public static final String                COLUMN_TABLE_TYPE              = "TABLE_TYPE";
    public static final String                COLUMN_COLUMN_NAME             = "COLUMN_NAME";
    public static final String                COLUMN_INDEX_SCHEMA            = "INDEX_SCHEMA";
    public static final String                COLUMN_INDEX_NAME              = "INDEX_NAME";
    public static final String                COLUMN_SCHEMA_NAME             = "SCHEMA_NAME";

    public static final int                   OFFSET_COLUMN_TABLE_SCHEMA     = -3;
    public static final int                   OFFSET_COLUMN_TABLE_NAME       = -2;
    public static final int                   OFFSET_COLUMN_TABLE_TYPE       = -1;
    public static final int                   OFFSET_COLUMN_COLUMN_NAME      = -1;
    public static final int                   OFFSET_COLUMN_INDEX_NAME       = -1;
    public static final int                   OFFSET_COLUMN_SCHEMA_NAME      = -1;

    public static final String                KEYWORD_SECONDARY_INDEX        = "._";

    public static final String                DEFAULT_TABLE_TYPE             = "BASE TABLE";

    public static final int                   LIMIT_DEFAULT_OFFSET           = 0;
    public static final int                   LIMIT_NO_OFFSET_SPECIFIED      = -1;
    public static final int                   LIMIT_NO_ROW_COUNT_SPECIFIED   = -1;

    public static final Set<String>           MYSQL_SYS_SCHEMAS              = new HashSet<>();
    public static final Set<String>           SUPPORTED_TABLE_TYPES          = new HashSet<>();

    public static final Set<String>           TABLES_STAT_NAMES              = new HashSet<>();
    public static final Set<Integer>          TABLES_STAT_INDEXES            = new HashSet<>();

    public static final Map<String, String>   TABLES_SPECIAL_COLUMNS         = new HashMap<>();
    public static final Map<String, Object[]> TABLES_COLUMN_MAPPING          = new HashMap<>();
    public static final Map<String, String>   TABLES_COLUMN_MAPPING_REVERSED = new HashMap<>();

    public static final Set<String>           COLUMN_ALIAS_MAPPING           = new HashSet<>();

    static {
        MYSQL_SYS_SCHEMAS.add("PERFORMANCE_SCHEMA");
        MYSQL_SYS_SCHEMAS.add("MYSQL");
        MYSQL_SYS_SCHEMAS.add("SYS");
        MYSQL_SYS_SCHEMAS.add("INFORMATION_SCHEMA");

        SUPPORTED_TABLE_TYPES.add(DEFAULT_TABLE_TYPE);
        SUPPORTED_TABLE_TYPES.add("VIEW");

        TABLES_STAT_NAMES.add("Rows");
        TABLES_STAT_NAMES.add("Avg_row_length");
        TABLES_STAT_NAMES.add("Data_length");
        TABLES_STAT_NAMES.add("Max_data_length");
        TABLES_STAT_NAMES.add("Index_length");
        TABLES_STAT_NAMES.add("Auto_increment");

        TABLES_SPECIAL_COLUMNS.put("Name", COLUMN_TABLE_NAME);
        TABLES_SPECIAL_COLUMNS.put("Rows", "TABLE_ROWS");
        TABLES_SPECIAL_COLUMNS.put("Collation", "TABLE_COLLATION");
        TABLES_SPECIAL_COLUMNS.put("Comment", "TABLE_COMMENT");

        for (int i = 0; i < SqlShowTableStatus.NUM_OF_COLUMNS; i++) {
            String columnName = SqlShowTableStatus.COLUMN_NAMES.get(i);

            if (TABLES_STAT_NAMES.contains(columnName)) {
                TABLES_STAT_INDEXES.add(i);
            }

            if (TABLES_SPECIAL_COLUMNS.keySet().contains(columnName)) {
                TABLES_COLUMN_MAPPING.put(TABLES_SPECIAL_COLUMNS.get(columnName), new Object[] { columnName, i });
                TABLES_COLUMN_MAPPING_REVERSED.put(columnName, TABLES_SPECIAL_COLUMNS.get(columnName));
            } else {
                TABLES_COLUMN_MAPPING.put(columnName.toUpperCase(), new Object[] { columnName, i });
                TABLES_COLUMN_MAPPING_REVERSED.put(columnName, columnName.toUpperCase());
            }
        }

        COLUMN_ALIAS_MAPPING.add(COLUMN_TABLE_SCHEMA);
        COLUMN_ALIAS_MAPPING.add(COLUMN_TABLE_NAME);
        COLUMN_ALIAS_MAPPING.add(COLUMN_INDEX_SCHEMA);
        COLUMN_ALIAS_MAPPING.add(COLUMN_SCHEMA_NAME);
        for (String statColName : TABLES_STAT_NAMES) {
            COLUMN_ALIAS_MAPPING.add(statColName.toUpperCase());
        }
    }

}

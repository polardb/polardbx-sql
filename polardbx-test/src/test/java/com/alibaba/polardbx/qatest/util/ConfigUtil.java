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

package com.alibaba.polardbx.qatest.util;

import com.alibaba.polardbx.qatest.data.ExecuteTableName;

/**
 * Case相关的其他配置都在这里
 */
public class ConfigUtil {

    public static boolean isSingleApp(String appName) {
        return appName.toUpperCase().contains("SINGLE");
    }

    // 主要用于不能跨库的语句判断
    public static boolean isStrictSameTopology(String tableName1, String tableName2) {

        // 表名后缀相同，拓扑一致
        if (tableName1.toLowerCase().contains(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX)
            && tableName2.toLowerCase().contains(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX)) {
            return true;
        }
        if (tableName1.toLowerCase().contains(ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX)
            && tableName2.toLowerCase().contains(ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX)) {
            return true;
        }

        if (tableName1.toLowerCase().contains(ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX)
            && tableName2.toLowerCase().contains(ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX)) {
            return true;
        }

        if (tableName1.toLowerCase().contains(ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX)
            && tableName2.toLowerCase().contains(ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX)) {
            return true;
        }

        return tableName1.toLowerCase().contains(ExecuteTableName.BROADCAST_TB_SUFFIX)
            && tableName2.toLowerCase().contains(ExecuteTableName.BROADCAST_TB_SUFFIX);
    }

    public static boolean isMultiTable(String tableName) {
        return tableName.toLowerCase().contains("multi") || tableName.contains("分");
    }

    /**
     * 有分库的情况
     */
    public static boolean isMultiDbTable(String tableName) {
        return tableName.toLowerCase().contains("multi_db") || (tableName.contains("分库") && !tableName.contains("不分库"));
    }

    public static boolean isSingleTable(String tableName) {
        return !isMultiTable(tableName) && !isMultiDbTable(tableName);
    }

    public static boolean allSingleTable(String table1, String table2) {
        return isSingleTable(table1) && isSingleTable(table2);
    }
}

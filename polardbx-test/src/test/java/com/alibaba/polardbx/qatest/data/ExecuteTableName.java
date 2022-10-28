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

package com.alibaba.polardbx.qatest.data;

/**
 * 操作表
 *
 * @author zhuoxue.yll
 * <p>
 */
public class ExecuteTableName {

    public static final String HINT_STRESS_FLAG = "/* //1/ */";
    // 不分库不分表
    public static String ONE_DB_ONE_TB_SUFFIX = "one_db_one_tb";
    // 不分库分表
    public static String ONE_DB_MUTIL_TB_SUFFIX = "one_db_multi_tb";
    // 分库不分表
    public static String MULTI_DB_ONE_TB_SUFFIX = "multi_db_one_tb";
    // 分库分表
    public static String MUlTI_DB_MUTIL_TB_SUFFIX = "multi_db_multi_tb";

    public static String HOT_MAPPING_KEY_BY_KEY = "_hot_key_by_key";

    // 广播表
    public static String BROADCAST_TB_SUFFIX = "broadcast";

    // 以varchar为拆分字段的表，表名加上这个关键字
    public static String HASH_BY_VARCHAR = "string_";

    // 以varchar为拆分字段的表，表名加上这个关键字
    public static String MASS_TB_PARTITION = "mass_";

    // 以日期为拆分字段的表，表名加上这个关键字
    public static String HASH_BY_DATE = "date_";

    public static String ONE = "one_";

    public static String TWO = "two_";

    public static String THREE = "three_";

    public static String FOUR = "four_";

    public static String DIFF_SHARDING_KEY = "diff_shard_key_";

    // 基础表的名称前缀
    public static String UPDATE_DELETE_BASE = "update_delete_base_";

    public static String UPDATE_DELETE_BASE_AUTONIC = "update_delete_base_autonic_";

    public static String SELECT_BASE = "select_base_";

    public static String NO_PRIMARY_KEY = "no_primary_key_";

    public static String COMPOSITE_PRIMARY_KEY = "composite_primary_key_";

    public static String UNIQUE_KEY = "unique_key";

    public static String WITH_INDEX = "with_index_";

    // 其他特殊表的名称前缀, 定义在这里，其他用例想用的时候可以用
    public static String LOAD_TEST = "load_data_";
    public static String SET_TYPE_TEST = "set_type_";
    public static String TINY_INT_TEST = "tiny_int_";
    public static String GEMO_TEST = "gemo_";
    public static String SENSITIVE_TYPE_TEST = "sensitive_type_";
    public static String SENSITIVE_TYPE_GBK_TEST = "sensitive_type_gbk_";
    public static String _TDDL_TABLE = "_tddl_";
    public static String TRUNCATE_VALUE_TEST = "truncate_value_";
    public static String KEY_WORD_TEST = "table_";
    public static String META_TEST = "meta_";
    public static String SIMPLE_GRANT_TEST = "simple_grant_";
    public static String JSON_TEST = "json_";
    public static String GSI_DML_TEST = "gsi_dml_";
    public static String ALL_TYPE = "all_type_update_delete_base_one_";

    // 分库分表键为string 规则
    public static String MULTI_DB_MUTIL_TB_BY_STRING_SUFFIX = "multi_db_multi_tb_by_string";

    /**
     * 四中基本类型表
     */
    public static String[][] allBaseTypeOneTable(String tablePrefix) {
        // String tablePrefix = classNameChangeToTablePrefix(className);

        String[][] object = {
            // 不分库不分表
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX},
            // 不分库分表
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX},

            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX},
            // 分库分表
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX},
            // 广播表
            {tablePrefix + BROADCAST_TB_SUFFIX},
        };

        return object;

    }

    /**
     * 四中基本类型表
     */
    public static Object[][] renameTableOfAllBaseType(String tablePrefix) {
        final String beforeTablePrefix = tablePrefix + "_origin_";
        final String afterTablePrefix = tablePrefix + "_target_";

        Object[][] object = { // 不分库不分表
            {beforeTablePrefix + ONE_DB_ONE_TB_SUFFIX, afterTablePrefix + ONE_DB_ONE_TB_SUFFIX, false},
            // 分表不分库
            {beforeTablePrefix + ONE_DB_MUTIL_TB_SUFFIX, afterTablePrefix + ONE_DB_MUTIL_TB_SUFFIX, false},
            // 分库不分表
            {beforeTablePrefix + MULTI_DB_ONE_TB_SUFFIX, afterTablePrefix + MULTI_DB_ONE_TB_SUFFIX, false},
            // 分库分表
            {beforeTablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, afterTablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, false},
            // 广播表
            {beforeTablePrefix + BROADCAST_TB_SUFFIX, afterTablePrefix + BROADCAST_TB_SUFFIX, false},
            // 不分库不分表
            {
                beforeTablePrefix + ONE_DB_ONE_TB_SUFFIX, afterTablePrefix + ONE_DB_ONE_TB_SUFFIX,
                true},
            // 分表不分库
            {
                beforeTablePrefix + ONE_DB_MUTIL_TB_SUFFIX, afterTablePrefix + ONE_DB_MUTIL_TB_SUFFIX,
                true},
            // 分库不分表
            {
                beforeTablePrefix + MULTI_DB_ONE_TB_SUFFIX, afterTablePrefix + MULTI_DB_ONE_TB_SUFFIX,
                true},
            // 分库分表
            {
                beforeTablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, afterTablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX,
                true},
            // 广播表
            {
                beforeTablePrefix + BROADCAST_TB_SUFFIX, afterTablePrefix + BROADCAST_TB_SUFFIX,
                true}

        };

        return object;

    }

    /**
     * 所有基本类型的表, 包含varchar拆分
     */
    public static String[][] allBaseTypeWithStringRuleOneTable(String tablePrefix) {
        // String tablePrefix = classNameChangeToTablePrefix(className);

        String[][] object = {
            // 不分库不分表
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX},

            // 不分库分表
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX},

            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX},
            // 分库分表
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX},
            // 分库分表键为string 规则
            {tablePrefix + HASH_BY_VARCHAR + ONE_DB_MUTIL_TB_SUFFIX},
            // 分库分表键为string 规则
            {tablePrefix + HASH_BY_VARCHAR + MULTI_DB_ONE_TB_SUFFIX},
            // 分库分表键为string 规则
            {tablePrefix + HASH_BY_VARCHAR + MUlTI_DB_MUTIL_TB_SUFFIX},
            // 广播表
            {tablePrefix + BROADCAST_TB_SUFFIX},
        };

        return object;

    }

    public static String[][] allBaseTypeRuleOneTable(String tablePrefix) {
        String[][] object = {
            // 不分库不分表
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX},
            // 不分库分表
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX},

            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX},
            // 分库分表
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX}
        };

        return object;

    }

    /**
     * 所有基本类型的表, 包含双字段拆分
     */
    public static String[][] allBaseTypeWithStringRuleOneTable2(String tablePrefix) {
        // String tablePrefix = classNameChangeToTablePrefix(className);

        String[][] object = {
            // 不分库不分表
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX},
            // 不分库分表
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX},

            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX},
            // 分库分表
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX},
            // 分库分表键为string 规则
            {tablePrefix + HASH_BY_VARCHAR + ONE_DB_MUTIL_TB_SUFFIX},
            // 分库分表键为string 规则
            {tablePrefix + HASH_BY_VARCHAR + MULTI_DB_ONE_TB_SUFFIX},
            // 分库分表键为string 规则
            {tablePrefix + HASH_BY_VARCHAR + MUlTI_DB_MUTIL_TB_SUFFIX},
            // 分库键为 pk 分表键为 date 规则
            {tablePrefix + HASH_BY_DATE + ONE + MUlTI_DB_MUTIL_TB_SUFFIX},
            // 广播表
            {tablePrefix + BROADCAST_TB_SUFFIX},};

        return object;

    }

    public static String[][] updateBaseOneTableForCollationTest() {
        String[][] object = {{"update_delete_base_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX}};
        return object;
    }

    public static String[][] allBaseTypeTwoTable(String tablePrefix) {
        // String tablePrefix = classNameChangeToTablePrefix(className);

        String[][] object = {

            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},

            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},

            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},

            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},

            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

        };

        return object;

    }

    public static String[][] allBaseTypeTwoTableForSubquery(String tablePrefix) {

        String[][] object = {

            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

        };
        return object;

    }

    public static String[][] allBaseTypeThreeTable(String tablePrefix) {
        // String tablePrefix = classNameChangeToTablePrefix(className);

        String[] params1 = new String[] {
            tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + ONE_DB_MUTIL_TB_SUFFIX,
            tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + MULTI_DB_ONE_TB_SUFFIX,
            tablePrefix + BROADCAST_TB_SUFFIX};
        String[] params2 = new String[] {
            tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX,
            tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX,
            tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX};
        String[] params3 = new String[] {
            tablePrefix + THREE + ONE_DB_ONE_TB_SUFFIX,
            tablePrefix + THREE + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + THREE + MUlTI_DB_MUTIL_TB_SUFFIX,
            tablePrefix + THREE + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + THREE + BROADCAST_TB_SUFFIX};

        String[][] object = mixParameters(params1, params2, params3);

        return object;

    }

    public static String[][] sencitiveTypeTable() {
        String[][] object = {
            {SENSITIVE_TYPE_TEST + ONE_DB_ONE_TB_SUFFIX},
            {SENSITIVE_TYPE_TEST + ONE_DB_MUTIL_TB_SUFFIX}, {SENSITIVE_TYPE_TEST + MUlTI_DB_MUTIL_TB_SUFFIX},
            {SENSITIVE_TYPE_TEST + MULTI_DB_ONE_TB_SUFFIX}, {SENSITIVE_TYPE_TEST + BROADCAST_TB_SUFFIX}

        };
        return object;
    }

    public static String[][] fiveBaseTypeTwoTable(String className) {
        String tablePrefix = classNameChangeToTablePrefix(className);

        String[][] object = {
            // 不分库不分表
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            // 不分库分表
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            // 分库不分表
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            // 分库分表
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            // 广播表
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX}};
        return object;

    }

    /**
     * 所有基本类型的表
     */
    public static String[][] allMultiTypeOneTable(String tablePrefix) {
        // String tablePrefix = classNameChangeToTablePrefix(className);

        String[][] object = {

            // 不分库分表
            // {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX},

            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX},
            // 分库分表
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX},

        };

        return object;

    }

    public static String[][] allBaseTypeTwoStrictSameTable(String tablePrefix) {
        // String tablePrefix = classNameChangeToTablePrefix(className);

        String[][] object = {
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

        };

        return object;

    }

    public static String[][] gsiDMLTableSimplfy() {
        return new String[][] {
            {GSI_DML_TEST + "unique_one_index_base", GSI_DML_TEST + "unique_multi_index_base"}
        };
    }

    /**
     * Normal DML for GSI.
     */
    public static String[][] gsiDMLTable() {
        return new String[][] {
            {"", GSI_DML_TEST + "unique_one_index_base", GSI_DML_TEST + "unique_multi_index_base"},
            {"", GSI_DML_TEST + "unique_multi_index_base", GSI_DML_TEST + "unique_one_index_base"},
            {"", GSI_DML_TEST + "no_unique_one_index_base", GSI_DML_TEST + "no_unique_multi_index_base"},
            {"", GSI_DML_TEST + "no_unique_multi_index_base", GSI_DML_TEST + "no_unique_one_index_base"},
            {"", GSI_DML_TEST + "composite_unique_one_index_base", GSI_DML_TEST + "composite_unique_multi_index_base"},
            {"", GSI_DML_TEST + "composite_unique_multi_index_base", GSI_DML_TEST + "composite_unique_one_index_base"},
            {"", GSI_DML_TEST + "global_unique_one_index_base", GSI_DML_TEST + "unique_one_index_base"},
            {"", GSI_DML_TEST + "no_unique_one_index_mp_base", GSI_DML_TEST + "no_unique_multi_index_base"},
            {"", GSI_DML_TEST + "no_unique_one_index_mpk_base", GSI_DML_TEST + "no_unique_multi_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "unique_one_index_base",
                GSI_DML_TEST + "unique_multi_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "unique_multi_index_base",
                GSI_DML_TEST + "unique_one_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "no_unique_one_index_base",
                GSI_DML_TEST + "no_unique_multi_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "no_unique_multi_index_base",
                GSI_DML_TEST + "no_unique_one_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "composite_unique_one_index_base",
                GSI_DML_TEST + "composite_unique_multi_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "composite_unique_multi_index_base",
                GSI_DML_TEST + "composite_unique_one_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "global_unique_one_index_base",
                GSI_DML_TEST + "unique_one_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "no_unique_one_index_mp_base",
                GSI_DML_TEST + "no_unique_multi_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(ALWAYS_REBUILD_PLAN=true)*/", GSI_DML_TEST + "no_unique_one_index_mpk_base",
                GSI_DML_TEST + "no_unique_multi_index_base"},
            {HINT_STRESS_FLAG, GSI_DML_TEST + "unique_one_index_base", GSI_DML_TEST + "unique_multi_index_base"},
            {HINT_STRESS_FLAG, GSI_DML_TEST + "global_unique_one_index_base", GSI_DML_TEST + "unique_one_index_base"}};
    }

    /**
     * Tables for GSI with sequence.
     */
    public static String[][] gsiDMLSequenceTable() {
        return new String[][] {
            {"", GSI_DML_TEST + "sequence_one_index_base", GSI_DML_TEST + "unique_one_index_base"},
            {
                "/*+TDDL:CMD_EXTRA(GSI_CONCURRENT_WRITE_OPTIMIZE=false)*/", GSI_DML_TEST + "sequence_one_index_base",
                GSI_DML_TEST + "unique_one_index_base"},
            {HINT_STRESS_FLAG, GSI_DML_TEST + "sequence_one_index_base", GSI_DML_TEST + "unique_one_index_base"}
        };
    }

    /**
     * 将类名按照大写通过下划线转换为表名钱缀
     */
    public static String classNameChangeToTablePrefix(String className) {
        String[] name = className.split("\\.");
        className = name[name.length - 1];
        String[] ss = className.split("(?<!^)(?=[A-Z])");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ss.length; i++) {
            if (!(i == ss.length - 1 && ss[i].equalsIgnoreCase("test"))) {
                sb.append(ss[i]);
            }

            if (i != ss.length - 1) {
                sb.append("_");
            }
        }
        return sb.toString().toLowerCase();
    }

    public static String[][] mixParameters(String[] params1, String[] params2, String[] params3) {
        String[][] result = new String[params1.length * params2.length * params3.length][3];
        int i = 0;
        for (String firstParam : params1) {
            for (String secondParam : params2) {
                for (String thirdParam : params3) {
                    result[i] = new String[3];
                    result[i][0] = firstParam;
                    result[i][1] = secondParam;
                    result[i][2] = thirdParam;
                    i++;
                }
            }
        }
        return result;
    }

    public static String[][] joinTable() {

        String[][] object = {{"update_delete_base_multi_db_one_tb", "update_delete_base_two_multi_db_multi_tb"},};
        return object;

    }

    public static String[][] newJoinTable() {

        String[][] object = {{"update_delete_base_two_multi_db_multi_tb", "update_delete_base_two_multi_db_one_tb"},};
        return object;

    }
}

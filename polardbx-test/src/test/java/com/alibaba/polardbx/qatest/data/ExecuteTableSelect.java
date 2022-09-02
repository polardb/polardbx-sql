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

import com.google.common.collect.ImmutableList;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 此类对运行的表做定义 数组中均为表名 表名后缀是Index表示二级索引，包含一个字段的索引 表名后缀是twoIndex表示二级索引，包含两个字段的组合索引
 * 表名后缀是threeIndex表示二级索引，包含两三字段的组合索引 表名后缀是oneGroup_oneAtom 表示单库单表
 * 表名后缀是oneGroup_mutilAtom 表示单库多表 表名后缀是oneGroup_mutilAtom 表示多库多表
 * 表中名包含msyql字段的调用的mysql数据库，其他的均为bdb数据库
 * 表normaltbl_mutilGroup_twoIndex_complexRule为模拟线上配置规则，特别测试用例
 */
public class ExecuteTableSelect {

    /**
     * 多库多表 两张表join
     */
    public static String[][] selectBaseOneBaseTwoMutilDbMutilTb() {

        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},
        };
        return object;

    }

    /**
     * 查询两张表情况，多种模式
     */
    public static String[][] selectBaseOneBaseTwo() {

        String[][] object = {
            // case[0]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX},
            // case[4]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX},
            // case[9]
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},
        };
        return object;

    }

    /**
     * 查询一张表，多种模式
     */
    public static String[][] selectBaseOneTable() {

        String[][] object = {
            {"select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},
            {"select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX}};
        return object;

    }

    // where 条件限定了只能路由到一个分库，用于此类情况的测试
    public static String[][] autoTableWithWhere() {
        String[][] object = {
            {"autonic_oneGroup_oneAtom", ""}, {"autonic_oneGroup_multiAtom", ""},
            {
                "autonic_oneGroup_multiAtom",
                "where pk in (1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49)"},
            {"autonic_multiGroup_oneAtom", ""},
            {
                "autonic_multiGroup_oneAtom",
                "where pk in (4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48)"},
            {"autonic_multiGroup_multiAtom", ""},
            {"autonic_multiGroup_multiAtom", "where pk in (8, 16, 24, 32, 40, 48) "},
            {"autonic_broadcast", ""}};
        return object;
    }

    // where 条件限定了只能路由到一个分库，用于此类情况的测试
    public static String[][] selectWithWhereTable(String className) {
        String tablePrefix = ExecuteTableName.classNameChangeToTablePrefix(className);

        String[][] object = {
            {tablePrefix + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, ""},
            {tablePrefix + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, ""},
            {
                tablePrefix + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "where pk in (4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48)"},
            {tablePrefix + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, ""},
            {
                tablePrefix + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
                "where pk in (4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48)"},
            {tablePrefix + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, ""},
            {tablePrefix + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "where pk in (16,32) "},
            {tablePrefix + ExecuteTableName.BROADCAST_TB_SUFFIX, ""}};
        return object;
    }

    public static String[][] selectWithMultiTableHint() {
        String no_hint = "";
        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},
            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},

            // case[4]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, no_hint},

            // case[9]
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},};
        return object;
    }

    // where 条件限定了只能路由到一个分库，用于此类情况的测试
    public static String[][] tableWithWhereTable(String tablePrefix) {

        String[][] object = {
            {tablePrefix + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, ""},
            {tablePrefix + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, ""},
            {
                tablePrefix + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "where pk in (4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48)"},
            {tablePrefix + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, ""},
            {
                tablePrefix + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
                "where pk in (4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48)"},
            {tablePrefix + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, ""},
            {tablePrefix + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "where pk in (16,32) "},
            {tablePrefix + ExecuteTableName.BROADCAST_TB_SUFFIX, ""}};
        return object;
    }

    /**
     * 查询一张表，多种模式带排序
     */
    public static String[][] selectBaseOneTableOrderbyList() {
        String[][] object = {
            // [1]不分库分表
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "varchar_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "char_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "blob_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "integer_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "tinyint_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "tinyint_1bit_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "smallint_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "mediumint_test"},
            // {
            // "select_base_one_"+ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
            // "bit_test" },
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "bigint_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "float_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "double_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "decimal_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "date_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "time_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "datetime_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "timestamp_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "year_test"},
            {"select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, "pk"},

            // [2]分库不分表
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "varchar_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "char_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "blob_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "integer_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "tinyint_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "tinyint_1bit_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "smallint_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "mediumint_test"},
            // {
            // "select_base_one_"+ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
            // "bit_test" },
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "bigint_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "float_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "double_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "decimal_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "date_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "time_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "datetime_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "timestamp_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "year_test"},
            {"select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, "pk"},

            // [3]分库分表
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "varchar_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "char_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "blob_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "integer_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "tinyint_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "tinyint_1bit_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "smallint_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "mediumint_test"},
            // {
            // "select_base_one_"+ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
            // "bit_test" },
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "bigint_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "float_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "double_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "decimal_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "date_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "time_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "datetime_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "timestamp_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "year_test"},
            {"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, "pk"}};
        return object;
    }

    /**
     * 查询三张表，多种模式
     */
    public static String[][] selectBaseOneBaseTwoBaseThree() {

        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX},
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},
            // case[4]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX},
            // case[9]
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},

        };
        return object;

    }

    /**
     * 查询一张表，单库单表模式
     */
    public static String[][] selectBaseOneOneDbOneTb() {

        String[][] object = {{"select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX}};
        return object;

    }

    /**
     * 查询四张表带hint ，多种模式
     */
    // TODO
    // 对应com.taobao.tddl.qatest.ExecuteTableName.hostinfoHostgoupStudentModuleinfoModulehostTableAndHint(String
    // db)方法
    public static String[][] selectBaseOneBaseTwoBaseThreeBaseFourWithHint() {
        String no_hint = "";

        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},
            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},

            // case[4]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, no_hint},

            // case[9]
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},};
        return object;

    }

    public static String[][] selectFourTableWithRuntimeFilter() {
        String enableRuntimeFilterHint = "/*+TDDL: ENABLE_RUNTIME_FILTER=true "
            + "FORCE_ENABLE_RUNTIME_FILTER_COLUMNS=\"*\" */";

        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, enableRuntimeFilterHint},
            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, enableRuntimeFilterHint},

            // case[4]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, enableRuntimeFilterHint},

            // case[9]
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, enableRuntimeFilterHint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, enableRuntimeFilterHint},};
        return object;
    }

    public static String[][] selectBushyJoinTestTable() {
        String no_hint = "";

        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.BROADCAST_TB_SUFFIX
            }};
        return object;

    }

    /**
     * 查两张表带hint，多种模式
     */
    public static String[][] selectBaseOneBaseTwoWithHint() {

        String no_hint = "";

        String[][] object = {
            // case[0]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

            // case[4]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, no_hint},

            // case[9]
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

        };
        return object;

    }

    /**
     * 查两张表带hint，多种模式
     */
    public static String[][] selectBaseOneBaseOneWithHint() {
        String no_hint = "";

        String[][] object = {

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

            // case[4]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX, no_hint},

            // case[9]
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},

        };
        return object;

    }

    /**
     * 查询一张表，多库多表模式
     */
    public static String[][] selectBaseOneTableMutilDbMutilTb() {
        String[][] object = {{"select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX}};
        return object;

    }

    /**
     * 查询广播表和基本表带hint，多种模式
     */
    public static String[][] selectBroadcastAndBaseWithHintTable() {

        String no_hint = "";

        String[][] object = {

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},
            // case[4]
            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, no_hint},

            // case[9]
            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, no_hint},};
        return object;

    }

    public static String[][] selectOneTableMultiRuleMode() {

        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX},
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX}};
        return object;

    }

    private static final Map<String, List<String>> GSI_DEFINITIONS = new LinkedHashMap<>();

    private static final List<String> GSI_DEF_HEAD = new ArrayList<>();
    private static final Map<String, List<String>> GSI_DEF_COLUMN_DB = new LinkedHashMap<>();
    private static final Map<String, List<String>> GSI_DEF_COLUMN_TB = new LinkedHashMap<>();
    private static final Map<String, List<String>> CREATE_GSI_DEF_COLUMN_DB = new LinkedHashMap<>();
    private static final Map<String, List<String>> CREATE_GSI_DEF_COLUMN_TB = new LinkedHashMap<>();
    private static final Map<String, List<String>> GSI_DEF_SHARDING_DB = new LinkedHashMap<>();
    private static final Map<String, List<String>> GSI_DEF_SHARDING_TB = new LinkedHashMap<>();
    private static final List<String> GSI_DEF_INDEX_OPTION = new ArrayList<>();

    static {
        GSI_DEF_HEAD.add("GLOBAL INDEX");
        //GSI_DEF_HEAD.add("GLOBAL UNIQUE");
        GSI_DEF_HEAD.add("UNIQUE GLOBAL INDEX");

        /*
         * global_secondary_index_option
         */

        GSI_DEF_COLUMN_DB.computeIfAbsent("id2", s -> {
            List<String> gsiDefColumnDb = new ArrayList<>();
            gsiDefColumnDb.add("gsi_id2(id2)");
            gsiDefColumnDb.add("gsi_id2(id2) COVERING (vc1)");
            gsiDefColumnDb.add("gsi_id2(id2) COVERING (vc1, vc2)");
            gsiDefColumnDb.add("gsi_id2 USING BTREE (id2) COVERING (vc1, vc2)");
            gsiDefColumnDb.add("gsi_id2 USING HASH (id2) COVERING (vc1, vc2)");
            return gsiDefColumnDb;
        });

        GSI_DEF_COLUMN_TB.computeIfAbsent("dt", s -> ImmutableList.of("gsi_id2(dt)"));

        GSI_DEF_COLUMN_TB.computeIfAbsent("id2,id3", s -> {
            List<String> gsiDefColumnTb = new ArrayList<>();
            gsiDefColumnTb.add("gsi_id2(id2, id3)");
            gsiDefColumnTb.add("gsi_id2(id2, id3) COVERING (vc1)");
            gsiDefColumnTb.add("gsi_id2(id2, id3) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING BTREE (id2, id3) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING HASH (id2, id3) COVERING (vc1, vc2)");
            return gsiDefColumnTb;
        });

        GSI_DEF_COLUMN_TB.computeIfAbsent("id2,dt", s -> {
            List<String> gsiDefColumnTb = new ArrayList<>();
            gsiDefColumnTb.add("gsi_id2(id2, dt)");
            gsiDefColumnTb.add("gsi_id2(id2, dt) COVERING (vc1)");
            gsiDefColumnTb.add("gsi_id2(id2, dt) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING BTREE (id2, dt) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING HASH (id2, dt) COVERING (vc1, vc2)");
            return gsiDefColumnTb;
        });

        GSI_DEF_COLUMN_TB.computeIfAbsent("pk,id2", s -> {
            List<String> gsiDefColumnTb = new ArrayList<>();
            gsiDefColumnTb.add("gsi_id2(pk, id2)");
            gsiDefColumnTb.add("gsi_id2(pk, id2) COVERING (vc1)");
            gsiDefColumnTb.add("gsi_id2(pk, id2) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING BTREE (pk, id2) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING HASH (pk, id2) COVERING (vc1, vc2)");
            return gsiDefColumnTb;
        });

        CREATE_GSI_DEF_COLUMN_DB.computeIfAbsent("id2", s -> {
            List<String> gsiDefColumnDb = new ArrayList<>();
            gsiDefColumnDb.add("gsi_id2 ON {0}(id2)");
            gsiDefColumnDb.add("gsi_id2 ON {0}(id2) COVERING (vc1)");
            gsiDefColumnDb.add("gsi_id2 ON {0}(id2) COVERING (vc1, vc2)");
            gsiDefColumnDb.add("gsi_id2 USING BTREE ON {0}(id2) COVERING (vc1, vc2)");
            gsiDefColumnDb.add("gsi_id2 USING HASH ON {0}(id2) COVERING (vc1, vc2)");
            return gsiDefColumnDb;
        });

        CREATE_GSI_DEF_COLUMN_TB.computeIfAbsent("dt", s -> ImmutableList.of("gsi_id2 ON {0}(dt)"));

        CREATE_GSI_DEF_COLUMN_TB.computeIfAbsent("id2,id3", s -> {
            List<String> gsiDefColumnTb = new ArrayList<>();
            gsiDefColumnTb.add("gsi_id2 ON {0}(id2, id3)");
            gsiDefColumnTb.add("gsi_id2 ON {0}(id2, id3) COVERING (vc1)");
            gsiDefColumnTb.add("gsi_id2 ON {0}(id2, id3) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING BTREE ON {0} (id2, id3) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING HASH ON {0} (id2, id3) COVERING (vc1, vc2)");
            return gsiDefColumnTb;
        });

        CREATE_GSI_DEF_COLUMN_TB.computeIfAbsent("id2,dt", s -> {
            List<String> gsiDefColumnTb = new ArrayList<>();
            gsiDefColumnTb.add("gsi_id2 ON {0}(id2, dt)");
            gsiDefColumnTb.add("gsi_id2 ON {0}(id2, dt) COVERING (vc1)");
            gsiDefColumnTb.add("gsi_id2 ON {0}(id2, dt) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING BTREE ON {0} (id2, dt) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING HASH ON {0} (id2, dt) COVERING (vc1, vc2)");
            return gsiDefColumnTb;
        });

        CREATE_GSI_DEF_COLUMN_TB.computeIfAbsent("pk,id2", s -> {
            List<String> gsiDefColumnTb = new ArrayList<>();
            gsiDefColumnTb.add("gsi_id2 ON {0}(pk, id2)");
            gsiDefColumnTb.add("gsi_id2 ON {0}(pk, id2) COVERING (vc1)");
            gsiDefColumnTb.add("gsi_id2 ON {0}(pk, id2) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING BTREE ON {0} (pk, id2) COVERING (vc1, vc2)");
            gsiDefColumnTb.add("gsi_id2 USING HASH ON {0} (pk, id2) COVERING (vc1, vc2)");
            return gsiDefColumnTb;
        });

        /*
         * drds_partition_options
         */

        GSI_DEF_SHARDING_DB.computeIfAbsent("id2", s -> {
            List<String> gsiDefShardingDb = new ArrayList<>();
            gsiDefShardingDb.add("DBPARTITION BY HASH (id2)");
            gsiDefShardingDb.add("DBPARTITION BY HASH (id2) TBPARTITION BY HASH(id2) TBPARTITIONS 3");
            gsiDefShardingDb.add("DBPARTITION BY UNI_HASH (id2) TBPARTITION BY UNI_HASH(id2) TBPARTITIONS 3");
            gsiDefShardingDb
                .add("DBPARTITION BY RIGHT_SHIFT (id2, 4) TBPARTITION BY RIGHT_SHIFT(id2, 4) TBPARTITIONS 3");
            return gsiDefShardingDb;
        });

        GSI_DEF_SHARDING_TB.computeIfAbsent("dt", s -> {
            List<String> gsiDefShardingTb = new ArrayList<>();
            gsiDefShardingTb.add("DBPARTITION BY YYYYMM (dt) TBPARTITION BY YYYYMM(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY YYYYWEEK (dt) TBPARTITION BY YYYYWEEK(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY YYYYDD (dt) TBPARTITION BY YYYYDD(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY YYYYMM_OPT (dt) TBPARTITION BY YYYYMM_OPT(dt) TBPARTITIONS 3");
            //gsiDefShardingTb.add("DBPARTITION BY YYYYWEEK_OPT (dt) TBPARTITION BY YYYYWEEK_OPT(dt) TBPARTITIONS 3");
            //gsiDefShardingTb.add("DBPARTITION BY YYYYDD_OPT (dt) TBPARTITION BY YYYYDD_OPT(dt) TBPARTITIONS 3");
            return gsiDefShardingTb;
        });

        GSI_DEF_SHARDING_TB.computeIfAbsent("id2,id3", s -> {
            List<String> gsiDefShardingTb = new ArrayList<>();
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY HASH(id3) TBPARTITIONS 3");
            return gsiDefShardingTb;
        });

        GSI_DEF_SHARDING_TB.computeIfAbsent("pk,id2",
            s -> {
                List<String> gsiDefShardingTb = new ArrayList<>();
                gsiDefShardingTb.add(
                    "DBPARTITION BY RANGE_HASH (pk, id2, 4) TBPARTITION BY RANGE_HASH (pk, id2, 3) TBPARTITIONS 3");
                return gsiDefShardingTb;
            });

        GSI_DEF_SHARDING_TB.computeIfAbsent("id2,dt", s -> {
            List<String> gsiDefShardingTb = new ArrayList<>();
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY MM(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY DD(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY WEEK(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY MMDD(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY YYYYMM(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY YYYYWEEK(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY YYYYDD(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY YYYYMM_OPT(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY YYYYWEEK_OPT(dt) TBPARTITIONS 3");
            gsiDefShardingTb.add("DBPARTITION BY HASH (id2) TBPARTITION BY YYYYDD_OPT(dt) TBPARTITIONS 3");
            return gsiDefShardingTb;
        });

        GSI_DEF_INDEX_OPTION.add("");
        GSI_DEF_INDEX_OPTION.add("COMMENT 'gsi test'");

        // for each head + option
        GSI_DEF_HEAD.forEach(head -> {
            GSI_DEF_INDEX_OPTION.forEach(option -> {
                GSI_DEF_COLUMN_DB.entrySet()
                    .stream()
                    .findAny()
                    .ifPresent(columnEntry -> columnEntry.getValue().forEach(column -> {
                        buildOneGsiDef(head, option, columnEntry, column, GSI_DEF_SHARDING_DB);

                    }));

                GSI_DEF_COLUMN_TB.entrySet()
                    .stream()
                    .findAny()
                    .ifPresent(columnEntry -> columnEntry.getValue().forEach(column -> {
                        buildOneGsiDef(head, option, columnEntry, column, GSI_DEF_SHARDING_TB);

                    }));
            });
        });

        // for each sharding db
        GSI_DEF_COLUMN_DB.forEach((shardingKeys, columns) -> columns.forEach(column -> {
            GSI_DEF_HEAD.forEach(head -> {
                final String option = GSI_DEF_INDEX_OPTION.stream().findAny().orElse("");
                buildGsiDef(shardingKeys, head, option, column, GSI_DEF_SHARDING_DB.get(shardingKeys), GSI_DEFINITIONS);
            });
        }));

        // for each sharding tb
        GSI_DEF_COLUMN_TB.forEach((shardingKeys, columns) -> columns.forEach(column -> {
            GSI_DEF_HEAD.forEach(head -> {
                final String option = GSI_DEF_INDEX_OPTION.stream().findAny().orElse("");
                buildGsiDef(shardingKeys, head, option, column, GSI_DEF_SHARDING_TB.get(shardingKeys), GSI_DEFINITIONS);
            });
        }));
    }

    private static void buildOneGsiDef(String head, String option, Entry<String, List<String>> columnEntry,
                                       String column, Map<String, List<String>> gsiDefSharding) {
        gsiDefSharding.get(columnEntry.getKey()).stream().findAny().ifPresent(sharding -> {
            GSI_DEFINITIONS.computeIfAbsent(columnEntry.getKey(), s -> new ArrayList<>());
            GSI_DEFINITIONS.get(columnEntry.getKey()).add("\t" + head + " " + column + " " + sharding + " " + option);
        });
    }

    private static void buildGsiDef(String key, String head, String option, String column,
                                    List<String> gsiDefSharding, Map<String, List<String>> gsiDefinitions) {
        for (String sharding : gsiDefSharding) {
            gsiDefinitions.computeIfAbsent(key, s -> new ArrayList<>());
            gsiDefinitions.get(key).add("\t" + head + " " + column + " " + sharding + " " + option);
        }
    }

    public static List<String[]> supportedGsiDef() {
        final List<String[]> result = new ArrayList<>();

        GSI_DEFINITIONS.forEach((k, v) -> v.forEach(s -> result.add(new String[] {s})));

        return result;
    }

    public static List<String[]> supportedGsiDefCreateAndAlter(String tableName) {
        final List<String[]> result = new ArrayList<>();

        // for each head + option
        GSI_DEF_HEAD.forEach(head -> {
            GSI_DEF_INDEX_OPTION.forEach(option -> {

                Iterator<Entry<String, List<String>>> createIndexIt = CREATE_GSI_DEF_COLUMN_DB.entrySet().iterator();
                Iterator<Entry<String, List<String>>> alterCreateIndexIt = GSI_DEF_COLUMN_DB.entrySet().iterator();

                if (alterCreateIndexIt.hasNext()) {
                    final Entry<String, List<String>> alterCreateIndexEntry = alterCreateIndexIt.next();
                    final List<String> createIndex = createIndexIt.next().getValue();
                    final List<String> alterCreateIndex = alterCreateIndexEntry.getValue();
                    final String shardingKey = alterCreateIndexEntry.getKey();

                    if (!alterCreateIndex.isEmpty()) {
                        final String createColumnDef = MessageFormat.format(createIndex.get(0), tableName);
                        final String alterColumnDef = alterCreateIndex.get(0);

                        GSI_DEF_SHARDING_DB.get(shardingKey)
                            .stream()
                            .findAny()
                            .ifPresent(sharding -> result.add(new String[] {
                                head + " " + createColumnDef + " " + sharding + " " + option,
                                head + " " + alterColumnDef + " " + sharding + " " + option}));
                    }
                }

                createIndexIt = CREATE_GSI_DEF_COLUMN_TB.entrySet().iterator();
                alterCreateIndexIt = GSI_DEF_COLUMN_TB.entrySet().iterator();
                if (alterCreateIndexIt.hasNext()) {
                    final Entry<String, List<String>> alterCreateIndexEntry = alterCreateIndexIt.next();
                    final List<String> createIndex = createIndexIt.next().getValue();
                    final List<String> alterCreateIndex = alterCreateIndexEntry.getValue();
                    final String shardingKey = alterCreateIndexEntry.getKey();

                    if (!alterCreateIndex.isEmpty()) {
                        final String createColumnDef = MessageFormat.format(createIndex.get(0), tableName);
                        final String alterColumnDef = alterCreateIndex.get(0);

                        GSI_DEF_SHARDING_TB.get(shardingKey)
                            .stream()
                            .findAny()
                            .ifPresent(sharding -> result.add(new String[] {
                                head + " " + createColumnDef + " " + sharding + " " + option,
                                head + " " + alterColumnDef + " " + sharding + " " + option}));
                    }
                }
            });
        });

        // for each sharding db
        Iterator<Entry<String, List<String>>> createIndexIt = CREATE_GSI_DEF_COLUMN_DB.entrySet().iterator();
        Iterator<Entry<String, List<String>>> alterCreateIndexIt = GSI_DEF_COLUMN_DB.entrySet().iterator();
        while (alterCreateIndexIt.hasNext()) {
            final Entry<String, List<String>> alterCreateIndexEntry = alterCreateIndexIt.next();
            final List<String> createIndex = createIndexIt.next().getValue();
            final List<String> alterCreateIndex = alterCreateIndexEntry.getValue();
            final String shardingKey = alterCreateIndexEntry.getKey();

            for (int i = 0; i < alterCreateIndex.size(); i++) {
                final String createColumnDef = MessageFormat.format(createIndex.get(i), tableName);
                final String alterColumnDef = alterCreateIndex.get(i);

                GSI_DEF_SHARDING_DB.get(shardingKey).forEach(sharding ->
                    GSI_DEF_HEAD.forEach(head -> GSI_DEF_INDEX_OPTION.stream()
                        .findAny()
                        .ifPresent(option -> result.add(new String[] {
                            head + " " + createColumnDef + " " + sharding + " " + option,
                            head + " " + alterColumnDef + " " + sharding + " " + option}))));
            }
        }

        // for each sharding tb
        createIndexIt = CREATE_GSI_DEF_COLUMN_TB.entrySet().iterator();
        alterCreateIndexIt = GSI_DEF_COLUMN_TB.entrySet().iterator();
        while (alterCreateIndexIt.hasNext()) {
            final Entry<String, List<String>> alterCreateIndexEntry = alterCreateIndexIt.next();
            final List<String> createIndex = createIndexIt.next().getValue();
            final List<String> alterCreateIndex = alterCreateIndexEntry.getValue();
            final String shardingKey = alterCreateIndexEntry.getKey();

            for (int i = 0; i < alterCreateIndex.size(); i++) {
                final String createColumnDef = MessageFormat.format(createIndex.get(i), tableName);
                final String alterColumnDef = alterCreateIndex.get(i);

                GSI_DEF_SHARDING_TB.get(shardingKey).forEach(sharding ->
                    GSI_DEF_HEAD.forEach(head -> GSI_DEF_INDEX_OPTION.stream()
                        .findAny()
                        .ifPresent(option -> result.add(new String[] {
                            head + " " + createColumnDef + " " + sharding + " " + option,
                            head + " " + alterColumnDef + " " + sharding + " " + option}))));
            }
        }

        return result;
    }

    public static final String GSI_PRIMARY_TABLE_NAME = "gsi_primary_table";

    public static final String FULL_TYPE_TABLE_TEMPLATE = "CREATE TABLE `{0}` (\n"
        + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "  `c_bit_1` bit(1) DEFAULT NULL,\n"
        + "  `c_bit_8` bit(8) DEFAULT NULL,\n"
        + "  `c_bit_16` bit(16) DEFAULT NULL,\n"
        + "  `c_bit_32` bit(32) DEFAULT NULL,\n"
        + "  `c_bit_64` bit(64) DEFAULT NULL,\n"
        + "  `c_tinyint_1` tinyint(1) DEFAULT NULL,\n"
        + "  `c_tinyint_1_un` tinyint(1) unsigned DEFAULT NULL,\n"
        + "  `c_tinyint_4` tinyint(4) DEFAULT NULL,\n"
        + "  `c_tinyint_4_un` tinyint(4) unsigned DEFAULT NULL,\n"
        + "  `c_tinyint_8` tinyint(8) DEFAULT NULL,\n"
        + "  `c_tinyint_8_un` tinyint(8) unsigned DEFAULT NULL,\n"
        + "  `c_smallint_1` smallint(1) DEFAULT NULL,\n"
        + "  `c_smallint_16` smallint(16) DEFAULT NULL,\n"
        + "  `c_smallint_16_un` smallint(16) unsigned DEFAULT NULL,\n"
        + "  `c_mediumint_1` mediumint(1) DEFAULT NULL,\n"
        + "  `c_mediumint_24` mediumint(24) DEFAULT NULL,\n"
        + "  `c_mediumint_24_un` mediumint(24) unsigned DEFAULT NULL,\n"
        + "  `c_int_1` int(1) DEFAULT NULL,\n"
        + "  `c_int_32` int(32) NOT NULL DEFAULT 0 COMMENT \"For multi pk.\",\n"
        + "  `c_int_32_un` int(32) unsigned DEFAULT NULL,\n"
        + "  `c_bigint_1` bigint(1) DEFAULT NULL,\n"
        + "  `c_bigint_64` bigint(64) DEFAULT NULL,\n"
        + "  `c_bigint_64_un` bigint(64) unsigned DEFAULT NULL,\n"
        + "  `c_decimal` decimal DEFAULT NULL,\n"
        + "  `c_decimal_pr` decimal(65,30) DEFAULT NULL,\n"
        + "  `c_float` float DEFAULT NULL,\n"
        + "  `c_float_pr` float(10,3) DEFAULT NULL,\n"
        + "  `c_float_un` float(10,3) unsigned DEFAULT NULL,\n"
        + "  `c_double` double DEFAULT NULL,\n"
        + "  `c_double_pr` double(10,3) DEFAULT NULL,\n"
        + "  `c_double_un` double(10,3) unsigned DEFAULT NULL,\n"
        + "  `c_date` date DEFAULT NULL COMMENT \"date\",\n"
        + "  `c_datetime` datetime DEFAULT NULL,\n"
        + "  `c_datetime_1` datetime(1) DEFAULT NULL,\n"
        + "  `c_datetime_3` datetime(3) DEFAULT NULL,\n"
        + "  `c_datetime_6` datetime(6) DEFAULT NULL,\n"
        + "  `c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
        + "  `c_timestamp_1` timestamp(1) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `c_timestamp_3` timestamp(3) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `c_timestamp_6` timestamp(6) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `c_time` time DEFAULT NULL,\n"
        + "  `c_time_1` time(1) DEFAULT NULL,\n"
        + "  `c_time_3` time(3) DEFAULT NULL,\n"
        + "  `c_time_6` time(6) DEFAULT NULL,\n"
        + "  `c_year` year DEFAULT NULL,\n"
        + "  `c_year_4` year(4) DEFAULT NULL,\n"
        + "  `c_char` char(10) DEFAULT NULL,\n"
        + "  `c_varchar` varchar(10) DEFAULT NULL,\n"
        + "  `c_binary` binary(10) DEFAULT NULL,\n"
        + "  `c_varbinary` varbinary(10) DEFAULT NULL,\n"
        + "  `c_blob_tiny` tinyblob DEFAULT NULL,\n"
        + "  `c_blob` blob DEFAULT NULL,\n"
        + "  `c_blob_medium` mediumblob DEFAULT NULL,\n"
        + "  `c_blob_long` longblob DEFAULT NULL,\n"
        + "  `c_text_tiny` tinytext DEFAULT NULL,\n"
        + "  `c_text` text DEFAULT NULL,\n"
        + "  `c_text_medium` mediumtext DEFAULT NULL,\n"
        + "  `c_text_long` longtext DEFAULT NULL,\n"
        + "  `c_enum` enum(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
        + "  `c_set` set(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
        + "  `c_json` json DEFAULT NULL,\n"
        + "  `c_geometory` geometry DEFAULT NULL,\n"
        + "  `c_point` point DEFAULT NULL,\n"
        + "  `c_linestring` linestring DEFAULT NULL,\n"
        + "  `c_polygon` polygon DEFAULT NULL,\n"
        + "  `c_multipoint` multipoint DEFAULT NULL,\n"
        + "  `c_multilinestring` multilinestring DEFAULT NULL,\n"
        + "  `c_multipolygon` multipolygon DEFAULT NULL,\n"
        + "  `c_geometrycollection` geometrycollection DEFAULT NULL,\n"
        + "  PRIMARY KEY ({1})\n"
        + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT=\"10000000\"\n {2}";

    public static final String FULL_TYPE_TABLE_TEMPLATE_SUPPORTED_GSI = "CREATE TABLE `{0}` (\n"
        + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "  `c_bit_1` bit(1) DEFAULT NULL,\n"
        + "  `c_bit_8` bit(8) DEFAULT NULL,\n"
        + "  `c_bit_16` bit(16) DEFAULT NULL,\n"
        + "  `c_bit_32` bit(32) DEFAULT NULL,\n"
        + "  `c_bit_64` bit(64) DEFAULT NULL,\n"
        + "  `c_tinyint_1` tinyint(1) DEFAULT NULL,\n"
        + "  `c_tinyint_1_un` tinyint(1) unsigned DEFAULT NULL,\n"
        + "  `c_tinyint_4` tinyint(4) DEFAULT NULL,\n"
        + "  `c_tinyint_4_un` tinyint(4) unsigned DEFAULT NULL,\n"
        + "  `c_tinyint_8` tinyint(8) DEFAULT NULL,\n"
        + "  `c_tinyint_8_un` tinyint(8) unsigned DEFAULT NULL,\n"
        + "  `c_smallint_1` smallint(1) DEFAULT NULL,\n"
        + "  `c_smallint_16` smallint(16) DEFAULT NULL,\n"
        + "  `c_smallint_16_un` smallint(16) unsigned DEFAULT NULL,\n"
        + "  `c_mediumint_1` mediumint(1) DEFAULT NULL,\n"
        + "  `c_mediumint_24` mediumint(24) DEFAULT NULL,\n"
        + "  `c_mediumint_24_un` mediumint(24) unsigned DEFAULT NULL,\n"
        + "  `c_int_1` int(1) DEFAULT NULL,\n"
        + "  `c_int_32` int(32) NOT NULL DEFAULT 0 COMMENT \"For multi pk.\",\n"
        + "  `c_int_32_un` int(32) unsigned DEFAULT NULL,\n"
        + "  `c_bigint_1` bigint(1) DEFAULT NULL,\n"
        + "  `c_bigint_64` bigint(64) DEFAULT NULL,\n"
        + "  `c_bigint_64_un` bigint(64) unsigned DEFAULT NULL,\n"
        + "  `c_decimal` decimal DEFAULT NULL,\n"
        + "  `c_decimal_pr` decimal(10,3) DEFAULT NULL,\n"
        + "  `c_float` float DEFAULT NULL,\n"
        + "  `c_float_pr` float(10,3) DEFAULT NULL,\n"
        + "  `c_float_un` float(10,3) unsigned DEFAULT NULL,\n"
        + "  `c_double` double DEFAULT NULL,\n"
        + "  `c_double_pr` double(10,3) DEFAULT NULL,\n"
        + "  `c_double_un` double(10,3) unsigned DEFAULT NULL,\n"
        + "  `c_date` date DEFAULT NULL COMMENT \"date\",\n"
        + "  `c_datetime` datetime DEFAULT NULL,\n"
        + "  `c_datetime_1` datetime(1) DEFAULT NULL,\n"
        + "  `c_datetime_3` datetime(3) DEFAULT NULL,\n"
        + "  `c_datetime_6` datetime(6) DEFAULT NULL,\n"
        + "  `c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
        + "  `c_timestamp_1` timestamp(1) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `c_timestamp_3` timestamp(3) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `c_timestamp_6` timestamp(6) DEFAULT \"2000-01-01 00:00:00\",\n"
        + "  `c_time` time DEFAULT NULL,\n"
        + "  `c_time_1` time(1) DEFAULT NULL,\n"
        + "  `c_time_3` time(3) DEFAULT NULL,\n"
        + "  `c_time_6` time(6) DEFAULT NULL,\n"
        + "  `c_year` year DEFAULT NULL,\n"
        + "  `c_year_4` year(4) DEFAULT NULL,\n"
        + "  `c_char` char(10) DEFAULT NULL,\n"
        + "  `c_varchar` varchar(10) DEFAULT NULL,\n"
        + "  `c_binary` binary(10) DEFAULT NULL,\n"
        + "  `c_varbinary` varbinary(10) DEFAULT NULL,\n"
        + "  `c_blob_tiny` tinyblob DEFAULT NULL,\n"
        + "  `c_blob` blob DEFAULT NULL,\n"
        + "  `c_blob_medium` mediumblob DEFAULT NULL,\n"
        + "  `c_blob_long` longblob DEFAULT NULL,\n"
        + "  `c_text_tiny` tinytext DEFAULT NULL,\n"
        + "  `c_text` text DEFAULT NULL,\n"
        + "  `c_text_medium` mediumtext DEFAULT NULL,\n"
        + "  `c_text_long` longtext DEFAULT NULL,\n"
        + "  `c_enum` enum(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
        + "  `c_set` set(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
        + "  `c_json` json DEFAULT NULL,\n"
        + "  `c_geometory` geometry DEFAULT NULL,\n"
        + "  `c_point` point DEFAULT NULL,\n"
        + "  `c_linestring` linestring DEFAULT NULL,\n"
        + "  `c_polygon` polygon DEFAULT NULL,\n"
        + "  `c_multipoint` multipoint DEFAULT NULL,\n"
        + "  `c_multilinestring` multilinestring DEFAULT NULL,\n"
        + "  `c_multipolygon` multipolygon DEFAULT NULL,\n"
        + "  `c_geometrycollection` geometrycollection DEFAULT NULL,\n"
        + "  PRIMARY KEY {1}\n"
        + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT=\"10000000\"\n {2}";

    public static final String TABLE_TEMPLATE =
        "CREATE TABLE `{0}` ({1},\n {2}) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 {3}";

    public static final String PRIMARY_KEY_TEMPLATE = "PRIMARY KEY (`{0}`)";

    public static String getFullTypeTableDef(String tableName, String partitioningDefinition) {
        return MessageFormat
            .format(FULL_TYPE_TABLE_TEMPLATE, tableName, "`id`", partitioningDefinition);
    }

    public static String getFullTypeMultiPkTableDef(String tableName, String partitioningDefinition) {
        return MessageFormat
            .format(FULL_TYPE_TABLE_TEMPLATE, tableName, "`id`,`c_int_32`", partitioningDefinition);
    }

    public static String getFullTypeTableDefWithGsi(String tableName, String primaryPartDef, List<String> gsiDef) {

        String gsiDefStr = "";
        for (int i = 0; i < gsiDef.size(); i++) {
            if (!gsiDefStr.isEmpty()) {
                gsiDefStr += ",\n";
            }
            gsiDefStr += gsiDef.get(i);
        }
        String primaryKeyAndGsiStr = "(id)";
        if (!gsiDefStr.isEmpty()) {
            primaryKeyAndGsiStr += ",\n";
            primaryKeyAndGsiStr += gsiDefStr;
        }

        return MessageFormat
            .format(FULL_TYPE_TABLE_TEMPLATE_SUPPORTED_GSI, tableName, primaryKeyAndGsiStr, primaryPartDef);
    }

    public static String getTableDef(String tableName, String columnDef, String indexDef,
                                     String partitioningDefinition) {
        return MessageFormat.format(TABLE_TEMPLATE, tableName, columnDef, indexDef, partitioningDefinition);
    }

    public static final String DEFAULT_PARTITIONING_DEFINITION =
        " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 3";

    public static final String DEFAULT_NEW_PARTITIONING_DEFINITION =
        " partition by key(id) partitions 3";

}

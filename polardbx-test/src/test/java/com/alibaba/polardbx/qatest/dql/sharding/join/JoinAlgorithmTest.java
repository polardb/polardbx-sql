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

package com.alibaba.polardbx.qatest.dql.sharding.join;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Join Algorithm 测试
 *
 * @author zilin.zl
 * @since 5.3.9
 */


public class JoinAlgorithmTest extends ReadBaseTestCase {

    @Parameterized.Parameters(
        name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},table5={5},table6={6},table7={7}," +
            "table8={8},table9={9},table10={10},table11={11},table12={12},table13={13},table14={14},table15={15}," +
            "table16={16},table17={17},table18={18},table19={19}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBushyJoinTestTable());
    }

    String[] group1;
    String[] group2;
    String[] group3;
    String[] group4;
    String[] group5;
    String[][] groupList;

    static String[] attributeList = {
        "pk",
        "varchar_test",
        "integer_test",
        "char_test",
//                                "blob_test",
        "tinyint_test",
        "tinyint_1bit_test",
        "smallint_test",
        "mediumint_test",
//                                "bit_test",
        "bigint_test",
//                                "float_test",
//                                "double_test",
//                                "decimal_test",
        "date_test",
        "time_test",
        "datetime_test",
        "timestamp_test",
        "year_test"
    };

    static String[] projectList = {
//        "pk",
        "varchar_test",
        "integer_test",
        "char_test",
        "tinyint_test",
        "tinyint_1bit_test",
        "smallint_test",
        "mediumint_test",
        "bigint_test",
        "date_test",
        "time_test",
        "datetime_test",
        "year_test"
    };

    String selectProject;

    {
        StringBuilder stringBuilder = new StringBuilder();
        Arrays.stream(projectList).forEach(col -> stringBuilder.append(" a." + col + ","));
        stringBuilder.setLength(stringBuilder.length() - 1);
        selectProject = stringBuilder.toString();

    }

    List<Pair<String, String>> attributePairList = new ArrayList<>();

    public JoinAlgorithmTest(String g1tb1, String g1tb2, String g1tb3, String g1tb4,
                             String g2tb1, String g2tb2, String g2tb3, String g2tb4,
                             String g3tb1, String g3tb2, String g3tb3, String g3tb4,
                             String g4tb1, String g4tb2, String g4tb3, String g4tb4,
                             String g5tb1, String g5tb2, String g5tb3, String g5tb4) {
        /**
         *  tables from same group have same rule
         *  tables from different group have different rule
         */
        group1 = new String[] {g1tb1, g1tb2, g1tb3, g1tb4};
        group2 = new String[] {g2tb1, g2tb2, g2tb3, g2tb4};
        group3 = new String[] {g3tb1, g3tb2, g3tb3, g3tb4};
        group4 = new String[] {g4tb1, g4tb2, g4tb3, g4tb4};
        group5 = new String[] {g5tb1, g5tb2, g5tb3, g5tb4};
        groupList = new String[][] {group1, group2, group3, group4, group5};
        for (int i = 0; i < attributeList.length; i++) {
            attributePairList.add(Pair.of(attributeList[i], attributeList[i]));
        }
    }

    @Test
    public void SortMergeJoinTest() {
        for (String[] group : groupList) {
            String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) SORT_MERGE_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = "select  " + selectProject + "  from " + group[1] + " a, " + group[3] + " b " +
                    "where a." + attribute1 + " = b." + attribute2 + " and a.pk < 20";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void HashJoinTest() {
        for (String[] group : groupList) {
            String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) HASH_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = "select  " + selectProject + "  from " + group[1] + " a, " + group[3] + " b " +
                    "where a." + attribute1 + " = b." + attribute2 + " and a.pk < 20";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void MultiJoinTypeHashJoinTest() {
        String[] joinTypeArray = {"inner", "left", "right"};
        for (String[] group : groupList) {
            for (String joinType : joinTypeArray) {
                String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) HASH_JOIN(" + group[1] + "," + group[3] + ")*/ ";
                String sql =
                    "select  " + selectProject + "  from " + group[1] + " a " + joinType + " join " + group[3] + " b " +
                        "on a.pk > b.pk and a.varchar_test = b.varchar_test and a.pk < 100";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void OuterJoinMultiJoinAlgorithmTest() {
        String[] joinAlgorithmHintArray = {"HASH_JOIN", "SORT_MERGE_JOIN", "BKA_JOIN"};
        for (String[] group : groupList) {
            for (String joinAlgorithm : joinAlgorithmHintArray) {
                String hint =
                    "/*+TDDL:cmd_extra(ENABLE_CBO=true) " + joinAlgorithm + "(" + group[1] + "," + group[3] + ")*/ ";
                String sql = "select  " + selectProject + "  from " + group[1] + " a  left join " + group[3] + " b " +
                    "on a.varchar_test = b.varchar_test where b.pk is null";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
            }
        }

        for (String[] group : groupList) {
            for (String joinAlgorithm : joinAlgorithmHintArray) {
                String hint =
                    "/*+TDDL:cmd_extra(ENABLE_CBO=true) " + joinAlgorithm + "(" + group[1] + "," + group[3] + ")*/ ";
                String sql = "select  " + selectProject + "  from " + group[1] + " a  right join " + group[3] + " b " +
                    "on a.varchar_test = b.varchar_test where a.pk is null";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
            }
        }
    }

    @Test
    public void MultiJoinTypeSortMergeJoinTest() {

        for (String[] group : groupList) {
            String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) SORT_MERGE_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            String sql = "select  " + selectProject + "  from " + group[1] + " a inner join " + group[3] + " b " +
                "on a.pk > b.pk and a.varchar_test = b.varchar_test and a.pk < 100";
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
        }

        String[] joinTypeArray = {"inner", "left", "right"};
        for (String[] group : groupList) {
            for (String joinType : joinTypeArray) {
                String hint =
                    "/*+TDDL:cmd_extra(ENABLE_CBO=true) SORT_MERGE_JOIN(" + group[1] + "," + group[3] + ")*/ ";
                String sql =
                    "select  " + selectProject + "  from " + group[1] + " a " + joinType + " join " + group[3] + " b " +
                        "on  a.varchar_test = b.varchar_test where a.pk < 50";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void BKAJoinTest() {
        for (String[] group : groupList) {
            String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) BKA_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = "select  " + selectProject + "  from " + group[1] + " a, " + group[3] + " b " +
                    "where a." + attribute1 + " = b." + attribute2 + " and a.pk < 20";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void NLJoinTest() {
        for (String[] group : groupList) {
            String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) NL_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = "select  " + selectProject + "  from " + group[1] + " a, " + group[3] + " b " +
                    "where a." + attribute1 + " = b." + attribute2 + " and a.pk < 20";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void MaterializedSemiJoinTest() {
        for (String[] group : groupList) {
            String hint =
                "/*+TDDL:cmd_extra(ENABLE_CBO=true) MATERIALIZED_SEMI_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = "select  " + selectProject + "  from " + group[1] + " a " +
                    "where a." + attribute1 + " in  ( select b." + attribute2 + " from " + group[3] + " b )"
                    + " and a.pk < 20";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void MaterializedSemiJoinPruningTest() {
        for (String[] group : groupList) {
            String hint =
                "/*+TDDL:cmd_extra(ENABLE_CBO=true) MATERIALIZED_SEMI_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            String attribute1 = "pk";
            String attribute2 = "integer_test";
            String sql = "select  " + selectProject + "  from " + group[1] + " a " +
                "where a." + attribute1 + " in  ( select b." + attribute2 + " from " + group[3] + " b where b.pk=2 )"
                + " and a.pk < 20";
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);

            // Use trace number to test pruning state.
            JdbcUtil.executeQuery("trace " + hint + sql, tddlConnection);
            List<List<String>> trace = getTrace(tddlConnection);
            final long bkaCount = trace.stream().filter(line -> line.get(9).contains(" IN ")).count();
            if (bkaCount > 0) {
                Assert.assertEquals("Bad BKA pruning.", 1, bkaCount);
            }
        }
    }

    @Test
    public void MaterializedAntiSemiJoinTest() {

        for (String[] group : groupList) {
            String hint =
                "/*+TDDL:cmd_extra(ENABLE_CBO=true) MATERIALIZED_SEMI_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = "select " + selectProject + " from " + group[1] + " a " +
                    "where a." + attribute1 + " not in  ( select b." + attribute2 + " from " + group[3] + " b )"
                    + " and a.pk < 20";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void SemiHashJoinTest() {
        for (String[] group : groupList) {
            String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) SEMI_HASH_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = "select " + selectProject + " from " + group[1] + " a " +
                    "where a." + attribute1 + " in ( select b." + attribute2 + " from " + group[3] + " b )"
                    + " and a.pk < 20";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void SemiSortMergeJoinTest() {
        for (String[] group : groupList) {
            String hint =
                "/*+TDDL:cmd_extra(ENABLE_CBO=true) SEMI_SORT_MERGE_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = "select  " + selectProject + "  from " + group[1] + " a " +
                    "where a." + attribute1 + " in ( select b." + attribute2 + " from " + group[3] + " b )"
                    + " and a.pk < 20";
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void HashJoinHashAggTest() {
        for (String[] group : groupList) {
            String hint =
                "/*+TDDL:cmd_extra(ENABLE_CBO=true,ENABLE_SORT_AGG=false,ENABLE_HASH_AGG=true) HASH_JOIN(" + group[1]
                    + "," + group[3] + ")*/ ";
            String sql = "select count(a.pk), min(a.pk) from " + group[1] + " a, " + group[3] + " b " +
                "where a.integer_test = b.integer_test and a.pk < 20";
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
        }

        for (String[] group : groupList) {
            String hint =
                "/*+TDDL:cmd_extra(ENABLE_CBO=true,ENABLE_SORT_AGG=false,ENABLE_HASH_AGG=true) HASH_JOIN(" + group[1]
                    + "," + group[3] + ")*/ ";
            String sql = "select count(a.pk), min(a.pk) from " + group[1] + " a, " + group[3] + " b " +
                "where a.integer_test = b.integer_test and a.pk < -20";
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
        }
    }

    @Test
    public void SemiNLJoinTest() {
        for (String[] group : groupList) {
            String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) SEMI_NL_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = " select  " + selectProject + "  from " + group[1] + " a " +
                    "where a." + attribute1 + " in ( select b." + attribute2 + " from " + group[3] + " b )"
                    + " and a.pk < 20";
//                System.out.println(sql);
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void SemiNLAntiJoinTest() {
        for (String[] group : groupList) {
            String hint = "/*+TDDL:cmd_extra(ENABLE_CBO=true) SEMI_NL_JOIN(" + group[1] + "," + group[3] + ")*/ ";
            for (Pair<String, String> attributePair : attributePairList) {
                String attribute1 = attributePair.getKey();
                String attribute2 = attributePair.getValue();
                String sql = " select  " + selectProject + "  from " + group[1] + " a " +
                    "where a." + attribute1 + " not in ( select b." + attribute2 + " from " + group[3] + " b )"
                    + " and a.pk < 20";
//                System.out.println(sql);
                selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }
}

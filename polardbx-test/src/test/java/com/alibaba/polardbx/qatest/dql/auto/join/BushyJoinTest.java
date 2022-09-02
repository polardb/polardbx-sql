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

package com.alibaba.polardbx.qatest.dql.auto.join;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Bushy Join测试
 *
 * @author zilin.zl
 * @since 5.3.5
 */

public class BushyJoinTest extends AutoReadBaseTestCase {

    private static final Log log = LogFactory.getLog(BushyJoinTest.class);

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

    public BushyJoinTest(String g1tb1, String g1tb2, String g1tb3, String g1tb4,
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
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerThreeBushyJoin() {
        for (String[] group : groupList) {
            String sql = "select * from " + group[0] + " c , " + group[1] + " a, " + group[3] + " b " +
                "where a.pk = b.integer_test and b.integer_test = c.pk and c.pk=50";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerFourBushyJoin() {
        for (String[] group : groupList) {
            String sql =
                "select * from " + group[0] + " c , " + group[1] + " a, " + group[2] + " b, " + group[3] + " d " +
                    "where a.pk = b.integer_test and b.integer_test = c.pk and c.pk=50 and d.pk = b.integer_test";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerThreeLeftBushyJoin() {
        for (String[] group : groupList) {
            String sql = "select * from " + group[0] + " a left join " +
                " ( select (case when t1.pk+t2.pk < 500 then t1.pk+t2.pk else null end) as pk, t1.integer_test " +
                "   from " + group[1] + " t1, " + group[2] + " t2 where t1.pk = t2.pk ) b " +
                " on a.pk = b.pk ";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerThreeRightBushyJoin() {
        for (String[] group : groupList) {
            String sql = "select * from " + group[0] + " a right join " +
                " ( select (case when t1.pk+t2.pk < 500 then t1.pk+t2.pk else null end) as pk, t1.integer_test " +
                "   from " + group[1] + " t1, " + group[2] + " t2 where t1.pk = t2.pk ) b " +
                " on a.pk = b.pk ";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerFourLeftBushyJoin() {
        for (String[] group : groupList) {
            String sql = "select count(pk0), count(pk1), count(pk2), count(pk3) from \n" +
                "( select (case when pk >= 750 then null else pk end ) as pk0 from " + group[0] + ") t0 \n" +
                "left join\n" +
                "( select (case when pk < 250 then null else pk end ) as pk1 from " + group[1]
                + ") t1 on t0.pk0 = t1.pk1\n" +
                "left join\n" +
                "( select (case when pk >= 250 and pk < 500 then null else pk end ) as pk2 from " + group[2]
                + ") t2 on t1.pk1 = t2.pk2\n" +
                "left join\n" +
                "( select (case when pk >= 750 then null else pk end ) as pk3 from " + group[3] + ") t3 on t2.pk2 = t3"
                + ".pk3";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerFourRightBushyJoin() {
        for (String[] group : groupList) {
            String sql = "select count(pk0), count(pk1), count(pk2), count(pk3) from \n" +
                "( select (case when pk >= 750 then null else pk end ) as pk0 from " + group[0] + ") t0 \n" +
                "right join\n" +
                "( select (case when pk < 250 then null else pk end ) as pk1 from " + group[1]
                + ") t1 on t0.pk0 = t1.pk1\n" +
                "right join\n" +
                "( select (case when pk >= 250 and pk < 500 then null else pk end ) as pk2 from " + group[2]
                + ") t2 on t1.pk1 = t2.pk2\n" +
                "right join\n" +
                "( select (case when pk >= 750 then null else pk end ) as pk3 from " + group[3] + ") t3 on t2.pk2 = t3"
                + ".pk3";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerEightLeftBushyJoin() {
        for (String[] group : groupList) {
            String leftSql = "select (case when a.pk < 10 then a.pk else null end) as pk from "
                + group[0] + " c , " + group[1] + " a, " + group[3] + " b ," + group[3] + " d " +
                "where a.pk = b.integer_test and b.integer_test = c.pk and d.pk = b.integer_test";
            String rightql = "select (case when a.pk < 8 then null else a.pk end) as pk from "
                + group[0] + " c , " + group[1] + " a, " + group[3] + " b, " + group[3] + " d " +
                "where a.pk = b.integer_test and b.integer_test = c.pk and d.pk = b.integer_test";

            String sql = "select * from (" + leftSql + ") t1 left join (" + rightql + ") t2 on t1.pk = t2.pk";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerEightRightBushyJoin() {
        for (String[] group : groupList) {
            String leftSql = "select (case when a.pk < 10 then a.pk else null end) as pk from "
                + group[0] + " c , " + group[1] + " a, " + group[3] + " b ," + group[3] + " d " +
                "where a.pk = b.integer_test and b.integer_test = c.pk and d.pk = b.integer_test";
            String rightql = "select (case when a.pk < 8 then null else a.pk end) as pk from "
                + group[0] + " c , " + group[1] + " a, " + group[3] + " b, " + group[3] + " d " +
                "where a.pk = b.integer_test and b.integer_test = c.pk and d.pk = b.integer_test";

            String sql = "select * from (" + leftSql + ") t1 right join (" + rightql + ") t2 on t1.pk = t2.pk";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerFiveBushyJoin() {
        for (String[] group : groupList) {
            String sql = "select * from " + group[0] + " a, "
                + group[1] + " b, "
                + group[2] + " c, "
                + group[3] + " d, "
                + group[0] + " e "
                + "where a.pk = b.integer_test and b.integer_test = c.pk and c.pk=50 and d.pk = b.integer_test and e.pk = a.pk";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerFourBushyJoinSubQuery() {
        for (int i = 0; i < groupList.length; i++) {
            String[] group = groupList[i];
            String sql = " select * from \n" +
                "( select  pk from " + group[0] + ") t0 \n" +
                "inner join\n" +
                "( select  pk from " + group[1] + ") t1 on t0.pk = t1.pk\n" +
                "inner join\n" +
                "( select  pk from " + group[2] + ") t2 on t1.pk = t2.pk\n" +
                "inner join\n" +
                "( select  pk from " + group[3] + ") t3 on t2.pk = t3.pk";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerFiveBushyJoinSubQuery() {
        for (int i = 0; i < groupList.length; i++) {
            String[] group = groupList[i];
            String sql = "select * from " + group[0] + " a, "
                + group[1] + " b, "
                + "(select s1.* from "
                + groupList[(i + 1) % groupList.length][0] + " s1 , " + groupList[(i + 1) % groupList.length][0]
                + " s2 where s1.pk = s2.pk )" + " c, "
                + group[3] + " d, "
                + group[0] + " e "
                + "where a.pk = b.integer_test and b.integer_test = c.pk and c.pk=50 and d.pk = b.integer_test and e.pk = a.pk";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerSixBushyJoin() {
        for (String[] group : groupList) {
            String sql = "select * from " + group[0] + " a, "
                + group[1] + " b, "
                + group[2] + " c, "
                + group[3] + " d, "
                + group[0] + " e, "
                + group[1] + " f "
                + "where a.pk = c.pk and c.pk = e.pk and e.pk = 1 and b.pk=d.pk and d.pk = f.pk and f.pk = 9";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerNineBushyJoin() {
        for (String[] group : groupList) {
            String sql = "select * from " + group[0] + " a, "
                + group[1] + " b, "
                + group[2] + " c, "
                + group[3] + " d, "
                + group[0] + " e, "
                + group[1] + " f, "
                + group[2] + " g, "
                + group[3] + " h, "
                + group[0] + " i "
                + "WHERE a.pk = c.pk "
                + "  AND c.pk = e.pk "
                + "  AND e.pk = 1 "
                + "  AND e.pk = g.pk "
                + "  AND b.pk = d.pk "
                + "  AND d.pk = f.pk "
                + "  AND f.pk = 9 "
                + "  AND f.pk = h.pk";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerNineBushyJoin2() {
        for (String[] group : groupList) {
            String sql = "select * from " + group[0] + " a, "
                + group[1] + " b, "
                + group[2] + " c, "
                + group[3] + " d, "
                + group[0] + " e, "
                + group[1] + " f, "
                + group[2] + " g, "
                + group[3] + " h, "
                + group[0] + " i "
                + "WHERE a.pk = d.pk "
                + "  AND d.pk = g.pk "
                + "  AND a.pk in (1,2,3,4,5,6,7,8,9)"
                + "  AND b.pk = e.pk "
                + "  AND e.pk = h.pk "
                + "  AND h.pk in (10,11,12,13,14,15)"
                + "  AND c.pk = f.pk "
                + "  AND f.pk = i.pk "
                + "  AND i.pk in (16,17,18,19,20,21)";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void InnerNineBushyJoin3() {
        for (String[] group : groupList) {
            String sql = "select * from " + group[0] + " a, "
                + group[1] + " b, "
                + group[2] + " c, "
                + group[3] + " d, "
                + group[0] + " e, "
                + group[1] + " f, "
                + group[2] + " g, "
                + group[3] + " h, "
                + group[0] + " i "
                + "WHERE a.pk = e.integer_test "
                + "  AND b.pk = e.integer_test "
                + "  AND c.pk = e.integer_test "
                + "  AND d.pk = e.integer_test "
                + "  AND h.pk = e.integer_test "
                + "  AND c.pk = e.integer_test "
                + "  AND f.pk = e.integer_test "
                + "  AND g.pk = e.integer_test "
                + "  AND i.pk = e.integer_test "
                + " order by a.pk";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void Inner16BushyJoinDifferentGroup() {
        String sql = "select * from " + groupList[0][0] + " t00, "
            + groupList[1][0] + " t10, "
            + groupList[2][0] + " t20, "
            + groupList[3][0] + " t30, "
            + groupList[0][1] + " t01, "
            + groupList[1][1] + " t11, "
            + groupList[2][1] + " t21, "
            + groupList[3][1] + " t31, "
            + groupList[0][2] + " t02, "
            + groupList[1][2] + " t12, "
            + groupList[2][2] + " t22, "
            + groupList[3][2] + " t32, "
            + groupList[0][3] + " t03, "
            + groupList[1][3] + " t13, "
            + groupList[2][3] + " t23, "
            + groupList[3][3] + " t33 "

            + "WHERE t00.pk = t01.pk "
            + "  AND t00.pk = t02.pk "
            + "  AND t00.pk = t03.pk "
            + "  AND t00.pk = 1 "

            + "  AND t10.pk = t11.pk "
            + "  AND t10.pk = t12.pk "
            + "  AND t10.pk = t13.pk "
            + "  AND t10.pk = 5 "

            + "  AND t20.pk = t21.pk "
            + "  AND t20.pk = t22.pk "
            + "  AND t20.pk = t23.pk "
            + "  AND t20.pk = 10 "

            + "  AND t30.pk = t31.pk "
            + "  AND t30.pk = t32.pk "
            + "  AND t30.pk = t33.pk "
            + "  AND t30.pk = 15 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        testBushyJoinRuleHasBeenUsed(sql);
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void Inner16BushyJoinDifferentGroup2() {
        String sql = "select * from " + groupList[0][0] + " t00, "
            + groupList[1][0] + " t10, "
            + groupList[2][0] + " t20, "
            + groupList[3][0] + " t30, "
            + groupList[0][1] + " t01, "
            + groupList[1][1] + " t11, "
            + groupList[2][1] + " t21, "
            + groupList[3][1] + " t31, "
            + groupList[0][2] + " t02, "
            + groupList[1][2] + " t12, "
            + groupList[2][2] + " t22, "
            + groupList[3][2] + " t32, "
            + groupList[0][3] + " t03, "
            + groupList[1][3] + " t13, "
            + groupList[2][3] + " t23, "
            + groupList[3][3] + " t33 "

            + "WHERE t00.pk = t01.pk "
            + "  AND t00.pk = t02.pk "
            + "  AND t00.pk = t03.pk "

            + "  AND t10.pk = t11.pk "
            + "  AND t10.pk = t12.pk "
            + "  AND t10.pk = t13.pk "

            + "  AND t20.pk = t21.pk "
            + "  AND t20.pk = t22.pk "
            + "  AND t20.pk = t23.pk "

            + "  AND t30.pk = t31.pk "
            + "  AND t30.pk = t32.pk "
            + "  AND t30.pk = t33.pk "

            + "  AND t00.pk = t10.pk "
            + "  AND t00.pk = t20.pk "
            + "  AND t00.pk = t30.pk "
            + "  AND t00.pk = 1 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        testBushyJoinRuleHasBeenUsed(sql);
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void Inner20BushyJoinDifferentGroup2() {
        String sql = "select * from " + groupList[0][0] + " t00, "
            + groupList[1][0] + " t10, "
            + groupList[2][0] + " t20, "
            + groupList[3][0] + " t30, "
            + groupList[4][0] + " t40, "
            + groupList[0][1] + " t01, "
            + groupList[1][1] + " t11, "
            + groupList[2][1] + " t21, "
            + groupList[3][1] + " t31, "
            + groupList[4][1] + " t41, "
            + groupList[0][2] + " t02, "
            + groupList[1][2] + " t12, "
            + groupList[2][2] + " t22, "
            + groupList[3][2] + " t32, "
            + groupList[4][2] + " t42, "
            + groupList[0][3] + " t03, "
            + groupList[1][3] + " t13, "
            + groupList[2][3] + " t23, "
            + groupList[3][3] + " t33, "
            + groupList[4][3] + " t43 "

            + "WHERE t00.pk = t01.pk "
            + "  AND t00.pk = t02.pk "
            + "  AND t00.pk = t03.pk "

            + "  AND t10.pk = t11.pk "
            + "  AND t10.pk = t12.pk "
            + "  AND t10.pk = t13.pk "

            + "  AND t20.pk = t21.pk "
            + "  AND t20.pk = t22.pk "
            + "  AND t20.pk = t23.pk "

            + "  AND t30.pk = t31.pk "
            + "  AND t30.pk = t32.pk "
            + "  AND t30.pk = t33.pk "

            + "  AND t40.pk = t41.pk "
            + "  AND t40.pk = t42.pk "
            + "  AND t40.pk = t43.pk "

            + "  AND t00.pk = t10.pk "
            + "  AND t00.pk = t20.pk "
            + "  AND t00.pk = t30.pk "
            + "  AND t00.pk = t40.pk "
            + "  AND t00.pk = 1 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        testBushyJoinRuleHasBeenUsed(sql);
    }

    /**
     * @since 5.3.5
     */
    @Test
    public void Inner21BushyJoinDifferentGroup2() {
        String sql = "select * from " + groupList[0][0] + " t00, "
            + groupList[1][0] + " t10, "
            + groupList[2][0] + " t20, "
            + groupList[3][0] + " t30, "
            + groupList[4][0] + " t40, "
            + groupList[0][1] + " t01, "
            + groupList[1][1] + " t11, "
            + groupList[2][1] + " t21, "
            + groupList[3][1] + " t31, "
            + groupList[4][1] + " t41, "
            + groupList[0][2] + " t02, "
            + groupList[1][2] + " t12, "
            + groupList[2][2] + " t22, "
            + groupList[3][2] + " t32, "
            + groupList[4][2] + " t42, "
            + groupList[0][3] + " t03, "
            + groupList[1][3] + " t13, "
            + groupList[2][3] + " t23, "
            + groupList[3][3] + " t33, "
            + groupList[4][3] + " t43 "
            + " left join " + groupList[3][2] + " ltable on t43.pk = ltable.pk "
            + "WHERE t00.pk = t01.pk "
            + "  AND t00.pk = t02.pk "
            + "  AND t00.pk = t03.pk "

            + "  AND t10.pk = t11.pk "
            + "  AND t10.pk = t12.pk "
            + "  AND t10.pk = t13.pk "

            + "  AND t20.pk = t21.pk "
            + "  AND t20.pk = t22.pk "
            + "  AND t20.pk = t23.pk "

            + "  AND t30.pk = t31.pk "
            + "  AND t30.pk = t32.pk "
            + "  AND t30.pk = t33.pk "

            + "  AND t40.pk = t41.pk "
            + "  AND t40.pk = t42.pk "
            + "  AND t40.pk = t43.pk "

            + "  AND t00.pk = t10.pk "
            + "  AND t00.pk = t20.pk "
            + "  AND t00.pk = t30.pk "
            + "  AND t00.pk = t40.pk "
            + "  AND t00.pk = 1 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        testBushyJoinRuleHasBeenUsed(sql);
    }

    /**
     * @since 5.3.7
     */
    public void testBushyJoinRuleHasBeenUsed(String sql) {
        String explainDisableSql =
            "explain /*+TDDL:cmd_extra(ENABLE_JOIN_CLUSTERING=false,DISABLE_BUSHY_JOIN=true,ENABLE_CBO_PUSH_JOIN=false)*/"
                + sql;
        String explainEnableSql =
            "explain /*+TDDL:cmd_extra(ENABLE_JOIN_CLUSTERING=true,DISABLE_BUSHY_JOIN=false,ENABLE_CBO_PUSH_JOIN=false)*/"
                + sql;
        String explainEnableString = "";
        String explainDisableString = "";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(explainEnableSql);
            while (rs.next()) {
                explainEnableString += rs.getString(1);
            }
            rs.close();
            rs = statement.executeQuery(explainDisableSql);
            while (rs.next()) {
                explainDisableString += rs.getString(1);
            }
            if (!usingNewPartDb()) {
                Assert.assertTrue(StringUtils.countMatches(explainEnableString, "LogicalView")
                    < StringUtils.countMatches(explainDisableString, "LogicalView"));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.7
     */
    @Test
    public void InnerLeft4BushyJoinParenthesesTest() {
        String sql =
            "select * from " + groupList[3][0] + " t1, " + groupList[2][1] + " t2, " + groupList[3][2] + " t3 " +
                " left join " + groupList[3][3]
                + " t4 on t3.pk = t4.pk  where t1.pk = t2.pk and t2.pk = t3.pk and t2.pk = 2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        testBushyJoinRuleHasBeenUsed(sql);
    }

    /**
     * @since 5.3.7
     */
    @Test
    public void Inner3BushyJoinBroadcastTest() {
        String sql =
            "select * from " + groupList[3][0] + " t1, " + groupList[2][1] + " t2, " + groupList[4][1] + " t3 " +
                " where t2.pk = t3.pk and t1.pk = 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        testBushyJoinRuleHasBeenUsed(sql);
    }

    /**
     * @since 5.3.7
     */
    @Test
    public void Inner4BushyJoinBroadcastTest() {
        String sql = "select * from " + groupList[3][0] + " t1, "
            + groupList[2][1] + " t2, "
            + groupList[4][1] + " t3, "
            + groupList[4][2] + " t4 " +
            " where t1.integer_test != t3.integer_test and t2.integer_test != t4.integer_test and  t1.pk = 1 and t2.pk = 1 and t3.pk = 2 and t4.pk = 2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        testBushyJoinRuleHasBeenUsed(sql);
    }

    /**
     * @since 5.3.7
     */
    @Test
    public void Inner20BushyJoinDifferentGroup3() {
        String sql = "select * from " + groupList[0][0] + " t00, "
            + groupList[1][0] + " t10, "
            + groupList[2][0] + " t20, "
            + groupList[3][0] + " t30, "
            + groupList[4][0] + " t40, "
            + groupList[0][1] + " t01, "
            + groupList[1][1] + " t11, "
            + groupList[2][1] + " t21, "
            + groupList[3][1] + " t31, "
            + groupList[4][1] + " t41, "
            + groupList[0][2] + " t02, "
            + groupList[1][2] + " t12, "
            + groupList[2][2] + " t22, "
            + groupList[3][2] + " t32, "
            + groupList[4][2] + " t42, "
            + groupList[0][3] + " t03, "
            + groupList[1][3] + " t13, "
            + groupList[2][3] + " t23, "
            + groupList[3][3] + " t33, "
            + groupList[4][3] + " t43 "

            + "WHERE t00.pk = t01.pk "
            + "  AND t00.pk = t02.pk "
            + "  AND t00.pk = t03.pk "
            + "  AND t00.pk = t40.pk "
            + "  AND t00.pk = 1 "

            + "  AND t10.pk = t11.pk "
            + "  AND t10.pk = t12.pk "
            + "  AND t10.pk = t13.pk "
            + "  AND t10.pk = t41.pk "
            + "  AND t10.pk = 2 "

            + "  AND t20.pk = t21.pk "
            + "  AND t20.pk = t22.pk "
            + "  AND t20.pk = t23.pk "
            + "  AND t20.pk = t42.pk "
            + "  AND t20.pk = 3 "

            + "  AND t30.pk = t31.pk "
            + "  AND t30.pk = t32.pk "
            + "  AND t30.pk = t33.pk "
            + "  AND t30.pk = t43.pk "
            + "  AND t30.pk = 4 ";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        testBushyJoinRuleHasBeenUsed(sql);
    }
}



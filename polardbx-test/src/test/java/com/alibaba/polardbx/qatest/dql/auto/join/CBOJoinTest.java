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
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * CBO Join 测试
 *
 * @author zilin.zl
 * @since 5.3.12
 */

@RunWith(CommonCaseRunner.class)
public class CBOJoinTest extends AutoReadBaseTestCase {

    private static final Log log = LogFactory.getLog(CBOJoinTest.class);

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

    public CBOJoinTest(String g1tb1, String g1tb2, String g1tb3, String g1tb4,
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
     * @since 5.3.12
     */
    @Test
    @FileStoreIgnore
    public void CBOPushSemiJoinTest() {
        for (int i = 1; i < 3; i++) {
            String sql = "select * from " + groupList[i][0] + " t1 , " + groupList[0][0] + " t2  " +
                "where t1.integer_test = t2.integer_test and t1.pk in (select pk from " + groupList[i][1]
                + " where pk < 10)";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            testCBOPushJoinWork(sql);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    @FileStoreIgnore
    public void CBOPushInnerJoinTest() {
        if (usingNewPartDb()) {
            return;
        }
        for (int i = 1; i < 3; i++) {
            String sql = "select * from " + groupList[i][0] + " t1 " +
                "left join " + groupList[0][0] + " t2 on t1.pk = t2.pk " +
                "inner join " + groupList[i][1] + " t3 on t1.pk = t3.pk and t3.pk < 10";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
            testCBOPushJoinWork(sql);
        }
    }

    /**
     * @since 5.3.12
     */
    @Test
    public void CBOJoinExechangeRuleTest() {
        for (int i = 0; i < 4; i++) {
            String v1 = groupList[i][0];
            String v2 = groupList[i][1];
            String v3 = groupList[i][2];
            String v4 = groupList[i][3];
            String sql =
                "/*+TDDL: HASH_JOIN(" + v1 + "," + v3 + ") HASH_JOIN(" + v2 + "," + v4 + ") HASH_JOIN((" + v2 + "," + v4
                    + "),(" + v1 + "," + v3 + ")) */\n"
                    + "SELECT avg(v1.pk + v2.pk + v3.pk + v4.pk), avg(v1.integer_test + v2.integer_test + v3.integer_test + v4.integer_test)\n"
                    + "FROM " + v1 + " v1\n"
                    + "INNER JOIN " + v2 + " v2 ON v1.pk = v2.pk\n"
                    + "INNER JOIN (" + v3 + " v3\n"
                    + "            INNER JOIN " + v4
                    + " v4 ON v3.integer_test = v4.integer_test and v3.integer_test in (1,2,3,4)) ON v1.varchar_test = v3.varchar_test\n"
                    + "AND v2.varchar_test = v4.varchar_test";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    public void testCBOPushJoinWork(String sql) {
        String explainDisableSql = "explain /*+TDDL:cmd_extra(enable_cbo=true, enable_cbo_push_join=false)*/" + sql;
        String explainEnableSql = "explain /*+TDDL:cmd_extra(enable_cbo=true, enable_cbo_push_join=true)*/" + sql;
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
            Assert.assertTrue(StringUtils.countMatches(explainEnableString, "LogicalView")
                < StringUtils.countMatches(explainDisableString, "LogicalView"));
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}

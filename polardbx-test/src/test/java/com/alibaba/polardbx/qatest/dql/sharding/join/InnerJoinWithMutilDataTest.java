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

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * 内连接结果为多条数据测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */


public class InnerJoinWithMutilDataTest extends ReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},hint={5}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwoBaseThreeBaseFourWithHint());
    }

    public InnerJoinWithMutilDataTest(String baseOneTableName, String baseTwoTableName, String baseThreeTableName,
                                      String baseFourTableName, String hint) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.baseThreeTableName = baseThreeTableName;
        this.baseFourTableName = baseFourTableName;
        this.hint = hint;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void innerJoinTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + " from " + baseOneTableName + " inner join "
                + baseTwoTableName + "  " + " on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithSomeValueTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + " from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAndTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void joinCacheTest() {
        for (int i = 50; i < 60; i++) {
            String sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereNumFiledTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".pk=52";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
            + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " inner join "
            + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk where "
            + baseOneTableName + ".pk>80";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereStringFieldTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk" + " where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue
                + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereAndTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "' and "
                + baseTwoTableName
                + ".varchar_test='" + columnDataGenerator.varchar_testValueTwo + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereAndSameFiledTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseTwoTableName + ".pk>20 and " + baseTwoTableName
                + ".pk<80";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereAndNoDataTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseTwoTableName + ".pk<20 and " + baseTwoTableName
                + ".pk>150";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereOrTest() {
        String sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
            + baseOneTableName
            + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
            + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
            + baseTwoTableName
            + ".pk where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "' or "
            + baseTwoTableName
            + ".varchar_test='" + columnDataGenerator.varchar_testValueTwo + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereBetweenTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".integer_test between 40 and 70";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereLimitTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".pk between 40 and 70 limit 10000";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereOrderByLimitTest() {
        // join时不开启调整顺序，如果右表存在orderby，则会需要临时表
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseOneTableName + " inner join "
                + baseTwoTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
                + ".pk where " + baseOneTableName + ".pk between 40 and 70 order by " + baseOneTableName
                + ".pk limit 10";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableTest() {
        String sql =
            hint + "SELECT * FROM " + baseOneTableName + " inner JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".integer_test=" + baseTwoTableName + ".pk " + "inner JOIN " + baseThreeTableName + " ON "
                + baseTwoTableName + ".pk=" + baseThreeTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void InnerJoinThreeTableTransfferTest() {
        String sql =
            hint + "SELECT * FROM " + baseOneTableName + " inner JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".pk" + " inner JOIN " + baseThreeTableName + " ON "
                + baseTwoTableName + ".pk=" + baseThreeTableName + ".pk " + " AND " + baseOneTableName
                + ".pk=1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Ignore
    @Test
    public void InnerJoinThreeTableTransfferTestWithNull() {
        String sql =
            hint + "SELECT * FROM " + baseOneTableName + " inner JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".pk" + " inner JOIN " + baseThreeTableName + " ON "
                + baseTwoTableName + ".pk=" + baseThreeTableName + ".pk " + " AND " + baseOneTableName
                + ".pk is null";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void InnerJoinThreeTableTransfferTestWithSubQuery() {
        String sql =
            hint + "SELECT * FROM " + baseOneTableName + " a1 inner JOIN (select * from " + baseTwoTableName + ") a2"
                + " ON a1.pk= a2.pk" + " inner JOIN " + baseThreeTableName + " ON a2.pk="
                + baseThreeTableName + ".pk " + " where " + "a2.varchar_test is null";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhere() {
        String sql =
            hint + "SELECT * from " + baseOneTableName + " INNER JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".pk INNER JOIN " + baseThreeTableName + "  ON " + baseThreeTableName
                + ".pk=" + baseTwoTableName + ".integer_test where " + baseThreeTableName + ".varchar_test='"
                + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhereWithOr() {
        String sql =
            hint + "SELECT * from " + baseOneTableName + " INNER JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".integer_test INNER JOIN " + baseThreeTableName + "  ON "
                + baseThreeTableName
                + ".pk=" + baseTwoTableName + ".integer_test where " + baseThreeTableName + ".varchar_test='"
                + columnDataGenerator.varchar_testValue + "' or "
                + baseThreeTableName + ".varchar_test='" + columnDataGenerator.varchar_testValueTwo + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhereWithAnd() {
        String sql =
            hint + "SELECT * from " + baseOneTableName + " INNER JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".integer_test INNER JOIN " + baseThreeTableName + "  ON "
                + baseThreeTableName
                + ".pk=" + baseTwoTableName + ".integer_test where " + baseThreeTableName + ".varchar_test='"
                + columnDataGenerator.varchar_testValue + "' and "
                + baseOneTableName + ".varchar_test like'" + columnDataGenerator.varchar_tesLikeValueOne + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAliasTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " a inner join " + baseTwoTableName + " b " + "on a.integer_test=b.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAliasAsTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a inner join " + baseTwoTableName + " as b " + "on a.integer_test=b.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderByTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a inner join " + baseTwoTableName + " as b "
            + "on a.integer_test=b.pk order by a.pk asc limit 100";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderByascTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a inner join " + baseTwoTableName + " as b "
            + "on a.integer_test=b.pk order by a.pk asc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderBydescTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a inner join " + baseTwoTableName + " as b "
            + "on a.integer_test=b.pk order by a.pk desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test()
    public void InnerJoinWithSubQueryTest() {
        String sql =
            hint + "select t1.sum1,t2.count2 from (select sum(pk) as sum1,integer_test from " + baseOneTableName
                + " group by integer_test) t1 " + "join (select count(pk) as count2,integer_test from "
                + baseTwoTableName + " group by integer_test) t2 on t1.integer_test=t2.integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}

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
import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 三种join类型测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */


public class JoinAndTest extends ReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    String[] joinType = {"inner", "left", "right"};
    String[] innerJoin = {"inner"};
    String[] leftJoin = {"left"};
    String sql = null;

    @Parameters(name = "{index}:table0={0},table1={1},hint={2}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwoWithHint());
    }

    public JoinAndTest(String baseOneTableName, String baseTwoTableName, String hint) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.hint = hint;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void AndTest() {
        for (int i = 0; i < joinType.length; i++) {
            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".integer_test and " + baseOneTableName + ".pk<52 where " + baseTwoTableName + ".pk<52";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void AndNumFiledTest() {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".pk=52";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".pk>80";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void whereNumNotEqualTest() {
        if (StringUtils.containsIgnoreCase(hint, "SORT_MERGE_JOIN")) {
            // 不支持sort merge join hint
            return;
        }

        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + ","
                + baseOneTableName
                + "  " + "where " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk - 2   and   "
                + baseOneTableName + ".pk< 100";

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk - 2   and   " + baseOneTableName + ".pk< 100";

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk - 2  where   " + baseOneTableName + ".pk< 100";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void StringFieldTest() {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk" + " where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue
                + "'";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore("todo 新增case 需要调试")
    public void straightJoinTest() throws Exception {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + " STRAIGHT_JOIN" + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk" + " where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue
                + "'";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void ThreeTableWithAndWithOr() {
        for (int i = 0; i < joinType.length; i++) {
            sql = hint + "SELECT * from " + baseOneTableName + " a " + joinType[i] + " JOIN " + baseOneTableName
                + " b ON a.pk=b.pk " + joinType[i] + " JOIN " + baseOneTableName
                + " c ON b.pk=c.pk and b.pk=1 and c.pk=1 where a.pk=1";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void AndAndTest() {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "' and "
                + baseTwoTableName
                + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        }
    }

    /**
     * @since 5.1.19
     */
    @Test
    @Ignore("JOIN ON 条件中有 like")
    public void AndWithNotEqualConditionTest() {
        if (StringUtils.containsIgnoreCase(hint, "SORT_MERGE_JOIN")) {
            // 不支持sort merge join hint
            return;
        }

        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test > "
                + baseTwoTableName
                + ".pk and " + baseOneTableName + ".varchar_test = " + baseTwoTableName + ".varchar_test   where  "
                + baseOneTableName + ".pk<100 ";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test > "
                + baseTwoTableName
                + ".pk and " + baseOneTableName + ".varchar_test like " + "concat(" + baseTwoTableName + ".varchar_test"
                + ", '%') where   " + baseOneTableName + ".pk<100 ";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void AndAndSameFiledTest() {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
                + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " "
                + joinType[i] + " join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test="
                + baseTwoTableName
                + ".pk where " + baseTwoTableName + ".pk>20 and " + baseTwoTableName
                + ".pk<80";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void joinWithDifferentTypeTest() {
        String sql1 = "select " + baseOneTableName + ".tinyint_test," + baseTwoTableName + ".decimal_test" + " from "
            + baseOneTableName + " left join " + baseTwoTableName
            + "  " + "on " + baseOneTableName + ".tinyint_test=" + baseTwoTableName + ".decimal_test where "
            + baseTwoTableName + ".pk<1000;";
        String sql2 = "select " + baseOneTableName + ".tinyint_test," + baseTwoTableName + ".decimal_test" + " from "
            + baseOneTableName + " left join " + baseTwoTableName
            + "  " + "on " + baseOneTableName + ".decimal_test=" + baseTwoTableName + ".tinyint_test where "
            + baseTwoTableName + ".pk<1000;";

        String sql3 = "select " + baseOneTableName + ".tinyint_test," + baseTwoTableName + ".decimal_test" + " from "
            + baseOneTableName + " right join " + baseTwoTableName
            + "  " + "on " + baseOneTableName + ".tinyint_test=" + baseTwoTableName + ".decimal_test where "
            + baseTwoTableName + ".pk<1000;";
        String sql4 = "select " + baseOneTableName + ".tinyint_test," + baseTwoTableName + ".decimal_test" + " from "
            + baseOneTableName + " right join " + baseTwoTableName
            + "  " + "on " + baseOneTableName + ".decimal_test=" + baseTwoTableName + ".tinyint_test where "
            + baseTwoTableName + ".pk<1000;";

        String sql5 = "select " + baseOneTableName + ".tinyint_test," + baseTwoTableName + ".decimal_test" + " from "
            + baseOneTableName + " join " + baseTwoTableName
            + "  " + "on " + baseOneTableName + ".tinyint_test=" + baseTwoTableName + ".decimal_test where "
            + baseTwoTableName + ".pk<1000;";
        String sql6 = "select " + baseOneTableName + ".tinyint_test," + baseTwoTableName + ".decimal_test" + " from "
            + baseOneTableName + " join " + baseTwoTableName
            + "  " + "on " + baseOneTableName + ".decimal_test=" + baseTwoTableName + ".tinyint_test where "
            + baseTwoTableName + ".pk<1000;";

        String sql7 = "select " + baseOneTableName + ".tinyint_test," + baseTwoTableName + ".decimal_test" + " from "
            + baseOneTableName + " inner join " + baseTwoTableName
            + "  " + "on " + baseOneTableName + ".tinyint_test=" + baseTwoTableName + ".decimal_test where "
            + baseTwoTableName + ".pk<1000;";
        String sql8 = "select " + baseOneTableName + ".tinyint_test," + baseTwoTableName + ".decimal_test" + " from "
            + baseOneTableName + " inner join " + baseTwoTableName
            + "  " + "on " + baseOneTableName + ".decimal_test=" + baseTwoTableName + ".tinyint_test where "
            + baseTwoTableName + ".pk<1000;";

        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql4, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql5, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql6, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql7, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(sql8, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void joinWithNoCondition() {
        sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
            + baseOneTableName
            + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
            + " join " + baseOneTableName + " where " + baseTwoTableName + ".pk<5 and " + baseTwoTableName
            + ".pk<5";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}

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


public class JoinAndAliasTest extends ReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    String[] joinType = {"inner", "left", "right"};
    String[] innerJoin = {"inner"};
    String[] leftJoin = {"left"};
    String sql = null;

    @Parameters(name = "{index}:table0={0},table1={1},hint={2}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseOneWithHint());
    }

    public JoinAndAliasTest(String baseOneTableName, String baseTwoTableName, String hint) {
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
            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " + baseTwoTableName + " a "
                + joinType[i] + " join " + baseOneTableName + " b "
                + "on a.integer_test=b.integer_test and a.pk<52 where b.pk<52";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void AndNumFiledTest() {
        if (baseOneTableName.equals(baseTwoTableName)) {
            return;
        }
        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " + baseTwoTableName + " a "
                + joinType[i] + " join " + baseOneTableName + " b " + "on a.integer_test=b.pk where b.pk=52";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " + baseTwoTableName + " a "
                + joinType[i] + " join " + baseOneTableName + " b " + "on a.integer_test=b.pk where b.pk > 80";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
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
            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " +
                baseTwoTableName + " a " + joinType[i] + " join " + baseOneTableName + " b "
                + "on a.integer_test=b.pk -2 and b.pk < 100";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " +
                baseTwoTableName + " a , " + baseOneTableName + " b " + "where a.integer_test=b.pk -2 and b.pk < 100";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " +
                baseTwoTableName + " a " + joinType[i] + " join " + baseOneTableName + " b "
                + "on a.integer_test=b.pk -2 where b.pk < 100";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void StringFieldTest() {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " +
                baseTwoTableName + " a " + joinType[i] + " join " + baseOneTableName + " b "
                + "on a.integer_test=b.pk  where a. varchar_test='" + columnDataGenerator.varchar_testValue + "'";
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
            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " +
                baseTwoTableName + " a " + joinType[i] + " join " + baseOneTableName + " b "
                + "on a.integer_test=b.pk  where a. varchar_test='" + columnDataGenerator.varchar_testValue
                + "' and b.varchar_test='" + columnDataGenerator.varchar_testValue + "'";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        }
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void AndWithNotEqualConditionTest() {
        if (StringUtils.containsIgnoreCase(hint, "SORT_MERGE_JOIN")) {
            // 不支持sort merge join hint
            return;
        }

        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " +
                baseTwoTableName + " a " + joinType[i] + " join " + baseOneTableName + " b "
                + "on a.integer_test > b.pk  and a. varchar_test= b.varchar_test where a.pk < 100";

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " +
                baseTwoTableName + " a " + joinType[i] + " join " + baseOneTableName + " b "
                + "on a.integer_test > b.pk  and a. varchar_test like concat(b.varchar_test, '%') where a.pk < 100";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void AndAndSameFiledTest() {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " +
                baseTwoTableName + " a " + joinType[i] + " join " + baseOneTableName + " b "
                + "on a.integer_test = b.pk  where b.pk > 20 and b.pk < 80";

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }
}

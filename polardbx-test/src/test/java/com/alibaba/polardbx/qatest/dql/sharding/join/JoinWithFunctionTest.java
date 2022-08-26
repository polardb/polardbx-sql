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
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * 带function的join测试
 */

public class JoinWithFunctionTest extends ReadBaseTestCase {
    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},hint={5}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwoBaseThreeBaseFourWithHint());
    }

    public JoinWithFunctionTest(String baseOneTableName, String baseTwoTableName, String baseThreeTableName,
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
    public void joinMaxMinTest() {
        String sql = hint + "select max(" + baseOneTableName + ".`pk`)  from " + baseOneTableName + " inner join "
            + baseTwoTableName + "  " + " on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = hint + "select min(" + baseOneTableName + ".`pk`)  from " + baseOneTableName + " inner join "
            + baseTwoTableName + "  " + " on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = hint + "select min(" + baseOneTableName + ".`pk`) min from " + baseOneTableName + " inner join "
            + baseTwoTableName + "  " + " on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void joinSumTest() {
        String sql = hint + "select sum(" + baseOneTableName + ".`pk`)  from " + baseOneTableName + " inner join "
            + baseTwoTableName + "  " + " on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void joinAvgTest() {
        String sql = hint + "select avg(" + baseOneTableName + ".`pk`) from " + baseOneTableName + " inner join "
            + baseTwoTableName + "  " + " on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void joinCountTest() {
        String sql = hint + "select count(" + baseOneTableName + ".`pk`)  from " + baseOneTableName + " inner join "
            + baseTwoTableName + "  " + " on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = hint + "select count(*)  from " + baseOneTableName + " inner join " + baseTwoTableName + "  " + " on "
            + baseOneTableName
            + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void JoinWithAndMaxTest() {
        String sql = hint + "select max(" + baseOneTableName + ".`pk`) from " + baseTwoTableName
            + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
            + ".pk where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void JoinWithWhereAndSumTest() {
        String sql = hint + "select sum(" + baseOneTableName + ".`pk`)  from " + baseTwoTableName
            + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
            + ".pk where " + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "' and "
            + baseTwoTableName
            + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void JoinWithWhereBetweenSumTest() {
        String sql = hint + "select sum(" + baseOneTableName + ".`pk`) from " + baseTwoTableName
            + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
            + ".pk where " + baseOneTableName + ".integer_test between 40 and 70";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void JoinWithWhereLimitCountTest() {
        String sql = hint + "select count(" + baseOneTableName + ".`pk`) from " + baseTwoTableName
            + " inner join " + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
            + ".pk where " + baseOneTableName + ".integer_test between 40 and 70 limit 1000";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void JoinWithWhereOrderByLimitMinTest() {
        String sql = hint + "select min(" + baseOneTableName + ".`integer_test`)," + baseOneTableName + ".pk from "
            + baseOneTableName + " inner join "
            + baseTwoTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
            + ".pk where " + baseOneTableName + ".integer_test >10 group by " + baseOneTableName + ".pk order by "
            + baseOneTableName
            + ".pk limit 1000";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithGroupbyTest() {
        String sql =
            hint + "select min(" + baseOneTableName + ".`pk`)," + "" + baseOneTableName + ".varchar_test name from "
                + baseOneTableName + " inner join " + baseTwoTableName + " on " + baseOneTableName + ".integer_test="
                + baseTwoTableName + ".pk group by " + baseOneTableName + ".varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderGroupbyTest() {
        String sql = hint + "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select min(" + baseOneTableName + ".`pk`)," + ""
            + baseOneTableName + ".varchar_test name from " + baseTwoTableName + " inner join " + baseOneTableName
            + " on "
            + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk group by " + baseOneTableName
            + ".varchar_test order by "
            + baseOneTableName + ".varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderGroupbyNumberTest() {
        String sql = hint + "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select min(" + baseOneTableName + ".`pk`)," + ""
            + baseOneTableName + ".varchar_test name from " + baseTwoTableName + " inner join " + baseOneTableName
            + " on "
            + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk group by " + baseOneTableName
            + ".varchar_test order by " + baseOneTableName
            + ".varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void JoinThreeTableWithWhereCount() {
        String sql = hint + "SELECT count(*) from " + baseOneTableName + " INNER JOIN " + baseTwoTableName + " ON "
            + baseOneTableName
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
    public void JoinWithAliasSumTest() {
        String sql = hint + "select sum(a.`pk`)  from " + baseOneTableName
            + " a inner join " + baseTwoTableName + " b " + "on a.integer_test=b.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}

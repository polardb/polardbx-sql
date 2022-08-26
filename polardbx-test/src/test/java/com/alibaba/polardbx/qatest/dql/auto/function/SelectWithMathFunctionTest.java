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

package com.alibaba.polardbx.qatest.dql.auto.function;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * 数学函数
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectWithMathFunctionTest extends AutoReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithMathFunctionTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void minTest() throws Exception {
        String sql = "SELECT MIN(pk) as m FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT MIN(pk) as m FROM " + baseOneTableName + " where integer_test>400";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void minWithAliasTest() throws Exception {
        String sql = "SELECT MIN(pk) AS min FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void maxTest() throws Exception {
        String sql = "SELECT MAX(pk) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT MAX(pk) FROM " + baseOneTableName + " where integer_test>400";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void maxMinTest() throws Exception {
        String sql = "SELECT MAX(pk),MIN(pk) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void sumTest() throws Exception {
        String sql = "SELECT SUM(pk) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT SUM(pk) FROM " + baseOneTableName + " where integer_test>400";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void sumFloatTest() throws Exception {
        String sql = "SELECT SUM(float_test) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void sumIntTest() throws Exception {
        String sql = "SELECT SUM(integer_test) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void sumMutilTest() throws Exception {
        String sql = "SELECT SUM(integer_test),sum(pk),sum(float_test) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void avgLongTest() throws Exception {
        String sql = "SELECT AVG(pk) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT AVG(pk) FROM " + baseOneTableName + " where integer_test >400";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void avgIntTest() throws Exception {
        String sql = "SELECT AVG(integer_test) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void avgFloatTest() throws Exception {
        String sql = "SELECT AVG(float_test) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countTest() throws Exception {
        String sql = "SELECT COUNT(pk) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countNonPKTest() throws Exception {
        String sql = "SELECT COUNT(float_test) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countAllTest() throws Exception {
        String sql = "SELECT COUNT(*) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void count1Test() throws Exception {
        String sql = "SELECT COUNT(1) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countWithWhereTest() throws Exception {
        String sql = "SELECT COUNT(pk) FROM " + baseOneTableName + " where integer_test>150";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countWithDistinctTest() throws Exception {
        String sql = "SELECT count(distinct varchar_test) FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countWithDistinctAndGroupByTest() throws Exception {
        String sql =
            "/* TDDL ALLOW_TEMPORARY_TABLE=True */ SELECT COUNT(DISTINCT integer_test) c FROM " + baseOneTableName
                + " group by varchar_test";
//        selectOrderAssert(sql, null ,mysqlConnection, tddlConnection);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void countWithDistinctMutilCloumnTest() throws Exception {
        String sql = "SELECT COUNT(distinct varchar_test,date_test) as d FROM " + baseOneTableName;
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void roundTest() throws Exception {
        String sql = "select round(float_test,2) as a from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select round(integer_test/pk,2) as a from " + baseOneTableName + " where varchar_test=? order by pk";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        selectOrderAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void intervalTest() throws Exception {
        String sql = "select interval(pk,integer_test) as d FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void divTest() throws Exception {
        String sql = "select SUM(integer_test) div sum(pk) as d FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void divisionTest() throws Exception {
        String sql = "select SUM(integer_test) / sum(pk) as d FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void bitAndTest() throws Exception {
        String sql = "select SUM(integer_test) & sum(pk) as d FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void bitOrTest() throws Exception {
        String sql = "select SUM(integer_test) | sum(pk) as d FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void bitXorTest() throws Exception {
        String sql = "select SUM(integer_test) ^ sum(pk) as d FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void bitLShiftTest() throws Exception {
        String sql = "select SUM(integer_test) << 2 as d FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void bitRShiftTest() throws Exception {
        String sql = "select SUM(integer_test) >> 2 as d FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}

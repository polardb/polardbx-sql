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

package com.alibaba.polardbx.qatest.dml.sharding.gsi;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Insert select into gsi table.
 *
 * @author minggong
 */

public class InsertSelectGsiTest extends GsiDMLTest {

    private static Map<String, String> tddlTables = new HashMap<>();
    private static Map<String, String> shadowTables = new HashMap<>();
    private static Map<String, String> mysqlTables = new HashMap<>();

    @BeforeClass
    public static void beforeCreateTables() {
        try {
            concurrentCreateNewTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void afterDropTables() {

        try {
            concurrentDropTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Parameterized.Parameters(name = "{index}:hint={0} table1={1} table2={2}")
    public static List<String[]> prepareData() {
        List<String[]> rets = doPrepareData();
        return prepareNewTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    public InsertSelectGsiTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    @Before
    public void initData() throws Exception {
        super.initData();
        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + "into "
            + baseTwoTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test)"
            + " values (?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 20; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
    }

    /**
     * all rows, all fields
     */
    @Test
    public void insertSelectAllTest() throws Exception {
        String sql = String.format(hint + "insert into %s select * from %s", baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertTablesSame(Lists.newArrayList(baseOneTableName, baseTwoTableName));

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * some rows, some fields
     */
    @Test
    public void insertSelectSomeTest() throws Exception {
        String sql = String.format(hint + "insert into %s"
                + "(pk,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test)"
                + " select pk+1000,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test"
                + " from %s where pk>10",
            baseOneTableName,
            baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * no data
     */
    @Test
    public void insertSelectNoDataTest() throws Exception {
        String sql =
            String.format(hint + "insert into %s select * from %s where pk>1000", baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Assert.assertTrue(!rs.next());
    }

    /**
     * insert ignore
     */
    @Test
    public void insertSelectIgnoreTest() throws Exception {
        String sql = String.format(hint + "insert ignore into %s select * from %s", baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = String
            .format(hint + "insert ignore into %s select * from %s where pk>10", baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert on duplicate key update
     */
    @Test
    public void upsertSelectTest() throws Exception {
        String sql = String.format(hint + "insert into %s select * from %s on duplicate key update float_test=1",
            baseOneTableName,
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = String.format(hint + "insert into %s select * from %s on duplicate key update float_test=1",
            baseOneTableName,
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert on duplicate key update xx=values(xx)
     */
    @Test
    public void upsertSelectValuesTest() throws Exception {
        String sql = String.format(hint + "insert into %s"
                + "(pk,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test)"
                + " select pk+1000,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test"
                + " from %s",
            baseOneTableName,
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = String.format(hint + "insert into %s"
                + "(pk,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test,float_test)"
                + " select pk+1000,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test,1"
                + " from %s on duplicate key update float_test=values(float_test)",
            baseOneTableName,
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * replace select
     */
    @Test
    public void replaceSelectTest() throws Exception {
        String sql = String.format(hint + "replace into %s select * from %s", baseOneTableName, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert select union
     */
    @Test
    public void insertSelectUnionTest() throws Exception {
        String sql = String
            .format(hint + "insert into %s(pk,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test) "
                    + "select pk+1000, integer_test+1000, concat(varchar_test,'99'), bigint_test+10000 as bi, concat(char_test,'99'), year_test, datetime_test from %s union all "
                    + "select pk+2000, integer_test+2000, concat(varchar_test,'xx'), bigint_test+20000 as bi, concat(char_test,'xx'), year_test, datetime_test from %s",
                baseOneTableName,
                baseTwoTableName,
                baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }

    /**
     * insert select complex
     */
    @Test
    public void unionDistinctLimitInsertSelectLimitTest() throws Exception {
        String sql = String
            .format(hint + "insert into %s(pk,integer_test,varchar_test,bigint_test,char_test,year_test,datetime_test)"
                    + " select distinct(pk) + 1000 as idd,integer_test+100,concat(varchar_test,'99') as va,bigint_test+2000,concat(char_test,'99'),year_test,datetime_test"
                    + " from %s where integer_test>15 or integer_test<5 union distinct "
                    + "(select pk + 300,integer_test,concat(varchar_test,'xx'),bigint_test+5000,concat(char_test,'xx'),year_test,datetime_test"
                    + " from %s where 8<pk  order by pk limit 10) order by va, idd limit 100",
                baseOneTableName,
                baseTwoTableName,
                baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        sql = hint + "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        assertIndexSame(baseOneTableName);

        assertRouteCorrectness(baseOneTableName);
    }
}

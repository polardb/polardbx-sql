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

package com.alibaba.polardbx.qatest.dml.sharding.delete;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.BROADCAST_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.TWO;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;
/**
 * 迁移至DeleteTest中
 */

/**
 * 复杂Delete测试
 */

public class ComplexDeleteTest extends CrudBasedLockTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareData() {
        final List<String[]> tableNames = new LinkedList<>(Arrays.asList(ExecuteTableName.allBaseTypeTwoStrictSameTable(
            ExecuteTableName.UPDATE_DELETE_BASE)));

        String[][] broadcastMulti = {
            {
                ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {
                ExecuteTableName.UPDATE_DELETE_BASE + TWO + MULTI_DB_ONE_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX},
            {
                ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + TWO + ONE_DB_ONE_TB_SUFFIX},
            {
                ExecuteTableName.UPDATE_DELETE_BASE + TWO + ONE_DB_ONE_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX},
            {
                ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {
                ExecuteTableName.UPDATE_DELETE_BASE + TWO + MUlTI_DB_MUTIL_TB_SUFFIX,
                ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX}};
        tableNames.addAll(ImmutableList.copyOf(broadcastMulti));
        return tableNames;
    }

    public ComplexDeleteTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Before
    public void prepare() throws Exception {
        //initCrossSchemaTableName();
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }
    /**
     * 从ComplexDelete类迁移过来
     */
    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteAll() throws Exception {
        String sql = "delete  b.* from " + baseOneTableName + " a, " + baseTwoTableName + " b where a.pk = b.pk";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteSomeTable1() throws Exception {
        String sql = "delete b.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and a. pk > 5";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteOne() throws Exception {
        String sql = "delete b.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and a.pk =?";
        List<Object> param = new ArrayList<Object>();
        param.add(5);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteWithBetweenAnd() throws Exception {
        String sql = "delete  b.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and a.pk BETWEEN 2 AND 7";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteWithOrTest() throws Exception {
        String sql = "delete  b.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and (a.pk =2 or a.pk=7)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteWithInTest() throws Exception {
        String sql = "delete b.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and a.pk in (2,7,10)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteNameWithPkTest() throws Exception {
        String sql = "delete b.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and a.varchar_test = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.varchar_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteWithNowTest() throws Exception {
        String sql = "delete b.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and a.date_test < now()";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteWithBroadcastTest_exception() throws Exception {
        String sql = "delete a.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and a.date_test < now()";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexDeleteWithBroadcastSingleTableTest_exception() throws Exception {
        String sql = "delete a.* from " + baseOneTableName + " a, " + baseTwoTableName
            + " b where a.pk = b.pk and a.pk = 2";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseOneTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithSubQuery() throws Exception {
        String sql = String.format(
            "delete a.* from %s a where a.integer_test not in (select c.integer_test from %s c where a.pk = 2 * c.pk)",
            baseOneTableName, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithSubQuery2() throws Exception {
        String sql = String.format(
            "delete a from %s a where a.integer_test in (select c.integer_test from %s c where c.pk > 15)",
            baseOneTableName, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        //用于判断没有删除任何数据
        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteIgnoreWithSubQuery() throws Exception {
        String sql = String.format(
            "delete ignore a.* from %s a where a.integer_test not in (select c.integer_test from %s c where a.pk = c.pk * 2)",
            baseOneTableName, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Ignore("Supported")
    @Test
    public void deleteParserError1() {
        String sql = String
            .format("delete from %s a using %s, %s a where %s.pk = a.pk", baseOneTableName, baseOneTableName,
                baseTwoTableName, baseOneTableName);
        executeErrorAssert(tddlConnection, sql, null, "an error in your SQL syntax");
        //用于判断是否删除了数据
        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithAliasAndUsing() {
        String sql = String
            .format("delete from a using %s, %s a where %s.pk = a.pk", baseOneTableName, baseTwoTableName,
                baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        //用于判断是否删除了数据
        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithAliasAndUsing2() {
        String sql = String
            .format("delete from %s, a using %s, %s a where %s.pk = a.pk and a.pk in (3,5,13)", baseOneTableName,
                baseOneTableName, baseTwoTableName, baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        //用于判断是否删除了数据
        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithAliasAndUsingError() {
        String sql = String
            .format("delete from %s, %s using %s, %s a where %s.pk = a.pk", baseOneTableName, baseTwoTableName,
                baseOneTableName, baseTwoTableName, baseOneTableName);

        executeErrorAssert(tddlConnection, sql, null, "Unknown table");

        //用于判断是否删除了数据
        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void deleteWithAliasAndUsingParseError() {
        String sql = String
            .format("DELETE FROM %s alias USING %s, %s alias WHERE %s.a = alias.a;", baseOneTableName, baseTwoTableName,
                baseOneTableName, baseTwoTableName, baseOneTableName);

        executeErrorAssert(mysqlConnection, sql, null, "You have an error in your SQL syntax");
        executeErrorAssert(tddlConnection, sql, null, "You have an error in your SQL syntax");

        //用于判断是否删除了数据
        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void complexdeleteSome1() throws Exception {

        String sql = "DELETE " + " a.* , b.* FROM " + baseOneTableName + " a , " + baseTwoTableName + " b "
            + " WHERE a.pk in (?,?) and a.pk = b.pk";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(9);
            param.add(19);
            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params, true);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        sql = "select * from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void deleteWhereTest() throws Exception {
        String sql = String.format("delete from %s,%s using %s,%s where %s.pk=%s.pk",
            baseOneTableName, baseTwoTableName, baseOneTableName,
            baseTwoTableName, baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void deleteWhereAliasTest() throws Exception {
        String sql = String.format("delete from a,b using %s a,%s b where a.pk=b.pk",
            baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void deleteLeftJoinTest() throws Exception {
        String sql = String.format("delete from %s,%s using %s left join %s on %s.pk=%s.pk",
            baseOneTableName, baseTwoTableName, baseOneTableName,
            baseTwoTableName, baseOneTableName, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void deleteLeftJoinAliasTest() throws Exception {
        String sql = String.format("delete from a,b using %s a left join %s b on a.pk=b.pk",
            baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);
        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void deleteJoinCrossTest() throws Exception {
        String sql = String
            .format("delete from a,b using %s a left join %s b on a.integer_test=b.pk",
                baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection, true);

        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    public void assertBroadcastTableSame(String tableName) {

        if (usingNewPartDb()) {
            /**
             * Ignore assert for broadcast in qatest of new part db  
             */
            return;
        }

        ResultSet resultSet =
            JdbcUtil.executeQuerySuccess(tddlConnection, "show topology from " + tableName);
        String physicalTableName = tableName;
        try {
            resultSet.next();
            physicalTableName = (String) JdbcUtil.getObject(resultSet, 3);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        String mysqlSql = "select * from " + tableName;
        String tddlSql = "select * from " + physicalTableName;
        if (tableName.contains("broadcast")) {
            for (int i = 0; i < 4; i++) {
                String hint = String.format("/*TDDL:node=%s*/", i);
                selectContentSameAssertWithDiffSql(
                    hint + tddlSql,
                    hint + mysqlSql,
                    null,
                    mysqlConnection,
                    tddlConnection,
                    true,
                    false,
                    true
                );
            }
        }
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testDeleteWithIdenticalColumnName() throws Exception {
        //		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String
            .format("DELETE a FROM %s a , %s b WHERE a.pk=b.pk and b.pk > 20", baseOneTableName, baseTwoTableName);

        // Might cross db
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testDeleteWithDerivedSubquery() throws Exception {
        //		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "DELETE a FROM %s a left join (select max(integer_test) m, pk from %s b group by pk) b on a.pk=b.pk WHERE b.pk = 2",
            baseOneTableName, baseTwoTableName);

        // Might cross db
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testDeleteWithDerivedSubquery1() throws Exception {
        //		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "DELETE a FROM %s a inner join (select max(integer_test) m, pk from %s b group by pk having pk >= 13 and pk <= 16 ) b on a.pk=b.pk ",
            baseOneTableName, baseTwoTableName);

        // Might cross db
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    @Test
    public void deleteWithUnion() {
        // Execute update
        String sql =
            String.format("delete a from %s a, "
                    + "(select 9527 as integer_test, 'hello1234' as varchar_test "
                    + "union select 27149 , 'safdwe') v "
                    + "where a.varchar_test = v.varchar_test",
                baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Check update result
        sql = "SELECT bigint_test FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void deleteWithUnion1() {
        // Execute update
        String sql =
            String.format("delete a from %s a, "
                    + "(select 9527 as integer_test, 'hello1234' as varchar_test "
                    + "union select 27149 , varchar_test from %s b where b.bigint_test < 15) v "
                    + "where a.varchar_test = v.varchar_test",
                baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        // Check update result
        sql = "SELECT bigint_test FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }
}

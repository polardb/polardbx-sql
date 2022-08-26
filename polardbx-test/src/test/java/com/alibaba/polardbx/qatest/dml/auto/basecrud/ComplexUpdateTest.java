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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * 复杂Update测试
 */

public class ComplexUpdateTest extends AutoCrudBasedLockTestCase {
    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeTwoStrictSameTable(ExecuteTableName.UPDATE_DELETE_BASE));

    }

    public ComplexUpdateTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        tableDataPrepare(baseTwoTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void updateWhereTest() throws Exception {
        String sql = String.format("update %s,%s set %s.varchar_test='"
                + columnDataGenerator.varchar_testValue + "', %s.varchar_test='"
                + columnDataGenerator.varchar_testValue + "' where %s.pk=%s.pk",
            baseOneTableName, baseTwoTableName, baseOneTableName,
            baseTwoTableName, baseOneTableName, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void updateWhereAliasTest() throws Exception {
        String sql = String.format("update %s a,%s b set a.varchar_test='"
                + columnDataGenerator.varchar_testValue + "', b.varchar_test='"
                + columnDataGenerator.varchar_testValue + "' where a.pk=b.pk",
            baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void updateWhereAliasTest1() throws Exception {
        String sql = String.format("update %s `a`,%s b set a.varchar_test='"
                + columnDataGenerator.varchar_testValue
                + "', `b`.varchar_test='" + columnDataGenerator.varchar_testValue
                + "' where a.pk=b.pk",
            baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void updateLeftJoinTest() throws Exception {
        String sql = String.format(
            "update %s left join %s on %s.pk=%s.pk  set %s.varchar_test='"
                + columnDataGenerator.varchar_testValue
                + "', %s.varchar_test='"
                + columnDataGenerator.varchar_testValue + "'",
            baseOneTableName, baseTwoTableName, baseOneTableName,
            baseTwoTableName, baseOneTableName, baseTwoTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void updateLeftJoinAliasTest() throws Exception {
        String sql = String.format(
            "update %s a left join %s b on a.pk=b.pk  set a.varchar_test='"
                + columnDataGenerator.varchar_testValue
                + "', b.varchar_test='"
                + columnDataGenerator.varchar_testValue + "'",
            baseOneTableName, baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void complexUpdateBroadcastTest_exception() throws Exception {
        String sql = String.format(
            "update %s a left join %s b on a.pk=b.pk  set a.varchar_test='" + columnDataGenerator.varchar_testValue
                + "', b.varchar_test='" + columnDataGenerator.varchar_testValue + "'",
            baseOneTableName,
            baseTwoTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
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
     * @since 5.1.18
     */
    @Test
    //@Ignore("fix RelToSqlConverter for join with duplicate column")
    public void complexUpdateBroadcastSingleTableTest_exception() throws Exception {
        String sql = String.format(
            "update %s left join %s on %s.pk=%s.pk  set %s.varchar_test='" + columnDataGenerator.varchar_testValue
                + "', %s.varchar_test='" + columnDataGenerator.varchar_testValue + "' where %s.pk = 2",
            baseOneTableName,
            baseTwoTableName,
            baseOneTableName,
            baseTwoTableName,
            baseOneTableName,
            baseTwoTableName,
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);
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
     * @since 5.1.18
     */
    @Test
    public void complexUpdateDerivedSubqueryWithDuplicatedColumnName() throws Exception {
        String sql = String.format(
            "UPDATE %s a left join (select max(integer_test) integer_test, pk from %s b group by pk having pk >= 13 and pk <= 16 ) b on a.pk=b.pk SET a.integer_test=b.integer_test + 1",
            baseOneTableName,
            baseOneTableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
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
     * @since 5.1.24
     */
    @Test
    @Ignore("do not support subquery")
    public void testUpdateWithAlwaysFalse1() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE  %s a left join %s b on a.pk=b.pk  SET a.integer_test=? WHERE b.pk NOT IN (SELECT pk FROM %s) AND b.pk IN (SELECT pk from %s)",
            baseOneTableName, baseTwoTableName, baseTwoTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    @Ignore("do not support subquery")
    public void testUpdateWithAlwaysFalse2() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE  %s a left join %s b on a.pk=b.pk   SET a.integer_test=? WHERE EXISTS (select * from %s where pk = 1 and pk = 0)",
            baseOneTableName, baseTwoTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    @Ignore("do not support subquery")
    public void testUpdateWithAlwaysFalse3() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE  %s a left join %s b on a.pk=b.pk   SET a.integer_test=? WHERE (select count(*) from %s ) < 0",
            baseOneTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    @Ignore("do not support subquery")
    public void testUpdateWithAlwaysTrue1() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE  %s a left join %s b on a.pk=b.pk  SET a.integer_test=? WHERE b.pk NOT IN (SELECT pk FROM %s) OR b.pk IN (SELECT pk from %s)",
            baseOneTableName, baseTwoTableName, baseTwoTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    @Ignore("do not support subquery")
    public void testUpdateWithAlwaysTrue2() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE  %s a left join %s b on a.pk=b.pk   SET a.integer_test=? WHERE NOT EXISTS (select * from %s where pk = 1 and pk = 0)",
            baseOneTableName, baseTwoTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    @Ignore("do not support subquery")
    public void testUpdateWithAlwaysTrue3() throws Exception {
//		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE  %s a left join %s b on a.pk=b.pk   SET a.integer_test=? WHERE (select count(*) from %s ) >= 0",
            baseOneTableName, baseTwoTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithIdenticalColumnName() throws Exception {
        //		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String
            .format("UPDATE  %s a , %s b SET a.integer_test = b.integer_test WHERE a.pk=b.pk and b.pk > 2",
                baseOneTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithDerivedSubquery() throws Exception {
        //		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE  %s a left join (select max(integer_test) m, pk from %s b group by pk) b on a.pk=b.pk SET a.integer_test=b.m + 1 WHERE b.pk = 2",
            baseOneTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithDerivedSubquery1() throws Exception {
        //		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE  %s a right join (select max(integer_test) m, pk from %s b group by pk having pk >= 13 and pk <= 16 ) b on a.pk=b.pk SET a.integer_test=b.m + 1 ",
            baseOneTableName, baseTwoTableName);

        List<Object> param = new ArrayList<Object>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testUpdateWithDerivedSubquery2() throws Exception {
        //		String[] interval_expr = {"INTERVAL MICROSECOND", "SECOND",  "MINUTE", "HOUR" };

        String sql = String.format(
            "UPDATE (select max(integer_test) m, pk from %s b group by pk having pk >= 13 and pk <= 18 ) b join %s a on a.integer_test=b.pk SET a.integer_test=b.m + 1 ",
            baseOneTableName, baseOneTableName);

        List<Object> param = new ArrayList<Object>();
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, param, true);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * @since 5.1.18
     */
    @Test
    public void updateSelfJoinTest() throws Exception {
        String sql = String.format(
            "update %s a join %s b on a.integer_test=b.pk  set a.varchar_test='"
                + columnDataGenerator.varchar_testValue
                + "', b.varchar_test='"
                + columnDataGenerator.varchar_testValueTwo + "'",
            baseOneTableName, baseOneTableName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            sql, null, true);

        sql = String.format("select * from %s", baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        sql = String.format("select * from %s", baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection,
            tddlConnection);

        assertBroadcastTableSame(baseOneTableName);
        assertBroadcastTableSame(baseTwoTableName);
    }

    public void assertBroadcastTableSame(String tableName) {
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
            for (int i = 0; i < 2; i++) {
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

}

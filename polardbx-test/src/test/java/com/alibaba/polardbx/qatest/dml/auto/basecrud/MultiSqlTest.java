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

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.usePrepare;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 多语句测试
 *
 * @author agapple 2014年11月3日 下午4:17:48
 * @since 5.1.14
 */

@RunWith(Parameterized.class)
public class MultiSqlTest extends AutoCrudBasedLockTestCase {

    static String clazz = Thread.currentThread().getStackTrace()[1].getClassName();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public MultiSqlTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {

        if (baseOneTableName.startsWith("broadcast")) {
            JdbcUtil.setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB, tddlConnection);
        }

        String sql = "delete from  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void multiSqlInsert() throws Exception {
        String sqlFormat = "insert into " + baseOneTableName + "(pk,integer_test,varchar_test) values({0},{1},\"{2}\")";
        String sql = "";
        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < 20; i++) {
            String key = String.valueOf(columnDataGenerator.pkValue + i);
            sql += MessageFormat.format(sqlFormat, key, key, columnDataGenerator.varchar_testValue) + ";";
            keys.add(key);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void multiSqlInsertUpdateDelete() throws Exception {

        List<Object> params = new ArrayList<Object>();

        String sqlFormat = "insert into " + baseOneTableName + "(pk,integer_test,varchar_test) values({0},{1},\"{2}\")";
        String sql = "";
        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < 20; i++) {
            String key = String.valueOf(columnDataGenerator.pkValue + i);
            sql += MessageFormat.format(sqlFormat, key, key, columnDataGenerator.varchar_testValue) + ";";
            keys.add(key);
        }

        sql += String.format("update %s set integer_test=integer_test+1 ;", baseOneTableName);
        sql += String.format("delete from %s where integer_test>?;", baseOneTableName);
        params.add(10);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void multiSqlSelect() throws Exception {
        String sqlFormat = "insert into "
            + baseOneTableName
            + "(pk,integer_test,varchar_test) values({0},{1},\"abc\");select pk,integer_test,varchar_test from "
            + baseOneTableName + " where pk = {0}";
        String sql = "";
        List<String> keys = new ArrayList<String>();
        String key = String.valueOf(columnDataGenerator.pkValue + 100);
        sql += MessageFormat.format(sqlFormat, key, key);
        keys.add(key);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void multiSqlSemicolon() throws Exception {
        String sqlFormat = "insert into "
            + baseOneTableName
            + "(pk,integer_test,varchar_test) values({0},{1},\"abc\");select pk,integer_test,varchar_test from "
            + baseOneTableName + " where pk = {0};";
        String sql = "";
        String key = String.valueOf(columnDataGenerator.pkValue + 100);
        sql += MessageFormat.format(sqlFormat, key, key);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void multiSqlInsert_prepare() throws Exception {
        String sqlFormat = "insert into " + baseOneTableName + "(pk,integer_test,varchar_test) values(?,?,?)";
        String sql = "";
        List<String> keys = new ArrayList<String>();
        List<Object> params = new ArrayList<Object>();
        for (int i = 0; i < 20; i++) {
            String key = String.valueOf(columnDataGenerator.pkValue + i);
            sql += sqlFormat + ";";
            params.add(key);
            params.add(key);
            params.add(columnDataGenerator.varchar_testValue);
            keys.add(key);
        }

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.14
     */
    @Test
    public void multiSqlSelect_prepare() throws Exception {
        String sqlFormat = "insert into " + baseOneTableName
            + "(pk,integer_test,varchar_test) values(?,?,?);select pk,integer_test,varchar_test from "
            + baseOneTableName + " where pk = ?";
        String sql = "";
        List<String> keys = new ArrayList<String>();
        List<Object> params = new ArrayList<Object>();
        String key = String.valueOf(columnDataGenerator.pkValue + 100);
        params.add(key);
        params.add(key);
        params.add(columnDataGenerator.varchar_testValue);
        params.add(key);
        sql += sqlFormat;
        keys.add(key);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = "select * from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSetWithEscape() {
        String sql = "SET NAMES utf8mb4;Select * from " + baseOneTableName + " where varchar_test='温\\'' or pk=1";
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    @Test
    public void testTrxInMultiStatement() throws Exception {
        if (usePrepare()) {
            // prepare 模式不支持语句
            return;
        }
        String sql = "begin;"
            + "set drds_transaction_policy='2PC';"
            + "insert into " + baseOneTableName + "(pk,integer_test,varchar_test) values (?,?,?), (?,?,?);"
            + "select * from " + baseOneTableName + ";"
            + "update " + baseOneTableName + " set integer_test = integer_test + ?;"
            + "commit";

        final Connection conn = tddlConnection;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, 1);
            ps.setInt(2, 6);
            ps.setString(3, "foo");
            ps.setInt(4, 2);
            ps.setInt(5, 7);
            ps.setString(6, "bar");
            ps.setInt(7, 1);

            assertFalse(ps.execute());
            // begin
            assertEquals(0, ps.getUpdateCount());
            // set
            assertFalse(ps.getMoreResults());
            assertEquals(0, ps.getUpdateCount());
            // insert
            assertFalse(ps.getMoreResults());
            assertEquals(2, ps.getUpdateCount());
            // select
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            // update
            assertFalse(ps.getMoreResults());
            assertEquals(2, ps.getUpdateCount());
            // commit
            assertFalse(ps.getMoreResults());
            assertEquals(0, ps.getUpdateCount());
            // END
            assertFalse(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
        }
    }

    @Test
    public void testMultiDalStatements() throws Exception {
        String sql = "select @@VERSION_COMMENT;"
            + "select ?;"
            + "select database();"
            + "show databases;"
            + "show warnings;"
            + "show errors;"
            + "select ?;";

        final Connection conn = tddlConnection;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "foo");
            ps.setInt(2, 123);
            assertTrue(ps.execute());
            // select @@version_comment
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            // select "foo"
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertEquals("foo", rs.getString(1));
                assertFalse(rs.next());
            }
            // select database()
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
            // show databases
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                while (rs.next()) {
                }
            }
            // show warnings
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                while (rs.next()) {
                }
            }
            // show errors
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                while (rs.next()) {
                }
            }
            // select 1
            assertTrue(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertEquals(123, rs.getInt(1));
                assertFalse(rs.next());
            }
            // END
            assertFalse(ps.getMoreResults());
            assertEquals(-1, ps.getUpdateCount());
        }
    }

    @Test
    public void testCommentStatements() {
        String sql = "/*comment*/";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);

        sql = "-- comment";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
    }

}

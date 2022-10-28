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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JDBC二进制prepare语句测试
 * 主要通过执行结果验证参数化后参数绑定是否正确
 */
@RunWith(Parameterized.class)
public class ServerPrepareTest extends CrudBasedLockTestCase {

    private Connection tddlPreparedConn;

    @Before
    public void beforePrepare() {
        if (!PropertiesUtil.usePrepare()) {
            this.tddlPreparedConn = tddlConnection;
        } else {
            // 对于prepare阶段报错的语句 不再尝试客户端prepare (JDBC驱动行为)
            this.tddlPreparedConn = getPolardbxConnectionWithExtraParams("&emulateUnsupportedPstmts=false");
        }
    }

    public ServerPrepareTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwo());
    }

    @Test
    public void prepareNoTableTest0() {
        String sql = "select concat(\"abc\", ?)";
        List<Object> params = new ArrayList<>();
        params.add("def");

        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareNoTableTest1() {
        String sql = "select concat(?, ?)";
        List<Object> params = new ArrayList<>();
        params.add("abc");
        params.add("def");

        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareNoTableTest2() {
        String sql = "select concat(?, \"abc\")";
        List<Object> params = new ArrayList<>();
        params.add("abc");

        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    // Notice: main purpose of this test is to verify whether 8.0 connector works well
    @Test
    public void executePrepareMultiTimes() throws SQLException {
        String sql = "select * from " + baseOneTableName + " where pk= ?;";
        executeSql(tddlPreparedConn, sql);
        executeSql(tddlPreparedConn, sql);
    }

    private void executeSql(Connection conn, String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                }
            }
        }
    }

    @Test
    public void prepareSelectTest1() {
        String sql = "select * from " + baseOneTableName + " where pk= ?;";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareSelectTest2() {
        String sql = "select * from " + baseOneTableName + " where pk > ? order by pk limit ?,?;";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        params.add(10);
        params.add(5);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareSelectTest3() {
        String sql = "select * from " + baseOneTableName + " where pk > ? order by pk limit ? offset ?;";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        params.add(5);
        params.add(10);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareSelectTest4() {
        String sql = "select * from " + baseOneTableName + " where pk > 15 order by pk limit ?,?;";
        List<Object> params = new ArrayList<>();
        params.add(10);
        params.add(5);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareSelectTest5() {
        String sql = "select * from " + baseOneTableName + " where pk > 15 order by pk limit ? offset ?;";
        List<Object> params = new ArrayList<>();
        params.add(5);
        params.add(10);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareSelectTest6() {
        String sql = "select * from " + baseOneTableName + " where pk > ? order by pk limit 10,?;";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        params.add(100);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareJoinTest0() {
        String sql =
            "select * from "
                + baseOneTableName
                + " t1, "
                + baseOneTableName
                + " t2 where t1.pk = t2.pk" + " and t1.pk = ? order by t1.pk";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareJoinTest1() {
        String sql =
            "select * from "
                + baseOneTableName
                + " t1, "
                + baseOneTableName
                + " t2 where t1.pk < t2.pk" + " and t1.pk = ? order by t1.pk, t2.pk limit ?,?";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        params.add(10);
        params.add(5);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareJoinTest2() {
        String sql =
            "select * from "
                + baseOneTableName
                + " t1, "
                + baseOneTableName
                + " t2 where t1.pk < t2.pk" + " and t1.pk = ? order by t1.pk, t2.pk limit ? offset 10";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        params.add(5);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareInTest1() {
        String sql =
            "select * from "
                + baseOneTableName
                + " t1, "
                + baseOneTableName
                + " t2 where t1.pk = t2.pk" + " and t1.pk in (?, 5, 10, 11, ?) order by t1.pk";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        params.add(ColumnDataGenerator.pkValue + 1);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareInTest2() {
        String sql =
            "select * from "
                + baseOneTableName
                + " t1, "
                + baseOneTableName
                + " t2 where t1.pk = t2.pk" + " and t1.pk in (?, ?, ?, ?, ?) order by t1.pk";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        params.add(ColumnDataGenerator.pkValue + 1);
        params.add(ColumnDataGenerator.pkValue + 2);
        params.add(ColumnDataGenerator.pkValue + 3);
        params.add(ColumnDataGenerator.pkValue + 4);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    /**
     * IN to SUB-QUERY
     */
    @Test
    public void prepareInTest3() {
        String sql =
            "/*+ TDDL: IN_SUB_QUERY_THRESHOLD=3*/ select * from "
                + baseOneTableName
                + " t1, "
                + baseOneTableName
                + " t2 where t1.pk = t2.pk" + " and t1.pk in (?, ?, ?, ?, ?, ?, ?, ?) order by t1.pk";
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        params.add(ColumnDataGenerator.pkValue + 1);
        params.add(ColumnDataGenerator.pkValue + 2);
        params.add(ColumnDataGenerator.pkValue + 3);
        params.add(ColumnDataGenerator.pkValue + 4);
        params.add(ColumnDataGenerator.pkValue + 5);
        params.add(ColumnDataGenerator.pkValue + 6);
        params.add(ColumnDataGenerator.pkValue + 7);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareSubqueryTest1() {
        String sql = "select a.* , (select count(1) from " + baseOneTableName + " where pk = ?) as COUNT from "
            + baseOneTableName + " a where pk = ?";
        List<Object> params = new ArrayList<>();
        params.add(1);
        params.add(2);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    @Test
    public void prepareSubqueryTest2() {
        String sql = "select a.pk from (select * from " + baseOneTableName
            + " where pk > 15 order by pk limit ? offset ?) as a limit ?,?;";
        List<Object> params = new ArrayList<>();
        params.add(5);
        params.add(10);

        params.add(1);
        params.add(3);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    /**
     * BLob 字段通过 COM_STMT_SEND_LONG_DATA 发送
     */
    @Test
    public void sendLongDataTest() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("select blob_test from " + baseOneTableName + " limit 1", tddlPreparedConn);
        Assert.assertTrue(rs.next());
        byte[] data = rs.getBytes(1);

        String sql = "select * from " + baseOneTableName + " where blob_test= ? and pk > ? order by pk;";
        PreparedStatement tddlPreparedStmt = null;
        PreparedStatement mysqlPreparedStmt = null;
        ResultSet mysqlRs = null;
        ResultSet tddlRs = null;
        InputStream tddlInputStream = new ByteArrayInputStream(data);
        InputStream mysqlInputStream = new ByteArrayInputStream(data);
        try {
            tddlPreparedStmt = tddlPreparedConn.prepareStatement(sql);
            mysqlPreparedStmt = mysqlConnection.prepareStatement(sql);
            tddlPreparedStmt.setBlob(1, tddlInputStream);
            tddlPreparedStmt.setLong(2, ColumnDataGenerator.pkValue);
            mysqlPreparedStmt.setBlob(1, mysqlInputStream);
            mysqlPreparedStmt.setLong(2, ColumnDataGenerator.pkValue);
            tddlRs = tddlPreparedStmt.executeQuery();
            mysqlRs = mysqlPreparedStmt.executeQuery();
            DataValidator.resultSetContentSameAssert(mysqlRs, tddlRs, true);

        } finally {
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
            JdbcUtil.close(tddlPreparedStmt);
            JdbcUtil.close(mysqlPreparedStmt);
        }
    }

    /**
     * 测试重复执行
     * 避免缓存问题
     */
    @Test
    public void prepareAndMultiExecuteTest() {
        final int execCount = 3;
        String sql = "select * from " + baseOneTableName + " where pk= ?;";
        PreparedStatement tddlPreparedStmt = null;
        PreparedStatement mysqlPreparedStmt = null;
        ResultSet mysqlRs = null;
        ResultSet tddlRs = null;
        try {
            tddlPreparedStmt = tddlPreparedConn.prepareStatement(sql);
            mysqlPreparedStmt = mysqlConnection.prepareStatement(sql);
            for (int i = 0; i < execCount; i++) {
                tddlPreparedStmt.setLong(1, ColumnDataGenerator.pkValue + i);
                mysqlPreparedStmt.setLong(1, ColumnDataGenerator.pkValue + i);
                tddlRs = tddlPreparedStmt.executeQuery();
                mysqlRs = mysqlPreparedStmt.executeQuery();
                DataValidator.resultSetContentSameAssert(mysqlRs, tddlRs, true);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
            JdbcUtil.close(tddlPreparedStmt);
            JdbcUtil.close(mysqlPreparedStmt);
        }
        List<Object> params = new ArrayList<>();
        params.add(ColumnDataGenerator.pkValue);
        assertServerPrepareTest(mysqlConnection, tddlPreparedConn, sql, params);
    }

    private void assertServerPrepareTest(
        Connection mysqlConnection,
        Connection tddlConnection,
        String sql,
        List<Object> params) {
        DataValidator.selectContentSameAssert(sql, params, mysqlConnection, tddlConnection);
    }
}

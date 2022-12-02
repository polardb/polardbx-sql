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

package com.alibaba.polardbx.qatest.dal.set;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;

/**
 * set命令测试
 *
 * @author chenhui
 * @since 5.1.0
 */
public class SetCommandTest extends DirectConnectionBaseTestCase {

    /**
     * @since 5.1.0
     */
    @Test
    @Ignore
    public void testSetNamesGbk() throws Exception {
        setName("gbk");

        String sql = "show variables like 'character_set_client'";
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").equals("gbk"));
        Assert.assertFalse(rs.next());
        JdbcUtil.close(tddlPs);
        JdbcUtil.close(rs);

        sql = "show variables like 'character_set_connection'";
        tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        rs = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").equals("gbk"));
        Assert.assertFalse(rs.next());
        JdbcUtil.close(tddlPs);
        JdbcUtil.close(rs);

        sql = "show variables like 'character_set_results'";
        tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        rs = JdbcUtil.executeQuery(sql, tddlPs);

        Assert.assertTrue(rs.next());
        // Assert.assertTrue(rs.getString("Value").equals("gbk"));
        Assert.assertFalse(rs.next());
        JdbcUtil.close(tddlPs);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void testSetNamesUtf8() throws Exception {
        setName("utf8mb4");

        String sql = "show variables like 'character_set_client'";
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").startsWith("utf8mb4"));
        Assert.assertFalse(rs.next());
        JdbcUtil.close(tddlPs);
        JdbcUtil.close(rs);

        sql = "show variables like 'character_set_connection'";
        tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        rs = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").startsWith("utf8mb4"));
        Assert.assertFalse(rs.next());
        JdbcUtil.close(tddlPs);
        JdbcUtil.close(rs);

        sql = "show variables like 'character_set_results'";
        tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
        rs = JdbcUtil.executeQuery(sql, tddlPs);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").startsWith("utf8mb4"));
        Assert.assertFalse(rs.next());
        JdbcUtil.close(tddlPs);
        JdbcUtil.close(rs);
    }

    /**
     * @since 5.3.9
     */
    @Test
    public void testSetSubQuery() {
        String sql = "set @a = (select pk from select_base_one_multi_db_multi_tb order by pk limit 1)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
        sql = "select * from select_base_one_multi_db_multi_tb where pk = @a";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.9
     */
    @Test
    public void testSetSubQueryMoreThanTwoColumn() {
        String sql = "set @a = (select pk, 1 from select_base_one_multi_db_multi_tb order by pk limit 1)";
        selectErrorAssert(sql, null, tddlConnection, "Operand should contain 1 column(s)");

    }

    /**
     * @since 5.3.9
     */
    @Test
    public void testSetSubQueryMoreThanTwoRow() {
        String sql = "set @a = (select pk from select_base_one_multi_db_multi_tb order by pk limit 2)";
        selectErrorAssert(sql, null, tddlConnection, "Subquery returns more than 1 row");
    }

    private void setName(String encode) throws Exception {
        String sql = "set names " + encode;
        JdbcUtil.updateDataTddl(tddlConnection, sql, null);
    }

    @Test
    public void testSetTxReadOnly() throws SQLException {
        String variable = "tx_read_only";
        if (isMySQL80()) {
            variable = "transaction_read_only";
        }
        String setSql = "set @@session." + variable + " = 0";
        String selectSql = "select @@session." + variable;

        boolean oldValue = getBooleanValue(selectSql);
        try {
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(selectSql) == false);

            setSql = "set @@session." + variable + " = 1";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(selectSql) == true);

            setSql = "set @@session." + variable + " = on";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(selectSql) == true);

            setSql = "set @@session." + variable + " = true";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(selectSql) == true);

            setSql = "set @@session." + variable + " = off";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(selectSql) == false);

            setSql = "set @@session." + variable + " = false";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(selectSql) == false);

            setSql = "set @@session." + variable + " = true";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(selectSql) == true);
        } finally {
            // Recover the oldValue
            setSql = "set @@session." + variable + " = " + oldValue;
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(selectSql) == oldValue);
        }
    }

    private boolean getBooleanValue(String sql) {
        boolean value = false;
        ResultSet rs = null;
        Statement statement = null;
        try {
            statement = tddlConnection.createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                value = rs.getBoolean(1);
            }
        } catch (SQLException e) {
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(statement);
        }
        return value;
    }

    private void setVar(String sql) throws SQLException {
        Statement statement = null;
        try {
            statement = tddlConnection.createStatement();
            statement.execute(sql);
        } finally {
            JdbcUtils.close(statement);
        }
    }

    @Test
    public void setUserDefVarFromExpr() {
        String setTableName = "SET @tbl_name = 'test'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, setTableName, null);
        selectContentSameAssert("SELECT @tbl_name", null, mysqlConnection, tddlConnection);

        String setStr =
            "SET @str = CONCAT(\"CREATE VIEW bug13095_v1(c1) AS SELECT stuff FROM \", CONCAT(@tbl_name, \"123\"));\n";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, setStr, null);
        selectContentSameAssert("SELECT @str", null, mysqlConnection, tddlConnection);

        setStr = "SET @str = CONCAT(\"INSERT INTO \", @tbl_name, \" VALUES('row1'),('row2'),('row3')\" )";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, setStr, null);
        selectContentSameAssert("SELECT @str", null, mysqlConnection, tddlConnection);
    }
}

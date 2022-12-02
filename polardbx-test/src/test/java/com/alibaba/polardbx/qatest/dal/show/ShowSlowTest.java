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

package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ShowSlowTest extends ReadBaseTestCase {

    private static final String SLOW_SQL = "select sleep(3)";

    private static final String USERNAME = "user_show_slow";
    private static final String PASSWORD = "123456";
    private static final String HOST = "%";
    private static final String DB_NAME1 = PropertiesUtil.polardbXDBName1(false);
    private static final String DB_NAME2 = PropertiesUtil.polardbXDBName2(false);
    private Connection userConn;

    @BeforeClass
    public static void setUpResources() {
        Assert.assertTrue("Host cannot be empty", StringUtils.isNotBlank(HOST));
        try (Connection polarxConn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.dropUser(polarxConn, USERNAME, HOST);
            JdbcUtil.createUser(polarxConn, USERNAME, HOST, PASSWORD);
            JdbcUtil.grantAllPrivOnDb(polarxConn, USERNAME, HOST, DB_NAME1);
            JdbcUtil.grantAllPrivOnDb(polarxConn, USERNAME, HOST, DB_NAME2);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @AfterClass
    public static void destroyResources() {
        try (Connection polarxConn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.dropUser(polarxConn, USERNAME, HOST);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Before
    public void setUp() {
        this.userConn = ConnectionManager.getInstance().newPolarDBXConnection(USERNAME, PASSWORD);
        JdbcUtil.useDb(this.userConn, DB_NAME1);
    }

    @After
    public void cleanUp() {
        JdbcUtil.executeSuccess(tddlConnection, "clear slow");
        JdbcUtil.close(userConn);
    }

    @Test
    public void testShowSlow() {
        testShowSlowByConnection(userConn, DB_NAME1, userConn, DB_NAME1);
        testShowSlowByConnection(userConn, DB_NAME1, tddlConnection, DB_NAME1);
    }

    @Test
    public void testShowSlowInstanceLevel() {
        testShowSlowByConnection(userConn, DB_NAME2, userConn, DB_NAME1);
        testShowSlowByConnection(userConn, DB_NAME2, tddlConnection, DB_NAME1);
    }

    private void testShowSlowByConnection(Connection slowConn, String slowSqlDB,
                                          Connection showConn, String showSqlDB) {
        JdbcUtil.useDb(slowConn, slowSqlDB);
        createSlowSql(slowConn);
        JdbcUtil.useDb(showConn, showSqlDB);

        assertSlowSqlExistence(true, slowSqlDB, showConn, false, USERNAME);
        assertSlowSqlExistence(true, slowSqlDB, showConn, true, USERNAME);
    }

    /**
     * 验证慢SQL是否存在
     * 且按照执行时长排序
     *
     * @param showConn 执行慢SQL的连接
     * @param isFull 是否为 show full slow
     * @param slowSqlUser 预期执行该条慢SQL的用户
     */
    private void assertSlowSqlExistence(boolean expectExists, String expectSlowSqlDb,
                                        Connection showConn, boolean isFull, String slowSqlUser) {
        boolean existsSlowSql = false;
        final String showSlowSql = isFull ? "show full slow" : "show slow";
        long executionTime = -1L;
        try (ResultSet rs = JdbcUtil.executeQuery(showSlowSql, showConn)) {
            while (rs.next()) {
                String user = rs.getString("USER");
                String slowSql = rs.getString("SQL");
                if (executionTime != -1) {
                    Assert.assertTrue(rs.getLong("EXECUTE_TIME") < executionTime);
                }
                executionTime = rs.getLong("EXECUTE_TIME");
                if (slowSqlUser.equals(user) && SLOW_SQL.equals(slowSql)
                    && expectSlowSqlDb.equalsIgnoreCase(rs.getString("DB"))) {
                    existsSlowSql = true;
                    break;
                }
            }
            JdbcUtil.close(rs);
            if (expectExists) {
                Assert.assertTrue(String.format("Cannot find slow sql by '%s'", showSlowSql), existsSlowSql);
            } else {
                Assert.assertFalse(String.format("Expect no slow sqls by '%s'", showSlowSql), existsSlowSql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

    }

    /**
     * 清除实例级别的 slow sql 记录
     */
    @Test
    public void testClearSlow() {
        createSlowSql(tddlConnection);

        JdbcUtil.executeSuccess(getPolardbxConnection(DB_NAME1), "clear slow");
        final String polardbxUser = ConnectionManager.getInstance().getPolardbxUser();
        assertSlowSqlExistence(false, DB_NAME1, tddlConnection, false, polardbxUser);
        assertSlowSqlExistence(false, DB_NAME1, tddlConnection, true, polardbxUser);
    }

    /**
     * 普通用户无法看到 root 账号的慢SQL
     */
    @Test
    public void testNormalUserShowSlow() {
        final String polardbxUser = ConnectionManager.getInstance().getPolardbxUser();
        Connection slowConn = getPolardbxConnection();
        createSlowSql(slowConn);
        assertSlowSqlExistence(false, DB_NAME1, userConn, false, polardbxUser);
        assertSlowSqlExistence(false, DB_NAME1, userConn, true, polardbxUser);
    }

    /**
     * 执行select sleep(10);指令生成慢SQL
     */
    private void createSlowSql(Connection connection) {
        JdbcUtil.executeSuccess(connection, SLOW_SQL);
    }
}

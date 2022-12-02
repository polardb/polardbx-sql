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

package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class ShowProcesslistTest extends BaseTestCase {

    @Test
    public void testShowProcessListWithCondition() throws Exception {
        Connection showProcesslistConnection = null;
        List<Connection> connections = new ArrayList<>(4);
        try {
            final StringBuilder error = new StringBuilder();
            showProcesslistConnection = getPolardbxDirectConnection();

            final String sql = "/*TDDL:NODE=0*/ select sleep(5);";
            for (int i = 0; i < 4; i++) {
                final Connection sleepConnection = getPolardbxDirectConnection();
                connections.add(sleepConnection);
                new Thread(() -> {
                    try {
                        sleepConnection.createStatement().execute(sql);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        error.append(e.getMessage());
                    }
                }).start();
            }
            Thread.sleep(2000);

            int count = 0;

            // Order by
            Long prevId = Long.MAX_VALUE;
            ResultSet rs = showProcesslistConnection.createStatement()
                .executeQuery("SHOW FULL PROCESSLIST ORDER BY ID DESC");
            while (rs.next()) {
                Long id = rs.getLong("id");
                Assert.assertTrue(prevId > id);
                prevId = id;
                count++;
            }
            Assert.assertTrue(count >= 4);

            // Limit
            count = 0;
            rs = showProcesslistConnection.createStatement().executeQuery("SHOW FULL PROCESSLIST LIMIT 1");
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(count, 1);

            // Where
            count = 0;
            rs = showProcesslistConnection.createStatement()
                .executeQuery("SHOW FULL PROCESSLIST WHERE USER IS NOT NULL AND HOST IS NOT NULL");
            while (rs.next()) {
                count++;
            }
            Assert.assertTrue(count >= 4);

            // Where & Order by & Limit
            count = 0;
            prevId = Long.MAX_VALUE;
            rs = showProcesslistConnection.createStatement()
                .executeQuery("SHOW FULL PROCESSLIST WHERE INFO LIKE '%sleep%' ORDER BY ID DESC LIMIT 3");
            while (rs.next()) {
                Assert.assertFalse(TStringUtil.containsIgnoreCase("SLEEP", rs.getString("info")));
                Long id = rs.getLong("id");
                Assert.assertTrue(prevId > id);
                prevId = id;
                count++;
            }
            Assert.assertEquals(count, 3);

            rs.close();

            // 清理下被关的
            showProcesslistConnection.createStatement().executeUpdate("reload datasources");
            showProcesslistConnection.close();
            showProcesslistConnection = null;
        } finally {
            if (showProcesslistConnection != null) {
                showProcesslistConnection.close();
                showProcesslistConnection = null;
            }

            for (Connection conn : connections) {
                if (conn != null) {
                    conn.close();
                }
            }
        }

    }

    @Test
    public void testShowPhysicalProcessListWithCondition() throws Exception {

        Connection showProcesslistConnection = null;
        List<Connection> connections = new ArrayList<>(4);
        try {
            final StringBuilder error = new StringBuilder();
            showProcesslistConnection = getPolardbxDirectConnection();

            final String sql = "/*TDDL:NODE=0*/ select sleep(5);";
            for (int i = 0; i < 4; i++) {
                final Connection sleepConnection = getPolardbxDirectConnection();
                connections.add(sleepConnection);
                new Thread(() -> {
                    try {
                        sleepConnection.createStatement().execute(sql);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        error.append(e.getMessage());
                    }
                }).start();
            }
            Thread.sleep(2000);

            int count = 0;

            // Order by
            Long prevId = Long.MAX_VALUE;
            ResultSet rs = showProcesslistConnection.createStatement()
                .executeQuery("SHOW FULL PHYSICAL_PROCESSLIST ORDER BY ID DESC");
            while (rs.next()) {
                Long id = rs.getLong("id");
                Assert.assertTrue(prevId > id);
                prevId = id;
                count++;
            }
            Assert.assertTrue(count >= 4);

            // Limit
            count = 0;
            rs = showProcesslistConnection.createStatement().executeQuery("SHOW FULL PHYSICAL_PROCESSLIST LIMIT 1");
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(count, 1);

            // Where
            count = 0;
            rs = showProcesslistConnection.createStatement()
                .executeQuery("SHOW FULL PHYSICAL_PROCESSLIST WHERE USER IS NOT NULL AND DB IS NOT NULL");
            while (rs.next()) {
                count++;
            }
            Assert.assertTrue(count >= 4);

            // Where & Order by & Limit
            count = 0;
            prevId = Long.MAX_VALUE;
            rs = showProcesslistConnection.createStatement()
                .executeQuery("SHOW FULL PHYSICAL_PROCESSLIST WHERE INFO LIKE '%sleep%' ORDER BY ID DESC LIMIT 3");
            while (rs.next()) {
                Assert.assertFalse(TStringUtil.containsIgnoreCase("SLEEP", rs.getString("info")));
                Long id = rs.getLong("id");
                Assert.assertTrue(prevId > id);
                prevId = id;
                count++;
            }
            Assert.assertEquals(count, 3);

            rs.close();

            // 清理下被关的
            showProcesslistConnection.createStatement().executeUpdate("reload datasources");
            showProcesslistConnection.close();
            showProcesslistConnection = null;
        } finally {
            if (showProcesslistConnection != null) {
                showProcesslistConnection.close();
                showProcesslistConnection = null;
            }

            for (Connection conn : connections) {
                if (conn != null) {
                    conn.close();
                }
            }
        }

    }

    @Test
    public void testKillALL() throws Exception {

        Connection killConnection = null;
        Connection refSleepConnection = null;
        try {
            final StringBuilder error = new StringBuilder();
            killConnection = getPolardbxDirectConnection();
            final Connection sleepConnection = getPolardbxDirectConnection();
            refSleepConnection = sleepConnection;
            new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        final long startTime = System.currentTimeMillis();
                        sleepConnection.createStatement()
                            .execute(
                                " /*TDDL:scan and socket_timeout=100000 and merge_concurrent=true*/select sleep(100);");
                        final long stopTime = System.currentTimeMillis();
                        if (stopTime - startTime < 7000) {
                            // Note: In Xproto, only session killed and sleep finish.
                            error.append("Communications link failure");
                        }
                        sleepConnection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        error.append(e.getMessage());
                    }
                }
            }).start();
            Thread.sleep(5000);
            Assert.assertTrue(killConnection.createStatement().executeUpdate("kill \'all\'") > 0);

            int i = 15;
            while (i > 0 && StringUtils.isBlank(error.toString())) {
                Thread.sleep(1000);
                i--;
            }

            // 清理下被关的
            killConnection.createStatement().executeUpdate("reload datasources");
            killConnection.close();
            killConnection = null;
            Thread.sleep(1000);

            Assert.assertTrue(error.toString(),
                error.toString().contains("Communications link failure") || error.toString()
                    .contains("XResult stream fetch result timeout") || error.toString()
                    .contains("Session was killed"));
        } finally {
            if (killConnection != null) {
                killConnection.close();
                killConnection = null;
            }

            if (refSleepConnection != null) {
                refSleepConnection.close();
            }
        }
    }

    @Test
    public void testShowProcessListInstanceLevel() throws Exception {
        // 测试不同AppName的连接
        Connection tddlConnectionApp1 = getPolardbxDirectConnection();
        Connection tddlConnectionApp2 = getPolardbxDirectConnection();

        try {
            ResultSet rs = tddlConnectionApp2.createStatement().executeQuery("select connection_id() as id;");
            Assert.assertTrue(rs.next());
            final long id = rs.getLong("id");

            final String sleepSql = "/*TDDL:NODE=0*/ select sleep(100);";
            final StringBuilder app2Error = new StringBuilder();
            new Thread(() -> {
                try {
                    tddlConnectionApp2.createStatement().execute(sleepSql);
                } catch (SQLException e) {
                    e.printStackTrace();
                    app2Error.append(e.getMessage());
                }
            }).start();
            Thread.sleep(2000);

            rs = tddlConnectionApp1.createStatement().executeQuery("SHOW FULL PROCESSLIST");
            boolean containsApp2Connection = false;
            while (rs.next()) {
                if (id == rs.getLong("id")) {
                    Assert.assertEquals("Query", rs.getString("Command"));
                    Assert.assertEquals(sleepSql, rs.getString("info"));
                    containsApp2Connection = true;
                }
            }
            rs.close();
            Assert.assertTrue(containsApp2Connection);

            tddlConnectionApp1.createStatement().executeUpdate("kill " + id);
            Thread.sleep(1000);

            rs = tddlConnectionApp1.createStatement().executeQuery("SHOW FULL PROCESSLIST");
            while (rs.next()) {
                Assert.assertFalse(TStringUtil.containsIgnoreCase("SLEEP", rs.getString("info")));
            }
            rs.close();
            int i = 15;
            while (i > 0 && StringUtils.isBlank(app2Error.toString())) {
                Thread.sleep(1000);
                i--;
            }
            Assert.assertTrue(app2Error.toString(), app2Error.toString().contains("Communications link failure"));
        } finally {
            JdbcUtil.close(tddlConnectionApp1);
            JdbcUtil.close(tddlConnectionApp2);
        }

    }

}


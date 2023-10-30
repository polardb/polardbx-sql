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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@NotThreadSafe
public class DeadlockTest extends CrudBasedLockTestCase {

    private static final Log logger = LogFactory.getLog(DeadlockTest.class);

    @Test(timeout = 60000)
    public void testAutoRollbackAfterLocalDeadlock() throws SQLException {
        long before = 0, after = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
            if (rs.next()) {
                before = rs.getLong("LOCAL_DEADLOCK_COUNT");
            }
        }

        final String tableName = "auto_rollback_after_local_deadlock_test";
        testFramework(tableName, true);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
            if (rs.next()) {
                after = rs.getLong("LOCAL_DEADLOCK_COUNT");
            }
        }

        Assert.assertTrue(
            "after.LOCAL_DEADLOCK_COUNT should > before.LOCAL_DEADLOCK_COUNT, but before is "
                + before + ", and after is " + after,
            after > before);
    }

    @Test(timeout = 60000)
    public void testAutoRollbackAfterGlobalDeadlock() throws SQLException {
        final String tableName = "auto_rollback_after_global_deadlock_test";
        testFramework(tableName, false);
    }

    private void testFramework(String tableName, boolean single) throws SQLException {
        final List<Connection> connections = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            connections.add(getPolardbxConnection());
        }
        try {
            createTable(tableName, single);
            innerTest(tableName, connections);
            testShowDeadlocks(tableName, single);
        } finally {
            clear(connections, tableName);
        }
    }

    private void innerTest(String tableName, List<Connection> connections) {
        // Insert some data
        String sql = "insert into " + tableName + " values (0), (1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Connection 0: select id = 0 for update
        JdbcUtil.executeQuerySuccess(connections.get(0), "begin");
        sql = "select * from " + tableName + " where id = 0 for update";
        JdbcUtil.executeQuerySuccess(connections.get(0), sql);

        // Connection 1: select id = 1 for update
        JdbcUtil.executeQuerySuccess(connections.get(1), "begin");
        sql = "select * from " + tableName + " where id = 1 for update";
        JdbcUtil.executeQuerySuccess(connections.get(1), sql);

        final ExecutorService threadPool = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory(DeadlockTest.class.getSimpleName(), false));
        final List<Future<Boolean>> futures = new LinkedList<>();

        // Connection 0: select id = 1 for update
        sql = "select * from " + tableName + " where id = 1 for update";
        futures.add(executeSqlAndCommit(threadPool, tableName, connections.get(0), sql));

        // Connection 1: select id = 0 for update
        sql = "select * from " + tableName + " where id = 0 for update";
        futures.add(executeSqlAndCommit(threadPool, tableName, connections.get(1), sql));

        for (Future<Boolean> future : futures) {
            try {
                if (!future.get(20, TimeUnit.SECONDS)) {
                    Assert.fail("Unexpected error occurs.");
                }
            } catch (Throwable e) {
                e.printStackTrace();
                Assert.fail("Get future failed.");
            }
        }
    }

    private void clear(Collection<Connection> connections, String tableName) {
        for (Connection connection : connections) {
            if (null != connection) {
                try {
                    JdbcUtil.executeQuerySuccess(connection, "commit");
                } catch (Throwable e) {
                    // ignore
                    e.printStackTrace();
                }
                try {
                    connection.close();
                } catch (Throwable e) {
                    // ignore
                    e.printStackTrace();
                }
            }
        }
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private Future<Boolean> executeSqlAndCommit(ExecutorService threadPool, String tableName,
                                                Connection connection, String sql) {
        return threadPool.submit(() -> {
            try {
                JdbcUtil.executeQuery(sql, connection);
            } catch (Throwable e) {
                if (e.getMessage()
                    .contains("Deadlock found when trying to get lock; try restarting transaction")) {
                    // Deadlock occurs in this connection.
                    // It should be rolled back already and can execute any sql immediately.
                    try {
                        testAlreadyRollback(connection, tableName);
                    } catch (Throwable e2) {
                        e2.printStackTrace();
                        return false;
                    }
                } else {
                    e.printStackTrace();
                    return false;
                }
            } finally {
                try {
                    // Try to commit
                    JdbcUtil.executeQuerySuccess(connection, "commit");
                } catch (Throwable e) {
                    // Ignore
                }
            }
            return true;
        });
    }

    private void testAlreadyRollback(Connection connection, String tableName) throws SQLException {
        JdbcUtil.executeQuerySuccess(connection, "begin");
        for (int i = 100; i < 110; i++) {
            final String sql = String.format("insert into %s values (%s)", tableName, i);
            JdbcUtil.executeUpdateSuccess(connection, sql);
        }
        JdbcUtil.executeQuerySuccess(connection, "rollback");
        // All changes should be rolled back, and id = 109 should not be in the table
        final String sql = "select * from " + tableName + " where id = 109";
        try (final ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql)) {
            Assert.assertFalse("id = 109 can be retrieved, which should be rolled back already", rs.next());
        }
    }

    @Test(timeout = 60000)
    @Ignore("fix by ???")
    public void testAutoRollbackAfterMdlDeadlock() throws SQLException {
        if (!enableMdlDetection() || getPolardbxConnection().getMetaData().getURL().toLowerCase()
            .contains("cursorfetch")) {
            return;
        }

        final List<Connection> connections = new ArrayList<>();
        final String tableName = "auto_rollback_after_mdl_deadlock_test";
        final String verifiedTableName = "auto_rollback_after_mdl_deadlock_test_verified";
        try {
            // Create a partition table
            String sql = "drop table if exists " + tableName;
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = "create table " + tableName + " (id int primary key) dbpartition by hash(id)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // Create another table for rollback verification
            sql = "drop table if exists " + verifiedTableName;
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = "create table " + verifiedTableName + " (id int primary key)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "insert into " + tableName + " values (0), (1)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
            Assert.assertTrue("topology is null", CollectionUtils.isNotEmpty(topology));
            final int connPoolSize = 2 + topology.size();

            final AtomicBoolean unexpectedError = new AtomicBoolean(false);
            for (int i = 0; i < connPoolSize; i++) {
                connections.add(getPolardbxConnection());
            }

            // Connection 0: select id = 0 for update
            JdbcUtil.executeQuerySuccess(connections.get(0), "begin");
            sql = "select * from " + tableName + " where id = 0 for update";
            JdbcUtil.executeQuerySuccess(connections.get(0), sql);

            // Connection 1: select id = 1 for update
            JdbcUtil.executeQuerySuccess(connections.get(1), "begin");
            sql = "select * from " + tableName + " where id = 1 for update";
            JdbcUtil.executeQuerySuccess(connections.get(1), sql);

            final ExecutorService threadPool = new ThreadPoolExecutor(connPoolSize, connPoolSize, 0L,
                TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                new NamedThreadFactory(DeadlockTest.class.getSimpleName(), false));

            final List<Future> futures = new LinkedList<>();

            // For each DN, perform a ddl statement
            for (int i = 0; i < topology.size(); i++) {
                if (unexpectedError.get()) {
                    return;
                }
                final String physicalGroup = topology.get(i).getKey();
                final String physicalTable = topology.get(i).getValue();
                final Connection ddlConnection = connections.get(2 + i);
                futures.add(threadPool.submit(() -> {
                    // make sure this DN enables the metadata_lock view
                    String sql0 = "/*+TDDL:node(" + physicalGroup + ")*/select ENABLED, TIMED "
                        + "from performance_schema.setup_instruments where NAME = 'wait/lock/metadata/sql/mdl'";
                    try {
                        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql0);
                        boolean isDnEnabled = false;
                        while (rs.next()) {
                            if ("yes".equalsIgnoreCase(rs.getString(1))
                                && "yes".equalsIgnoreCase(rs.getString(2))) {
                                isDnEnabled = true;
                            }
                        }

                        if (!isDnEnabled) {
                            unexpectedError.set(true);
                            return;
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                        unexpectedError.set(true);
                        return;
                    }

                    // Perform a physical ddl on DN
                    sql0 =
                        "/*+TDDL:node(" + physicalGroup + ")*/alter table " + physicalTable + " add column cc int";
                    Statement stmt = null;
                    try {
                        stmt = ddlConnection.createStatement();
                        stmt.setQueryTimeout(15);
                        stmt.execute(sql0);
                    } catch (SQLTimeoutException e) {
                        handleTimeout(stmt, sql0, unexpectedError);
                    } catch (Throwable e) {
                        // Ignore other exceptions
                        e.printStackTrace();
                    }
                }));
            }

            if (unexpectedError.get()) {
                Assert.fail("Unexpected error happens before an MDL deadlock.");
            }

            // Connection 0: select id = 1 for update
            sql = "select * from " + tableName + " where id = 1 for update";
            futures.add(executeSqlMdl(threadPool, connections.get(0), unexpectedError, sql, verifiedTableName));

            // Connection 1: select id = 0 for update
            sql = "select * from " + tableName + " where id = 0 for update";
            futures.add(executeSqlMdl(threadPool, connections.get(1), unexpectedError, sql, verifiedTableName));

            for (Future future : futures) {
                if (unexpectedError.get()) {
                    return;
                }
                try {
                    future.get(20, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    handleTimeout(null, "", unexpectedError);
                } catch (Throwable e) {
                    // Ignore
                    e.printStackTrace();
                }
            }

            if (unexpectedError.get()) {
                Assert.fail("Unexpected error happens during/after an MDL deadlock.");
            }

        } finally {
            clear(connections, tableName);
            clear(connections, verifiedTableName);
        }
    }

    private boolean enableMdlDetection() {
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show mdl_deadlock_detection");
            while (rs.next()) {
                if (polardbxOneDB.equalsIgnoreCase(rs.getString(1))) {
                    return "enable".equals(rs.getString(2));
                }
            }
        } catch (SQLException e) {
            // ignore
            e.printStackTrace();
        }
        return false;
    }

    private void handleTimeout(Statement stmt, String sql, AtomicBoolean terminated) {
        // Maybe DN does not enable the MDL instruments, and PolarX can not detect MDL deadlock,
        // and hence, a timeout occurs
        if (null != stmt) {
            try {
                stmt.cancel();
            } catch (SQLException e) {
                // Ignore
                e.printStackTrace();
            }
        }
        terminated.set(true);
        logger.warn("executing sql: " + sql + ", causes time out, maybe DN does not "
            + "enable the MDL instruments, and PolarX can not detect MDL deadlock, skip this test");
    }

    private Future executeSqlMdl(ExecutorService threadPool, Connection connection,
                                 AtomicBoolean unexpectedError, String sql, String tableName) {
        return threadPool.submit(() -> {
            Statement stmt = null;
            try {
                Thread.sleep(1000);
                stmt = connection.createStatement();
                stmt.setQueryTimeout(5);
                stmt.execute(sql);
            } catch (SQLTimeoutException e) {
                handleTimeout(stmt, sql, unexpectedError);
            } catch (Throwable e) {
                if (!e.getMessage().contains("Deadlock found when trying to get lock; try restarting transaction")) {
                    unexpectedError.set(true);
                    e.printStackTrace();
                } else {
                    // Deadlock occurs in this connection.
                    // It should be rollbacked already and can execute any sql immediately.
                    try {
                        testAlreadyRollback(connection, tableName);
                    } catch (Throwable e2) {
                        unexpectedError.set(true);
                        e2.printStackTrace();
                    }
                }
            } finally {
                try {
                    JdbcUtil.executeQuerySuccess(connection, "commit");
                } catch (Throwable e) {
                    // Ignore
                    e.printStackTrace();
                }
            }
        });
    }

    private void createTable(String tableName, boolean single) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Create a partition table
        sql = "create table " + tableName + " (id int primary key)" + (single ? "" : " dbpartition by hash(id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void testShowDeadlocks(String tableName, boolean single) throws SQLException {
        final ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW GRANTS");
        boolean allPrivilege = single;
        while (rs.next() && !allPrivilege) {
            final String privilege = rs.getString(1);
            System.out.println(privilege);
            if (StringUtils.startsWithIgnoreCase(privilege, "GRANT ALL PRIVILEGES ON *.*")) {
                allPrivilege = true;
            }
        }
        if (!allPrivilege) {
            return;
        }
        final String sql = "SHOW " + (single ? "LOCAL " : "GLOBAL ") + "DEADLOCKS";
        final ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        boolean flag = false;
        while (rs2.next()) {
            final String deadlock = rs2.getString(2);
            System.out.println(deadlock);
            // MetaDB may cause a deadlock and overwrite the deadlock info of DN0 since we put GMS into DN0.
            // And user could not see deadlock of metaDB, seeing 'No deadlocks detected.' instead.
            flag |= ("No deadlocks detected.".equalsIgnoreCase(deadlock) || StringUtils
                .containsIgnoreCase(deadlock, tableName));
        }
        Assert.assertTrue(flag);
    }
}


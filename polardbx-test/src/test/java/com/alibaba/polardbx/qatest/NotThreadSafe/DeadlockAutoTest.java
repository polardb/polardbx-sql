package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.NotThreadSafe.DeadlockTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public class DeadlockAutoTest extends DDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        JdbcUtil.executeSuccess(tddlConnection, "set global ENABLE_DEADLOCK_DETECTION_80 = true");
    }

    @Test(timeout = 60000)
    public void testAutoRollbackAfterLocalDeadlock() throws SQLException {
        final String tableName = "auto_rollback_after_local_deadlock_test";
        testFramework(tableName, true);
    }

    @Test(timeout = 60000)
    public void testAutoRollbackAfterGlobalDeadlock() throws SQLException {
        if (isMySQL80()) {
            return;
        }
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

    private void createTable(String tableName, boolean single) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Create a partition table
        sql = "create table " + tableName + " (id int primary key)" + (single ? "" :
            " partition by hash(id) partitions 3");
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

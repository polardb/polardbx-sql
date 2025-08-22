package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeadlockComplicatedTest extends DDLBaseNewDBTestCase {
    private static final String tableName = "complicatedtest";
    private static final String dropTable = "drop table if exists " + tableName;
    private static final String createTable = "create table if not exists " + tableName
        + " (id int primary key) partition by range(id) ("
        + "partition p1 values less than (100),"
        + "partition p2 values less than (200)"
        + ")";
    private static final String insert = "insert into " + tableName + " values (10), (20), (110)";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void setUp() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global PRINT_MORE_INFO_FOR_DEADLOCK_DETECTION = true");
    }

    @After
    public void tearDown() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
    }

    @Test(timeout = 60000)
    public void test3Trx() throws InterruptedException {
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(3);
        CountDownLatch finish = new CountDownLatch(3);
        AtomicBoolean deadlockDetected = new AtomicBoolean(false);
        // trx1 hold lock1 in p1, and want to acquire lock3 in p2
        new Thread(() -> {
            try (Connection connection = getPolardbxConnection()) {
                try {
                    connection.setAutoCommit(false);
                    JdbcUtil.executeUpdateSuccess(connection,
                        "select * from " + tableName + " where id = 10 for update");
                    latch.countDown();
                    latch.await();
                    JdbcUtil.executeUpdateSuccess(connection,
                        "select * from " + tableName + " where id = 110 for update");
                } finally {
                    connection.rollback();
                }
            } catch (Throwable t) {
                t.printStackTrace();
                if (!t.getMessage().contains("Deadlock")) {
                    errors.add(t);
                } else {
                    deadlockDetected.set(true);
                }
            } finally {
                finish.countDown();
            }
        }).start();

        // trx2 hold lock2 in p1, and want to acquire lock1 in p1, trx2 is an allow-read trx
        new Thread(() -> {
            try (Connection connection = getPolardbxConnection()) {
                try {
                    JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = allow_read");
                    connection.setAutoCommit(false);
                    JdbcUtil.executeUpdateSuccess(connection,
                        "select * from " + tableName + " where id = 20 for update");
                    latch.countDown();
                    latch.await();
                    JdbcUtil.executeUpdateSuccess(connection,
                        "select * from " + tableName + " where id = 10 for update");
                } finally {
                    connection.rollback();
                }
            } catch (Throwable t) {
                t.printStackTrace();
                if (!t.getMessage().contains("Deadlock")) {
                    errors.add(t);
                } else {
                    deadlockDetected.set(true);
                }
            } finally {
                finish.countDown();
            }
        }).start();

        // trx3 hold lock3 in p2, and want to acquire lock2 in p1
        new Thread(() -> {
            try (Connection connection = getPolardbxConnection()) {
                try {
                    connection.setAutoCommit(false);
                    JdbcUtil.executeUpdateSuccess(connection,
                        "select * from " + tableName + " where id = 110 for update");
                    latch.countDown();
                    latch.await();
                    JdbcUtil.executeUpdateSuccess(connection,
                        "select * from " + tableName + " where id = 20 for update");
                } finally {
                    connection.rollback();
                }
            } catch (Throwable t) {
                t.printStackTrace();
                if (!t.getMessage().contains("Deadlock")) {
                    errors.add(t);
                } else {
                    deadlockDetected.set(true);
                }
            } finally {
                finish.countDown();
            }
        }).start();

        finish.await();

        Assert.assertTrue(errors.isEmpty());
        Assert.assertTrue(deadlockDetected.get());
    }
}

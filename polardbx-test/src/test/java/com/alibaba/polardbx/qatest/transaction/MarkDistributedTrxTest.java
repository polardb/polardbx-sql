package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@NotThreadSafe
public class MarkDistributedTrxTest extends CrudBasedLockTestCase {
    private final String tableName = "MarkDistributedTrxTest";
    private final String createSql = "create table if not exists " + tableName + "("
        + "id int primary key, "
        + "a int)";
    private final String option = "dbpartition by hash(id)";
    private final String dropSql = "drop table if exists " + tableName;
    private final String selectSql = "select * from " + tableName;
    private final Connection mysqlConn = getMysqlConnection();
    private final Connection polarxConn = getPolardbxConnection();
    private final Random random = new SecureRandom();

    @Before
    public void before() throws SQLException, InterruptedException {
        if (isMySQL80()) {
            return;
        }
        JdbcUtil.executeUpdate(tddlConnection, "SET GLOBAL CONN_POOL_XPROTO_SLOW_THRESH = 0");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set global ENABLE_TRX_DEBUG_MODE = true");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set enable_xa_tso = true");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set enable_auto_commit_tso = true");
        JdbcUtil.executeSuccess(polarxConn, "set global innodb_prepare_wait_timeout = 20000");
        JdbcUtil.executeUpdateSuccess(mysqlConn, dropSql);
        JdbcUtil.executeUpdateSuccess(polarxConn, dropSql);
        JdbcUtil.executeUpdateSuccess(mysqlConn, createSql);
        JdbcUtil.executeUpdateSuccess(polarxConn, createSql + option);
        final String insertSql = "insert into " + tableName + " values (0, 0), (1, 1), (2, 2), (3, 3)";
        JdbcUtil.executeUpdateSuccess(mysqlConn, insertSql);
        JdbcUtil.executeUpdateSuccess(polarxConn, insertSql);
        JdbcUtil.waitUntilVariableChanged(polarxConn, "ENABLE_TRX_DEBUG_MODE", "TRUE", 10);
    }

    @After
    public void after() throws SQLException {
        if (isMySQL80()) {
            return;
        }
        JdbcUtil.executeUpdate(tddlConnection, "SET GLOBAL CONN_POOL_XPROTO_SLOW_THRESH = 1000");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set global ENABLE_TRX_DEBUG_MODE = false");
        tddlConnection.setAutoCommit(true);
        polarxConn.setAutoCommit(true);
    }

    @Test
    public void testFailAfterPrimaryCommit() throws Throwable {
        if (isMySQL80()) {
            return;
        }
        AtomicReference<Throwable> t = new AtomicReference<>(null);
        runWithPurgeTrans(2, () -> {
            try {
                String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_AFTER_PRIMARY_COMMIT') */";
                String sql = "update " + tableName + " set a = " + random.nextInt(10000);

                // XA write, but will act as a TSO transaction.
                JdbcUtil.executeUpdateSuccess(polarxConn, "set TRANSACTION_POLICY = XA");

                polarxConn.setAutoCommit(false);
                mysqlConnection.setAutoCommit(false);

                try {
                    executeOnMysqlAndTddl(mysqlConnection, polarxConn, sql, null, true);
                    selectContentSameAssert(hint + selectSql, null, mysqlConnection, polarxConn);
                    printTrxInfo(polarxConn);
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
                try {
                    polarxConn.commit();
                } catch (Exception ex) {
                    // ignore
                }
                mysqlConnection.commit();
                polarxConn.setAutoCommit(true);
                mysqlConnection.setAutoCommit(true);

                String randomHint = "/*" + UUID.randomUUID() + "*/";
                // TSO read should be blocked, and see latest data.
                JdbcUtil.executeUpdateSuccess(polarxConn, "set TRANSACTION_POLICY = TSO");
                selectContentSameAssert(randomHint + selectSql, null, mysqlConnection, polarxConn, true);
                System.out.println("success");
            } catch (Throwable t0) {
                t.set(t0);
            }
        });

        if (null != t.get()) {
            throw t.get();
        }
    }

    @Test
    public void testFailDuringPrimaryCommit() throws Throwable {
        if (isMySQL80()) {
            return;
        }
        AtomicReference<Throwable> t = new AtomicReference<>(null);
        runWithPurgeTrans(2, () -> {
            try {
                String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_DURING_PRIMARY_COMMIT') */";
                String sql = "update " + tableName + " set a = " + random.nextInt(10000);

                // XA write, but will act as a TSO transaction.
                JdbcUtil.executeUpdateSuccess(polarxConn, "set TRANSACTION_POLICY = XA");

                polarxConn.setAutoCommit(false);
                mysqlConnection.setAutoCommit(false);

                try {
                    executeOnMysqlAndTddl(mysqlConnection, polarxConn, sql, null, true);
                    selectContentSameAssert(hint + selectSql, null, mysqlConnection, polarxConn);
                    printTrxInfo(polarxConn);
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
                try {
                    polarxConn.commit();
                } catch (Exception ex) {
                    // ignore
                }
                // trx failed during commit will be rolled back eventually
                mysqlConnection.rollback();
                polarxConn.setAutoCommit(true);
                mysqlConnection.setAutoCommit(true);

                String randomHint = "/*" + UUID.randomUUID() + "*/";
                JdbcUtil.executeUpdateSuccess(polarxConn, "set TRANSACTION_POLICY = TSO");
                selectContentSameAssert(randomHint + selectSql, null, mysqlConnection, polarxConn, true);
            } catch (Throwable t0) {
                t.set(t0);
            }
        });

        if (null != t.get()) {
            throw t.get();
        }
    }

    @Test
    public void testDelayBeforeWriteCommitLog() throws Throwable {
        if (isMySQL80()) {
            return;
        }

        AtomicReference<Throwable> t = new AtomicReference<>(null);
        runWithPurgeTrans(1, () -> {
            try {
                String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='DELAY_BEFORE_WRITE_COMMIT_LOG') */";
                String sql = "update " + tableName + " set a = " + random.nextInt(10000);

                // XA write, but will act as a TSO transaction.
                JdbcUtil.executeUpdateSuccess(polarxConn, "set TRANSACTION_POLICY = XA");

                polarxConn.setAutoCommit(false);
                mysqlConnection.setAutoCommit(false);

                try {
                    executeOnMysqlAndTddl(mysqlConnection, polarxConn, sql, null, true);
                    selectContentSameAssert(hint + selectSql, null, mysqlConnection, polarxConn);
                    printTrxInfo(polarxConn);
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Future f = executor.submit(() -> {
                    try {
                        polarxConn.commit();
                        polarxConn.setAutoCommit(true);
                    } catch (Exception ex) {
                        // ignore
                    }
                });

                mysqlConnection.commit();
                mysqlConnection.setAutoCommit(true);

                long before = System.currentTimeMillis();
                try {
                    // Sleep 1s to make read tso > commit tso.
                    Thread.sleep(1000);
                } catch (Throwable t0) {
                    // ignore
                }
                // Another thread should be blocked when seeing prepared data.
                String randomHint = "/*" + UUID.randomUUID() + "*/";
                JdbcUtil.executeUpdateSuccess(tddlConnection, "set TRANSACTION_POLICY = TSO");
                selectContentSameAssert(randomHint + selectSql, null, mysqlConnection, tddlConnection, true);
                Assert.assertTrue("Should wait at least 5 seconds", System.currentTimeMillis() - 5000 > before);
                f.get();
            } catch (Throwable t0) {
                t.set(t0);
            }
        });

        if (null != t.get()) {
            throw t.get();
        }
    }
}

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@NotThreadSafe
public class TransLogCleanTest extends CrudBasedLockTestCase {
    final private Connection metaDbConn = getMetaConnection();
    final private Connection polarxConn = getPolardbxConnection();
    final private Connection dn0Conn = getMysqlConnection();
    final private String QUERY_METADB = "select status, flag from trx_log_status";
    final private String QUERY_SCHEDULE_ID =
        "select schedule_id from scheduled_jobs where executor_type = 'CLEAN_LOG_TABLE_V2'";
    final private String PURGE_TRANS = "purge trans v2";
    final private String GET_MAX_SEQ = "show global status like 'Innodb_max_sequence'";
    final private String RECOVER_TIMESTAMP_SQL = "SET innodb_snapshot_seq = 18446744073709551611";

    /**
     * This value should be purged.
     */
    final private String INSERT_TRX_LOG_TABLE =
        "insert ignore into mysql.polarx_global_trx_log values (%s, 0, 0)";

    final private String SELECT_TRX_LOG_TABLE = "select * from mysql.polarx_global_trx_log where txid = %s";

    final private String SELECT_TRX_LOG_TABLE_ARCHIVE =
        "select * from mysql.polarx_global_trx_log_archive where txid = %s";

    @Before
    public void before() throws SQLException {
        clear();
        long scheduleId = getScheduleId();
        JdbcUtil.executeUpdateSuccess(tddlConnection, "pause schedule " + scheduleId);
    }

    private void clear() {
        JdbcUtil.executeUpdateSuccess(polarxConn, "SET @fp_clear = true");
    }

    @After
    public void after() throws SQLException {
        runIgnoreAllErrors(() -> {
            long scheduleId = getScheduleId();
            JdbcUtil.executeUpdateSuccess(tddlConnection, "continue schedule " + scheduleId);
            return 0;
        });

        runIgnoreAllErrors(() -> {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRANSACTION_RECOVER_TASK = true");
            return 0;
        });

        runIgnoreAllErrors(() -> {
            JdbcUtil.waitUntilVariableChanged(tddlConnection, "ENABLE_TRANSACTION_RECOVER_TASK", "true", 10);
            return 0;
        });

        runIgnoreAllErrors(() -> {
            clear();
            return 0;
        });

        runIgnoreAllErrors(() -> {
            JdbcUtil.executeUpdateSuccess(polarxConn, PURGE_TRANS);
            return 0;
        });
    }

    @Test
    public void testBasic() throws SQLException {
        // Insert a fake trx log.
        JdbcUtil.executeUpdateSuccess(dn0Conn, String.format(INSERT_TRX_LOG_TABLE, 0));
        long before = getStatus();
        Assert.assertEquals("initial status not 0, but was " + before, 0, before);
        JdbcUtil.executeUpdateSuccess(polarxConn, PURGE_TRANS);
        long after = getStatus();
        Assert.assertEquals("expect status 0, but was " + after, 0, after);
        ResultSet rs = JdbcUtil.executeQuerySuccess(dn0Conn, String.format(SELECT_TRX_LOG_TABLE, 0));
        Assert.assertFalse("expect record 0 purged, but found", rs.next());
    }

    @Test
    public void testFailedPointsInject() throws SQLException {
        final List<String> fps = ImmutableList.of(
            "FP_TRX_LOG_TB_FAILED_BEFORE_CREATE_TMP",
            "FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP",
            "FP_TRX_LOG_TB_FAILED_BEFORE_SWITCH_TABLE",
            "FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE",
            "FP_TRX_LOG_TB_FAILED_BEFORE_DROP_TABLE",
            "FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE"
        );
        for (String fp : fps) {
            System.out.println("Test fp: " + fp);
            JdbcUtil.executeUpdateSuccess(dn0Conn, String.format(INSERT_TRX_LOG_TABLE, 0));
            JdbcUtil.executeUpdateSuccess(polarxConn, String.format("SET @%s = 'true'", fp));
            long before = getStatus();
            Assert.assertEquals("initial status not 0, but was " + before, 0, before);
            JdbcUtil.executeUpdateFailed(polarxConn, PURGE_TRANS, fp);
            clear();
            JdbcUtil.executeUpdateSuccess(polarxConn, PURGE_TRANS);
            long after = getStatus();
            Assert.assertEquals("expect status 0, but was " + after, 0, after);
            ResultSet rs = JdbcUtil.executeQuerySuccess(dn0Conn, String.format(SELECT_TRX_LOG_TABLE, 0));
            Assert.assertFalse("expect record 0 purged, but found", rs.next());
        }
    }

    @Test
    public void testPreparedTrxBlock() throws SQLException, InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRANSACTION_RECOVER_TASK = false");
        JdbcUtil.waitUntilVariableChanged(tddlConnection, "ENABLE_TRANSACTION_RECOVER_TASK", "false", 10);
        long before = getStatus();
        Assert.assertEquals("initial status not 0, but was " + before, 0, before);
        long txid = 100;
        String txidStr = Long.toHexString(txid);
        try (Connection conn1 = getMysqlConnection()) {
            // 1. Mock a prepared trx with a small txid 100.
            JdbcUtil.executeUpdateSuccess(conn1, String.format("xa begin 'drds-%s@xxx'", txidStr));
            JdbcUtil.executeUpdateSuccess(conn1, String.format(INSERT_TRX_LOG_TABLE, 0));
            JdbcUtil.executeUpdateSuccess(conn1, String.format("xa end 'drds-%s@xxx'", txidStr));
            JdbcUtil.executeUpdateSuccess(conn1, String.format("xa prepare 'drds-%s@xxx'", txidStr));

            // 2. Insert txid 101 into log table.
            JdbcUtil.executeUpdateSuccess(dn0Conn, String.format(INSERT_TRX_LOG_TABLE, 101));

            // 3. Purge trans should fail due to mdl lock wait timeout.
            JdbcUtil.executeUpdateFailed(tddlConnection, PURGE_TRANS,
                "Lock wait timeout exceeded; try restarting transaction");

            // 4. Kill connection of prepared trx, make it a dangling trx.
            ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "select connection_id()");
            Assert.assertTrue(rs.next());
            long connId = rs.getLong(1);
            JdbcUtil.executeUpdateSuccess(dn0Conn, "kill " + connId);

            // 5. Purge trans should fail due to max retry exceeds.
            JdbcUtil.executeUpdateFailed(tddlConnection, PURGE_TRANS, "Max retry exceeds",
                "Lock wait timeout exceeded; try restarting transaction");

            // 6. Commit the dangling trx.
            JdbcUtil.executeUpdateSuccess(dn0Conn, String.format("xa commit 'drds-%s@xxx'", txidStr));

            // 7. Now, purge trans should succeed.
            JdbcUtil.executeUpdateSuccess(polarxConn, PURGE_TRANS);
            long after = getStatus();
            Assert.assertEquals("expect status 0, but was " + after, 0, after);
            rs = JdbcUtil.executeQuerySuccess(dn0Conn, String.format(SELECT_TRX_LOG_TABLE, 0));
            Assert.assertFalse("expect record 0 purged, but found", rs.next());
            rs = JdbcUtil.executeQuerySuccess(dn0Conn, String.format(SELECT_TRX_LOG_TABLE, 101));
            Assert.assertFalse("expect record 101 purged, but found", rs.next());
        } finally {
            // Ensure trx is done.
            try {
                JdbcUtil.executeUpdate(dn0Conn, String.format("xa end 'drds-%s@xxx'", txidStr));
            } catch (Throwable t0) {
                // Ignore.
            } finally {
                try {
                    JdbcUtil.executeUpdate(dn0Conn, String.format("xa rollback 'drds-%s@xxx'", txidStr));
                } catch (Throwable t1) {
                    // Ignore.
                }
                JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRANSACTION_RECOVER_TASK = true");
            }
        }
    }

    @Test
    public void testAsyncCommitTrxVisibility() throws SQLException, InterruptedException {
        if (isMySQL80()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRANSACTION_RECOVER_TASK = false");
        Thread.sleep(2000);
        long before = getStatus();
        Assert.assertEquals("initial status not 0, but was " + before, 0, before);
        ResultSet rs = JdbcUtil.executeQuerySuccess(dn0Conn, GET_MAX_SEQ);
        Assert.assertTrue("get max seq failed", rs.next());
        long maxSeq = rs.getLong(2);
        long txid = 200;
        String txidStr = Long.toHexString(txid);
        try (Connection conn1 = getMysqlConnection()) {
            // 1. Mock a prepared trx with a small txid 200.
            JdbcUtil.executeUpdateSuccess(conn1, String.format("xa begin 'drds-%s@xxx'", txidStr));
            JdbcUtil.executeUpdateSuccess(conn1, "set innodb_snapshot_seq = " + maxSeq);
            JdbcUtil.executeUpdateSuccess(conn1, String.format(INSERT_TRX_LOG_TABLE, 0));
            JdbcUtil.executeUpdateSuccess(conn1, "set innodb_commit_seq = " + maxSeq);
            JdbcUtil.executeUpdateSuccess(conn1, String.format("xa end 'drds-%s@xxx'", txidStr));
            JdbcUtil.executeUpdateSuccess(conn1, String.format("xa prepare 'drds-%s@xxx'", txidStr));

            // 2. Insert txid 201 into log table.
            JdbcUtil.executeUpdateSuccess(dn0Conn, String.format(INSERT_TRX_LOG_TABLE, 201));

            // 3. Use recover timestamp to see 0.
            JdbcUtil.executeUpdateSuccess(dn0Conn, RECOVER_TIMESTAMP_SQL);
            rs = JdbcUtil.executeQuerySuccess(dn0Conn, String.format(SELECT_TRX_LOG_TABLE, 0));
            Assert.assertTrue("expect record 0 can be seen, but not found", rs.next());

            // 4. Kill connection of prepared trx, make it a dangling trx.
            rs = JdbcUtil.executeQuerySuccess(conn1, "select connection_id()");
            Assert.assertTrue(rs.next());
            long connId = rs.getLong(1);
            JdbcUtil.executeUpdateSuccess(dn0Conn, "kill " + connId);

            // 5. Purge trans should fail due to max retry exceeds.
            JdbcUtil.executeUpdateFailed(tddlConnection, PURGE_TRANS, "Max retry exceeds",
                "Caused by: Lock wait timeout exceeded; try restarting transaction.");

            // 6. Now, record 0 should be in archive table.
            JdbcUtil.executeUpdateSuccess(dn0Conn, RECOVER_TIMESTAMP_SQL);
            rs = JdbcUtil.executeQuerySuccess(dn0Conn, String.format(SELECT_TRX_LOG_TABLE_ARCHIVE, 0));
            Assert.assertTrue("expect record 0 can be seen in archive table, but not found", rs.next());

            // 7. Commit and purge trans should succeed.
            JdbcUtil.executeUpdateSuccess(dn0Conn, String.format("xa commit 'drds-%s@xxx'", txidStr));
            JdbcUtil.executeUpdateSuccess(polarxConn, PURGE_TRANS);
            long after = getStatus();
            Assert.assertEquals("expect status 0, but was " + after, 0, after);
            rs = JdbcUtil.executeQuerySuccess(dn0Conn, String.format(SELECT_TRX_LOG_TABLE, 0));
            Assert.assertFalse("expect record 0 purged, but found", rs.next());
            rs = JdbcUtil.executeQuerySuccess(dn0Conn, String.format(SELECT_TRX_LOG_TABLE, 101));
            Assert.assertFalse("expect record 101 purged, but found", rs.next());
        } finally {
            // Ensure trx is done.
            try {
                JdbcUtil.executeUpdate(dn0Conn, String.format("xa end 'drds-%s@xxx'", txidStr));
            } catch (Throwable t0) {
                // Ignore.
            } finally {
                try {
                    JdbcUtil.executeUpdate(dn0Conn, String.format("xa rollback 'drds-%s@xxx'", txidStr));
                } catch (Throwable t1) {
                    // Ignore.
                }
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRANSACTION_RECOVER_TASK = true");
        }
    }

    public long getStatus() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(metaDbConn, QUERY_METADB);
        if (rs.next()) {
            return rs.getLong(1);
        }
        Assert.fail("Get status empty.");
        return -1;
    }

    public long getFlag() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(metaDbConn, QUERY_METADB);
        if (rs.next()) {
            return rs.getLong(2);
        }
        Assert.fail("Get flag empty.");
        return -1;
    }

    public long getScheduleId() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(metaDbConn, QUERY_SCHEDULE_ID);
        if (rs.next()) {
            return rs.getLong(1);
        }
        Assert.fail("Get schedule id empty.");
        return -1;
    }
}

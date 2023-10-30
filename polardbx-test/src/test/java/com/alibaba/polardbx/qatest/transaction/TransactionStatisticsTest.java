package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TransactionStatisticsTest extends CrudBasedLockTestCase {
    private final static String dbName = "TransactionStatisticsTestDB";
    private final static String tbName = "TransactionStatisticsTestTB";

    @BeforeClass
    public static void before() throws SQLException {
        try (final Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeSuccess(conn, "DROP DATABASE IF EXISTS " + dbName);
            JdbcUtil.executeSuccess(conn, "CREATE DATABASE " + dbName);
            JdbcUtil.executeSuccess(conn, "USE " + dbName);
            JdbcUtil.executeSuccess(conn, "CREATE TABLE " + tbName + "( id int primary key)dbpartition by hash(id)");
            JdbcUtil.executeSuccess(conn, "INSERT INTO " + tbName + " VALUES (0), (1)");
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        try (final Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeSuccess(conn, "DROP DATABASE IF EXISTS " + dbName);
        }
    }

    @Test
    public void testTSOReadOnly() throws SQLException, InterruptedException {
        final Connection conn0 = getPolardbxConnection(dbName);
        final Connection conn1 = getPolardbxConnection(dbName);
        long before = 0, after = 0, beforeRO = 0, afterRO = 0, beforeRollback = 0, afterRollback = 0;

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                before = rs.getLong("TRANS_COUNT_TSO");
                beforeRO = rs.getLong("TRANS_COUNT_TSO_RO");
                beforeRollback = rs.getLong("ROLLBACK_COUNT");
            }
        }

        JdbcUtil.executeSuccess(conn0, "SET TRANSACTION_POLICY = TSO");
        JdbcUtil.executeSuccess(conn0, "BEGIN");
        JdbcUtil.executeSuccess(conn0, "SELECT * FROM " + tbName);

        // Make it a slow trans.
        Thread.sleep(3000);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                long curSlowTransCount = rs.getLong("CUR_SLOW_TRANS_COUNT");
                long curSlowTransCountRo = rs.getLong("CUR_SLOW_TRANS_COUNT_RO");
                long curSlowTransTimeMaxRo = rs.getLong("CUR_SLOW_TRANS_TIME_MAX_RO");
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT should > 0", curSlowTransCount > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT_RO should > 0", curSlowTransCountRo > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_TIME_MAX_RO should > 3000", curSlowTransTimeMaxRo > 3000);
            }
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn0,
            "SELECT * FROM INFORMATION_SCHEMA.polardbx_trx WHERE schema = '" + dbName + "'")) {
            Assert.assertTrue("Can not find the trx.", rs.next());

            String trxType = rs.getString("TRX_TYPE");
            Assert.assertTrue("Trx type should be TSO, but is " + trxType, "TSO".equalsIgnoreCase(trxType));

            long idleTime = rs.getLong("IDLE_TIME");
            Assert.assertTrue("Idle time should > 3000000 us, but is " + idleTime, idleTime > 3000000);

            long readRows = rs.getLong("READ_ROWS");
            Assert.assertTrue("Read rows should >= 2, but is " + readRows, readRows >= 2);

            Assert.assertFalse("More than 1 trx in this schema.", rs.next());
        }

        JdbcUtil.executeSuccess(conn0, "ROLLBACK");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                after = rs.getLong("TRANS_COUNT_TSO");
                afterRO = rs.getLong("TRANS_COUNT_TSO_RO");
                afterRollback = rs.getLong("ROLLBACK_COUNT");
                Assert.assertTrue("after.TRANS_COUNT_TSO should > before.TRANS_COUNT_TSO", after > before);
                Assert.assertTrue("after.TRANS_COUNT_TSO_RO should > before.TRANS_COUNT_TSO_RO", afterRO > beforeRO);
                Assert.assertTrue("after.ROLLBACK_COUNT should > before.ROLLBACK_COUNT",
                    afterRollback > beforeRollback);
            }
        }

    }

    @Test
    public void testXAReadOnly() throws SQLException, InterruptedException {
        final Connection conn0 = getPolardbxConnection(dbName);
        final Connection conn1 = getPolardbxConnection(dbName);

        long before = 0, after = 0, beforeRO = 0, afterRO = 0, beforeRollback = 0, afterRollback = 0;

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                before = rs.getLong("TRANS_COUNT_XA");
                beforeRO = rs.getLong("TRANS_COUNT_XA_RO");
                beforeRollback = rs.getLong("ROLLBACK_COUNT");
            }
        }

        JdbcUtil.executeSuccess(conn0, "SET TRANSACTION_POLICY = XA");
        JdbcUtil.executeSuccess(conn0, "BEGIN");
        JdbcUtil.executeSuccess(conn0, "SELECT * FROM " + tbName);

        // Make it a slow trans.
        Thread.sleep(3000);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                long curSlowTransCount = rs.getLong("CUR_SLOW_TRANS_COUNT");
                long curSlowTransCountRo = rs.getLong("CUR_SLOW_TRANS_COUNT_RO");
                long curSlowTransTimeMaxRo = rs.getLong("CUR_SLOW_TRANS_TIME_MAX_RO");
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT should > 0", curSlowTransCount > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT_RO should > 0", curSlowTransCountRo > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_TIME_MAX_RO should > 3000", curSlowTransTimeMaxRo > 3000);
            }
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn0,
            "SELECT * FROM INFORMATION_SCHEMA.polardbx_trx WHERE schema = '" + dbName + "'")) {
            Assert.assertTrue("Can not find the trx.", rs.next());

            String trxType = rs.getString("TRX_TYPE");
            Assert.assertTrue("Trx type should be XA, but is " + trxType, "XA".equalsIgnoreCase(trxType));

            long idleTime = rs.getLong("IDLE_TIME");
            Assert.assertTrue("Idle time should > 3000000 us, but is " + idleTime, idleTime > 3000000);

            long readRows = rs.getLong("READ_ROWS");
            Assert.assertTrue("Read rows should >= 2, but is " + readRows, readRows >= 2);

            Assert.assertFalse("More than 1 trx in this schema.", rs.next());
        }

        JdbcUtil.executeSuccess(conn0, "ROLLBACK");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                after = rs.getLong("TRANS_COUNT_XA");
                afterRO = rs.getLong("TRANS_COUNT_XA_RO");
                afterRollback = rs.getLong("ROLLBACK_COUNT");
                Assert.assertTrue("after.TRANS_COUNT_XA should > before.TRANS_COUNT_XA", after > before);
                Assert.assertTrue("after.TRANS_COUNT_XA_RO should > before.TRANS_COUNT_XA_RO", afterRO > beforeRO);
                Assert.assertTrue("after.ROLLBACK_COUNT should > before.ROLLBACK_COUNT",
                    afterRollback > beforeRollback);
            }
        }
    }

    @Test
    public void testTSOReadOnly2() throws SQLException, InterruptedException {
        final Connection conn0 = getPolardbxConnection(dbName);
        final Connection conn1 = getPolardbxConnection(dbName);

        long before = 0, after = 0, beforeRO = 0, afterRO = 0;

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                before = rs.getLong("TRANS_COUNT_XA");
                beforeRO = rs.getLong("TRANS_COUNT_XA_RO");
            }
        }

        JdbcUtil.executeSuccess(conn0, "SET TRANSACTION_POLICY = TSO");
        JdbcUtil.executeSuccess(conn0, "START TRANSACTION READ ONLY");
        JdbcUtil.executeSuccess(conn0, "SELECT * FROM " + tbName);

        // Make it a slow trans.
        Thread.sleep(3000);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                long curSlowTransCount = rs.getLong("CUR_SLOW_TRANS_COUNT");
                long curSlowTransCountRo = rs.getLong("CUR_SLOW_TRANS_COUNT_RO");
                long curSlowTransTimeMaxRo = rs.getLong("CUR_SLOW_TRANS_TIME_MAX_RO");
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT should > 0", curSlowTransCount > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT_RO should > 0", curSlowTransCountRo > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_TIME_MAX_RO should > 3000", curSlowTransTimeMaxRo > 3000);
            }
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn0,
            "SELECT * FROM INFORMATION_SCHEMA.polardbx_trx WHERE schema = '" + dbName + "'")) {
            Assert.assertTrue("Can not find the trx.", rs.next());

            String trxType = rs.getString("TRX_TYPE");
            Assert.assertTrue("Trx type should be TSO_RO, but is " + trxType, "TSO_RO".equalsIgnoreCase(trxType));

            long idleTime = rs.getLong("IDLE_TIME");
            Assert.assertTrue("Idle time should > 3000000 us, but is " + idleTime, idleTime > 3000000);

            long readRows = rs.getLong("READ_ROWS");
            Assert.assertTrue("Read rows should >= 2, but is " + readRows, readRows >= 2);

            Assert.assertFalse("More than 1 trx in this schema.", rs.next());
        }

        JdbcUtil.executeSuccess(conn0, "set autocommit = 1");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                after = rs.getLong("TRANS_COUNT_TSO");
                afterRO = rs.getLong("TRANS_COUNT_TSO_RO");
                Assert.assertTrue("after.TRANS_COUNT_TSO should > before.TRANS_COUNT_TSO", after > before);
                Assert.assertTrue("after.TRANS_COUNT_TSO_RO should > before.TRANS_COUNT_TSO_RO", afterRO > beforeRO);
            }
        }
    }

    @Test
    public void testTSOReadWrite() throws SQLException, InterruptedException {
        final Connection conn0 = getPolardbxConnection(dbName);
        final Connection conn1 = getPolardbxConnection(dbName);

        long before = 0, after = 0, beforeRW = 0, afterRW = 0, beforeCrossGroup = 0, afterCrossGroup = 0;

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                before = rs.getLong("TRANS_COUNT_TSO");
                beforeRW = rs.getLong("TRANS_COUNT_TSO_RW");
                beforeCrossGroup = rs.getLong("TRANS_COUNT_CROSS_GROUP");
            }
        }

        JdbcUtil.executeSuccess(conn0, "SET TRANSACTION_POLICY = TSO");
        JdbcUtil.executeSuccess(conn0, "BEGIN");
        JdbcUtil.executeSuccess(conn0, "INSERT INTO " + tbName + " VALUES (100), (101)");
        JdbcUtil.executeSuccess(conn0, "DELETE FROM " + tbName + " WHERE id in (100, 101)");

        // Make it a slow trans.
        Thread.sleep(3000);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                long curSlowTransCount = rs.getLong("CUR_SLOW_TRANS_COUNT");
                long curSlowTransCountRo = rs.getLong("CUR_SLOW_TRANS_COUNT_RW");
                long curSlowTransTimeMaxRo = rs.getLong("CUR_SLOW_TRANS_TIME_MAX_RW");
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT should > 0", curSlowTransCount > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT_RW should > 0", curSlowTransCountRo > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_TIME_MAX_RW should > 3000", curSlowTransTimeMaxRo > 3000);
            }
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn0,
            "SELECT * FROM INFORMATION_SCHEMA.polardbx_trx WHERE schema = '" + dbName + "'")) {
            Assert.assertTrue("Can not find the trx.", rs.next());

            String trxType = rs.getString("TRX_TYPE");
            Assert.assertTrue("Trx type should be TSO, but is " + trxType, "TSO".equalsIgnoreCase(trxType));

            long idleTime = rs.getLong("IDLE_TIME");
            Assert.assertTrue("Idle time should > 3000000 us, but is " + idleTime, idleTime > 3000000);

            long writeRows = rs.getLong("WRITE_ROWS");
            Assert.assertEquals("Read rows should = 4, but is " + writeRows, 4, writeRows);

            Assert.assertFalse("More than 1 trx in this schema.", rs.next());
        }

        JdbcUtil.executeSuccess(conn0, "COMMIT");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                after = rs.getLong("TRANS_COUNT_TSO");
                afterRW = rs.getLong("TRANS_COUNT_TSO_RW");
                afterCrossGroup = rs.getLong("TRANS_COUNT_CROSS_GROUP");
                Assert.assertTrue("after.TRANS_COUNT_TSO should > before.TRANS_COUNT_TSO", after > before);
                Assert.assertTrue("after.TRANS_COUNT_TSO_RW should > before.TRANS_COUNT_TSO_RW", afterRW > beforeRW);
                Assert.assertTrue("after.TRANS_COUNT_CROSS_GROUP should > before.TRANS_COUNT_CROSS_GROUP",
                    afterCrossGroup > beforeCrossGroup);
            }
        }
    }

    @Test
    public void testXAReadWrite() throws SQLException, InterruptedException {
        final Connection conn0 = getPolardbxConnection(dbName);
        final Connection conn1 = getPolardbxConnection(dbName);

        long before = 0, after = 0, beforeRW = 0, afterRW = 0, beforeCrossGroup = 0, afterCrossGroup = 0;

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                before = rs.getLong("TRANS_COUNT_XA");
                beforeRW = rs.getLong("TRANS_COUNT_XA_RW");
                beforeCrossGroup = rs.getLong("TRANS_COUNT_CROSS_GROUP");
            }
        }

        JdbcUtil.executeSuccess(conn0, "SET TRANSACTION_POLICY = XA");
        JdbcUtil.executeSuccess(conn0, "BEGIN");
        JdbcUtil.executeSuccess(conn0, "INSERT INTO " + tbName + " VALUES (100), (101)");
        JdbcUtil.executeSuccess(conn0, "DELETE FROM " + tbName + " WHERE id in (100, 101)");

        // Make it a slow trans.
        Thread.sleep(3000);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                long curSlowTransCount = rs.getLong("CUR_SLOW_TRANS_COUNT");
                long curSlowTransCountRo = rs.getLong("CUR_SLOW_TRANS_COUNT_RW");
                long curSlowTransTimeMaxRo = rs.getLong("CUR_SLOW_TRANS_TIME_MAX_RW");
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT should > 0", curSlowTransCount > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_COUNT_RW should > 0", curSlowTransCountRo > 0);
                Assert.assertTrue("CUR_SLOW_TRANS_TIME_MAX_RW should > 3000", curSlowTransTimeMaxRo > 3000);
            }
        }

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn0,
            "SELECT * FROM INFORMATION_SCHEMA.polardbx_trx WHERE schema = '" + dbName + "'")) {
            Assert.assertTrue("Can not find the trx.", rs.next());

            String trxType = rs.getString("TRX_TYPE");
            Assert.assertTrue("Trx type should be XA, but is " + trxType, "XA".equalsIgnoreCase(trxType));

            long idleTime = rs.getLong("IDLE_TIME");
            Assert.assertTrue("Idle time should > 3000000 us, but is " + idleTime, idleTime > 3000000);

            long writeRows = rs.getLong("WRITE_ROWS");
            Assert.assertEquals("Read rows should = 4, but is " + writeRows, 4, writeRows);

            Assert.assertFalse("More than 1 trx in this schema.", rs.next());
        }

        JdbcUtil.executeSuccess(conn0, "COMMIT");

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn1, "SHOW TRANS STATS")) {
            while (rs.next()) {
                after = rs.getLong("TRANS_COUNT_XA");
                afterRW = rs.getLong("TRANS_COUNT_XA_RW");
                afterCrossGroup = rs.getLong("TRANS_COUNT_CROSS_GROUP");
                Assert.assertTrue("after.TRANS_COUNT_XA should > before.TRANS_COUNT_XA", after > before);
                Assert.assertTrue("after.TRANS_COUNT_XA_RW should > before.TRANS_COUNT_XA_RW", afterRW > beforeRW);
                Assert.assertTrue("after.TRANS_COUNT_CROSS_GROUP should > before.TRANS_COUNT_CROSS_GROUP",
                    afterCrossGroup > beforeCrossGroup);
            }
        }
    }
}

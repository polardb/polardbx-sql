package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Test for XA_TSO trx.
 */

@NotThreadSafe
public class XATsoTransactionTest extends CrudBasedLockTestCase {

    private static final int MAX_DATA_SIZE = 20;

    private static final String SELECT_FROM = "SELECT pk, varchar_test, integer_test, char_test, blob_test, "
        + "tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bit_test, bigint_test, float_test, "
        + "double_test, decimal_test, date_test, time_test, datetime_test, year_test FROM ";

    public XATsoTransactionTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<Object[]> prepare() throws SQLException {
        List<Object[]> ret = new ArrayList<>();

        for (String[] tables : ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE)) {
            ret.add(new Object[] {tables[0]});
        }
        return ret;
    }

    @Before
    public void initData() throws Exception {
        JdbcUtil.executeUpdate(tddlConnection, "SET GLOBAL CONN_POOL_XPROTO_SLOW_THRESH = 0");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRX_DEBUG_MODE = true");
        String sql = "DELETE FROM  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @After
    public void after() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "SET GLOBAL CONN_POOL_XPROTO_SLOW_THRESH = 1000");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRX_DEBUG_MODE = false");
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

    @Test
    public void testFailAfterPrimaryCommit() throws Exception {
        if (isMySQL80()) {
            return;
        }
        long before = 0, after = 0, beforeCommitError = 0, afterCommitError = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
            if (rs.next()) {
                before = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
            }
        }
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE, TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME,
            mysqlConnection, tddlConnection, columnDataGenerator);
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='SYNC_COMMIT,FAIL_AFTER_PRIMARY_COMMIT') */";
        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_XA_TSO = true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set transaction_policy = XA");

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
            printTrxInfo(tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        try {
            tddlConnection.commit();
        } catch (Exception ex) {
            // ignore
        }
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        // TSO trx should be blocked until XA_CTS trx is recovered and committed, and see the latest data.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set transaction_policy = TSO");
        if (!isMySQL80()) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global innodb_prepare_wait_timeout = 20000");
        }

        sql = SELECT_FROM + baseOneTableName;
        tddlConnection.setAutoCommit(false);
        String randomHint = "/*" + UUID.randomUUID() + "*/";
        selectContentSameAssert(randomHint + sql, null, mysqlConnection, tddlConnection, true);
        tddlConnection.setAutoCommit(true);

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
            if (rs.next()) {
                after = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
            }
        }

        Assert.assertTrue(
            "after.RECOVER_COMMIT_BRANCH_COUNT should > before.RECOVER_COMMIT_BRANCH_COUNT, but before is " + before
                + ", and after is " + after, after > before);
        Assert.assertTrue(
            "after.COMMIT_ERROR_COUNT should > before.COMMIT_ERROR_COUNT, but before is " + beforeCommitError
                + ", and after is " + afterCommitError, afterCommitError > beforeCommitError);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_XA_TSO = false");
    }

    @Test
    public void testRollbackSavepoint() throws SQLException, InterruptedException {
        if (isMySQL80()) {
            return;
        }
        final String tableName = "testRollbackSavepoint_xacts";
        final String dropTable = "drop table if exists " + tableName;
        final String createTable = "create table if not exists " + tableName + "(id int primary key)";
        final String suffix = "dbpartition by hash(id)";
        final String insertSql = "insert into " + tableName + " values (0), (1), (2), (3)";

        JdbcUtil.executeUpdateSuccess(mysqlConnection, dropTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + suffix);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE, TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME,
            mysqlConnection, tddlConnection, columnDataGenerator);
        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_XA_TSO = true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set transaction_policy = XA");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set enable_auto_savepoint = true");

        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='SYNC_COMMIT,FAIL_AFTER_PRIMARY_COMMIT') */";

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        try {
            // Make auto savepoint rollback.
            JdbcUtil.executeUpdateFailed(mysqlConnection, insertSql, "Duplicate entry");
            JdbcUtil.executeUpdateFailed(tddlConnection, insertSql, "Duplicate entry");

            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        try {
            tddlConnection.commit();
        } catch (Throwable t) {
            // ignore
        }
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set transaction_policy = TSO");
        if (!isMySQL80()) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global innodb_prepare_wait_timeout = 20000");
        }

        sql = SELECT_FROM + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testSessionVariable() throws SQLException, InterruptedException {
        if (isMySQL80()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_XA_TSO = true");

        final String tableName = "testSessionVariable_xacts";
        final String dropTable = "drop table if exists " + tableName;
        final String createTable = "create table if not exists " + tableName + "(id int primary key)";
        final String suffix = "dbpartition by hash(id)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + suffix);

        JdbcUtil.waitUntilVariableChanged(tddlConnection, "ENABLE_XA_TSO", "true", 10);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set transaction_policy = XA");

        tddlConnection.setAutoCommit(false);
        try {
            JdbcUtil.executeSuccess(tddlConnection, "select * from " + tableName + " for update");
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select connection_id()");
            Assert.assertTrue("should have one result", rs.next());
            long connectionId = rs.getLong(1);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show trans");
            // Should start a XA_TSO trx.
            while (rs.next()) {
                long connId = rs.getLong("PROCESS_ID");
                if (connId == connectionId) {
                    String type = rs.getString("TYPE");
                    Assert.assertTrue(type.equalsIgnoreCase("XATSO"));
                }
            }
            // commit
            tddlConnection.setAutoCommit(true);

            // Change session variable.
            JdbcUtil.executeSuccess(tddlConnection, "set enable_xa_tso = false");
            tddlConnection.setAutoCommit(false);
            // Now should start a XA trx.
            JdbcUtil.executeSuccess(tddlConnection, "select * from " + tableName + " for update");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show trans");
            while (rs.next()) {
                long connId = rs.getLong("PROCESS_ID");
                if (connId == connectionId) {
                    String type = rs.getString("TYPE");
                    Assert.assertTrue(type.equalsIgnoreCase("XA"));
                }
            }
        } finally {
            tddlConnection.setAutoCommit(true);
        }
    }

}

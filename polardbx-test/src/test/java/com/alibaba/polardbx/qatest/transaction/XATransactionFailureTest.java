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

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Test for default distributed transaction scheme（XA for DRDS, TSO for PolarDB-X)
 */

@NotThreadSafe
public class XATransactionFailureTest extends CrudBasedLockTestCase {

    private static final int MAX_DATA_SIZE = 20;
    private final boolean shareReadView;
    private final String trxPolicy;
    private final String asyncCommit;

    private static final String SELECT_FROM =
        "/*TDDL:enable_mpp=false*/SELECT pk, varchar_test, integer_test, char_test, blob_test, " +
            "tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bit_test, bigint_test, float_test, " +
            "double_test, decimal_test, date_test, time_test, datetime_test, year_test FROM ";

    @Parameters(name = "{index}:table={0},shareReadView={1},trxPolicy={2},asyncCommit={3}")
    public static List<Object[]> prepare() throws SQLException {
        boolean supportShareReadView;
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            supportShareReadView = JdbcUtil.supportShareReadView(connection);
        }
        List<Object[]> ret = new ArrayList<>();
        String[] trxPolicy = {"XA", "TSO", "ARCHIVE"};
        String[] asyncCommit = {/*"TRUE",*/ "FALSE"};
        for (String policy : trxPolicy) {
            for (String ac : asyncCommit) {
                for (String[] tables : ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE)) {
                    ret.add(new Object[] {tables[0], false, policy, ac});
                    if (supportShareReadView) {
                        ret.add(new Object[] {tables[0], true, policy, ac});
                    }
                }
            }
        }
        return ret;
    }

    public XATransactionFailureTest(String baseOneTableName, boolean shareReadView, String trxPolicy,
                                    String asyncCommit) {
        this.baseOneTableName = baseOneTableName;
        this.shareReadView = shareReadView;
        this.trxPolicy = trxPolicy;
        this.asyncCommit = asyncCommit;
    }

    @Before
    public void initData() throws Exception {
        JdbcUtil.executeUpdate(tddlConnection, "SET GLOBAL CONN_POOL_XPROTO_SLOW_THRESH = 0");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRX_DEBUG_MODE = true");
        String sql = "DELETE FROM  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @After
    public void after() {
        JdbcUtil.executeUpdate(tddlConnection, "SET GLOBAL CONN_POOL_XPROTO_SLOW_THRESH = 1000");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_TRX_DEBUG_MODE = false");
    }

    @Test
    public void testFailAfterPrimaryCommit() throws Throwable {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        AtomicReference<Throwable> t = new AtomicReference<>(null);
        runWithPurgeTrans(2, () -> {
            try {
                long before = 0, after = 0, beforeCommitError = 0, afterCommitError = 0;
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
                    if (rs.next()) {
                        before = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                        beforeCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                    }
                }

                String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='SYNC_COMMIT,FAIL_AFTER_PRIMARY_COMMIT') */";
                String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
                List<Object> param = new ArrayList<Object>();
                param.add(columnDataGenerator.integer_testValue);
                param.add(columnDataGenerator.date_testValue);
                param.add(columnDataGenerator.float_testValue);

                JdbcUtil.executeUpdateSuccess(tddlConnection, "set TRANSACTION_POLICY = " + trxPolicy);
                JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_ASYNC_COMMIT = " + asyncCommit);

                tddlConnection.setAutoCommit(false);
                mysqlConnection.setAutoCommit(false);
                JdbcUtil.setShareReadView(shareReadView, tddlConnection);

                try {
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
                    sql = SELECT_FROM + baseOneTableName;
                    selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
                    printTrxInfo(tddlConnection);
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
                boolean exception = false;
                try {
                    tddlConnection.commit();
                } catch (Exception ex) {
                    // ignore
                    exception = true;
                }
                Assert.assertTrue(exception);
                mysqlConnection.commit();
                tddlConnection.setAutoCommit(true);
                mysqlConnection.setAutoCommit(true);
                String randomHint = "/*" + UUID.randomUUID() + "*/";
                sql = randomHint + SELECT_FROM + baseOneTableName + " FOR UPDATE";
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
                    if (rs.next()) {
                        after = rs.getLong("RECOVER_COMMIT_BRANCH_COUNT");
                        afterCommitError = rs.getLong("COMMIT_ERROR_COUNT");
                    }
                }

                Assert.assertTrue(
                    "after.COMMIT_ERROR_COUNT should > before.COMMIT_ERROR_COUNT, but before is "
                        + beforeCommitError + ", and after is " + afterCommitError,
                    afterCommitError > beforeCommitError);
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
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        AtomicReference<Throwable> t = new AtomicReference<>(null);
        runWithPurgeTrans(2, () -> {
            try {

                long before = 0, after = 0;
                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
                    if (rs.next()) {
                        before = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                    }
                }

                String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='SYNC_COMMIT,FAIL_DURING_PRIMARY_COMMIT') */";
                String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
                List<Object> param = new ArrayList<Object>();
                param.add(columnDataGenerator.integer_testValue);
                param.add(columnDataGenerator.date_testValue);
                param.add(columnDataGenerator.float_testValue);

                tddlConnection.setAutoCommit(false);
                mysqlConnection.setAutoCommit(false);
                JdbcUtil.setShareReadView(shareReadView, tddlConnection);

                try {
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
                    sql = SELECT_FROM + baseOneTableName;
                    selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
                    printTrxInfo(tddlConnection);
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
                boolean exception = false;
                try {
                    tddlConnection.commit();
                } catch (Exception ex) {
                    // ignore
                    exception = true;
                }
                Assert.assertTrue(exception);
                mysqlConnection.rollback(); // expect data to be rollbacked
                tddlConnection.setAutoCommit(true);
                mysqlConnection.setAutoCommit(true);

                String randomHint = "/*" + UUID.randomUUID() + "*/";
                sql = randomHint + SELECT_FROM + baseOneTableName + " FOR UPDATE";
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

                try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW TRANS STATS")) {
                    if (rs.next()) {
                        after = rs.getLong("RECOVER_ROLLBACK_BRANCH_COUNT");
                    }
                }

                Assert.assertTrue(
                    "after.RECOVER_ROLLBACK_BRANCH_COUNT should > before.RECOVER_ROLLBACK_BRANCH_COUNT, but before is "
                        + before + ", and after is " + after,
                    after > before);
            } catch (Throwable t0) {
                t.set(t0);
            }
        });

        if (null != t.get()) {
            throw t.get();
        }
    }

    @Test
    public void testFailBeforePrimaryCommit() throws Throwable {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        AtomicReference<Throwable> t = new AtomicReference<>(null);
        runWithPurgeTrans(2, () -> {
            try {
                String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='SYNC_COMMIT,FAIL_BEFORE_PRIMARY_COMMIT') */";
                String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
                List<Object> param = new ArrayList<Object>();
                param.add(columnDataGenerator.integer_testValue);
                param.add(columnDataGenerator.date_testValue);
                param.add(columnDataGenerator.float_testValue);

                JdbcUtil.executeUpdateSuccess(tddlConnection, "set TRANSACTION_POLICY = " + trxPolicy);
                JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_ASYNC_COMMIT = " + asyncCommit);

                tddlConnection.setAutoCommit(false);
                mysqlConnection.setAutoCommit(false);
                JdbcUtil.setShareReadView(shareReadView, tddlConnection);

                try {
                    executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
                    sql = SELECT_FROM + baseOneTableName;
                    selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
                    printTrxInfo(tddlConnection);
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
                boolean exception = false;
                try {
                    tddlConnection.commit();
                } catch (Exception ex) {
                    // ignore
                    exception = true;
                }
                Assert.assertTrue(exception);
                mysqlConnection.rollback(); // expect data to be rollbacked
                tddlConnection.setAutoCommit(true);
                mysqlConnection.setAutoCommit(true);

                String randomHint = "/*" + UUID.randomUUID() + "*/";
                sql = randomHint + SELECT_FROM + baseOneTableName;
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
            } catch (Throwable t0) {
                t.set(t0);
            }
        });

        if (null != t.get()) {
            throw t.get();
        }
    }

    @Test
    public void testDelayBeforeWriteCommitLog() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='DELAY_BEFORE_WRITE_COMMIT_LOG') */";
        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        JdbcUtil.setShareReadView(shareReadView, tddlConnection);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
            printTrxInfo(tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        boolean exception = false;
        try {
            tddlConnection.commit();
        } catch (Exception ex) {
            // ignore
            exception = true;
        }
        // Should not cause commit fail in this case!
        Assert.assertFalse(exception);
        mysqlConnection.commit(); // expect data to be committed
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        String randomHint = "/*" + UUID.randomUUID() + "*/";
        sql = randomHint + SELECT_FROM + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }
}

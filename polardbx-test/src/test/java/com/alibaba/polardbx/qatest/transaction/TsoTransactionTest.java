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
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class TsoTransactionTest extends CrudBasedLockTestCase {
    /**
     * only works for RR isolation level
     */
    private final boolean shareReadView;

    private final String asyncCommit;

    @Parameterized.Parameters(name = "{index}:table={0},shareReadView={1},asyncCommit={2}")
    public static List<Object[]> prepare() throws SQLException {
        boolean supportShareReadView;
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            supportShareReadView = JdbcUtil.supportShareReadView(connection);
        }
        String[] asyncCommit = {/*"TRUE",*/ "FALSE"};
        List<Object[]> ret = new ArrayList<>();
        for (String ac : asyncCommit) {
            for (String[] tables : ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE)) {
                ret.add(new Object[] {tables[0], false, ac});
                if (supportShareReadView) {
                    ret.add(new Object[] {tables[0], true, ac});
                }
            }
        }
        return ret;
    }

    public TsoTransactionTest(String baseOneTableName, boolean shareReadView, String asyncCommit) {
        this.baseOneTableName = baseOneTableName;
        this.shareReadView = shareReadView;
        this.asyncCommit = asyncCommit;
    }

    @Before
    public void before() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_ASYNC_COMMIT = " + asyncCommit);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + baseOneTableName, null);
        final String sql = "insert into " + baseOneTableName + "(pk, integer_test, varchar_test) values(?, ?, ?)";
        final List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            params.add(ImmutableList.of(i, i, "test" + i));
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params, true);
    }

    private void setShareReadView(Connection conn) throws SQLException {
        JdbcUtil.setShareReadView(shareReadView, conn);
    }

    @Test
    public void checkAutoCommitUsingTSO() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO_READONLY)*/ SELECT * FROM " + baseOneTableName);
        }
    }

    @Test
    public void checkTransUsingTSO() throws Exception {
        tddlConnection.setAutoCommit(false);
        setShareReadView(tddlConnection);

        try (Statement stmt = tddlConnection.createStatement()) {
            String varchar_test = "12";
            stmt.execute(" SELECT count(*) FROM " + baseOneTableName);
            stmt.execute(" UPDATE " + baseOneTableName + " set varchar_test = " + varchar_test + " where pk=1");
            stmt.execute(" SELECT count(*) FROM " + baseOneTableName);
            stmt.execute(" UPDATE " + baseOneTableName + " set varchar_test = " + varchar_test + " where pk=1");
            stmt.execute(" SELECT count(*) FROM " + baseOneTableName);
        }
        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);
    }

    @Test
    public void checkTransUsingTSO2() throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        setShareReadView(tddlConnection);

        List<Object> params = Lists.newArrayList("12");
        List<List<Object>> results;
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(0)), results.get(0).get(0));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, " UPDATE " + baseOneTableName
            + " set varchar_test = ? where pk = 1", params);
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(1)), results.get(0).get(0));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, " UPDATE " + baseOneTableName
            + " set varchar_test = ? where pk = 2", params);
        // 重复查询两次 避免连接复用状态问题
        selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
            mysqlConnection, tddlConnection);
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(2)), results.get(0).get(0));
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

    @Test
    public void checkTransUsingTSOWithRollback() throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        setShareReadView(tddlConnection);

        List<Object> params = Lists.newArrayList("12");
        List<List<Object>> results;
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(0)), results.get(0).get(0));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, " UPDATE " + baseOneTableName
            + " set varchar_test = ? where pk = 1", params);
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(1)), results.get(0).get(0));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, " UPDATE " + baseOneTableName
            + " set varchar_test = ? where pk = 2", params);
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(2)), results.get(0).get(0));
        tddlConnection.rollback();
        mysqlConnection.rollback();
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(0)), results.get(0).get(0));
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

    @Test
    public void checkMultiStatement() throws Exception {
        Connection connection = null;
        try {
            connection = getPolardbxDirectConnection();
            JdbcUtil.executeUpdateSuccess(connection,
                MessageFormat.format("update {0} set integer_test = 123 where pk > 0 ", baseOneTableName));

            connection.setAutoCommit(false);
            JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='READ-COMMITTED'");
            JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
            final String multiStatement = MessageFormat.format(
                "set autocommit = 1 ; update {0} set integer_test = 456 where pk > 0 ; select integer_test from {0} where pk > 0",
                baseOneTableName);
            JdbcUtil.executeUpdateSuccess(connection, multiStatement);
            JdbcUtil.executeUpdate(connection, "ROLLBACK");

            final ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
                MessageFormat.format("select integer_test from {0} where pk > 0", baseOneTableName));

            while (rs.next()) {
                final long integerTest = rs.getLong(1);
                Assert.assertEquals(multiStatement, 456L, integerTest);
            }

        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void checkIsolationLevel() throws Throwable {

        final List<Throwable> errors = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();

        final Semaphore read0Semaphore = new Semaphore(0);
        final Semaphore read1Semaphore = new Semaphore(0);
        final Semaphore write0Semaphore = new Semaphore(1);
        final Semaphore write1Semaphore = new Semaphore(1);
        final AtomicBoolean stopped = new AtomicBoolean(false);

        final long initValue = 0L;
        final AtomicLong value = new AtomicLong(initValue);

        Thread thdWrite = new Thread(() -> {
            Connection connection = null;
            try {
                connection = getPolardbxDirectConnection();
                connection.setAutoCommit(false);

                for (int j = 0; j < 50 && !stopped.get(); j++) {
                    write0Semaphore.acquire();
                    write1Semaphore.acquire();

                    JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='READ-COMMITTED'");
                    JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
                    JdbcUtil.executeUpdateSuccess(connection,
                        "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO)*/ update " + baseOneTableName
                            + " set integer_test = " + value.incrementAndGet()
                            + " where pk = 1 ");
                    JdbcUtil.executeUpdate(connection, "COMMIT");

                    read0Semaphore.release();
                    read1Semaphore.release();
                }

            } catch (Exception | AssertionError ae) {
                errors.add(ae);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                stopped.set(true);
                read0Semaphore.release();
                read1Semaphore.release();
            }
        });
        thdWrite.start();
        threads.add(thdWrite);

        Thread thdRead0 = new Thread(() -> {
            Connection connection = null;
            try {
                connection = getPolardbxDirectConnection();
                connection.setAutoCommit(false);

                JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='READ-COMMITTED'");
                JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
                for (int j = 0; j < 50 && !stopped.get(); j++) {
                    read0Semaphore.acquire();

                    final ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
                        "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO)*/ select integer_test from " + baseOneTableName
                            + " where pk = 1");
                    Assert.assertTrue(rs.next());
                    final long valueFromDb = rs.getLong(1);

                    Assert.assertEquals(valueFromDb, value.get());

                    write0Semaphore.release();
                }

            } catch (Exception | AssertionError ae) {
                errors.add(ae);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                stopped.set(true);
                write0Semaphore.release();
            }
        });
        thdRead0.start();
        threads.add(thdRead0);

        Thread thdRead1 = new Thread(() -> {
            Connection connection = null;
            try {
                read1Semaphore.acquire();
                connection = getPolardbxDirectConnection();
                connection.setAutoCommit(false);

                JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='REPEATABLE-READ'");
                JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
                for (int j = 0; j < 50 && !stopped.get(); j++) {

                    final ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
                        "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO)*/ select integer_test from " + baseOneTableName
                            + " where pk = 1");
                    Assert.assertTrue(rs.next());
                    final long valueFromDb = rs.getLong(1);

                    Assert.assertEquals(initValue + 1, valueFromDb);

                    write1Semaphore.release();
                    read1Semaphore.acquire();
                }

            } catch (Exception | AssertionError ae) {
                errors.add(ae);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                stopped.set(true);
                write1Semaphore.release();
            }
        });
        thdRead1.start();
        threads.add(thdRead1);

        for (Thread thd : threads) {
            thd.join();
        }

        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
    }

    private static final AtomicInteger log_seq = new AtomicInteger(0);

    private static void log(String role, String msg) {
//        synchronized (log_seq) {
//            System.out.println(log_seq.getAndIncrement() + " [" + role + "] " + msg);
//        }
    }

    @Test
    public void checkIsolationLevelForReadOnlyTrx() throws Throwable {

        final List<Throwable> errors = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();

        final Semaphore readSemaphore = new Semaphore(0);
        final Semaphore writeSemaphore = new Semaphore(1);
        final AtomicBoolean stopped = new AtomicBoolean(false);

        final long initValue = 0L;
        final AtomicLong value = new AtomicLong(initValue);

        Thread thdWrite = new Thread(() -> {
            Connection connection = null;
            try {
                connection = getPolardbxDirectConnection();
                connection.setAutoCommit(false);

                log("writer", "autocommit = 0");

                for (int j = 0; j < 50 && !stopped.get(); j++) {
                    writeSemaphore.acquire();

                    JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='READ-COMMITTED'");
                    log("writer", "set isolation level");
                    JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
                    log("writer", "set transaction policy");
                    JdbcUtil.executeUpdateSuccess(connection,
                        "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO)*/ update " + baseOneTableName
                            + " set integer_test = " + value.incrementAndGet()
                            + " where pk = 1 ");
                    log("writer", "update");
                    JdbcUtil.executeUpdate(connection, "COMMIT");
                    log("writer", "commit");

                    readSemaphore.release();
                }

            } catch (Exception | AssertionError ae) {
                errors.add(ae);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                stopped.set(true);
                readSemaphore.release();
            }
        });
        thdWrite.start();
        threads.add(thdWrite);

        Thread thdRead1 = new Thread(() -> {
            Connection connection = null;
            try {
                readSemaphore.acquire();
                connection = getPolardbxDirectConnection();

                JdbcUtil.executeUpdateSuccess(connection, "START TRANSACTION READ ONLY");
                log("reader", "start transaction");
                JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='REPEATABLE-READ'");
                log("reader", "set isolation level");
                JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
                log("reader", "set transaction policy");
                for (int j = 0; j < 50 && !stopped.get(); j++) {

                    final ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
                        "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO_READONLY)*/ select integer_test from "
                            + baseOneTableName
                            + " where pk = 1");

                    log("reader", "select");
                    Assert.assertTrue(rs.next());
                    final long valueFromDb = rs.getLong(1);

                    Assert.assertEquals(initValue + 1, valueFromDb);

                    writeSemaphore.release();
                    readSemaphore.acquire();
                }

            } catch (Exception | AssertionError ae) {
                errors.add(ae);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                stopped.set(true);
                writeSemaphore.release();
            }
        });
        thdRead1.start();
        threads.add(thdRead1);

        for (Thread thd : threads) {
            thd.join();
        }

        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
    }

    @Test
    public void checkIsolationLevelForReadOnlyTrx1() throws Throwable {

        final List<Throwable> errors = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();

        final Semaphore read0Semaphore = new Semaphore(0);
        final Semaphore read1Semaphore = new Semaphore(0);
        final Semaphore write0Semaphore = new Semaphore(1);
        final Semaphore write1Semaphore = new Semaphore(1);
        final AtomicBoolean stopped = new AtomicBoolean(false);

        final long initValue = 0L;
        final AtomicLong value = new AtomicLong(initValue);

        Thread thdWrite = new Thread(() -> {
            Connection connection = null;
            try {
                connection = getPolardbxDirectConnection();
                connection.setAutoCommit(false);

                for (int j = 0; j < 2 && !stopped.get(); j++) {
                    write0Semaphore.acquire();
                    write1Semaphore.acquire();

                    JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='READ-COMMITTED'");
                    JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
                    JdbcUtil.executeUpdateSuccess(connection,
                        "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO)*/ update " + baseOneTableName
                            + " set integer_test = " + value.incrementAndGet()
                            + " where pk = 1 ");
                    JdbcUtil.executeUpdate(connection, "COMMIT");

                    read0Semaphore.release();
                    read1Semaphore.release();
                }

            } catch (Exception | AssertionError ae) {
                errors.add(ae);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                stopped.set(true);
                read0Semaphore.release();
                read1Semaphore.release();
            }
        });
        thdWrite.start();
        threads.add(thdWrite);

        Thread thdRead0 = new Thread(() -> {
            Connection connection = null;
            try {
                connection = getPolardbxDirectConnection();
                connection.setAutoCommit(false);

                JdbcUtil.executeUpdateSuccess(connection, "START TRANSACTION READ ONLY");
                JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='READ-COMMITTED'");
                JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
                for (int j = 0; j < 50 && !stopped.get(); j++) {
                    read0Semaphore.acquire();

                    final ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
                        "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO_READONLY)*/ select integer_test from "
                            + baseOneTableName
                            + " where pk = 1");
                    Assert.assertTrue(rs.next());
                    final long valueFromDb = rs.getLong(1);

                    Assert.assertEquals(valueFromDb, value.get());

                    write0Semaphore.release();
                }

            } catch (Exception | AssertionError ae) {
                errors.add(ae);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                stopped.set(true);
                write0Semaphore.release();
            }
        });
        thdRead0.start();
        threads.add(thdRead0);

        Thread thdRead1 = new Thread(() -> {
            Connection connection = null;
            try {
                read1Semaphore.acquire();
                connection = getPolardbxDirectConnection();

                JdbcUtil.executeUpdateSuccess(connection, "START TRANSACTION READ ONLY");
                JdbcUtil.executeUpdateSuccess(connection, "SET TX_ISOLATION='REPEATABLE-READ'");
                JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");
                for (int j = 0; j < 50 && !stopped.get(); j++) {

                    final ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
                        "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO_READONLY)*/ select integer_test from "
                            + baseOneTableName
                            + " where pk = 1");
                    Assert.assertTrue(rs.next());
                    final long valueFromDb = rs.getLong(1);

                    Assert.assertEquals(initValue + 1, valueFromDb);

                    write1Semaphore.release();
                    read1Semaphore.acquire();
                }

            } catch (Exception | AssertionError ae) {
                errors.add(ae);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                stopped.set(true);
                write1Semaphore.release();
            }
        });
        thdRead1.start();
        threads.add(thdRead1);

        for (Thread thd : threads) {
            thd.join();
        }

        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
    }

    @Test
    public void checkReadOnlyTrx() throws Throwable {

        Connection connection = null;
        try {
            connection = getPolardbxDirectConnection();

            connection.setAutoCommit(false);
            setShareReadView(connection);
            JdbcUtil.executeUpdateSuccess(connection, "START TRANSACTION READ ONLY");
            JdbcUtil.executeUpdateSuccess(connection, "SET DRDS_TRANSACTION_POLICY='TSO'");

            // Test read
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
                "/*+TDDL:CMD_EXTRA(TRX_CLASS_REQUIRED=TSO_READONLY)*/ select count(1) from "
                    + baseOneTableName);
            Assert.assertTrue(rs.next());
            final int rowCount = rs.getInt(1);
            rs.close();

            // Test insert
            String sql =
                "insert into " + baseOneTableName + "(pk, integer_test, varchar_test) values(100, 100, 'xyz')";
            JdbcUtil.executeUpdateFailed(connection, sql, "Cannot execute statement in a READ ONLY transaction.",
                "Connection is read-only.");

            // Check row count
            rs = JdbcUtil.executeQuerySuccess(connection, "select count(1) from " + baseOneTableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rowCount, rs.getInt(1));
            rs.close();

            // Test delete
            sql = "delete from " + baseOneTableName + " where pk >= 10";
            JdbcUtil.executeUpdateFailed(connection, sql, "Cannot execute statement in a READ ONLY transaction.",
                "Connection is read-only.");

            // Check row count
            rs = JdbcUtil.executeQuerySuccess(connection, "select count(1) from " + baseOneTableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rowCount, rs.getInt(1));
            rs.close();

            // Test update
            sql = "update " + baseOneTableName + " set integer_test = integer_test + 1 where integer_test >= 10";
            JdbcUtil.executeUpdateFailed(connection, sql, "Cannot execute statement in a READ ONLY transaction.",
                "Connection is read-only.");

        } finally {
            if (connection != null) {
                try {
                    connection.rollback();
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

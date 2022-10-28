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

package com.alibaba.polardbx.qatest.dml.sharding.gsi;

import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.GSI_DML_TEST;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;

/**
 * Modifying gsi table with transactions.
 *
 * @author minggong
 */

@Ignore

public class TransactionTest extends GsiDMLTest {

    private static Map<String, String> tddlTables = new HashMap<>();
    private static Map<String, String> shadowTables = new HashMap<>();
    private static Map<String, String> mysqlTables = new HashMap<>();

    @BeforeClass
    public static void beforeCreateTables() {
        try {
            concurrentCreateNewTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void afterDropTables() {

        try {
            concurrentDropTables(tddlTables, shadowTables, mysqlTables);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Parameterized.Parameters(name = "{index}:hint={0} table={1}")
    public static List<String[]> prepareData() {
        List<String[]> rets = Arrays.asList(new String[][] {
            {"", ExecuteTableName.GSI_DML_TEST + "no_unique_one_index_base"},
            {HINT_STRESS_FLAG, ExecuteTableName.GSI_DML_TEST + "no_unique_one_index_base"}
        });
        return prepareNewTableNames(rets, tddlTables, shadowTables, mysqlTables);
    }

    protected static List<String[]> prepareNewTableNames(List<String[]> inputs, Map<String, String> tddlTables,
                                                         Map<String, String> shadowTddlTables,
                                                         Map<String, String> mysqlTables) {
        List<String[]> rets = new ArrayList<>();
        try (Connection tddlConn = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Connection mysqlConn = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(tddlConn, polardbXShardingDBName1());
            JdbcUtil.useDb(mysqlConn, mysqlDBName1());
            for (String[] strings : inputs) {
                Preconditions.checkArgument(strings.length == 2);
                final String replaceFlag = randomTableName(GSI_DML_TEST, 6);
                String[] ret = new String[2];
                String hint = strings[0];
                String table1 =
                    prepareNewTableDefine(tddlConn, mysqlConn, strings[1], replaceFlag, tddlTables, shadowTddlTables,
                        mysqlTables, hint != null && hint.contains(HINT_STRESS_FLAG));
                ret[0] = hint;
                ret[1] = table1;
                rets.add(ret);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        return rets;
    }

    public TransactionTest(String hint, String baseOneTableName) throws Exception {
        super(hint, baseOneTableName);
    }

    /**
     * use free transaction
     */
    @Test
    public void startOtherTransactionTest() throws Exception {
        tddlConnection.setAutoCommit(false);
        try {
            String startSql = "set drds_transaction_policy='free'";
            JdbcUtil.executeUpdateSuccess(tddlConnection, startSql);

            String sql = hint + "update " + baseOneTableName + " set float_test=0 where pk=1";
            executeErrorAssert(tddlConnection,
                sql,
                Lists.newArrayList(),
                "ERR_GLOBAL_SECONDARY_INDEX_ONLY_SUPPORT_XA");
        } finally {
            tddlConnection.setAutoCommit(true);
        }
    }

    /**
     * It really start a XA?
     */
    @Test
    public void checkXATransactionStarted() throws Exception {
        final String sql = hint + "/*+TDDL:CMD_EXTRA(DISTRIBUTED_TRX_REQUIRED=TRUE)*/ update " + baseOneTableName
            + " set float_test=0 where pk in (1,2);";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    /**
     * Does XA really started if we specify TRANSACTION_POLICY=XA when default transaction policy is TSO?
     */
    @Test
    public void checkXATransactionUsed() throws Exception {
        final String sql = hint
            + "/*+TDDL:CMD_EXTRA(DISTRIBUTED_TRX_REQUIRED=TRUE, TRANSACTION_POLICY=XA, TRX_CLASS_REQUIRED=XA)*/ update "
            + baseOneTableName
            + " set float_test=0 where pk in (1,2);";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    /**
     * The transaction will rollback automatically when error occurs
     */
    @Test
    public void rollbackTest() throws Exception {
        // Sharding keys of base table are different, but sharding keys of
        // secondary table are the same, to create conflict.
        String sql = String.format(hint + "insert %s(pk, integer_test, bigint_test) value(?, ?, ?)", baseOneTableName);

        List<Object> params = Lists.newArrayList(0, 0, 0);
        JdbcUtil.updateData(tddlConnection, sql, params);

        params = Lists.newArrayList(0, 1, 0);
        executeErrorAssert(tddlConnection, sql, params, "Duplicate");

        sql = hint + "select count(1) from " + baseOneTableName;
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        Assert.assertTrue(rs.getLong(1) == 1);
    }

    /**
     * Forbid continue to executeSuccess any sql after writing gsi failed.
     * Since auto-savepoint is supported, transaction can still continue after writing GSI failed.
     * So we ignore this case.
     */
    @Ignore
    public void forbidContinueTest() throws Exception {
        tddlConnection.setAutoCommit(false);

        try {
            String startSql = "set drds_transaction_policy='XA'";
            JdbcUtil.executeUpdateSuccess(tddlConnection, startSql);

            String sql =
                String.format(hint + "insert %s(pk, integer_test, bigint_test) value(?, ?, ?)", baseOneTableName);

            List<Object> params = Lists.newArrayList(0, 0, 0);
            JdbcUtil.updateData(tddlConnection, sql, params);

            params = Lists.newArrayList(0, 1, 0);
            executeErrorAssert(tddlConnection, sql, params, "Duplicate");

            // Can't executeSuccess any further sql
            executeErrorAssert(tddlConnection,
                sql,
                params,
                "ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL");
        } finally {
            tddlConnection.rollback();
            tddlConnection.setAutoCommit(true);
        }
    }

    /**
     * Forbid commit transaction after writing gsi failed
     * Since auto-savepoint is supported, transaction can still continue after writing GSI failed.
     * So we ignore this case.
     */
    @Ignore
    public void forbidCommitTest() throws Exception {
        tddlConnection.setAutoCommit(false);

        try {
            String startSql = "set drds_transaction_policy='XA'";
            JdbcUtil.executeUpdateSuccess(tddlConnection, startSql);

            String sql =
                String.format(hint + "insert %s(pk, integer_test, bigint_test) value(?, ?, ?)", baseOneTableName);

            List<Object> params = Lists.newArrayList(0, 0, 0);
            JdbcUtil.updateData(tddlConnection, sql, params);

            params = Lists.newArrayList(0, 1, 0);
            executeErrorAssert(tddlConnection, sql, params, "Duplicate");

            // Can't executeSuccess any further sql
            sql = "commit";
            params = Lists.newArrayList();
            executeErrorAssert(tddlConnection,
                sql,
                params,
                "ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL");
        } finally {
            tddlConnection.rollback();
            tddlConnection.setAutoCommit(true);
        }
    }

    private static final AtomicInteger SEQ = new AtomicInteger(0);
    private final ExecutorService selectForUpdatePool = Executors.newFixedThreadPool(2);

    private static void print(String msg) {
//        System.out.println(MessageFormat.format("[{0}] {1}", SEQ.getAndIncrement(), msg));
    }

    private class SelectForUpdateRunner implements Runnable {

        private final String tableName;
        public final Semaphore holdingSLock = new Semaphore(0);
        public final Semaphore readyToQuit = new Semaphore(0);
        public final Semaphore finishQuit = new Semaphore(0);

        private SelectForUpdateRunner(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public void run() {
            final Connection conn = TransactionTest.this.getPolardbxDirectConnection();
            try {
                conn.setAutoCommit(false);

                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy='XA'");
                JdbcUtil.executeUpdateSuccess(conn,
                    hint + "insert into " + tableName + " (pk, integer_test, bigint_test) values(102, 1, 11)");
                JdbcUtil.executeUpdateSuccess(conn,
                    hint + "select * from " + tableName + " where pk = 101 and integer_test = 1 lock in share mode");
                print("Runner holding S lock");
                holdingSLock.release();
                print("Runner release semaphore");
                JdbcUtil.executeUpdateSuccess(conn,
                    hint + "replace into " + tableName + "(pk, integer_test, bigint_test) values(101, 1, 11)");
                print("Runner finish replace");

                readyToQuit.acquire();
                print("Runner start to rollback");

                JdbcUtil.executeUpdateSuccess(conn, "rollback");
                print("Runner quit");
                finishQuit.release();
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException("Select for update failed!", e);
            } finally {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    /**
     * Forbid commit transaction after writing gsi failed
     */
    @Test
    @Ignore
    public void forbidCommitTest1() throws Exception {
        // Init data
        String sql =
            String.format(hint + "insert %s(pk, integer_test, bigint_test) value(?, ?, ?)", baseOneTableName);

        List<Object> params = Lists.newArrayList(101, 1, 11);
        JdbcUtil.updateData(tddlConnection, sql, params);

        tddlConnection.setAutoCommit(false);

        SelectForUpdateRunner runner = null;
        Throwable ex = null;
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set drds_transaction_policy='XA'");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                hint + "insert into " + baseOneTableName + " (pk, integer_test, bigint_test) values(103, 1, 11)");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                hint + "select * from " + baseOneTableName + " where pk = 101 and integer_test = 1 lock in share mode");
            print("Primary holding S lock");

            runner = new SelectForUpdateRunner(baseOneTableName);
            selectForUpdatePool.submit(runner);
            print("Primary waiting semaphore");
            runner.holdingSLock.acquire();
            print("Primary got semaphore");

            JdbcUtil.executeUpdateFailed(tddlConnection,
                hint + "/*+TDDL:CMD_EXTRA(SOCKET_TIMEOUT=0)*/select * from " + baseOneTableName
                    + " where pk = 101 and integer_test = 1 for update",
                "Deadlock found when trying to get lock; try restarting transaction");

            print("Primary encounter deadlock error");

            // Can't executeSuccess any further sql
            executeErrorAssert(tddlConnection, "commit", null, "ERR_TRANS_DEADLOCK");
            print("Primary commit failed");

            print("Primary quit");
            runner.readyToQuit.release();
            runner.finishQuit.acquire();
            tddlConnection.rollback();
        } catch (Throwable e) {
            ex = e;
            e.printStackTrace();
            throw e;
        } finally {
            if (runner != null && ex != null) {
                runner.readyToQuit.release();
                runner.finishQuit.acquire();
                tddlConnection.rollback();
            }

            tddlConnection.setAutoCommit(true);
        }

        assertIndexSame(baseOneTableName);
    }
}

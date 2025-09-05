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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_BIGINT_64;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_32;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_32_UN;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MEDIUMINT_24;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;

/**
 * @version 1.0
 */
@NotThreadSafe
public class GsiCheckTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "gsi_check_primary";
    private static final String INDEX_TABLE_NAME = "g_i_check";
    private static final String FULL_TYPE_TABLE = ExecuteTableSelect
        .getFullTypeTableDef(PRIMARY_TABLE_NAME, DEFAULT_PARTITIONING_DEFINITION);
    private static final String FULL_TYPE_TABLE_MYSQL = ExecuteTableSelect
        .getFullTypeTableDef(PRIMARY_TABLE_NAME, "");
    private static final String FULL_TYPE_MULTI_PK_TABLE = ExecuteTableSelect
        .getFullTypeMultiPkTableDef(PRIMARY_TABLE_NAME, DEFAULT_PARTITIONING_DEFINITION);
    private static final String FULL_TYPE_MULTI_PK_TABLE_MYSQL = ExecuteTableSelect
        .getFullTypeMultiPkTableDef(PRIMARY_TABLE_NAME, "");
    private static final String INDEX_SK = C_INT_32;

    private static final List<String> GSI_INSERTS = GsiConstant
        .getInsertWithShardKey(PRIMARY_TABLE_NAME, C_ID, INDEX_SK);

    private static final String DML_ON_GSI_HINT = "/*+TDDL:cmd_extra(DML_ON_GSI=true)*/";
    private static final String SCAN_HINT = "/*+TDDL: scan()*/";
    private static final String CHECK_HINT =
        "/*+TDDL: cmd_extra(GSI_DEBUG=\"recheck\",GSI_CHECK_PARALLELISM=4, GSI_CHECK_BATCH_SIZE=1024, GSI_CHECK_SPEED_LIMITATION=-1)*/";

    private final int concurrentThreadNumber = 4;
    private final ExecutorService dmlPool = Executors
        .newFixedThreadPool(concurrentThreadNumber);

    private class InsertRunner implements Runnable {

        private final AtomicBoolean stop;

        public InsertRunner(AtomicBoolean stop) {
            this.stop = stop;
        }

        @Override
        public void run() {
            try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection();
                Connection mysqlConnection = ConnectionManager.getInstance().newMysqlConnection()) {
                JdbcUtil.useDb(conn, tddlDatabase1);
                JdbcUtil.useDb(mysqlConnection, mysqlDatabase1);
                // List<Pair< sql, error_message >>
                List<Pair<String, Exception>> failedList = new ArrayList<>();

                int count = 0;
                do {
                    final String insert = GSI_INSERTS.get(ThreadLocalRandom.current().nextInt(GSI_INSERTS.size()));

                    // remove enum of 0 and '00'
                    if (insert.contains("c_enum") && (insert.contains("311, '00'") || insert.contains("311, 0"))) {
                        // ignore bad inserts
                        continue;
                    }

                    count += gsiExecuteUpdate(conn, mysqlConnection, insert, failedList, true, true);
                } while (!stop.get());

                System.out.println(Thread.currentThread().getName() + " quit after " + count + " records inserted");
            } catch (SQLException e) {
                throw new RuntimeException("Insert failed!", e);
            }

        }

    }

    private void gsiIntegrityCheck(String primary, String index, boolean withLock) {
        final ResultSet rs = JdbcUtil
            .executeQuery(CHECK_HINT + "check global index " + index/* + (withLock ? " lock" : "") */, tddlConnection);
        List<String> result = JdbcUtil.getStringResult(rs, false)
            .stream()
            .map(row -> row.get(row.size() - 1))
            .collect(Collectors.toList());
        for (int i = 0; i < result.size(); ++i) {
            System.out.println("Checker: " + result.get(i));
        }
        Assert.assertTrue(result.get(result.size() - 1), result.get(result.size() - 1).contains("OK"));
    }

    @Before
    public void before() {

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + INDEX_TABLE_NAME);
    }

    @Test
    @Ignore
    public void testSimpleCheckCorrection() {
        // Create table.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_TABLE_MYSQL);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = new ArrayList<>();
        for (int i = 0; i < concurrentThreadNumber; ++i) {
            inserts.add(dmlPool.submit(new InsertRunner(stop)));
        }

        try {
            TimeUnit.SECONDS.sleep(1); // Run 3s to get more data.
        } catch (InterruptedException e) {
            // ignore exception
        }

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        // All write stopped and no concurrent.

        final String indexSk = INDEX_SK;
        final String primary = PRIMARY_TABLE_NAME;
        final String index = INDEX_TABLE_NAME;
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.hashPartitioning(indexSk);

        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSk, covering, partitioning);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT_NO_RESTRICTION + createGsi);

        gsiIntegrityCheck(primary, index, false);
        makeAndCheckDiff();
        errorShardingTest(false);
    }

    @Test
    @Ignore
    public void testSingelPkSingleIndexCheck() {
        // Create table.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_TABLE_MYSQL);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = new ArrayList<>();
        for (int i = 0; i < concurrentThreadNumber; ++i) {
            inserts.add(dmlPool.submit(new InsertRunner(stop)));
        }

        try {
            TimeUnit.SECONDS.sleep(1); // Run 3s to get more data.
        } catch (InterruptedException e) {
            // ignore exception
        }

        final String indexSk = INDEX_SK;
        final String primary = PRIMARY_TABLE_NAME;
        final String index = INDEX_TABLE_NAME;
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.hashPartitioning(indexSk);

        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSk, covering, partitioning);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT_NO_RESTRICTION + createGsi);

        // Check with lock and check with concurrent write.
        gsiIntegrityCheck(primary, index, true);

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        gsiIntegrityCheck(primary, index, false);

        makeAndCheckDiff();
        errorShardingTest(false);
    }

    @Test
    @Ignore
    public void testMultiPkMultiIndexCheck() {
        // Create table.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_MULTI_PK_TABLE_MYSQL);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_MULTI_PK_TABLE);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = new ArrayList<>();
        for (int i = 0; i < concurrentThreadNumber; ++i) {
            inserts.add(dmlPool.submit(new InsertRunner(stop)));
        }

        try {
            TimeUnit.SECONDS.sleep(1); // Run 3s to get more data.
        } catch (InterruptedException e) {
            // ignore exception
        }

        final List<String> indexes = ImmutableList.of(C_MEDIUMINT_24, C_INT_32, C_BIGINT_64);
        final String primary = PRIMARY_TABLE_NAME;
        final String index = INDEX_TABLE_NAME;
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexes);
        final String partitioning = GsiConstant.hashPartitioning(INDEX_SK);

        final String createGsi = GsiConstant
            .getCreateGsi(primary, index, String.join(",", indexes), covering, partitioning);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT_NO_RESTRICTION + createGsi);

        // Check with lock and check with concurrent write.
        gsiIntegrityCheck(primary, index, true);

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        gsiIntegrityCheck(primary, index, false);

        makeAndCheckDiff();
        errorShardingTest(true);
    }

    private void makeAndCheckDiff() {
        // Select 3 row randomly and make conflicts.
        ResultSet rs = JdbcUtil
            .executeQuery("select id,c_int_32 from " + PRIMARY_TABLE_NAME + " order by rand() limit 3", tddlConnection);
        List<List<String>> results = JdbcUtil.getStringResult(rs, false);
        Assert.assertEquals(results.size(), 3);

        // Make missing, orphan and conflict.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            DML_ON_GSI_HINT + "delete from " + INDEX_TABLE_NAME + " where (id,c_int_32) <=> (" + results.get(0).get(0)
                + "," + results.get(0).get(1) + ")");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            SCAN_HINT + "delete from " + PRIMARY_TABLE_NAME + " where (id,c_int_32) <=> (" + results.get(1).get(0) + ","
                + results.get(1).get(1) + ")");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            DML_ON_GSI_HINT + "update " + INDEX_TABLE_NAME + " set " + C_INT_32_UN + "=123456 where (id,c_int_32) <=> ("
                + results.get(2).get(0) + "," + results.get(2).get(1) + ")");

        ResultSet checkRs = JdbcUtil.executeQuery(CHECK_HINT + "check global index " + INDEX_TABLE_NAME + " lock",
            tddlConnection);

        List<String> checkResults = JdbcUtil.getStringResult(checkRs, false)
            .stream()
            .map(row -> String.join(" ", row))
            .collect(Collectors.toList());

        System.out.println(String.join("\n", checkResults));

        Assert.assertEquals(1,
            checkResults.stream()
                .filter(res -> res.contains("MISSING") && res.contains("\"id\":" + results.get(0).get(0)))
                .count());
        Assert.assertEquals(1,
            checkResults.stream()
                .filter(res -> res.contains("ORPHAN") && res.contains("\"id\":" + results.get(1).get(0)))
                .count());
        Assert.assertEquals(2,
            checkResults.stream()
                .filter(res -> res.contains("CONFLICT") && res.contains("\"id\":" + results.get(2).get(0)))
                .count());
        Assert.assertEquals(1,
            checkResults.stream()
                .filter(res -> res.contains("GSI_CONFLICT") && res.contains("\"id\":" + results.get(2).get(0)))
                .count());

        // Test correction.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            CHECK_HINT + "check global index " + INDEX_TABLE_NAME + " correction_based_on_primary");

        gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, true);

        // Test out of count.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            DML_ON_GSI_HINT + "delete from " + INDEX_TABLE_NAME + " where 1=1");

        checkRs = JdbcUtil.executeQuery(CHECK_HINT + "check global index " + INDEX_TABLE_NAME + " lock",
            tddlConnection);
        checkResults = JdbcUtil.getStringResult(checkRs, false)
            .stream()
            .map(row -> String.join(" ", row))
            .collect(Collectors.toList());

        System.out.println(String.join("\n", checkResults));

        Assert.assertTrue(checkResults.stream()
            .filter(row -> row.contains("summary"))
            .allMatch(row -> row.contains("Error limit exceeded")));

        // Fix it.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            CHECK_HINT + "check global index " + INDEX_TABLE_NAME + " correction_based_on_primary");

        gsiIntegrityCheck(PRIMARY_TABLE_NAME, INDEX_TABLE_NAME, true);
    }

    @Test
    public void testSingelPkSingleIndexCorrectionConcurrently() {
        // Create table.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_TABLE_MYSQL);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = new ArrayList<>();
        for (int i = 0; i < concurrentThreadNumber; ++i) {
            inserts.add(dmlPool.submit(new InsertRunner(stop)));
        }

        try {
            TimeUnit.SECONDS.sleep(1); // Run 3s to get more data.
        } catch (InterruptedException e) {
            // ignore exception
        }

        final String indexSk = INDEX_SK;
        final String primary = PRIMARY_TABLE_NAME;
        final String index = INDEX_TABLE_NAME;
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.hashPartitioning(indexSk);

        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSk, covering, partitioning);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT_NO_RESTRICTION + createGsi);

        // Check with lock and check with concurrent write.
        gsiIntegrityCheck(primary, index, true);

        // Make error, check and correction with write concurrently.
        makeAndCheckDiff();

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        gsiIntegrityCheck(primary, index, false);
    }

    @Test
    public void testMultiPkMultiIndexCorrectionConcurrently() {
        // Create table.
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_MULTI_PK_TABLE_MYSQL);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_MULTI_PK_TABLE);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = new ArrayList<>();
        for (int i = 0; i < concurrentThreadNumber; ++i) {
            inserts.add(dmlPool.submit(new InsertRunner(stop)));
        }

        try {
            TimeUnit.SECONDS.sleep(1); // Run 3s to get more data.
        } catch (InterruptedException e) {
            // ignore exception
        }

        final List<String> indexes = ImmutableList.of(C_MEDIUMINT_24, C_INT_32, C_BIGINT_64);
        final String primary = PRIMARY_TABLE_NAME;
        final String index = INDEX_TABLE_NAME;
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexes);
        final String partitioning = GsiConstant.hashPartitioning(INDEX_SK);

        final String createGsi = GsiConstant
            .getCreateGsi(primary, index, String.join(",", indexes), covering, partitioning);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT_NO_RESTRICTION + createGsi);

        // Check with lock and check with concurrent write.
        gsiIntegrityCheck(primary, index, true);

        try {
            // Make error, check and correction with write concurrently.
            makeAndCheckDiff();
        } catch (Throwable ex) {
            logger.warn(ex);
        }

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        gsiIntegrityCheck(primary, index, false);
    }

    private void errorShardingTest(boolean multiPK) {
        // Select 3 row randomly and make errors.
        ResultSet rs = JdbcUtil
            .executeQuery("select id,c_int_32 from " + PRIMARY_TABLE_NAME + " order by rand() limit 3", tddlConnection);
        List<List<String>> idAndInt32results = JdbcUtil.getStringResult(rs, false);
        Assert.assertEquals(idAndInt32results.size(), 3);

        // Make 3 types of error sharding:
        // 1: primary error sharding
        // 2: gsi error sharding
        // 3: both

        final long diffIdGap = 10000000;

        rs = JdbcUtil.executeQuery("show topology " + PRIMARY_TABLE_NAME, tddlConnection);
        List<String> phyTablesPrimary = JdbcUtil.getStringResult(rs, false)
            .stream()
            .map(row -> row.get(2))
            .collect(Collectors.toList());
        rs = JdbcUtil.executeQuery("show topology " + INDEX_TABLE_NAME, tddlConnection);
        List<String> phyTablesGsi = JdbcUtil.getStringResult(rs, false)
            .stream()
            .map(row -> row.get(2))
            .collect(Collectors.toList());

        // 1: primary miss sharding
        String sql = "/*+TDDL: cmd_extra(DML_ON_GSI=true,PUSHDOWN_HINT_ON_GSI=true) node("
            + (Long.parseLong(idAndInt32results.get(0).get(0)) % 12 / 3) + ")*/update "
            + phyTablesPrimary.get((int) (Long.parseLong(idAndInt32results.get(0).get(0)) % 6))
            + " set id=id+" + diffIdGap + " where (id,c_int_32) <=> (" + idAndInt32results.get(0).get(0) + ","
            + idAndInt32results.get(0).get(1) + ")";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 2: gsi error sharding
        sql = "/*+TDDL: cmd_extra(DML_ON_GSI=true,PUSHDOWN_HINT_ON_GSI=true) node("
            + (Long.parseLong(idAndInt32results.get(1).get(1)) % 28 / 7) + ")*/update "
            + phyTablesGsi.get((int) (Long.parseLong(idAndInt32results.get(1).get(1)) % 14))
            + " set c_int_32=c_int_32+1 where (id,c_int_32) <=> (" + idAndInt32results.get(1).get(0) + ","
            + idAndInt32results.get(1).get(1) + ")";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 3: both
        sql = "/*+TDDL: cmd_extra(DML_ON_GSI=true,PUSHDOWN_HINT_ON_GSI=true) node("
            + (Long.parseLong(idAndInt32results.get(2).get(0)) % 12 / 3) + ")*/update "
            + phyTablesPrimary.get((int) (Long.parseLong(idAndInt32results.get(2).get(0)) % 6)) + " set id=id+"
            + diffIdGap + " where (id,c_int_32) <=> (" + idAndInt32results.get(2).get(0) + ","
            + idAndInt32results.get(2).get(1) + ")";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "/*+TDDL: cmd_extra(DML_ON_GSI=true,PUSHDOWN_HINT_ON_GSI=true) node("
            + (Long.parseLong(idAndInt32results.get(2).get(1)) % 28 / 7) + ")*/update "
            + phyTablesGsi.get((int) (Long.parseLong(idAndInt32results.get(2).get(1)) % 14))
            + " set c_int_32=c_int_32+1 where (id,c_int_32) <=> (" + idAndInt32results.get(2).get(0) + ","
            + idAndInt32results.get(2).get(1) + ")";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Check.
        ResultSet checkRs = JdbcUtil.executeQuery(CHECK_HINT + "check global index " + INDEX_TABLE_NAME/* + " lock"*/,
            tddlConnection);
        List<String> checkResults = JdbcUtil.getStringResult(checkRs, false)
            .stream()
            .map(row -> String.join(" ", row))
            .collect(Collectors.toList());

        System.out.println(String.join("\n", checkResults));

        //
        // Asserts.
        //

        // All error sharding should be found. So 4 ERROR_SHARD.
        Assert
            .assertEquals(1,
                checkResults.stream()
                    .filter(res -> res.contains("ERROR_SHARD") && res
                        .contains("\"id\":" + (Long.parseLong(idAndInt32results.get(0).get(0)) + diffIdGap)))
                    .count());
        Assert.assertEquals(1,
            checkResults.stream()
                .filter(res -> res.contains("ERROR_SHARD") && res.contains("\"id\":" + idAndInt32results.get(1).get(0)))
                .count());
        Assert.assertEquals(1,
            checkResults.stream()
                .filter(res -> res.contains("ERROR_SHARD") && res.contains("\"id\":" + idAndInt32results.get(2).get(0)))
                .count());
        Assert
            .assertEquals(1,
                checkResults.stream()
                    .filter(res -> res.contains("ERROR_SHARD") && res
                        .contains("\"id\":" + (Long.parseLong(idAndInt32results.get(2).get(0)) + diffIdGap)))
                    .count());

        // All changes to PK will cause missing. So 2 of them.
        Assert
            .assertEquals(1,
                checkResults.stream()
                    .filter(res -> res.contains("MISSING") && res
                        .contains("\"id\":" + (Long.parseLong(idAndInt32results.get(0).get(0)) + diffIdGap)))
                    .count());
        Assert
            .assertEquals(1,
                checkResults.stream()
                    .filter(res -> res.contains("MISSING") && res
                        .contains("\"id\":" + (Long.parseLong(idAndInt32results.get(2).get(0)) + diffIdGap)))
                    .count());

        // All changes to PK will cause GSI orphan. So 2 of them.
        Assert.assertEquals(1,
            checkResults.stream()
                .filter(res -> res.contains("ORPHAN") && res.contains("\"id\":" + idAndInt32results.get(0).get(0)))
                .count());
        Assert.assertEquals(1,
            checkResults.stream()
                .filter(res -> res.contains("ORPHAN") && res.contains("\"id\":" + idAndInt32results.get(2).get(0)))
                .count());

        if (multiPK) {
            Assert.assertEquals(1,
                checkResults.stream()
                    .filter(res -> res.contains("MISSING")
                        && res.contains("\"id\":" + Long.parseLong(idAndInt32results.get(1).get(0))))
                    .count());
            Assert.assertEquals(1,
                checkResults.stream()
                    .filter(res -> res.contains("ORPHAN")
                        && res.contains("\"id\":" + Long.parseLong(idAndInt32results.get(1).get(0))))
                    .count());
        } else {
            // Change only on GSI SK will cause conflict.
            Assert.assertEquals(1,
                checkResults.stream()
                    .filter(
                        res -> res.contains("CONFLICT") && res.contains("\"id\":" + idAndInt32results.get(1).get(0)))
                    .count());
            Assert.assertEquals(1,
                checkResults.stream()
                    .filter(
                        res -> res.contains("GSI_CONFLICT") && res.contains("\"id\":" + idAndInt32results.get(1).get(0)))
                    .count());
        }
    }
}

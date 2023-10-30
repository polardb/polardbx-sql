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
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
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
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_TIMESTAMP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;

/**
 * @author chenmo.cm
 */
public class GsiBackfillConcurrentWriteTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "gsi_concurrent_primary";
    private static final String INDEX_TABLE_NAME = "g_i_concurrent";
    private static final String FULL_TYPE_TABLE = ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
        DEFAULT_PARTITIONING_DEFINITION);
    private static final String FULL_TYPE_TABLE_MYSQL = ExecuteTableSelect.getFullTypeTableDef(PRIMARY_TABLE_NAME,
        "");
    private static final String INDEX_SK = C_BIGINT_64;

    private static final List<String> GSI_INSERTS = GsiConstant
        .getInsertWithShardKey(PRIMARY_TABLE_NAME, C_ID, INDEX_SK);

    private final ExecutorService dmlPool = Executors.newFixedThreadPool(2);

    private static class InsertRunner implements Runnable {

        private final AtomicBoolean stop;
        private final String tddlDb;
        private final String mysqlDB;

        public InsertRunner(AtomicBoolean stop, String tddlDb, String mysqlDB) {
            this.stop = stop;
            this.mysqlDB = mysqlDB;
            this.tddlDb = tddlDb;
        }

        @Override
        public void run() {
            try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection();
                Connection mysqlConnection = ConnectionManager.getInstance().newMysqlConnection()) {
                JdbcUtil.useDb(conn, tddlDb);
                JdbcUtil.useDb(mysqlConnection, mysqlDB);
                // List<Pair< sql, error_message >>
                List<Pair<String, Exception>> failedList = new ArrayList<>();

                int count = 0;
                do {
                    final String insert = GSI_INSERTS.get(ThreadLocalRandom.current().nextInt(GSI_INSERTS.size()));

                    count += gsiExecuteUpdate(conn, mysqlConnection, insert, failedList, true, true);
                    if (count >= 100000){
                        break;
                    }
                } while (!stop.get());

                System.out.println(Thread.currentThread().getName() + " quit after " + count + " records inserted");
            } catch (SQLException e) {
                throw new RuntimeException("Insert failed!", e);
            }

        }

    }

    private void gsiIntegrityCheck(String primary, String index) {
        final String columnList = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .collect(Collectors.joining(", "));

        final String columnList1 = FULL_TYPE_TABLE_COLUMNS.stream()
            .filter(column -> !column.equalsIgnoreCase(C_TIMESTAMP))
            .filter(column -> !column.equalsIgnoreCase(C_ID))
            .collect(Collectors.joining(", "));

        gsiIntegrityCheck(primary, index, columnList, columnList1, true);
    }

    @Before
    public void before() {
        // JDBC handles zero-date differently in prepared statement and statement, so ignore this case in cursor fetch
        org.junit.Assume.assumeTrue(!PropertiesUtil.useCursorFetch());

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, FULL_TYPE_TABLE_MYSQL);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, FULL_TYPE_TABLE);
    }

    @Test
    public void testConcurrentWithInsert() {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts =
            ImmutableList.of(dmlPool.submit(new InsertRunner(stop, tddlDatabase1, mysqlDatabase1)),
                dmlPool.submit(new InsertRunner(stop, tddlDatabase1, mysqlDatabase1)));

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            // ignore exception
        }

        final String indexSk = INDEX_SK;
        final String primary = PRIMARY_TABLE_NAME;
        final String index = INDEX_TABLE_NAME;
        final String covering = GsiConstant.getCoveringColumns(C_ID, indexSk);
        final String partitioning = GsiConstant.hashPartitioning(indexSk);

        final String createGsi = GsiConstant.getCreateGsi(primary, index, indexSk, covering, partitioning);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT + createGsi);

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        gsiIntegrityCheck(primary, index);
    }

    @Test
    public void testConcurrentWithInsertForSingleTableGsi() {

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts =
            ImmutableList.of(dmlPool.submit(new InsertRunner(stop, tddlDatabase1, mysqlDatabase1)),
                dmlPool.submit(new InsertRunner(stop, tddlDatabase1, mysqlDatabase1)));

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            // ignore exception
        }

        final String primary = PRIMARY_TABLE_NAME;
        final String index = INDEX_TABLE_NAME;

        //mock a single table gsi
        boolean skipCutover = RandomUtils.nextBoolean();
        final String hint =
            String.format(
                "/*+TDDL:CMD_EXTRA(REPARTITION_SKIP_CUTOVER=%s,REPARTITION_SKIP_CLEANUP=true,REPARTITION_FORCE_GSI_NAME=%s) */",
                skipCutover, index);
        final String ddl = hint + String.format("alter table %s single", primary);

        JdbcUtil.executeUpdateSuccess(tddlConnection, GSI_ALLOW_ADD_HINT + ddl);

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        gsiIntegrityCheck(primary, index);
    }

    @Test
    public void testConcurrentWithInsertForBroadcastTableGsi() {

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = ImmutableList.of(dmlPool.submit(new InsertRunner(
                stop, tddlDatabase1, mysqlDatabase1)),
            dmlPool.submit(new InsertRunner(stop, tddlDatabase1, mysqlDatabase1)));

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            // ignore exception
        }

        final String primary = PRIMARY_TABLE_NAME;
        final String index = INDEX_TABLE_NAME;

        //mock a broadcast table gsi
        boolean skipCutover = RandomUtils.nextBoolean();
        final String hint =
            String.format(
                "/*+TDDL:CMD_EXTRA(REPARTITION_SKIP_CUTOVER=%s,REPARTITION_SKIP_CLEANUP=true,REPARTITION_FORCE_GSI_NAME=%s) */",
                skipCutover, index);
        final String ddl = hint + String.format("alter table %s broadcast", primary);

        JdbcUtil.executeUpdateSuccess(tddlConnection, ddl);

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        gsiIntegrityCheck(primary, index);
    }
}

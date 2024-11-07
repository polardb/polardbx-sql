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
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

@Ignore("2024-08-08 之后update由串行改成group并发执行物理sql，此用例预期内会概率出现死锁，忽略")
public class XATransactionStressTest extends CrudBasedLockTestCase {

    private static final int MAX_DATA_SIZE = 1000;

    private static final int NUM_WORKERS = 8;

    private static final int TEST_DURATION_SECS = 10;

    public XATransactionStressTest() {
        // Test on table `update_delete_base_multi_db_multi_tb`
        this.baseOneTableName = ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX;
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    @Test
    public void testMultiThreads() throws Exception {
        AtomicBoolean shutdown = new AtomicBoolean();

        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_WORKERS);
        List<Future> futures = new ArrayList<>();

        try {
            for (int i = 0; i < NUM_WORKERS; i++) {
                Connection connection = getPolardbxDirectConnection();
                TestWorkload workload = new TestWorkload(shutdown, i, connection, baseOneTableName);
                futures.add(threadPool.submit(workload));
            }

            Thread.sleep(TEST_DURATION_SECS * 1000L);
            shutdown.set(true);

            for (Future future : futures) {
                // If something bad happened, exception should be thrown here
                future.get();
            }
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    private static class TestWorkload implements Callable<Void> {

        private final String POINT_UPDATE_SQL;
        private final String RANGE_UPDATE_SQL;
        private final String POINT_SELECT_SQL;
        private final String RANGE_SELECT_SQL;

        private final AtomicBoolean shutdown;
        private final Random random;
        private final Connection connection;

        public TestWorkload(AtomicBoolean shutdown, int randomSeed, Connection connection, String baseOneTableName) {
            this.shutdown = shutdown;
            this.random = new Random(randomSeed);
            this.connection = connection;
            POINT_UPDATE_SQL = "update " + baseOneTableName + " set integer_test=? where pk=?";
            RANGE_UPDATE_SQL =
                "update " + baseOneTableName + " set integer_test=? where pk>=? and pk<?";
            POINT_SELECT_SQL = "select integer_test from " + baseOneTableName + " where pk=?";
            RANGE_SELECT_SQL =
                "select integer_test from " + baseOneTableName + " where pk>=? and pk<?";
        }

        @Override
        public Void call() throws Exception {
            System.out.println("Worker started");
            try {
                connection.setAutoCommit(false);
                int count = 0;
                while (!shutdown.get()) {
                    switch (random.nextInt(4)) {
                    case 0:
                        doPointUpdate();
                        break;
                    case 1:
                        doRangeUpdate();
                        break;
                    case 2:
                        doPointSelect();
                        break;
                    case 3:
                        doRangeSelect();
                        break;
                    }
                    connection.commit();
                    count++;
                }
                System.out.println("Completed " + count + " rounds");
            } finally {
                JdbcUtil.close(connection);
            }
            return null;
        }

        private void doPointUpdate() throws SQLException {
            List<Object> params = ImmutableList.of(random.nextInt(), nextPk());
            JdbcUtil.updateData(connection, POINT_UPDATE_SQL, params);
        }

        private void doRangeUpdate() throws SQLException {
            int pk = nextRangeStartPk();
            List<Object> params = ImmutableList.of(random.nextInt(), pk, pk + 8);
            JdbcUtil.updateData(connection, RANGE_UPDATE_SQL, params);
        }

        private void doPointSelect() throws SQLException {
            try (PreparedStatement ps = connection.prepareStatement(POINT_SELECT_SQL)) {
                ps.setInt(1, nextPk());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        // ignore results
                    }
                }
            }
        }

        private void doRangeSelect() throws SQLException {
            try (PreparedStatement ps = connection.prepareStatement(RANGE_SELECT_SQL)) {
                int pk = nextRangeStartPk();
                ps.setInt(1, pk);
                ps.setInt(2, pk + 8);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        // ignore results
                    }
                }
            }
        }

        private int nextPk() {
            return random.nextInt(MAX_DATA_SIZE);
        }

        private int nextRangeStartPk() {
            /*
             * Align range boundary to 16 so that the connection will be established in a fixed group order:
             *   GROUP0 -> GROUP1 -> GROUP2 -> GROUP3 -> GROUP0 -> ...
             * Note that we assume that the test environment has at 4/8/16 groups
             */
            return random.nextInt(MAX_DATA_SIZE) / 16 * 16;
        }
    }
}

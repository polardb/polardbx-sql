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

package com.alibaba.polardbx.qatest.ddl.auto.autoNewPartition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @version 1.0
 */

public class AutoPartitionColumnTest extends BaseAutoPartitionNewPartition {

    private final static String CREATE_TABLE_TEMPLATE = "CREATE TABLE `{0}` (\n"
        + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `c0` int,\n"
        + "  `t` timestamp default current_timestamp,\n"
        + "  `order_id` int DEFAULT NULL,\n"
        + "  `seller_id` int DEFAULT NULL,\n"
        + "  PRIMARY KEY (`pk`),\n"
        + "  CLUSTERED INDEX `{1}` using btree (`seller_id`),\n"
        + "  UNIQUE CLUSTERED INDEX `{2}` using btree (`order_id`)\n"
        + ");";
    private static final String CREATE_TABLE_NO_PK_TEMPLATE = "CREATE TABLE `{0}` (\n"
        + "  `c0` int,\n"
        + "  `t` timestamp default current_timestamp,\n"
        + "  `order_id` int DEFAULT NULL,\n"
        + "  `seller_id` int DEFAULT NULL,\n"
        + "  CLUSTERED INDEX `{1}` using btree (`seller_id`),\n"
        + "  UNIQUE CLUSTERED INDEX `{2}` using btree (`order_id`)\n"
        + ");";

    @Parameterized.Parameters(name = "{index}:createTable={0}")
    public static List<String[]> prepareDate() {
        return ImmutableList.of(new String[] {
            CREATE_TABLE_TEMPLATE
        }, new String[] {
            CREATE_TABLE_NO_PK_TEMPLATE
        });
    }

    private String createTableStmt;
    private String TABLE_NAME;
    private String CGSI_NAME;
    private String UCGSI_NAME;
    private final boolean hasPk;

    public AutoPartitionColumnTest(String createTable) {
        String tableName = randomTableName("auto_partition_col_tb", 4);
        String gsiName = randomTableName("cg_i_ap_col", 4);
        String uGsiName = randomTableName("cug_i_ap_col", 4);
        this.createTableStmt = MessageFormat.format(createTable, tableName, gsiName, uGsiName);
        this.TABLE_NAME = tableName;
        this.CGSI_NAME = gsiName;
        this.UCGSI_NAME = uGsiName;
        this.hasPk = createTable.contains("PRIMARY KEY");
    }

    private void assertPartitioned() {
        final List<List<String>> stringResult = JdbcUtil.getStringResult(
            JdbcUtil.executeQuery("show full create table " + TABLE_NAME, tddlConnection), true);
        Assert.assertTrue(stringResult.get(0).get(1).contains("PARTITION BY"));
        Assert.assertTrue(stringResult.get(0).get(1).contains("CREATE PARTITION TABLE"));
    }

    @Before
    public void before() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(CGSI_NAME, UCGSI_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableStmt);
        assertPartitioned();
    }

    @After
    public void after() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(CGSI_NAME, UCGSI_NAME));
    }

    @Test
    public void addColumnPlanTest() {

        final String sql = MessageFormat.format("select * from `{0}` where `seller_id` = 123", TABLE_NAME);
        final String sqlNo = MessageFormat
            .format("select * from `{0}` ignore index({1},{2}) where `seller_id` = 123", TABLE_NAME, CGSI_NAME,
                UCGSI_NAME);

        final String beforeExplain = getExplainResult(sql);
        Assert.assertTrue(beforeExplain.contains("IndexScan"));
        final String beforeNoExplain = getExplainResult(sqlNo);
        Assert.assertFalse(beforeNoExplain.contains("IndexScan"));

        // Add column.
        final String addCol = MessageFormat.format("alter table `{0}` add column `c1` int", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, addCol);

        final String afterExplain = getExplainResult(sql);
        Assert.assertTrue(afterExplain.contains("IndexScan") && afterExplain.contains("`c1`"));
        final String afterNoExplain = getExplainResult(sqlNo);
        Assert.assertFalse(afterNoExplain.contains("IndexScan"));
    }

    @Test
    public void addColumnConcurrentReadWriteTest() throws Throwable {

        final int concurrentNumber = 4;
        final String selectGSI =
            MessageFormat.format("select * from `{0}` force index({1}) order by `seller_id`", TABLE_NAME, CGSI_NAME);
        final String selectPrimary = MessageFormat
            .format("select * from `{0}` ignore index({1},{2}) order by `seller_id`", TABLE_NAME, CGSI_NAME,
                UCGSI_NAME);

        final List<Thread> runners = new ArrayList<>();
        final AtomicBoolean exit = new AtomicBoolean(false);
        final AtomicInteger idGen = new AtomicInteger(1);
        final List<Throwable> excps = new CopyOnWriteArrayList<>();
        final int baseIdx = hasPk ? 1 : 0;

        // insert some data before alter to prevent empty records
        for (int i = 0; i < 10; ++i) {
            final int tmp = idGen.getAndIncrement();
            final String tmpSql;
            if (hasPk) {
                tmpSql = MessageFormat
                    .format(
                        "insert into `{0}` (pk,c0,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#},{4,number,#})",
                        TABLE_NAME, tmp, tmp + 100, tmp + 1000, tmp % 100);
            } else {
                tmpSql = MessageFormat
                    .format(
                        "insert into `{0}` (c0,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#})",
                        TABLE_NAME, tmp + 100, tmp + 1000, tmp % 100);
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, tmpSql);
        }

        // insert & check threads.
        for (int i = 0; i < concurrentNumber; ++i) {
            runners.add(new Thread(() -> {
                boolean colAdded = false;

                try (Connection conn = getPolardbxDirectConnection()) {
                    while (!exit.get()) {
                        final String insertSql;
                        final int gened = idGen.getAndIncrement();
                        if (colAdded) {
                            if (hasPk) {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` values ({1,number,#},{2,number,#},current_timestamp,{3,number,#},{4,number,#},{5,number,#})",
                                        TABLE_NAME, gened, gened + 100, gened + 1000, gened % 100, gened + 100);
                            } else {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` values ({1,number,#},current_timestamp,{2,number,#},{3,number,#},{4,number,#})",
                                        TABLE_NAME, gened + 100, gened + 1000, gened % 100, gened + 100);
                            }
                        } else {
                            if (hasPk) {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` (pk,c0,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#},{4,number,#})",
                                        TABLE_NAME, gened, gened + 100, gened + 1000, gened % 100);
                            } else {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` (c0,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#})",
                                        TABLE_NAME, gened + 100, gened + 1000, gened % 100);
                            }
                        }

                        JdbcUtil.executeUpdateSuccess(conn, insertSql);

                        // Select primary first.
                        try (Statement statement = conn.createStatement()) {
                            final List<List<String>> result =
                                JdbcUtil.getStringResult(statement.executeQuery(selectPrimary), false);
                            Assert.assertFalse(result.isEmpty());
                            final int cols = result.get(0).size();
                            if (baseIdx + 5 == cols) {
                                colAdded = true;

                                // Check new added col correctness.
                                for (List<String> row : result) {
                                    if (row.get(baseIdx + 4) != null) {
                                        Assert.assertEquals(row.get(baseIdx), row.get(baseIdx + 4));
                                    }
                                }
                            } else {
                                Assert.assertEquals(baseIdx + 4, cols);
                                Assert.assertFalse(colAdded);
                            }
                        }

                        // Select from GSI.
                        Assert.assertTrue(getExplainResult(selectGSI).contains("IndexScan"));
                        try (Statement statement = conn.createStatement()) {
                            final List<List<String>> result =
                                JdbcUtil.getStringResult(statement.executeQuery(selectGSI), false);
                            Assert.assertFalse(result.isEmpty());
                            final int cols = result.get(0).size();
                            if (baseIdx + 5 == cols) {
                                colAdded = true;

                                // Check new added col correctness.
                                for (List<String> row : result) {
                                    if (row.get(baseIdx + 4) != null) {
                                        Assert.assertEquals(row.get(baseIdx), row.get(baseIdx + 4));
                                    }
                                }
                            } else {
                                Assert.assertEquals(baseIdx + 4, cols);
                                Assert.assertFalse(colAdded);
                            }
                        }
                    }
                } catch (Throwable e) {
                    excps.add(e);
                }
            }));
        }

        runners.forEach(Thread::start);

        System.out.println("Strart add col.");

        // Now add col.
        final String addCol = MessageFormat.format("alter table `{0}` add column c1 int", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, addCol);

        System.out.println("Finish add col.");

        // Finish threads.
        exit.set(true);
        for (Thread t : runners) {
            t.join();
        }

        if (!excps.isEmpty()) {
            throw excps.get(0);
        }

        System.out.println(idGen.get() + " rows added.");

        // Assert that column added and covered in GSI.
        Assert.assertTrue(getExplainResult(selectGSI).contains("IndexScan"));
        try (Statement statement = tddlConnection.createStatement()) {
            final List<List<String>> result =
                JdbcUtil.getStringResult(statement.executeQuery(selectGSI), false);
            Assert.assertFalse(result.isEmpty());
            final int cols = result.get(0).size();
            Assert.assertEquals(baseIdx + 5, cols);
            // Check new added col correctness.
            for (List<String> row : result) {
                if (row.get(baseIdx + 4) != null) {
                    Assert.assertEquals(row.get(baseIdx), row.get(baseIdx + 4));
                }
            }
        }

        // Assert that data identical.
        selectContentSameAssert(selectPrimary, selectGSI, null, tddlConnection,
            tddlConnection);
    }

    @Test
    public void dropColumnConcurrentReadWriteTest() throws Throwable {

        final int concurrentNumber = 4;
        final String selectGSI =
            MessageFormat.format("select * from `{0}` force index({1}) order by `seller_id`", TABLE_NAME, CGSI_NAME);
        final String selectPrimary = MessageFormat
            .format("select * from `{0}` ignore index({1},{2}) order by `seller_id`", TABLE_NAME, CGSI_NAME,
                UCGSI_NAME);

        // Now add col.
        final String addCol = MessageFormat.format("alter table `{0}` add column c1 int", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, addCol);

        final List<Thread> runners = new ArrayList<>();
        final AtomicBoolean exit = new AtomicBoolean(false);
        final AtomicInteger idGen = new AtomicInteger(1);
        final List<Throwable> excps = new CopyOnWriteArrayList<>();
        final int baseIdx = hasPk ? 1 : 0;

        // insert some data before alter to prevent empty records
        for (int i = 0; i < 10; ++i) {
            final int tmp = idGen.getAndIncrement();
            final String tmpSql;
            if (hasPk) {
                tmpSql = MessageFormat
                    .format(
                        "insert into `{0}` values ({1,number,#},{2,number,#},current_timestamp,{3,number,#},{4,number,#},{5,number,#})",
                        TABLE_NAME, tmp, tmp + 100, tmp + 1000, tmp % 100, tmp + 100);
            } else {
                tmpSql = MessageFormat
                    .format(
                        "insert into `{0}` values ({1,number,#},current_timestamp,{2,number,#},{3,number,#},{4,number,#})",
                        TABLE_NAME, tmp + 100, tmp + 1000, tmp % 100, tmp + 100);
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, tmpSql);
        }

        // insert & check threads.
        for (int i = 0; i < concurrentNumber; ++i) {
            runners.add(new Thread(() -> {
                boolean colAdded = true;

                try (Connection conn = getPolardbxDirectConnection()) {
                    while (!exit.get()) {
                        final String insertSql;
                        final int gened = idGen.getAndIncrement();
                        if (colAdded) {
                            if (hasPk) {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` values ({1,number,#},{2,number,#},current_timestamp,{3,number,#},{4,number,#},{5,number,#})",
                                        TABLE_NAME, gened, gened + 100, gened + 1000, gened % 100, gened + 100);
                            } else {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` values ({1,number,#},current_timestamp,{2,number,#},{3,number,#},{4,number,#})",
                                        TABLE_NAME, gened + 100, gened + 1000, gened % 100, gened + 100);
                            }
                        } else {
                            if (hasPk) {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` (pk,c0,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#},{4,number,#})",
                                        TABLE_NAME, gened, gened + 100, gened + 1000, gened % 100);
                            } else {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` (c0,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#})",
                                        TABLE_NAME, gened + 100, gened + 1000, gened % 100);
                            }
                        }

                        try (Statement statement = conn.createStatement()) {
                            statement.executeUpdate(insertSql);
                        } catch (SQLException e) {
                            // just ignore missing column
                            if (!e.getMessage().contains("Number of INSERT target")) {
                                throw e;
                            }
                        }

                        // Select primary first.
                        try (Statement statement = conn.createStatement()) {
                            final List<List<String>> result =
                                JdbcUtil.getStringResult(statement.executeQuery(selectPrimary), false);
                            Assert.assertFalse(result.isEmpty());
                            final int cols = result.get(0).size();
                            if (baseIdx + 4 == cols) {
                                colAdded = false;
                            } else {
                                Assert.assertEquals(baseIdx + 5, cols);
                                Assert.assertTrue(colAdded);

                                // Check new added col correctness.
                                for (List<String> row : result) {
                                    Assert.assertEquals(row.get(baseIdx), row.get(baseIdx + 4));
                                }
                            }
                        }

                        // Select from GSI.
                        Assert.assertTrue(getExplainResult(selectGSI).contains("IndexScan"));
                        try (Statement statement = conn.createStatement()) {
                            final List<List<String>> result =
                                JdbcUtil.getStringResult(statement.executeQuery(selectGSI), false);
                            Assert.assertFalse(result.isEmpty());
                            final int cols = result.get(0).size();
                            if (baseIdx + 4 == cols) {
                                colAdded = false;
                            } else {
                                Assert.assertEquals(baseIdx + 5, cols);
                                Assert.assertTrue(colAdded);

                                // Check new added col correctness.
                                for (List<String> row : result) {
                                    Assert.assertEquals(row.get(baseIdx), row.get(baseIdx + 4));
                                }
                            }
                        }
                    }
                } catch (Throwable e) {
                    excps.add(e);
                }
            }));
        }

        runners.forEach(Thread::start);

        System.out.println("Strart drop col.");

        // Now add col.
        final String dropCol = MessageFormat.format("alter table `{0}` drop column c1", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropCol);

        System.out.println("Finish dorp col.");

        // Finish threads.
        exit.set(true);
        for (Thread t : runners) {
            t.join();
        }

        if (!excps.isEmpty()) {
            throw excps.get(0);
        }

        System.out.println(idGen.get() + " rows added.");

        // Assert that column added and covered in GSI.
        Assert.assertTrue(getExplainResult(selectGSI).contains("IndexScan"));
        try (Statement statement = tddlConnection.createStatement()) {
            final List<List<String>> result =
                JdbcUtil.getStringResult(statement.executeQuery(selectGSI), false);
            Assert.assertFalse(result.isEmpty());
            final int cols = result.get(0).size();
            Assert.assertEquals(baseIdx + 4, cols);
        }

        // Assert that data identical.
        selectContentSameAssert(selectPrimary, selectGSI, null, tddlConnection,
            tddlConnection);
    }

    @Test
    public void testAddColumnRollbackWithAllFail() throws Exception {

        // Note: This try 2 times to ensure correct sync of table meta and get the same error and rollback correctly.
        for (int i = 0; i < 2; ++i) {
            final String addedColumn = "addedx";
            final String alter =
                MessageFormat.format("alter table `{0}` add column `{1}` int(1024);", TABLE_NAME, addedColumn);
            JdbcUtil
                .executeUpdateFailed(tddlConnection, alter,
                    "Not all physical DDLs have been executed successfully");

            // Assert no column.
            Assert.assertFalse(JdbcUtil.showCreateTable(tddlConnection, TABLE_NAME).contains(addedColumn));
            Assert.assertFalse(JdbcUtil.showCreateTable(tddlConnection, CGSI_NAME).contains(addedColumn));
            Assert.assertFalse(JdbcUtil.showCreateTable(tddlConnection, UCGSI_NAME).contains(addedColumn));

            // Assert no column in GSI meta.
            try (ResultSet rs = JdbcUtil.executeQuery("show global index", tddlConnection)) {
                List<List<String>> res = JdbcUtil.getAllStringResult(rs, false, null);
                Assert.assertFalse("Bad rollback.",
                    res.stream().filter(line -> line.get(1).equalsIgnoreCase(TABLE_NAME))
                        .anyMatch(line -> line.get(5).contains(addedColumn)));
            }

            // Now insert the correct column and drop it.
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                MessageFormat.format("alter table `{0}` add column `{1}` int(10);", TABLE_NAME, addedColumn));

            // Assert it exists.
            Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, TABLE_NAME).contains(addedColumn));
            Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, CGSI_NAME).contains(addedColumn));
            Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, UCGSI_NAME).contains(addedColumn));
            try (ResultSet rs = JdbcUtil.executeQuery("show global index", tddlConnection)) {
                List<List<String>> res = JdbcUtil.getAllStringResult(rs, false, null);
                Assert.assertTrue("Bad add column.",
                    res.stream().filter(line -> line.get(1).equalsIgnoreCase(TABLE_NAME))
                        .anyMatch(line -> line.get(5).contains(addedColumn)));
            }

            // Now drop it.
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                MessageFormat.format("alter table `{0}` drop column `{1}`", TABLE_NAME, addedColumn));
        }
    }

    @Test
    public void changeColumnDefaultConcurrentReadWriteTest() throws Throwable {

        final int concurrentNumber = 4;
        final String selectGSI =
            MessageFormat.format("select * from `{0}` force index({1})", TABLE_NAME, CGSI_NAME);
        final String selectPrimary = MessageFormat
            .format("select * from `{0}` ignore index({1},{2})", TABLE_NAME, CGSI_NAME,
                UCGSI_NAME);

        final String alterDefaultTmpl = "alter table `" + TABLE_NAME + "` alter `c0` set default {0}";

        // Set c0 initial default value. Init to 1.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(alterDefaultTmpl, 1));

        final List<Thread> runners = new ArrayList<>();
        final AtomicBoolean exit = new AtomicBoolean(false);
        final AtomicInteger idGen = new AtomicInteger(1);
        final List<Throwable> excps = new CopyOnWriteArrayList<>();
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        // insert some data before alter to prevent empty records
        for (int i = 0; i < 10; ++i) {
            final int tmp = idGen.getAndIncrement();
            final String tmpSql;
            if (hasPk) {
                tmpSql = MessageFormat
                    .format(
                        "insert into `{0}` (pk,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#})",
                        TABLE_NAME, tmp, tmp + 1000, tmp % 100);
            } else {
                tmpSql = MessageFormat
                    .format(
                        "insert into `{0}` (order_id,seller_id) values ({1,number,#},{2,number,#})",
                        TABLE_NAME, tmp + 1000, tmp % 100);
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, tmpSql);
        }

        // insert & check threads.
        for (int i = 0; i < concurrentNumber; ++i) {
            runners.add(new Thread(() -> {
                try (Connection conn = getPolardbxDirectConnection()) {
                    while (!exit.get()) {
                        final String insertSql;
                        final int gened = idGen.getAndIncrement();

                        // Gen insert without c0.
                        if (hasPk) {
                            insertSql = MessageFormat
                                .format(
                                    "insert into `{0}` (pk,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#})",
                                    TABLE_NAME, gened, gened + 1000, gened % 100);
                        } else {
                            insertSql = MessageFormat
                                .format(
                                    "insert into `{0}` (order_id,seller_id) values ({1,number,#},{2,number,#})",
                                    TABLE_NAME, gened + 1000, gened % 100);
                        }

                        // Concurrent write.
                        try {
                            lock.readLock().lock();
                            JdbcUtil.executeUpdateSuccess(conn, insertSql);
                        } finally {
                            lock.readLock().unlock();
                        }

                        final int maxAvailableGenedId = idGen.get() - concurrentNumber; // Ignore max pending insertion.
                        if (maxAvailableGenedId > 1) {
                            final String cond = MessageFormat
                                .format(" where `order_id` < {0,number,#} order by `seller_id`",
                                    maxAvailableGenedId + 1000);
                            final String selectPrimaryCond = selectPrimary + cond;
                            final String selectGSICond = selectGSI + cond;

                            // Assert that go through primary or GSI.
                            Assert.assertFalse(getExplainResult(selectPrimaryCond).contains("IndexScan"));
                            Assert.assertTrue(getExplainResult(selectGSICond).contains("IndexScan"));

                            // Sequential read.
                            try {
                                lock.writeLock().lock();
                                selectContentSameAssert(selectPrimaryCond, selectGSICond, null, conn, conn);
                            } finally {
                                lock.writeLock().unlock();
                            }
                        }
                    }
                } catch (Throwable e) {
                    excps.add(e);
                }
            }));
        }

        runners.forEach(Thread::start);

        System.out.println("Start alter col default.");

        // Now alter col default.
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(alterDefaultTmpl, 2));

        System.out.println("Finish alter col default.");

        // Finish threads.
        exit.set(true);
        for (Thread t : runners) {
            t.join();
        }

        if (!excps.isEmpty()) {
            throw excps.get(0);
        }

        System.out.println(idGen.get() + " rows added.");

        // Assert that column default changed and covered in GSI.
        Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, TABLE_NAME).contains("DEFAULT '2'"));
        Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, CGSI_NAME).contains("DEFAULT '2'"));
        Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, UCGSI_NAME).contains("DEFAULT '2'"));

        // Assert that data identical.
        selectContentSameAssert(selectPrimary + " order by `seller_id`", selectGSI + " order by `seller_id`", null,
            tddlConnection, tddlConnection);

        System.out.println("done");
    }

    @Test
    public void dropColumnDefaultConcurrentReadWriteTest() throws Throwable {

        final int concurrentNumber = 4;
        final String selectGSI =
            MessageFormat.format("select * from `{0}` force index({1})", TABLE_NAME, CGSI_NAME);
        final String selectPrimary = MessageFormat
            .format("select * from `{0}` ignore index({1},{2})", TABLE_NAME, CGSI_NAME,
                UCGSI_NAME);

        // Set c0 initial default value. Init to 1.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format("alter table `{0}` alter `c0` set default {1}", TABLE_NAME, 1));

        final List<Thread> runners = new ArrayList<>();
        final AtomicBoolean exit = new AtomicBoolean(false);
        final AtomicInteger idGen = new AtomicInteger(1);
        final List<Throwable> excps = new CopyOnWriteArrayList<>();
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        // insert some data before alter to prevent empty records
        for (int i = 0; i < 10; ++i) {
            final int tmp = idGen.getAndIncrement();
            final String tmpSql;
            if (hasPk) {
                tmpSql = MessageFormat
                    .format(
                        "insert into `{0}` (pk,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#})",
                        TABLE_NAME, tmp, tmp + 1000, tmp % 100);
            } else {
                tmpSql = MessageFormat
                    .format(
                        "insert into `{0}` (order_id,seller_id) values ({1,number,#},{2,number,#})",
                        TABLE_NAME, tmp + 1000, tmp % 100);
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, tmpSql);
        }

        // insert & check threads.
        for (int i = 0; i < concurrentNumber; ++i) {
            runners.add(new Thread(() -> {
                boolean defaultDropped = false;

                try (Connection conn = getPolardbxDirectConnection()) {
                    while (!exit.get()) {
                        final String insertSql;
                        final int gened = idGen.getAndIncrement();

                        // Gen insert without c0.
                        if (hasPk) {
                            if (defaultDropped) {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` (pk,c0,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#},{4,number,#})",
                                        TABLE_NAME, gened, 3, gened + 1000, gened % 100);
                            } else {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` (pk,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#})",
                                        TABLE_NAME, gened, gened + 1000, gened % 100);
                            }
                        } else {
                            if (defaultDropped) {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` (c0,order_id,seller_id) values ({1,number,#},{2,number,#},{3,number,#})",
                                        TABLE_NAME, 3, gened + 1000, gened % 100);
                            } else {
                                insertSql = MessageFormat
                                    .format(
                                        "insert into `{0}` (order_id,seller_id) values ({1,number,#},{2,number,#})",
                                        TABLE_NAME, gened + 1000, gened % 100);
                            }
                        }

                        // Concurrent write.
                        try {
                            lock.readLock().lock();
                            JdbcUtil.executeUpdateSuccess(conn, insertSql);
                        } catch (Throwable t) {
                            if (t.getMessage().contains("Field 'c0' doesn't have a default value")) {
                                if (defaultDropped) {
                                    throw t;
                                }
                                defaultDropped = true;
                            } else {
                                throw t;
                            }
                        } finally {
                            lock.readLock().unlock();
                        }

                        final int maxAvailableGenedId = idGen.get() - concurrentNumber; // Ignore max pending insertion.
                        if (maxAvailableGenedId > 1) {
                            final String cond = MessageFormat
                                .format(" where `order_id` < {0,number,#} order by `seller_id`",
                                    maxAvailableGenedId + 1000);
                            final String selectPrimaryCond = selectPrimary + cond;
                            final String selectGSICond = selectGSI + cond;

                            // Assert that go through primary or GSI.
                            Assert.assertFalse(getExplainResult(selectPrimaryCond).contains("IndexScan"));
                            Assert.assertTrue(getExplainResult(selectGSICond).contains("IndexScan"));

                            // Sequential read.
                            try {
                                lock.writeLock().lock();
                                selectContentSameAssert(selectPrimaryCond, selectGSICond, null, conn, conn);
                            } finally {
                                lock.writeLock().unlock();
                            }
                        }
                    }
                } catch (Throwable e) {
                    excps.add(e);
                }
            }));
        }

        runners.forEach(Thread::start);

        System.out.println("Start alter col default.");

        // Now alter col default.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format("alter table `{0}` alter `c0` drop default", TABLE_NAME));

        System.out.println("Finish alter col default.");

        // Finish threads.
        exit.set(true);
        for (Thread t : runners) {
            t.join();
        }

        if (!excps.isEmpty()) {
            throw excps.get(0);
        }

        System.out.println(idGen.get() + " rows added.");

        // Assert that column default changed and covered in GSI.
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME).contains("`c0` int(11),"));
        Assert.assertTrue(showCreateTable(tddlConnection, CGSI_NAME).contains("`c0` int(11),"));
        Assert.assertTrue(showCreateTable(tddlConnection, UCGSI_NAME).contains("`c0` int(11),"));

        // Assert that data identical.
        selectContentSameAssert(selectPrimary + " order by `seller_id`", selectGSI + " order by `seller_id`", null,
            tddlConnection, tddlConnection);

        System.out.println("done");
    }

    // Set nullable to others and rollback(back to default null).
    @Test
    public void changeColumnDefaultRollbackTest0() throws Throwable {

        // Init default to null.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format("alter table `{0}` alter `c0` set default null", TABLE_NAME));

        // Note: This try 2 times to ensure correct sync of table meta and get the same error and rollback correctly.
        for (int i = 0; i < 2; ++i) {
            // Go bad alter.
            JdbcUtil.executeUpdateFailed(tddlConnection,
                MessageFormat.format("alter table `{0}` alter `c0` set default {1}", TABLE_NAME, "'hehe'"),
                "Not all physical DDLs have been executed successfully");

            // Assert that default not changed.
            Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME).contains("`c0` int(11) DEFAULT NULL,"));
            Assert.assertTrue(showCreateTable(tddlConnection, CGSI_NAME).contains("`c0` int(11) DEFAULT NULL,"));
            Assert.assertTrue(showCreateTable(tddlConnection, UCGSI_NAME).contains("`c0` int(11) DEFAULT NULL,"));
        }
    }

    // Set not nullable to others and rollback(back to no default).
    @Test
    public void changeColumnDefaultRollbackTest1() throws Throwable {

        // Init default to null.
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format("alter table `{0}` alter `t` drop default",
                TABLE_NAME)); // timestamp default not null if original default if current_timestamp.
        // Note: If original default if current_timestamp and we can't rollback because set default to current_timestamp is not allowed.

        // Note: This try 2 times to ensure correct sync of table meta and get the same error and rollback correctly.
        for (int i = 0; i < 2; ++i) {
            // Go bad alter.
            JdbcUtil.executeUpdateFailed(tddlConnection,
                MessageFormat.format("alter table `{0}` alter `t` set default {1}", TABLE_NAME, "'haha'"),
                "Not all physical DDLs have been executed successfully");

            // Assert that default not changed.
            Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, TABLE_NAME)
                .contains("`t` timestamp NOT NULL,"));
            Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, CGSI_NAME)
                .contains("`t` timestamp NOT NULL,"));
            Assert.assertTrue(JdbcUtil.showCreateTable(tddlConnection, UCGSI_NAME)
                .contains("`t` timestamp NOT NULL,"));
        }
    }

}

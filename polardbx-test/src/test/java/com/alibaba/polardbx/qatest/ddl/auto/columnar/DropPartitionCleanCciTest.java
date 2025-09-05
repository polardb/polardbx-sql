/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.DropPrimaryTblPartitionCleanColumnarDataTask;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;

@CdcIgnore(ignoreReason = "CDC 暂时不支持")
public class DropPartitionCleanCciTest extends DDLBaseNewDBTestCase {
    private static final String PRIMARY_TABLE_PREFIX = "drop_partition_cci_prim";
    private static final String INDEX_PREFIX = "drop_partition_cci_cci";
    private static final String PRIMARY_TABLE_NAME1 = PRIMARY_TABLE_PREFIX + "_1";
    private static final String INDEX_NAME1 = INDEX_PREFIX + "_1";
    private static final String PRIMARY_TABLE_NAME2 = PRIMARY_TABLE_PREFIX + "_2";
    private static final String INDEX_NAME2 = INDEX_PREFIX + "_2";
    private static final String PRIMARY_TABLE_NAME3 = PRIMARY_TABLE_PREFIX + "_3";
    private static final String INDEX_NAME3 = INDEX_PREFIX + "_2";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @After
    public void after() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @Test
    public void testDropPartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        final String creatTableTmpl = "CREATE TABLE `%s` (\n"
            + "    `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `user_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `instrument_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `partition_time` datetime(3) NOT NULL,\n"
            + "    `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `%s` (`create_time`, `instrument_id`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 64\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`user_id`)\n"
            + "PARTITIONS 4\n"
            + "SUBPARTITION BY RANGE(TO_DAYS(`partition_time`))\n"
            + "(SUBPARTITION `p202411` VALUES LESS THAN (739586),\n"
            + " SUBPARTITION `p202412` VALUES LESS THAN (739617),\n"
            + " SUBPARTITION `p202501` VALUES LESS THAN (739648),\n"
            + " SUBPARTITION `p202502` VALUES LESS THAN (739676))\n";
        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            PRIMARY_TABLE_NAME1,
            INDEX_NAME1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT INTO PARTITION p202502, 共插入5K行
        int batchSize = 1000;
        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间在 2025-02-01 ~ 2025-02-28 之间
                long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
                LocalDate randomDate = startDate.plusDays(random.nextInt((int) daysDiff));
                LocalTime randomTime = LocalTime.of(
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                );
                String partitionTime = LocalDateTime.of(randomDate, randomTime).toString();

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, partitionTime));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502");

        final List<CdcDdlRecord> cdcDdlRecordsPrim = queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME1);
        final List<CdcDdlRecord> cdcDdlRecordsShadow = queryDdlRecordBySchemaTable(tddlDatabase1,
            DropPrimaryTblPartitionCleanColumnarDataTask.getBlackHoleTableName(PRIMARY_TABLE_NAME1));

        Assert.assertEquals(cdcDdlRecordsPrim.get(0).ddlSql,
            "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502");
        Assert.assertEquals(cdcDdlRecordsShadow.get(0).ddlSql,
            "DROP TABLE IF EXISTS `" + DropPrimaryTblPartitionCleanColumnarDataTask.getBlackHoleTableName(
                PRIMARY_TABLE_NAME1) + "`");
        Assert.assertTrue(cdcDdlRecordsShadow.get(1).ddlSql.contains(
            DropPrimaryTblPartitionCleanColumnarDataTask.getBlackHoleTableName(PRIMARY_TABLE_NAME1))
            && cdcDdlRecordsShadow.get(1).ddlSql.contains("BLACKHOLE"));
    }

    @Test
    public void testDropPartitionMultiPk() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        final String creatTableTmpl = "CREATE TABLE `%s` (\n"
            + "    `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `user_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `instrument_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `partition_time` datetime(3) NOT NULL,\n"
            + "    `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "    PRIMARY KEY (`id`,`user_id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `%s` (`create_time`, `instrument_id`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 64\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`user_id`)\n"
            + "PARTITIONS 4\n"
            + "SUBPARTITION BY RANGE(TO_DAYS(`partition_time`))\n"
            + "(SUBPARTITION `p202411` VALUES LESS THAN (739586),\n"
            + " SUBPARTITION `p202412` VALUES LESS THAN (739617),\n"
            + " SUBPARTITION `p202501` VALUES LESS THAN (739648),\n"
            + " SUBPARTITION `p202502` VALUES LESS THAN (739676))\n";
        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            PRIMARY_TABLE_NAME1,
            INDEX_NAME1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT INTO PARTITION p202502, 共插入5K行
        int batchSize = 1000;
        for (int k = 0; k < 6; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间在 2025-02-01 ~ 2025-02-28 之间
                long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
                LocalDate randomDate = startDate.plusDays(random.nextInt((int) daysDiff));
                LocalTime randomTime = LocalTime.of(
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                );
                String partitionTime = LocalDateTime.of(randomDate, randomTime).toString();

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, partitionTime));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502");

        final List<CdcDdlRecord> cdcDdlRecordsPrim = queryDdlRecordBySchemaTable(tddlDatabase1, PRIMARY_TABLE_NAME1);
        final List<CdcDdlRecord> cdcDdlRecordsShadow = queryDdlRecordBySchemaTable(tddlDatabase1,
            DropPrimaryTblPartitionCleanColumnarDataTask.getBlackHoleTableName(PRIMARY_TABLE_NAME1));

        Assert.assertEquals(cdcDdlRecordsPrim.get(0).ddlSql,
            "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502");
        Assert.assertEquals(cdcDdlRecordsShadow.get(0).ddlSql,
            "DROP TABLE IF EXISTS `" + DropPrimaryTblPartitionCleanColumnarDataTask.getBlackHoleTableName(
                PRIMARY_TABLE_NAME1) + "`");
        Assert.assertTrue(cdcDdlRecordsShadow.get(1).ddlSql.contains(
            DropPrimaryTblPartitionCleanColumnarDataTask.getBlackHoleTableName(PRIMARY_TABLE_NAME1))
            && cdcDdlRecordsShadow.get(1).ddlSql.contains("BLACKHOLE"));
    }

    @Test
    public void testDropPartitionFailed() throws SQLException, InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_DROP_TRUNCATE_CCI_PARTITION = TRUE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_SHADOW_INSERT_ON_DROP_PARTITION = TRUE");

        final String creatTableTmpl = "CREATE TABLE `%s` (\n"
            + "    `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `user_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `instrument_id` bigint NOT NULL DEFAULT '0',\n"
            + "    `partition_time` datetime(3) NOT NULL,\n"
            + "    `create_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    CLUSTERED COLUMNAR INDEX `%s` (`create_time`, `instrument_id`)\n"
            + "        PARTITION BY HASH(`id`)\n"
            + "        PARTITIONS 64\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`user_id`)\n"
            + "PARTITIONS 4\n"
            + "SUBPARTITION BY RANGE(TO_DAYS(`partition_time`))\n"
            + "(SUBPARTITION `p202411` VALUES LESS THAN (739586),\n"
            + " SUBPARTITION `p202412` VALUES LESS THAN (739617),\n"
            + " SUBPARTITION `p202501` VALUES LESS THAN (739648),\n"
            + " SUBPARTITION `p202502` VALUES LESS THAN (739676))\n";
        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            PRIMARY_TABLE_NAME1,
            INDEX_NAME1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // INSERT INTO PARTITION p202502, 共插入5K行
        int batchSize = 1000;
        for (int k = 0; k < 10; k++) {
            StringBuilder insertSql = new StringBuilder();
            insertSql.append("INSERT INTO `").append(PRIMARY_TABLE_NAME1).append("` ")
                .append("(`user_id`, `instrument_id`, `partition_time`) VALUES ");

            Random random = new Random();

            LocalDate startDate = LocalDate.of(2025, 2, 1);
            LocalDate endDate = LocalDate.of(2025, 3, 1);

            for (int i = 0; i < batchSize; i++) {
                long userId = 1000 + random.nextInt(100);
                long instrumentId = 2000 + random.nextInt(200);

                // 随机时间在 2025-02-01 ~ 2025-02-28 之间
                long daysDiff = ChronoUnit.DAYS.between(startDate, endDate);
                LocalDate randomDate = startDate.plusDays(random.nextInt((int) daysDiff));
                LocalTime randomTime = LocalTime.of(
                    random.nextInt(24),
                    random.nextInt(60),
                    random.nextInt(60)
                );
                String partitionTime = LocalDateTime.of(randomDate, randomTime).toString();

                insertSql.append(String.format("(%d, %d, '%s')", userId, instrumentId, partitionTime));

                if (i < batchSize - 1) {
                    insertSql.append(",");
                }
                insertSql.append("\n");
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql.toString());
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_SIZE = 100");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET SHADOW_INSERT_BATCH_INTERVAL = 1000");

        String sql = "ALTER TABLE `" + PRIMARY_TABLE_NAME1 + "` DROP SUBPARTITION p202502 ASYNC = TRUE";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Thread.sleep(20000);

        Long jobId = DdlStateCheckUtil.getRootDdlJobIdFromPattern(tddlConnection, sql);
        if (jobId == -1) {
            return;
        }
        DdlStateCheckUtil.pauseDdl(tddlConnection, jobId);

        Thread.sleep(2000);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "rollback ddl " + jobId);

        ResultSet rs = JdbcUtil.executeQuery(
            "show tables like ' " + DropPrimaryTblPartitionCleanColumnarDataTask.getBlackHoleTableName(
                PRIMARY_TABLE_NAME1) + "'", tddlConnection);
        // Empty
        Assert.assertFalse(rs.next());
    }
}

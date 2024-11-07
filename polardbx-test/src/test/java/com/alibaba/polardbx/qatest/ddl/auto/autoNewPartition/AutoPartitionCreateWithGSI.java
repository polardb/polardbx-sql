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
import org.junit.Test;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;

/**
 * @version 1.0
 */
public class AutoPartitionCreateWithGSI extends BaseAutoPartitionNewPartition {

    private static final String TABLE_NAME = "auto_with_gsi";
    private static final String INDEX_NAME = "i_auto_with_gsi";

    @Before
    public void before() {

        dropTableWithGsi(TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    @After
    public void after() {

        dropTableWithGsi(TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    @Test
    public void createWithGSITest() {

        final String createTable = "CREATE TABLE {0} (\n"
            + "  `x` int,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  global index {1} (`seller_id`)\n"
            + ");";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createTable, TABLE_NAME, INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_with_gsi` (\n"
            + "\t`x` int(11) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tINDEX `i_auto_with_gsi` (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `i_auto_with_gsi_$` (\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `auto_shard_key_seller_id` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME)
                .replaceAll("_\\$[0-9a-f]{4}", Matcher.quoteReplacement("_$")));
    }

    @Test
    public void createWithUGSITest() {

        final String createTable = "CREATE TABLE {0} (\n"
            + "  `x` int,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  unique global index {1} (`seller_id`)\n"
            + ");";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createTable, TABLE_NAME, INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_with_gsi` (\n"
            + "\t`x` int(11) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tUNIQUE INDEX `i_auto_with_gsi` (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `i_auto_with_gsi_$` (\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tUNIQUE KEY `auto_shard_key_seller_id` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME)
                .replaceAll("_\\$[0-9a-f]{4}", Matcher.quoteReplacement("_$")));
    }

    @Test
    public void createWithCGSITest() {

        final String createTable = "CREATE TABLE {0} (\n"
            + "  `x` int,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  clustered index {1} (`seller_id`)\n"
            + ");";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createTable, TABLE_NAME, INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_with_gsi` (\n"
            + "\t`x` int(11) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `i_auto_with_gsi` (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `i_auto_with_gsi_$` (\n"
                + "\t`x` int(11) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_i_auto_with_gsi` (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME)
                .replaceAll("_\\$[0-9a-f]{4}", Matcher.quoteReplacement("_$")));
    }

    @Test
    public void createWithUCGSITest() {

        final String createTable = "CREATE TABLE {0} (\n"
            + "  `x` int,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  unique clustered index {1} (`seller_id`)\n"
            + ");";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createTable, TABLE_NAME, INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_with_gsi` (\n"
            + "\t`x` int(11) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tUNIQUE CLUSTERED INDEX `i_auto_with_gsi` (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `i_auto_with_gsi_$` (\n"
                + "\t`x` int(11) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tUNIQUE KEY `_local_i_auto_with_gsi` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME)
                .replaceAll("_\\$[0-9a-f]{4}", Matcher.quoteReplacement("_$")));
    }

    @Test
    public void testCreateIndexDoubleDrop0() {

        final String primaryTable = "t_idx_order";
        final String indexTable = "g_i_idx_seller";

        dropTableWithGsi(primaryTable, ImmutableList.of());

        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`)\n"
                + ");",
            primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "CREATE INDEX `%s` on `%s`(`seller_id`);",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Double drop.
        sql = String.format(
            "/*+TDDL: cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/\n"
                + "drop index `%s` on `%s`;",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Wait tail task all finished.
        final long start = System.currentTimeMillis();
        List<Map<String, String>> full_ddl = null;
        while (System.currentTimeMillis() - start < 20_000) {
            full_ddl = showFullDDL();
            Optional<Map<String, String>> childJobOp = full_ddl.stream()
                .filter(m -> m.get("OBJECT_NAME").equals(indexTable))
                .findFirst();

            if (!childJobOp.isPresent()) {
                break;
            }
        }
//        com.taobao.tddl.common.utils.Assert.assertTrue(full_ddl != null && full_ddl.isEmpty(), full_ddl.toString());

        // Assert that global index dropped.
        sql = String.format("show global index from `%s`", primaryTable);
        final List<List<Object>> result = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection));
        com.alibaba.polardbx.common.utils.Assert.assertTrue(result.isEmpty());

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }

    @Test
    public void testCreateIndexDoubleDrop1() {

        final String primaryTable = "t_idx_order";
        final String indexTable = "g_i_idx_seller";

        dropTableWithGsi(primaryTable, ImmutableList.of());

        String sql = String.format("CREATE TABLE `%s` (\n"
                + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `x` int,\n"
                + "  `order_id` varchar(20) DEFAULT NULL,\n"
                + "  `seller_id` varchar(20) DEFAULT NULL,\n"
                + "  PRIMARY KEY (`id`)\n"
                + ");",
            primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "CREATE INDEX `%s` on `%s`(`seller_id`);",
            indexTable, primaryTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Double drop.
        sql = String.format(
            "/*+TDDL: cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true)*/\n"
                + "alter table `%s` drop index `%s`;",
            primaryTable, indexTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Wait tail task all finished.
        final long start = System.currentTimeMillis();
        List<Map<String, String>> full_ddl = null;
        while (System.currentTimeMillis() - start < 20_000) {
            full_ddl = showFullDDL();
            Optional<Map<String, String>> childJobOp = full_ddl.stream()
                .filter(m -> m.get("OBJECT_NAME").equals(indexTable))
                .findFirst();

            if (!childJobOp.isPresent()) {
                break;
            }
        }
//        com.taobao.tddl.common.utils.Assert.assertTrue(full_ddl != null && full_ddl.isEmpty(), full_ddl.toString());

        // Assert that global index dropped.
        sql = String.format("show global index from `%s`", primaryTable);
        final List<List<Object>> result = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection));
        com.alibaba.polardbx.common.utils.Assert.assertTrue(result.isEmpty());

        dropTableWithGsi(primaryTable, ImmutableList.of());
    }
}

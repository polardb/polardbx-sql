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

package com.alibaba.polardbx.qatest.ddl.sharding.autoPartition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.text.MessageFormat;
import java.util.List;

/**
 * @version 1.0
 */

public class AutoPartitionAddIndexTest extends AutoPartitionTestBase {

    private String TABLE_NAME = "auto_partition_add_idx_tb";
    private String INDEX_NAME = "ap_add_idx";
    private static final String CREATE_TABLE_NO_PK_TMPL = "CREATE PARTITION TABLE `{0}` (\n"
        + "  `t` timestamp null default CURRENT_TIMESTAMP,\n"
        + "  `x` int default 3,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  LOCAL INDEX `l_seller` using btree (`seller_id`),\n"
        + "  UNIQUE LOCAL INDEX `l_order` using btree (`order_id`)"
        + ")";
    private static final String CREATE_TABLE_PK_TMPL = "CREATE PARTITION TABLE `{0}` (\n"
        + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `t` timestamp null default CURRENT_TIMESTAMP,\n"
        + "  `x` int default 3,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  LOCAL INDEX `l_seller` using btree (`seller_id`),\n"
        + "  UNIQUE LOCAL INDEX `l_order` using btree (`order_id`),"
        + "  PRIMARY KEY (`pk`)\n"
        + ")";

    @Parameterized.Parameters(name = "{index}:createTable={0}")
    public static List<String[]> prepareDate() {
        return ImmutableList.of(new String[] {CREATE_TABLE_PK_TMPL}, new String[] {CREATE_TABLE_NO_PK_TMPL});
    }

    private final String createTableStmt;
    private final boolean hasPk;

    public AutoPartitionAddIndexTest(String createTable) {
        this.createTableStmt = createTable;
        this.hasPk = createTable.contains("PRIMARY KEY");
        this.TABLE_NAME = randomTableName(TABLE_NAME, 3);
        this.INDEX_NAME = randomTableName(INDEX_NAME, 3);
    }

    @Before
    public void before() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createTableStmt, TABLE_NAME));
    }

    @After
    public void after() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    @Test
    public void createLocalIndexTest0() {

        final String sql =
            MessageFormat.format("create local index `{0}` on `{1}` (`order_id`)", INDEX_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Assert that only local index.
        if (hasPk) {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `" + INDEX_NAME + "` (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`pk`)",
                showCreateTable(tddlConnection, TABLE_NAME));
        } else {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `" + INDEX_NAME + "` (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ",
                showCreateTable(tddlConnection, TABLE_NAME));
        }
    }

    @Test
    public void createLocalIndexTest1() {

        final String sql =
            MessageFormat.format("alter table `{0}` add local index `{1}` (`order_id`)", TABLE_NAME, INDEX_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Assert that only local index.
        if (hasPk) {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `" + INDEX_NAME + "` (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`pk`)",
                showCreateTable(tddlConnection, TABLE_NAME));
        } else {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `" + INDEX_NAME + "` (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ",
                showCreateTable(tddlConnection, TABLE_NAME));
        }
    }

    @Test
    public void createGlobalIndexTest0() {

        final String sql =
            MessageFormat.format("create global index `{0}` on `{1}` (`order_id`)", INDEX_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Assert that only global index.
        if (hasPk) {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tGLOBAL INDEX `" + INDEX_NAME + "`(`order_id`) COVERING (`pk`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`pk`)",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL,\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        } else {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tGLOBAL INDEX `" + INDEX_NAME + "`(`order_id`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        }
    }

    @Test
    public void createGlobalIndexTest1() {

        final String sql =
            MessageFormat.format("alter table `{0}` add global index `{1}` (`order_id`)", TABLE_NAME, INDEX_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Assert that only clustered index.
        if (hasPk) {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tGLOBAL INDEX `" + INDEX_NAME + "`(`order_id`) COVERING (`pk`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`pk`)",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL,\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        } else {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tGLOBAL INDEX `" + INDEX_NAME + "`(`order_id`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        }
    }

    @Test
    public void createClusteredIndexTest0() {

        final String sql =
            MessageFormat.format("create clustered index `{0}` on `{1}` (`order_id`)", INDEX_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Assert that only clustered index.
        if (hasPk) {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tCLUSTERED INDEX `" + INDEX_NAME + "`(`order_id`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`pk`)",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tKEY `l_seller` USING BTREE (`seller_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        } else {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tCLUSTERED INDEX `" + INDEX_NAME + "`(`order_id`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tKEY `l_seller` USING BTREE (`seller_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        }
    }

    @Test
    public void createClusteredIndexTest1() {

        final String sql =
            MessageFormat.format("alter table `{0}` add clustered index `{1}` (`order_id`)", TABLE_NAME, INDEX_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Assert that only clustered index.
        if (hasPk) {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tCLUSTERED INDEX `" + INDEX_NAME + "`(`order_id`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`pk`)",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tKEY `l_seller` USING BTREE (`seller_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        } else {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tCLUSTERED INDEX `" + INDEX_NAME + "`(`order_id`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tKEY `l_seller` USING BTREE (`seller_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        }
    }

    @Test
    public void createIndexTest0() {

        final String sql =
            MessageFormat.format("create index `{0}` on `{1}` (`order_id`)", INDEX_NAME, TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Assert that only global index.
        if (hasPk) {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tGLOBAL INDEX `" + INDEX_NAME + "`(`order_id`) COVERING (`pk`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`pk`)",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL,\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        } else {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tGLOBAL INDEX `" + INDEX_NAME + "`(`order_id`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        }
    }

    @Test
    public void createIndexTest1() {

        final String sql =
            MessageFormat.format("alter table `{0}` add index `{1}` (`order_id`)", TABLE_NAME, INDEX_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Assert that only global index.
        if (hasPk) {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tGLOBAL INDEX `" + INDEX_NAME + "`(`order_id`) COVERING (`pk`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`pk`)",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`pk` bigint(11) NOT NULL,\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\tPRIMARY KEY (`pk`),\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        } else {
            Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
                    + "\t`t` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n"
                    + "\t`x` int(11) DEFAULT '3',\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE LOCAL KEY `l_order` USING BTREE (`order_id`),\n"
                    + "\tLOCAL KEY `l_seller` USING BTREE (`seller_id`),\n"
                    + "\tLOCAL KEY `_local_" + INDEX_NAME + "` (`order_id`),\n"
                    + "\tGLOBAL INDEX `" + INDEX_NAME + "`(`order_id`) DBPARTITION BY HASH(`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ",
                showCreateTable(tddlConnection, TABLE_NAME));

            Assert.assertEquals("CREATE TABLE `" + INDEX_NAME + "` (\n"
                    + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                    + "\tUNIQUE KEY `l_order` USING BTREE (`order_id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`order_id`)",
                showCreateTable(tddlConnection, INDEX_NAME));
        }
    }

}

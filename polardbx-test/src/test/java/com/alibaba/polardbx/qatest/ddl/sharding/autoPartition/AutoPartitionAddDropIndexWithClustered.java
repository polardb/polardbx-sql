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

import java.text.MessageFormat;

/**
 * @version 1.0
 */
public class AutoPartitionAddDropIndexWithClustered extends AutoPartitionTestBase {

    private final String TABLE_NAME = "auto_idx_with_clustered_o";
    private final String INDEX_NAME = "c_i_idx_with_clustered_o";
    private final String LOCAL_INDEX_NAME = "l_i_idx_with_clustered";

    private static final String CREATE_TABLE = "CREATE PARTITION TABLE {0} (\n"
        + "  `t` timestamp default CURRENT_TIMESTAMP,\n"
        + "  `x` int default 3,\n"
        + "  `y` int default null,\n"
        + "  `z` int null,\n"
        + "  `i` int not null,\n"
        + "  `j` decimal(6,2),\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  CLUSTERED INDEX {1} using btree (`seller_id`) dbpartition by hash(`seller_id`)\n"
        + ");";

    @Before
    public void before() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(CREATE_TABLE, TABLE_NAME, INDEX_NAME));
    }

    @After
    public void after() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    @Test
    public void createDropIndexTest() {
        final String createIndex = "create local index {0} on {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `l_i_idx_with_clustered` (`i`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
                + "\tLOCAL KEY `l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));

        final String dropIndex = "drop index {0} on {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(dropIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));
    }

    @Test
    public void alterTableAddDropIndexTest() {
        final String createIndex = "alter table {0} add local index {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `l_i_idx_with_clustered` (`i`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
                + "\tLOCAL KEY `l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));

        final String dropIndex = "alter table {0} drop index {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(dropIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));
    }

    @Test
    public void createDropAutoIndexTest() {
        final String createIndex = "create index {0} on {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`),\n"
            + "\tGLOBAL INDEX `l_i_idx_with_clustered`(`i`) DBPARTITION BY HASH(`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
                + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));

        final String dropIndex = "drop index {0} on {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(dropIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));
    }

    @Test
    public void alterTableAddDropAutoIndexTest() {
        final String createIndex = "alter table {0} add index {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`),\n"
            + "\tGLOBAL INDEX `l_i_idx_with_clustered`(`i`) DBPARTITION BY HASH(`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
                + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));

        final String dropIndex = "alter table {0} drop index {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(dropIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));
    }

    @Test
    public void createDropGsiTest() {
        final String createIndex = "create global index {0} on {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`),\n"
            + "\tGLOBAL INDEX `l_i_idx_with_clustered`(`i`) DBPARTITION BY HASH(`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
                + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));

        final String dropIndex = "drop index {0} on {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(dropIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));
    }

    @Test
    public void alterTableAddDropGsiTest() {
        final String createIndex = "alter table {0} add global index {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`),\n"
            + "\tGLOBAL INDEX `l_i_idx_with_clustered`(`i`) DBPARTITION BY HASH(`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
                + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));

        final String dropIndex = "alter table {0} drop index {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(dropIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));
    }

    @Test
    public void createDropClusteredTest() {
        final String createIndex = "create clustered index {0} on {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`),\n"
            + "\tCLUSTERED INDEX `l_i_idx_with_clustered`(`i`) DBPARTITION BY HASH(`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
                + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));

        final String dropIndex = "drop index {0} on {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(dropIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));
    }

    @Test
    public void alterTableAddDropClusteredTest() {
        final String createIndex = "alter table {0} add clustered index {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(createIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`),\n"
            + "\tCLUSTERED INDEX `l_i_idx_with_clustered`(`i`) DBPARTITION BY HASH(`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
                + "\tLOCAL KEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));

        final String dropIndex = "alter table {0} drop index {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(dropIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE PARTITION TABLE `" + TABLE_NAME + "` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `" + INDEX_NAME + "` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  ", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE PARTITION TABLE `" + INDEX_NAME + "` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tLOCAL KEY `_local_" + INDEX_NAME + "` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`seller_id`)",
            showCreateTable(tddlConnection, INDEX_NAME));
    }

}

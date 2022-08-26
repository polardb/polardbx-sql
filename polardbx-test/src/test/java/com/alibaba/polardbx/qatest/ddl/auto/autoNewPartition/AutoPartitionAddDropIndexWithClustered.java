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
import java.util.regex.Matcher;

/**
 * @version 1.0
 */
public class AutoPartitionAddDropIndexWithClustered extends BaseAutoPartitionNewPartition {

    private static final String TABLE_NAME = "auto_idx_with_clustered";
    private static final String INDEX_NAME = "c_i_idx_with_clustered";
    private static final String LOCAL_INDEX_NAME = "l_i_idx_with_clustered";

    private static final String CREATE_TABLE = "CREATE TABLE {0} (\n"
        + "  `t` timestamp default CURRENT_TIMESTAMP,\n"
        + "  `x` int default 3,\n"
        + "  `y` int default null,\n"
        + "  `z` int null,\n"
        + "  `i` int not null,\n"
        + "  `j` decimal(6,2),\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  CLUSTERED INDEX {1} using btree (`seller_id`)\n"
        + ");";

    @Before
    public void before() {

        dropTableWithGsi(TABLE_NAME, ImmutableList.of(INDEX_NAME));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(CREATE_TABLE, TABLE_NAME, INDEX_NAME));
    }

    @After
    public void after() {

        dropTableWithGsi(TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    @Test
    public void createDropIndexTest() {

        final String createIndex = "create local index {0} on {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `l_i_idx_with_clustered` (`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
                + "\tKEY `l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));

        final String dropIndex = "drop index {0} on {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(dropIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));
    }

    @Test
    public void alterTableAddDropIndexTest() {

        final String createIndex = "alter table {0} add local index {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
            + "\tLOCAL KEY `l_i_idx_with_clustered` (`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
                + "\tKEY `l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));

        final String dropIndex = "alter table {0} drop index {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(dropIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));
    }

    @Test
    public void createDropAutoIndexTest() {

        final String createIndex = "create index {0} on {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
            + "\tINDEX `l_i_idx_with_clustered` (`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
                + "\tKEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));

        final String dropIndex = "drop index {0} on {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(dropIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));
    }

    @Test
    //慢
    public void alterTableAddDropAutoIndexTest() {

        final String createIndex = "alter table {0} add index {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
            + "\tINDEX `l_i_idx_with_clustered` (`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
                + "\tKEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));

        final String dropIndex = "alter table {0} drop index {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(dropIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));
    }

    @Test
    public void createDropGsiTest() {

        final String createIndex = "create global index {0} on {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
            + "\tINDEX `l_i_idx_with_clustered` (`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
                + "\tKEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));

        final String dropIndex = "drop index {0} on {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(dropIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));
    }

    @Test
    public void alterTableAddDropGsiTest() {

        final String createIndex = "alter table {0} add global index {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
            + "\tINDEX `l_i_idx_with_clustered` (`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
                + "\tKEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));

        final String dropIndex = "alter table {0} drop index {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(dropIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));
    }

    @Test
    public void createDropClusteredTest() {

        final String createIndex = "create clustered index {0} on {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `l_i_idx_with_clustered` (`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
                + "\tKEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));

        final String dropIndex = "drop index {0} on {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(dropIndex, LOCAL_INDEX_NAME, TABLE_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));
    }

    @Test
    public void alterTableAddDropClusteredTest() {

        final String createIndex = "alter table {0} add clustered index {1} (i)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(createIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
            + "\tCLUSTERED INDEX `l_i_idx_with_clustered` (`i`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`),\n"
                + "\tKEY `_local_l_i_idx_with_clustered` (`i`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));

        final String dropIndex = "alter table {0} drop index {1}";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(dropIndex, TABLE_NAME, LOCAL_INDEX_NAME));

        Assert.assertEquals("CREATE TABLE `auto_idx_with_clustered` (\n"
            + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\t`x` int(11) DEFAULT '3',\n"
            + "\t`y` int(11) DEFAULT NULL,\n"
            + "\t`z` int(11) DEFAULT NULL,\n"
            + "\t`i` int(11) NOT NULL,\n"
            + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\tCLUSTERED INDEX `c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4", showCreateTable(tddlConnection, TABLE_NAME));
        Assert.assertEquals("CREATE TABLE `c_i_idx_with_clustered_$` (\n"
                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
                + "\t`x` int(11) DEFAULT '3',\n"
                + "\t`y` int(11) DEFAULT NULL,\n"
                + "\t`z` int(11) DEFAULT NULL,\n"
                + "\t`i` int(11) NOT NULL,\n"
                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
                + "\tKEY `_local_c_i_idx_with_clustered` USING BTREE (`seller_id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`seller_id`,`_drds_implicit_id_`)\n"
                + "PARTITIONS 3",
            showCreateTable(tddlConnection, INDEX_NAME).replaceAll("_\\$[0-9a-f]{4}",
                Matcher.quoteReplacement("_$")));
    }

}

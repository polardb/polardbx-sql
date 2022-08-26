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
import org.junit.Assert;
import org.junit.Test;

/**
 * @version 1.0
 */
public class AutoPartitionShowCreateTest extends BaseAutoPartitionNewPartition {

    private static final String TABLE_NAME = "auto_partition_show_create";

    @Test
    //慢
    public void testCreateWithShowCreateTableNoPK() {

        dropTableIfExists(TABLE_NAME);

        // Create partition table.
        final String createTable = "CREATE TABLE " + quoteSpecialName(TABLE_NAME) + " (\n"
            + "  `x` int,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  LOCAL INDEX `l_seller` using btree (`seller_id`), -- 强制指定为本地索引\n"
            + "  UNIQUE LOCAL INDEX `l_order` using btree (`order_id`), -- 强制指定为本地唯一索引\n"
            + "  INDEX `i_seller` using btree (`seller_id`), -- 会被替换为GSI，自动拆分\n"
            + "  UNIQUE INDEX `i_order` using btree (`order_id`), -- 会被替换为UGSI，自动拆分\n"
            + "  GLOBAL INDEX `g_seller` using btree (`seller_id`), -- 自动拆分\n"
            + "  UNIQUE GLOBAL INDEX `g_order` using btree (`order_id`), -- 自动拆分\n"
            + "  CLUSTERED INDEX `c_seller` using btree (`seller_id`), -- 自动拆分聚簇\n"
            + "  UNIQUE CLUSTERED INDEX `c_order` using btree (`order_id`) -- 自动拆分聚簇\n"
            + ");";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        final String showCreate = showCreateTable(tddlConnection, quoteSpecialName(TABLE_NAME));
        System.out.println("show create: " + showCreate);
        dropTableIfExists(TABLE_NAME);

        // Recreate and assert that table same.
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        final String newShowCreate = showCreateTable(tddlConnection, quoteSpecialName(TABLE_NAME));
        Assert.assertEquals(showCreate, newShowCreate);

        dropTableIfExists(quoteSpecialName(TABLE_NAME));
    }

    @Test
    public void testCreateWithShowCreateTablePK() {

        dropTableIfExists(TABLE_NAME);

        // Create partition table.
        final String createTable = "CREATE TABLE " + quoteSpecialName(TABLE_NAME) + " (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `x` int,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  LOCAL INDEX `l_seller` using btree (`seller_id`), -- 强制指定为本地索引\n"
            + "  UNIQUE LOCAL INDEX `l_order` using btree (`order_id`), -- 强制指定为本地唯一索引\n"
            + "  INDEX `i_seller` using btree (`seller_id`), -- 会被替换为GSI，自动拆分\n"
            + "  UNIQUE INDEX `i_order` using btree (`order_id`), -- 会被替换为UGSI，自动拆分\n"
            + "  GLOBAL INDEX `g_seller` using btree (`seller_id`), -- 自动拆分\n"
            + "  UNIQUE GLOBAL INDEX `g_order` using btree (`order_id`), -- 自动拆分\n"
            + "  CLUSTERED INDEX `c_seller` using btree (`seller_id`), -- 自动拆分聚簇\n"
            + "  UNIQUE CLUSTERED INDEX `c_order` using btree (`order_id`) -- 自动拆分聚簇\n"
            + ");";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        final String showCreate = showCreateTable(tddlConnection, quoteSpecialName(TABLE_NAME));
        System.out.println("show create: " + showCreate);
        dropTableIfExists(TABLE_NAME);

        // Recreate and assert that table same.
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        final String newShowCreate = showCreateTable(tddlConnection, quoteSpecialName(TABLE_NAME));
        Assert.assertEquals(showCreate, newShowCreate);

        dropTableIfExists(quoteSpecialName(TABLE_NAME));
    }

    @Test
    public void testCreateWithShowCreateTableLocalPrefix0() {

        dropTableIfExists(TABLE_NAME);

        // Create partition table.
        final String createTable = "CREATE PARTITION TABLE " + quoteSpecialName(TABLE_NAME) + " (\n"
            + "        `pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "        `c0` int(11) DEFAULT NULL,\n"
            + "        `t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "        `order_id` int(11) DEFAULT NULL,\n"
            + "        `seller_id` int(11) DEFAULT NULL,\n"
            + "        PRIMARY KEY (`pk`),\n"
            + "        UNIQUE LOCAL KEY `_local_cug_i_ap_col` USING BTREE (`order_id`),\n"
            + "        LOCAL KEY `_local_cg_i_ap_col` USING BTREE (`seller_id`),\n"
            + "        CLUSTERED INDEX `cg_i_ap_col` USING BTREE(`seller_id`) partition BY key(`seller_id`),\n"
            + "        UNIQUE CLUSTERED KEY `cug_i_ap_col` USING BTREE (`order_id`) partition BY key(`order_id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 partition by hash(`pk`);";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        final String showCreate = showCreateTable(tddlConnection, quoteSpecialName(TABLE_NAME));
        System.out.println("show create: " + showCreate);
        dropTableIfExists(TABLE_NAME);

        // Recreate and assert that table same.
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        final String newShowCreate = showCreateTable(tddlConnection, quoteSpecialName(TABLE_NAME));
        Assert.assertEquals(showCreate, newShowCreate);

        dropTableIfExists(quoteSpecialName(TABLE_NAME));
    }

    @Test
    public void testCreateWithShowCreateTableLocalPrefix1() {

        dropTableIfExists(TABLE_NAME);

        // Create partition table.
        final String createTable = "CREATE TABLE " + quoteSpecialName(TABLE_NAME)
            + " (id int primary key,name varchar(128), key idx1 using btree (name));";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        final String showCreate = showCreateTable(tddlConnection, quoteSpecialName(TABLE_NAME));
        System.out.println("show create: " + showCreate);
        dropTableIfExists(TABLE_NAME);

        // Recreate and assert that table same.
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        final String newShowCreate = showCreateTable(tddlConnection, quoteSpecialName(TABLE_NAME));
        Assert.assertEquals(showCreate, newShowCreate);

        dropTableIfExists(quoteSpecialName(TABLE_NAME));
    }

}

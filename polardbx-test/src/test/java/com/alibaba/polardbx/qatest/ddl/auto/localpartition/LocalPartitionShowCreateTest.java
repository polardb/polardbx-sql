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

package com.alibaba.polardbx.qatest.ddl.auto.localpartition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class LocalPartitionShowCreateTest extends LocalPartitionBaseTest {

    private String primaryTableName;
    private String gsi1TableName;
    private String ugsi1TableName;
    private String cgsi1TableName;
    private String ucgsi1TableName;

    @Before
    public void before() {
        primaryTableName = randomTableName("t_create", 4);
        gsi1TableName = randomTableName("gsi1_create", 4);
        ugsi1TableName = randomTableName("ugsi1_create", 4);
        cgsi1TableName = randomTableName("cgsi1_create", 4);
        ucgsi1TableName = randomTableName("ucgsi1_create", 4);
    }

    @After
    public void after() {

    }

    @Test
    @Ignore("fix by ???")
    public void testShowCreateLocalPartition1() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        String showCreateTableInfo = showCreateTable(tddlConnection, primaryTableName);
        Assert.assertEquals(
            "CREATE TABLE `" + primaryTableName + "` (\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`gmt_modified` datetime NOT NULL,\n"
                + "\tPRIMARY KEY (`gmt_modified`),\n"
                + "\tKEY `auto_shard_key_c1` USING BTREE (`c1`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_0900_ai_ci\n"
                + "PARTITION BY KEY(`c1`)\n"
                + "PARTITIONS 4\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER 12\n"
                + "PRE ALLOCATE 6\n"
                + "PIVOTDATE NOW()\n",
            showCreateTableInfo
        );
    }

    @Test
    public void testShowCreateLocalPartition2() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + ";", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        String showCreateTableInfo = showCreateTable(tddlConnection, primaryTableName);
        Assert.assertEquals(
            "CREATE TABLE `" + primaryTableName + "` (\n"
                + "\t`c1` bigint(20) DEFAULT NULL,\n"
                + "\t`c2` bigint(20) DEFAULT NULL,\n"
                + "\t`c3` bigint(20) DEFAULT NULL,\n"
                + "\t`gmt_modified` datetime NOT NULL,\n"
                + "\tPRIMARY KEY (`gmt_modified`),\n"
                + "\tKEY `auto_shard_key_c1` USING BTREE (`c1`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`c1`)\n"
                + "PARTITIONS 4\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER -1\n"
                + "PRE ALLOCATE 3\n"
                + "PIVOTDATE NOW()\n",
            showCreateTableInfo
        );
    }
}
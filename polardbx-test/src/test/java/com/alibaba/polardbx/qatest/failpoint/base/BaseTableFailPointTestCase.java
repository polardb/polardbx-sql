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

package com.alibaba.polardbx.qatest.failpoint.base;

import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.text.MessageFormat;

public abstract class BaseTableFailPointTestCase extends BaseFailPointTestCase {

    protected static final String EXTENDED_SINGLE = "";
    protected static final String EXTENDED_BROADCAST = "broadcast";
    protected static final String EXTENDED_SHARDING = "dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";

    protected static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE `{0}` (\n"
        + "  `id` bigint NOT NULL,\n"
        + "  `c_bit_1` bit(1) DEFAULT NULL,\n"
        + "  `c_tinyint_4` tinyint(4) DEFAULT NULL,\n"
        + "  `c_smallint_16` smallint(16) DEFAULT NULL,\n"
        + "  `c_mediumint_24` mediumint(24) DEFAULT NULL,\n"
        + "  `c_int` int DEFAULT NULL,\n"
        + "  `c_bigint` bigint DEFAULT NULL,\n"
        + "  `c_decimal` decimal DEFAULT NULL,\n"
        + "  `c_float` float DEFAULT NULL,\n"
        + "  `c_double` double DEFAULT NULL,\n"
        + "  `c_date` date DEFAULT NULL COMMENT \"date\",\n"
        + "  `c_datetime` datetime DEFAULT NULL,\n"
        + "  `c_timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,\n"
        + "  `c_time` time DEFAULT NULL,\n"
        + "  `c_year` year DEFAULT NULL,\n"
        + "  `c_char` char(10) DEFAULT NULL,\n"
        + "  `c_varchar` varchar(10) DEFAULT NULL,\n"
        + "  `c_binary` binary(10) DEFAULT NULL,\n"
        + "  `c_varbinary` varbinary(10) DEFAULT NULL,\n"
        + "  `c_blob_tiny` tinyblob DEFAULT NULL,\n"
        + "  `c_blob` blob DEFAULT NULL,\n"
        + "  `c_blob_medium` mediumblob DEFAULT NULL,\n"
        + "  `c_blob_long` longblob DEFAULT NULL,\n"
        + "  `c_text_tiny` tinytext DEFAULT NULL,\n"
        + "  `c_text` text DEFAULT NULL,\n"
        + "  `c_text_medium` mediumtext DEFAULT NULL,\n"
        + "  `c_text_long` longtext DEFAULT NULL,\n"
        + "  `c_enum` enum(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
        + "  `c_set` set(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
        + "  `c_json` json DEFAULT NULL,\n"
        + "  `c_geometory` geometry DEFAULT NULL,\n"
        + "  `c_point` point DEFAULT NULL,\n"
        + "  `c_linestring` linestring DEFAULT NULL,\n"
        + "  `c_polygon` polygon DEFAULT NULL,\n"
        + "  `c_multipoint` multipoint DEFAULT NULL,\n"
        + "  `c_multilinestring` multilinestring DEFAULT NULL,\n"
        + "  `c_multipolygon` multipolygon DEFAULT NULL,\n"
        + "  `c_geometrycollection` geometrycollection DEFAULT NULL,\n"
        + "  PRIMARY KEY (id)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT=\"test fail point\"\n {1}";

    public BaseTableFailPointTestCase() {
    }

    @Before
    public void doBefore() {
        clearFailPoints();
    }

    @After
    public void doAfter() {
        clearFailPoints();
    }

    protected void execDdlWithFailPoints() {
        testSingleTable();
        testBroadcastTable();
        testShardingTable();
    }

    protected abstract void testSingleTable();

    protected abstract void testBroadcastTable();

    protected abstract void testShardingTable();

    @Test
    public void test_FP_EACH_DDL_TASK_BACK_AND_FORTH() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_FAIL_ONCE() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_FAIL_ONCE, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_EXECUTE_TWICE() {
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_PHYSICAL_DDL_EXCEPTION() {
        enableFailPoint(FailPointKey.FP_RANDOM_PHYSICAL_DDL_EXCEPTION, "30");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_FAIL() {
        enableFailPoint(FailPointKey.FP_RANDOM_FAIL, "30");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_SUSPEND() {
        enableFailPoint(FailPointKey.FP_RANDOM_SUSPEND, "30,3000");
        execDdlWithFailPoints();
    }

    protected String createAndCheckTable(String tableNamePrefix, String extendedSyntax) {
        String tableName = randomTableName(tableNamePrefix);
        String createTableStmt = MessageFormat.format(CREATE_TABLE_TEMPLATE, tableName, extendedSyntax);
        JdbcUtil.executeUpdateSuccess(failPointConnection, createTableStmt);
        Assert.assertTrue(isTableConsistent(tableName));
        return tableName;
    }

    protected void executeAndCheckTable(String ddlStmt, String tableName) {
        JdbcUtil.executeUpdateSuccess(failPointConnection, ddlStmt);
        Assert.assertTrue(isTableConsistent(tableName));
    }

    protected void executeAndCheckTableGone(String ddlStmt, String tableName) {
        JdbcUtil.executeUpdateSuccess(failPointConnection, ddlStmt);
        Assert.assertTrue(isTableCleanedUp(tableName));
    }

    protected String randomTableName(String prefix) {
        return randomTableName(prefix, 4);
    }

    protected Connection getServerConnection() {
        return failPointConnection;
    }

}

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
import org.junit.runners.Parameterized;

import java.sql.Statement;
import java.text.MessageFormat;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @version 1.0
 */

public class AutoPartitionChangeColumnTest extends BaseAutoPartitionNewPartition {
    private static final String HINT = "/*+TDDL:cmd_extra(ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI=true)*/";
    private static final String TABLE_NAME = "auto_partition_chg_col_tb";
    private static final String CGSI_NAME = "cg_i_ap_chg_col";
    private static final String UCGSI_NAME = "cug_i_ap_chg_col";
    private static final String CREATE_TABLE = MessageFormat.format("CREATE TABLE `{0}` (\n"
        + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `i0` int,\n"
        + "  `i1` int,\n"
        + "  `c0` varchar(20),\n"
        + "  `c1` varchar(20),\n"
        + "  `d0` decimal(20, 0),\n"
        + "  `t` timestamp default current_timestamp,\n"
        + "  `order_id` int DEFAULT NULL,\n"
        + "  `seller_id` int DEFAULT NULL,\n"
        + "  PRIMARY KEY (`pk`),\n"
        + "  CLUSTERED INDEX `{1}` using btree (`seller_id`),\n"
        + "  UNIQUE CLUSTERED INDEX `{2}` using btree (`order_id`)\n"
        + ");", TABLE_NAME, CGSI_NAME, UCGSI_NAME);
    private static final String CREATE_TABLE_NO_PK = MessageFormat.format("CREATE TABLE `{0}` (\n"
        + "  `i0` int,\n"
        + "  `i1` int,\n"
        + "  `c0` varchar(20),\n"
        + "  `c1` varchar(20),\n"
        + "  `d0` decimal(20, 0),\n"
        + "  `t` timestamp default current_timestamp,\n"
        + "  `order_id` int DEFAULT NULL,\n"
        + "  `seller_id` int DEFAULT NULL,\n"
        + "  CLUSTERED INDEX `{1}` using btree (`seller_id`),\n"
        + "  UNIQUE CLUSTERED INDEX `{2}` using btree (`order_id`)\n"
        + ");", TABLE_NAME, CGSI_NAME, UCGSI_NAME);

    @Parameterized.Parameters(name = "{index}:pk={0}")
    public static List<String[]> prepareDate() {
        return ImmutableList.of(new String[] {CREATE_TABLE}, new String[] {CREATE_TABLE_NO_PK});
    }

    private final String createTableStmt;
    private final boolean hasPk;
    final String selectGSI0;
    final String selectGSI1;
    final String selectPrimary;

    public AutoPartitionChangeColumnTest(String createTable) {
        this.createTableStmt = createTable;
        this.hasPk = createTable.contains("PRIMARY KEY");
        selectGSI0 =
            MessageFormat.format("select * from `{0}` force index({1}) order by `seller_id`", TABLE_NAME, CGSI_NAME);
        selectGSI1 =
            MessageFormat.format("select * from `{0}` force index({1}) order by `seller_id`", TABLE_NAME, UCGSI_NAME);
        selectPrimary = MessageFormat
            .format("select * from `{0}` ignore index({1},{2}) order by `seller_id`", TABLE_NAME, CGSI_NAME,
                UCGSI_NAME);
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

        if (hasPk) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(
                    "insert into `{0}` values (1,100,101,\"vc0\",\"vc1\",233, \"2020-12-03 19:41:03.0\",1001,10001)",
                    TABLE_NAME));
        } else {
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(
                    "insert into `{0}` values (100,101,\"vc0\",\"vc1\",233, \"2020-12-03 19:41:03.0\",1001,10001)",
                    TABLE_NAME));
        }
    }

    @After
    public void after() {
        dropTableWithGsi(TABLE_NAME, ImmutableList.of(CGSI_NAME, UCGSI_NAME));
    }

    @Test
    public void changeColumnRenameTest() throws Exception {

        final String oldName = "i0";
        final String newName = "i_new";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat
                .format(HINT + "alter table `{0}` change column `{1}` `{2}` int", TABLE_NAME, oldName, newName));

        // Assert that data correct.
        try (Statement statement = tddlConnection.createStatement()) {
            final String result =
                JdbcUtil.getStringResult(statement.executeQuery(selectPrimary), false).stream().map(line -> String
                    .join(",", line)).collect(Collectors.joining("\n"));
            if (hasPk) {
                Assert.assertEquals("1,100,101,vc0,vc1,233,2020-12-03 19:41:03.0,1001,10001", result);
            } else {
                Assert.assertEquals("100,101,vc0,vc1,233,2020-12-03 19:41:03.0,1001,10001", result);
            }
        }
        selectContentSameAssert(selectGSI0, selectPrimary, null, tddlConnection,
            tddlConnection);
        selectContentSameAssert(selectGSI1, selectPrimary, null, tddlConnection,
            tddlConnection);

        // Assert that name changed.
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME).contains(newName));
        Assert.assertTrue(showCreateTable(tddlConnection, CGSI_NAME).contains(newName));
        Assert.assertTrue(showCreateTable(tddlConnection, UCGSI_NAME).contains(newName));
    }

    @Test
    public void changeColumnTypeTest() throws Exception {

        final String colName = "i0";
        final String newType = "varchar(23)";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat
                .format(HINT + "alter table `{0}` change column `{1}` `{2}` {3}", TABLE_NAME, colName, colName,
                    newType));

        // Assert that data correct.
        try (Statement statement = tddlConnection.createStatement()) {
            final String result =
                JdbcUtil.getStringResult(statement.executeQuery(selectPrimary), false).stream().map(line -> String
                    .join(",", line)).collect(Collectors.joining("\n"));
            if (hasPk) {
                Assert.assertEquals("1,100,101,vc0,vc1,233,2020-12-03 19:41:03.0,1001,10001", result);
            } else {
                Assert.assertEquals("100,101,vc0,vc1,233,2020-12-03 19:41:03.0,1001,10001", result);
            }
        }
        selectContentSameAssert(selectGSI0, selectPrimary, null, tddlConnection,
            tddlConnection);
        selectContentSameAssert(selectGSI1, selectPrimary, null, tddlConnection,
            tddlConnection);

        // Assert that type changed.
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME).contains(newType));
        Assert.assertTrue(showCreateTable(tddlConnection, CGSI_NAME).contains(newType));
        Assert.assertTrue(showCreateTable(tddlConnection, UCGSI_NAME).contains(newType));
    }

    @Test
    public void changeColumnDefaultTest() throws Exception {

        final String colName = "i0";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat
                .format(HINT + "alter table `{0}` change column `{1}` `{2}` int default 233", TABLE_NAME, colName,
                    colName));

        // Try insert one with new default.
        if (hasPk) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(
                    "insert into `{0}` (pk,i1,c0,c1,d0,t,order_id,seller_id) values (2,102,\"vc02\",\"vc12\",333, \"2020-12-04 19:41:03.0\",1002,10002)",
                    TABLE_NAME));
        } else {
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(
                    "insert into `{0}` (i1,c0,c1,d0,t,order_id,seller_id) values (102,\"vc02\",\"vc12\",333, \"2020-12-04 19:41:03.0\",1002,10002)",
                    TABLE_NAME));
        }

        // Assert that data correct.
        try (Statement statement = tddlConnection.createStatement()) {
            final String result =
                JdbcUtil.getStringResult(statement.executeQuery(selectPrimary), false).stream().map(line -> String
                    .join(",", line)).collect(Collectors.joining("\n"));
            if (hasPk) {
                Assert.assertEquals(
                    "1,100,101,vc0,vc1,233,2020-12-03 19:41:03.0,1001,10001\n"
                        + "2,233,102,vc02,vc12,333,2020-12-04 19:41:03.0,1002,10002", result);
            } else {
                Assert.assertEquals(
                    "100,101,vc0,vc1,233,2020-12-03 19:41:03.0,1001,10001\n"
                        + "233,102,vc02,vc12,333,2020-12-04 19:41:03.0,1002,10002", result);
            }
        }
        selectContentSameAssert(selectGSI0, selectPrimary, null, tddlConnection,
            tddlConnection);
        selectContentSameAssert(selectGSI1, selectPrimary, null, tddlConnection,
            tddlConnection);
    }

    @Test
    public void changeColumnCommentTest() throws Exception {

        final String colName = "i0";
        final String newComment = "hehe?";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat
                .format("alter table `{0}` change column `{1}` `{2}` int comment \"{3}\"", TABLE_NAME, colName, colName,
                    newComment));

        // Assert that data correct.
        try (Statement statement = tddlConnection.createStatement()) {
            final String result =
                JdbcUtil.getStringResult(statement.executeQuery(selectPrimary), false).stream().map(line -> String
                    .join(",", line)).collect(Collectors.joining("\n"));

            if (hasPk) {
                Assert.assertEquals(
                    "1,100,101,vc0,vc1,233,2020-12-03 19:41:03.0,1001,10001", result);
            } else {
                Assert.assertEquals(
                    "100,101,vc0,vc1,233,2020-12-03 19:41:03.0,1001,10001", result);
            }
        }
        selectContentSameAssert(selectGSI0, selectPrimary, null, tddlConnection,
            tddlConnection);
        selectContentSameAssert(selectGSI1, selectPrimary, null, tddlConnection,
            tddlConnection);

        // Assert that comment changed.
        Assert.assertTrue(showCreateTable(tddlConnection, TABLE_NAME).contains(newComment));
        Assert.assertTrue(showCreateTable(tddlConnection, CGSI_NAME).contains(newComment));
        Assert.assertTrue(showCreateTable(tddlConnection, UCGSI_NAME).contains(newComment));
    }

    @Test
    public void changeColumnOrderTest() throws Exception {

        final String colName = "i0";
        final String afterColName = "c1";

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat
                .format("alter table `{0}` change column `{1}` `{2}` int after `{3}`", TABLE_NAME, colName, colName,
                    afterColName));

        // Assert that data correct and in correct order.
        try (Statement statement = tddlConnection.createStatement()) {
            final String result =
                JdbcUtil.getStringResult(statement.executeQuery(selectPrimary), false).stream().map(line -> String
                    .join(",", line)).collect(Collectors.joining("\n"));
            if (hasPk) {
                Assert.assertEquals(
                    "1,101,vc0,vc1,100,233,2020-12-03 19:41:03.0,1001,10001", result);
            } else {
                Assert.assertEquals(
                    "101,vc0,vc1,100,233,2020-12-03 19:41:03.0,1001,10001", result);
            }
        }
        selectContentSameAssert(selectGSI0, selectPrimary, null, tddlConnection,
            tddlConnection);
        selectContentSameAssert(selectGSI1, selectPrimary, null, tddlConnection,
            tddlConnection);
    }

}

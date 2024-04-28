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

import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @version 1.0
 */
@BinlogIgnore(ignoreReason = "drop local partition的动作目前无法透传给下游，导致binlog实验室上下游数据不一致，暂时忽略")
public class LocalPartitionWithAutoPartitionTest extends LocalPartitionBaseTest {

    private static final String TABLE_NAME = "local_partition_with_auto_partition_tb";

    private final String fullTableNameDrds = '`' + TABLE_NAME + '`';
    private final String fullTableNameMySQL = '`' + TABLE_NAME + '`';

    private final String localPartitionSuffix =
        "\nLOCAL PARTITION BY RANGE (gmt_created)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n";

    @Before
    public void cleanEnv() {

    }

    @After
    public void clean() {

    }

    private void assertPartitioned() {
        final List<List<String>> stringResult = JdbcUtil.getStringResult(
            JdbcUtil.executeQuery("show full create table " + TABLE_NAME, tddlConnection), true);
        Assert.assertTrue(stringResult.get(0).get(1).contains("PARTITION BY"));
        Assert.assertTrue(stringResult.get(0).get(1).contains("CREATE PARTITION TABLE"));
    }

    @Test
    public void testCreateAutoPartitionNoPk() {

        dropTableIfExists(tddlConnection, fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);

        final String createSqlX =
            "create table " + fullTableNameDrds + "(x int, gmt_created date, primary key(x, gmt_created))"
                + localPartitionSuffix;
        final String insertSqlX = "insert into " + fullTableNameDrds + "values (1, now())";
        final String selectSqlX = "select * from " + fullTableNameDrds;

        final String createSql =
            "create table " + fullTableNameMySQL + "(x int, gmt_created date, primary key(x, gmt_created))";
        final String insertSql = "insert into " + fullTableNameMySQL + "values (1, now())";
        final String selectSql = "select * from " + fullTableNameMySQL;

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        assertPartitioned();
        validateLocalPartition(tddlConnection, fullTableNameDrds);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        selectContentSameAssert(selectSql, selectSqlX, null, mysqlConnection, tddlConnection);

        dropTableIfExists(tddlConnection, fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);
    }

    @Test
    public void testCreateAutoPartitionSinglePk() {

        dropTableIfExists(tddlConnection, fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);

        final String createSqlX =
            "create partition table " + fullTableNameDrds + "(x int, gmt_created date, primary key(x, gmt_created))"
                + localPartitionSuffix;
        final String insertSqlX = "insert into " + fullTableNameDrds + "values (1, now())";
        final String selectSqlX = "select * from " + fullTableNameDrds;

        final String createSql =
            "create table " + fullTableNameMySQL + "(x int, gmt_created date, primary key(x, gmt_created))";
        final String insertSql = "insert into " + fullTableNameMySQL + "values (1, now())";
        final String selectSql = "select * from " + fullTableNameMySQL;

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        assertPartitioned();
        validateLocalPartition(tddlConnection, fullTableNameDrds);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        selectContentSameAssert(selectSql, selectSqlX, null, mysqlConnection, tddlConnection);

        dropTableIfExists(tddlConnection, fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);
    }

    @Test
    public void testCreateAutoPartitionCompositePk() {

        dropTableIfExists(tddlConnection, fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);

        final String createSqlX = "create partition table " + fullTableNameDrds
            + "(x int, y int, gmt_created date, primary key(x, y, gmt_created))" + localPartitionSuffix;
        final String insertSqlX = "insert into " + fullTableNameDrds + "values (1, 2, now())";
        final String selectSqlX = "select * from " + fullTableNameDrds;

        final String createSql =
            "create table " + fullTableNameMySQL + "(x int, y int, gmt_created date, primary key(x, y, gmt_created))";
        final String insertSql = "insert into " + fullTableNameMySQL + "values (1, 2, now())";
        final String selectSql = "select * from " + fullTableNameMySQL;

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql);
        assertPartitioned();
        validateLocalPartition(tddlConnection, fullTableNameDrds);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSqlX);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);
        selectContentSameAssert(selectSql, selectSqlX, null, mysqlConnection, tddlConnection);

        dropTableIfExists(tddlConnection, fullTableNameDrds);
        dropTableIfExistsInMySql(fullTableNameMySQL);
    }

    @Test
    public void testCreateAutoPartitionWithIndex1() {
        final String primaryTableName = randomTableName("testCreateAutoPartitionWithIndex", 4);
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    id bigint,\n"
            + "    seller_id bigint,\n"
            + "    gmt_modified DATETIME,\n"
            + "    primary key(id, gmt_modified),\n"
            + "    index sdx(seller_id)\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        validateScheduledJob(tddlConnection, false, tddlDatabase1, primaryTableName);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName, getMetaConnection());
    }

    @Test
    public void testCreateAutoPartitionWithIndex2() {
        final String primaryTableName = randomTableName("testCreateAutoPartitionWithIndex", 4);
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    id bigint,\n"
            + "    seller_id bigint,\n"
            + "    gmt_modified DATETIME,\n"
            + "    primary key(id, gmt_modified)\n"
            + ")\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);

        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "ALTER TABLE %s ADD INDEX sdx(seller_id)", primaryTableName
        ));
        validateLocalPartition(tddlConnection, primaryTableName);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        validateScheduledJob(tddlConnection, false, tddlDatabase1, primaryTableName);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName, getMetaConnection());
    }
}

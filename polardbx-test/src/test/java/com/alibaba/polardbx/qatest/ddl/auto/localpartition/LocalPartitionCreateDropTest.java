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

import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * todo 待补充
 * 3. 各种参数组合
 * <p>
 * 4. 校验各个数值类型字面量的取值范围
 */
public class LocalPartitionCreateDropTest extends LocalPartitionBaseTest {

    private String primaryTableName;
    private String primaryTableNameForLikeTest;
    private String gsi1TableName;
    private String ugsi1TableName;
    private String cgsi1TableName;
    private String ucgsi1TableName;

    @Before
    public void before() {
        primaryTableName = randomTableName("t_create", 4);
        primaryTableNameForLikeTest = randomTableName("t_create", 4);
        gsi1TableName = randomTableName("gsi1_create", 4);
        ugsi1TableName = randomTableName("ugsi1_create", 4);
        cgsi1TableName = randomTableName("cgsi1_create", 4);
        ucgsi1TableName = randomTableName("ucgsi1_create", 4);
    }

    @After
    public void after() {

    }

    @Test
    public void testCreateLocalPartitionTableSuccess1() {
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
        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);

        String addGsiSql = String.format(
            "ALTER TABLE %s ADD GLOBAL INDEX `%s`(`c1`, `c2`, `gmt_modified`) PARTITION BY HASH(C2) PARTITIONS 7",
            primaryTableName, gsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, addGsiSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        addGsiSql = String.format(
            "ALTER TABLE %s ADD UNIQUE GLOBAL INDEX `%s`(`c1`, `c2`, `gmt_modified`) PARTITION BY HASH(C2) PARTITIONS 7",
            primaryTableName, ugsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, addGsiSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        addGsiSql = String.format(
            "ALTER TABLE %s ADD CLUSTERED INDEX `%s`(`c1`, `c2`, `gmt_modified`) PARTITION BY HASH(C2) PARTITIONS 7",
            primaryTableName, cgsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, addGsiSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        addGsiSql = String.format(
            "ALTER TABLE %s ADD CLUSTERED INDEX `%s`(`c1`, `c2`, `gmt_modified`) PARTITION BY HASH(C2) PARTITIONS 7",
            primaryTableName, ucgsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, addGsiSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("create table %s like %s", primaryTableNameForLikeTest, primaryTableName));
        validateLocalPartition(tddlConnection, primaryTableNameForLikeTest);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName,
            getMetaConnection());

        String dropTableSqlForLike = String.format("DROP TABLE %s", primaryTableNameForLikeTest);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSqlForLike);
        assertNotExistsTable(primaryTableNameForLikeTest, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableNameForLikeTest,
            getMetaConnection());
    }

    @Test
    public void testCreateLocalPartitionTableSuccess2() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    global index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + "DISABLE SCHEDULE"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        validateScheduledJob(tddlConnection, false, tddlDatabase1, primaryTableName);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName,
            getMetaConnection());
    }

    @Test
    public void testCreateLocalPartitionTableSuccess3() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique global index %s(gmt_modified, c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName,
            getMetaConnection());
    }

    @Test
    public void testCreateLocalPartitionTableSuccess4() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    clustered index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName,
            getMetaConnection());
    }

    @Test
    public void testCreateLocalPartitionTableSuccess5() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique clustered index %s(gmt_modified, c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 7);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName,
            getMetaConnection());
    }

    @Test
    public void testCreateLocalPartitionTableSuccess6() {

        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusMonths(6L);

        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique clustered index %s(gmt_modified, c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName, startWithDate);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 14);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName,
            getMetaConnection());
    }

    @Test
    public void testCreateLocalPartitionTableSuccess7() {

        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.plusMonths(1L);

        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique clustered index %s(gmt_modified, c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + ";", primaryTableName, gsi1TableName, startWithDate);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 4);

        String dropTableSql = String.format("DROP TABLE %s", primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        assertNotExistsTable(primaryTableName, tddlConnection);
        assertTableMetaNotExists(tddlDatabase1, primaryTableName,
            getMetaConnection());
    }

    @Test
    public void testCreateScheduleSuccess1() throws SQLException {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique clustered index %s(gmt_modified, c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "DISABLE SCHEDULE"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        String sql = String.format("CREATE SCHEDULE FOR LOCAL_PARTITION ON %s CRON '0 0 12 1/5 * ?' TIMEZONE '+00:00'",
            primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format("SELECT * FROM INFORMATION_SCHEMA.LOCAL_PARTITIONS_SCHEDULE "
                + "WHERE table_schema='%s' and table_name='%s'", tddlDatabase1, primaryTableName),
            tddlConnection
        );
        Assert.assertTrue(resultSet.next());

        String scheduleId = resultSet.getString("SCHEDULE_ID");
        Assert.assertEquals(resultSet.getString("TABLE_SCHEMA"), tddlDatabase1);
        Assert.assertEquals(resultSet.getString("TABLE_NAME"), primaryTableName);
        Assert.assertEquals(resultSet.getString("SCHEDULE_EXPR"), "0 0 12 1/5 * ?");
        Assert.assertEquals(resultSet.getString("SCHEDULE_COMMENT"), "at 12:00 every 5 days");
        Assert.assertEquals(resultSet.getString("STATUS"), "ENABLED");
        Assert.assertEquals(resultSet.getString("TIME_ZONE"), "+00:00");
        JdbcUtil.executeSuccess(tddlConnection, "DROP SCHEDULE " + scheduleId);

        resultSet = JdbcUtil.executeQuery(
            String.format("SELECT * FROM INFORMATION_SCHEMA.LOCAL_PARTITIONS_SCHEDULE "
                + "WHERE table_schema='%s' and table_name='%s'", tddlDatabase1, primaryTableName),
            tddlConnection
        );
        Assert.assertTrue(!resultSet.next());

        sql = String.format("CREATE SCHEDULE FOR LOCAL_PARTITION ON %s CRON '0 0 12 1/5 * ?' TIMEZONE '+00:00'",
            primaryTableName + "_some_suffix");
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown table");
    }

    @Test
    public void testCreateLocalPartitionTableFail1() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTableSql,
            "Primary/Unique Key must contain local partition column");

        assertNotExistsTable(primaryTableName, tddlConnection);
    }

    @Test
    public void testCreateLocalPartitionTableFail2() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique clustered index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTableSql,
            "Primary/Unique Key must contain local partition column");

        assertNotExistsTable(primaryTableName, tddlConnection);
    }

    @Test
    public void testCreateLocalPartitionTableFail3() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique key %s(c2)\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTableSql,
            "Primary/Unique Key must contain local partition column");

        assertNotExistsTable(primaryTableName, tddlConnection);
    }

    @Test
    public void testCreateLocalPartitionTableFail4() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique key %s(c2)\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '2000-01-01'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTableSql,
            "Creating table with too many local partitions");

        assertNotExistsTable(primaryTableName, tddlConnection);
    }

    @Test
    public void testCreateLocalPartitionTableFail5() {
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
            + "EXPIRE AFTER 0\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTableSql,
            "The value of EXPIRE AFTER must be greater than 0");

        assertNotExistsTable(primaryTableName, tddlConnection);
    }

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPivotDateExprFormat1() {
        LocalPartitionDefinitionInfo.assertValidPivotDateExpr(
            new MySqlExprParser(ByteString.from("NOW()")).expr());
        LocalPartitionDefinitionInfo.assertValidPivotDateExpr(
            new MySqlExprParser(ByteString.from("DATE_ADD(NOW(), interval 1 month)")).expr());
        LocalPartitionDefinitionInfo.assertValidPivotDateExpr(
            new MySqlExprParser(ByteString.from("DATE_SUB(NOW(), interval 3 month)")).expr());

        thrown.expect(ParserException.class);
        LocalPartitionDefinitionInfo.assertValidPivotDateExpr(
            new MySqlExprParser(ByteString.from("1+1")).expr());
        LocalPartitionDefinitionInfo.assertValidPivotDateExpr(
            new MySqlExprParser(ByteString.from("DATE_ADD(20220101, interval 1 month)")).expr());
    }

    @Test
    public void testGenerateLocalPartitionStmtForCreate() {
        LocalPartitionDefinitionInfo definitionInfo = new LocalPartitionDefinitionInfo();
        definitionInfo.setStartWithDate(StringTimeParser.parseDatetime("2021-01-01".getBytes()));
        definitionInfo.setTableName(primaryTableName);
        definitionInfo.setTableSchema(tddlDatabase1);
        definitionInfo.setColumnName("gmt_created");
        definitionInfo.setPivotDateExpr("NOW()");
        definitionInfo.setExpireAfterCount(12);
        definitionInfo.setIntervalCount(1);
        definitionInfo.setIntervalUnit("MONTH");
        definitionInfo.setPreAllocateCount(3);

        SQLPartitionByRange sqlPartitionByRange =
            LocalPartitionDefinitionInfo.generateLocalPartitionStmtForCreate(definitionInfo,
                StringTimeParser.parseDatetime("2021-09-01".getBytes()));

        Assert.assertEquals(sqlPartitionByRange.toString(),
            "RANGE COLUMNS (gmt_created) (\n"
                + "\tPARTITION p20210101 VALUES LESS THAN ('2021-01-01') COMMENT '',\n"
                + "\tPARTITION p20210201 VALUES LESS THAN ('2021-02-01') COMMENT '',\n"
                + "\tPARTITION p20210301 VALUES LESS THAN ('2021-03-01') COMMENT '',\n"
                + "\tPARTITION p20210401 VALUES LESS THAN ('2021-04-01') COMMENT '',\n"
                + "\tPARTITION p20210501 VALUES LESS THAN ('2021-05-01') COMMENT '',\n"
                + "\tPARTITION p20210601 VALUES LESS THAN ('2021-06-01') COMMENT '',\n"
                + "\tPARTITION p20210701 VALUES LESS THAN ('2021-07-01') COMMENT '',\n"
                + "\tPARTITION p20210801 VALUES LESS THAN ('2021-08-01') COMMENT '',\n"
                + "\tPARTITION p20210901 VALUES LESS THAN ('2021-09-01') COMMENT '',\n"
                + "\tPARTITION p20211001 VALUES LESS THAN ('2021-10-01') COMMENT '',\n"
                + "\tPARTITION p20211101 VALUES LESS THAN ('2021-11-01') COMMENT '',\n"
                + "\tPARTITION pmax VALUES LESS THAN MAXVALUE COMMENT ''\n"
                + ")");
    }

    @Test
    public void testCreateTableLike1() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique key %s(c2, gmt_modified)\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);

        JdbcUtil.executeUpdate(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("create table %s like %s", primaryTableNameForLikeTest, primaryTableName));
        validateLocalPartition(tddlConnection, primaryTableNameForLikeTest);

        String fullCreateTable = showCreateTable(tddlConnection, primaryTableNameForLikeTest);
        Assert.assertTrue(StringUtils.containsIgnoreCase(fullCreateTable,
            "PARTITION BY KEY(`c1`)\n"
                + "PARTITIONS 4\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER -1\n"
                + "PRE ALLOCATE 6\n"
                + "PIVOTDATE NOW()"));
    }

    @Test
    public void testCreateTableLike2() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique key %s(c2, gmt_modified)\n"
            + ")\n"
            + "SINGLE\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);

        JdbcUtil.executeUpdate(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("create table %s like %s", primaryTableNameForLikeTest, primaryTableName));
        validateLocalPartition(tddlConnection, primaryTableNameForLikeTest);

        String fullCreateTable = showCreateTable(tddlConnection, primaryTableNameForLikeTest);
        Assert.assertTrue(StringUtils.containsIgnoreCase(fullCreateTable,
            "SINGLE\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER -1\n"
                + "PRE ALLOCATE 6\n"
                + "PIVOTDATE NOW()"));
    }

    @Test
    public void testCreateTableLike3() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL,\n"
            + "    unique key %s(c2, gmt_modified)\n"
            + ")\n"
            + "BROADCAST\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", primaryTableName, gsi1TableName);

        JdbcUtil.executeUpdate(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("create table %s like %s", primaryTableNameForLikeTest, primaryTableName));
        validateLocalPartition(tddlConnection, primaryTableNameForLikeTest);

        String fullCreateTable = showCreateTable(tddlConnection, primaryTableNameForLikeTest);
        Assert.assertTrue(StringUtils.containsIgnoreCase(fullCreateTable,
            "BROADCAST\n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "INTERVAL 1 MONTH\n"
                + "EXPIRE AFTER -1\n"
                + "PRE ALLOCATE 6\n"
                + "PIVOTDATE NOW()"));
    }

}
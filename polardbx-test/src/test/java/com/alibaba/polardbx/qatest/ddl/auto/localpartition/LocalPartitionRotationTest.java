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
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

@NotThreadSafe
@BinlogIgnore(ignoreReason = "drop local partition的动作目前无法透传给下游，导致binlog实验室上下游数据不一致，暂时忽略")
public class LocalPartitionRotationTest extends LocalPartitionBaseTest {

    private String primaryTableName;
    private String gsi1TableName;

    @Before
    public void before() {
        primaryTableName = randomTableName("t_rotation", 4);
        gsi1TableName = randomTableName("gsi1_rotation", 4);
        JdbcUtil.executeSuccess(tddlConnection, "set @FP_OVERRIDE_NOW=null");
    }

    @After
    public void after() {
        JdbcUtil.executeSuccess(tddlConnection, "set @FP_OVERRIDE_NOW=NULL");
    }

    /**
     * 测试步骤：
     * 1. 创建一个local partition表，按月分区
     * 2. 插入N条数据
     * 3. 修改时间，每次加一个月
     * 4. 分区滚动：预期老分区被删除、新分区被创建、数据随老分区一起被删除
     */
    @Test
    public void testRotation1() throws SQLException {
        final LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusMonths(12L);

        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATE PRIMARY KEY NOT NULL,\n"
            + "    global index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + ";", primaryTableName, gsi1TableName, startWithDate);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 20);

        LocalDate lowestDate = startWithDate;
        LocalDate dateIterator = startWithDate;
        int i = 0;
        while (dateIterator.isBefore(now) || dateIterator.equals(now)) {
            JdbcUtil.executeSuccess(
                tddlConnection,
                String.format("INSERT INTO %s VALUES (1,2,3,'%s')", primaryTableName, dateIterator.toString())
            );
            dateIterator = startWithDate.plusMonths(++i);
        }

        for (i = 0; i < 13; i++) {
            ResultSet resultSet = JdbcUtil.executeQuery(
                String.format("select * from %s order by gmt_modified limit 1", primaryTableName), tddlConnection);
            Assert.assertTrue(resultSet.next());
            String date = resultSet.getString("gmt_modified");
            Assert.assertEquals("Found Error", LocalDate.parse(date), lowestDate);

            lowestDate = startWithDate.plusMonths(i + 1);
            JdbcUtil.executeSuccess(tddlConnection,
                String.format("set @FP_OVERRIDE_NOW='%s'", now.plusMonths(i + 1))
            );
            JdbcUtil.executeSuccess(tddlConnection,
                String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
            );
            JdbcUtil.executeSuccess(tddlConnection,
                String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
            );
        }

        validateLocalPartitionCount(tddlConnection, primaryTableName, 19);
    }

    @Test
    public void testRotation2() throws SQLException {
        LocalDate now = LocalDate.now();

        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATE PRIMARY KEY NOT NULL,\n"
            + "    global index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 2\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 4);

        LocalDate dateIterator = now;
        int i = 0;
        for (; i < 12; i++) {
            JdbcUtil.executeSuccess(
                tddlConnection,
                String.format("INSERT INTO %s VALUES (1,2,3,'%s')", primaryTableName, dateIterator.toString())
            );
            dateIterator = now.plusMonths(i + 1);
        }

        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format("select count(*) as cnt from %s", primaryTableName), tddlConnection);
        Assert.assertTrue(resultSet.next());
        int cnt = resultSet.getInt("cnt");
        Assert.assertEquals("Found Error", cnt, 12);

        dateIterator = now.plusMonths(i + 6);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("set @FP_OVERRIDE_NOW='%s'", dateIterator)
        );
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
        );
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );

        resultSet = JdbcUtil.executeQuery(
            String.format("select count(*) as cnt from %s", primaryTableName), tddlConnection);
        Assert.assertTrue(resultSet.next());
        cnt = resultSet.getInt("cnt");
        Assert.assertEquals("Found Error", cnt, 6);
    }

    @Test
    public void testRotation3() throws SQLException {
        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusMonths(12L);

        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATE PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(gmt_modified)\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + ";", primaryTableName, startWithDate);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 17);

        final LocalDate lowestDate = startWithDate;
        LocalDate dateIterator = startWithDate;
        while (dateIterator.isBefore(now) || dateIterator.equals(now)) {
            JdbcUtil.executeSuccess(
                tddlConnection,
                String.format("INSERT INTO %s VALUES (1,2,3,'%s')", primaryTableName, dateIterator.toString())
            );
            dateIterator = dateIterator.plusMonths(1L);
        }

        dateIterator = now;

        for (int i = 0; i < 13; i++) {
            ResultSet resultSet = JdbcUtil.executeQuery(
                String.format("select * from %s order by gmt_modified limit 1", primaryTableName),
                tddlConnection);
            Assert.assertTrue(resultSet.next());
            String date = resultSet.getString("gmt_modified");
            Assert.assertEquals("Found Error", LocalDate.parse(date), lowestDate);

            dateIterator = dateIterator.plusMonths(1L);
            JdbcUtil.executeSuccess(tddlConnection,
                String.format("set @FP_OVERRIDE_NOW='%s'", dateIterator)
            );
            JdbcUtil.executeSuccess(tddlConnection,
                String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
            );
            JdbcUtil.executeSuccess(tddlConnection,
                String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
            );
        }

        validateLocalPartitionCount(tddlConnection, primaryTableName, 30);
    }

    @Test
    public void testRotation4() throws SQLException {
        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusMonths(12L);

        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATE PRIMARY KEY NOT NULL,\n"
            + "    global index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + ";", primaryTableName, gsi1TableName, startWithDate);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 20);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION %s", primaryTableName,
                "p" + startWithDate.plusMonths(1L).toString().replace("-", "")),
            "is not yet expired"
        );

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("set @FP_OVERRIDE_NOW='%s'", now.plusMonths(6L))
        );
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 26);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION %s", primaryTableName,
                "p" + startWithDate.plusMonths(1L).toString().replace("-", ""))
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 25);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 19);

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("set @FP_OVERRIDE_NOW='%s'", now.plusMonths(26L))
        );
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 1);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 33);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 19);
    }

    @Test
    public void testRotation5() throws SQLException {
        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusDays(12L);

        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATE PRIMARY KEY NOT NULL,\n"
            + "    global index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 DAY\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + ";", primaryTableName, gsi1TableName, startWithDate);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 20);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION %s", primaryTableName,
                "p" + startWithDate.plusDays(1L).toString().replace("-", "")),
            "is not yet expired"
        );

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("set @FP_OVERRIDE_NOW='%s'", now.plusDays(6L))
        );
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 26);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION %s", primaryTableName,
                "p" + startWithDate.plusDays(1L).toString().replace("-", ""))
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 25);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 19);

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("set @FP_OVERRIDE_NOW='%s'", now.plusDays(26L))
        );
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 1);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 33);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 19);
    }

    @Test
    public void testRotation6() throws SQLException {
        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusYears(12L);

        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATE PRIMARY KEY NOT NULL,\n"
            + "    global index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s'\n"
            + "INTERVAL 1 YEAR\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + ";", primaryTableName, gsi1TableName, startWithDate);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartitionCount(tddlConnection, primaryTableName, 20);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION %s", primaryTableName,
                "p" + startWithDate.plusYears(1L).toString().replace("-", "")),
            "is not yet expired"
        );

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("set @FP_OVERRIDE_NOW='%s'", now.plusYears(6L))
        );
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 26);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION %s", primaryTableName,
                "p" + startWithDate.plusYears(1L).toString().replace("-", ""))
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 25);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 19);

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("set @FP_OVERRIDE_NOW='%s'", now.plusYears(26L))
        );
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 1);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 33);
        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", primaryTableName)
        );
        validateLocalPartitionCount(tddlConnection, primaryTableName, 19);
    }

    @Test
    public void testRotation7() throws SQLException {
        String createTableSql = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATE PRIMARY KEY NOT NULL,\n"
            + "    global index %s(c2) partition by hash(c2) partitions 4\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 1\n"
            + ";", primaryTableName, gsi1TableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTableSql,
            "The value of PRE ALLOCATE must be greater than 1");
    }
}
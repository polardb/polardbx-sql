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
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import net.jcip.annotations.NotThreadSafe;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

@NotThreadSafe
public class LocalPartitionRepartitionTest extends LocalPartitionBaseTest {

    private String primaryTableName;
    private String gsi1TableName;

    @Before
    public void before() {
        primaryTableName = randomTableName("t_create", 4);
        gsi1TableName = randomTableName("gsi1_create", 4);
        JdbcUtil.executeSuccess(tddlConnection, "set @FP_OVERRIDE_NOW=NULL");
    }

    @After
    public void after() {
        JdbcUtil.executeSuccess(tddlConnection, "set @FP_OVERRIDE_NOW=NULL");
    }

    /**
     * non-local partition table -> local partition table
     */
    @Test
    public void test1() throws SQLException {
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
            System.out.println("current @fp_show: " +
                JdbcUtil.executeQueryAndGetFirstStringResult("select @fp_show", tddlConnection));
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

    /**
     * local partition table -> local partition table
     */
    @Test
    public void test2() {
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
            , primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeSuccess(tddlConnection,
            String.format("ALTER TABLE %s \n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "INTERVAL 1 MONTH\n", primaryTableName)
        );
        validateLocalPartition(tddlConnection, primaryTableName);
    }

    /**
     * non-local partition table -> local partition table
     */
    @Test
    public void testWithFailure1() {
        String createTableSql = String.format("CREATE TABLE %s (\n"
                + "    c1 bigint,\n"
                + "    c2 bigint,\n"
                + "    c3 bigint,\n"
                + "    gmt_modified DATETIME NOT NULL\n"
                + ")\n"
                + "PARTITION BY HASH(c1)\n"
                + "PARTITIONS 4\n"
            , primaryTableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
        validateNotLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s \n"
                + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                + "INTERVAL 1 MONTH\n", primaryTableName),
            "Unsupported index table structure, Primary/Unique Key must contain local partition column"
        );
        validateNotLocalPartition(tddlConnection, primaryTableName);
    }

    /**
     * non-local partition table -> local partition table
     */
    @Test
    public void testWithFailure2() {
        String createTableSql = String.format(
            "create table %s (c1 int, c2 datetime primary key) partition by hash(c1) partitions 2"
            , primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        validateNotLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "alter table %s add unique global index %s(c1) partition by hash(c1) partitions 2", primaryTableName,
            gsi1TableName
        ));

        //fail to repartition to 'local partition table', because GSI contains uk which does not contain local partition column c2
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format("ALTER TABLE %s \n"
                + "LOCAL PARTITION BY RANGE (c2)\n"
                + "INTERVAL 1 MONTH\n", primaryTableName),
            "Unsupported index table structure, Primary/Unique Key must contain local partition column"
        );
        validateNotLocalPartition(tddlConnection, primaryTableName);

        JdbcUtil.executeSuccess(tddlConnection, String.format(
            "drop index %s on %s", gsi1TableName, primaryTableName
        ));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s \n"
                + "LOCAL PARTITION BY RANGE (c2)\n"
                + "INTERVAL 1 MONTH\n", primaryTableName)
        );
        validateLocalPartition(tddlConnection, primaryTableName);
    }

}
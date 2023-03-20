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

package com.alibaba.polardbx.qatest.ddl.sharding.cdc;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

/**
 * 针对分区表的DDL打标测试
 * <p>
 * created by ziyang.lb
 **/
public class CdcLocalPartitionTableDdlRecordTest extends CdcBaseTest {
    private String T_ORDER = String.format("CREATE TABLE `t_order` (\n"
        + "\t`id` bigint(20) DEFAULT NULL,\n"
        + "\t`gmt_modified` datetime NOT NULL,\n"
        + "\tPRIMARY KEY (`gmt_modified`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
        + "PARTITION BY KEY(`gmt_modified`)\n"
        + "PARTITIONS 8\n"
        + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
        + "STARTWITH '%s-01-01'\n"
        + "INTERVAL 1 MONTH\n"
        + "EXPIRE AFTER 12\n"
        + "PRE ALLOCATE 6\n"
        + "PIVOTDATE NOW()\n", Calendar.getInstance().get(Calendar.YEAR) - 2);

    @Test
    public void testCdcDdlRecord() throws SQLException, InterruptedException {
        String sql;
        String tokenHints;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select database()");

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database if exists cdc_ddl_local_partition";
            stmt.execute(sql);
            Thread.sleep(2000);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));

            tokenHints = buildTokenHints();
            sql = tokenHints + "create database cdc_ddl_local_partition partition_mode = 'partitioning'";
            stmt.execute(sql);
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));

            sql = "use cdc_ddl_local_partition";
            stmt.execute(sql);

            doDDl(stmt);

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database cdc_ddl_local_partition";
            stmt.execute(sql);
            stmt.execute("use __cdc__");
            Assert.assertEquals(sql, getDdlRecordSql(tokenHints));
        }
    }

    private void doDDl(Statement stmt)
        throws SQLException {
        String sql;
        String tokenHints;

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + T_ORDER;
        stmt.execute(sql);
        Assert.assertTrue(StringUtils.startsWith(getDdlRecordSql(tokenHints), tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("ALTER TABLE t_order EXPIRE LOCAL PARTITION p%s%s01",
            Calendar.getInstance().get(Calendar.YEAR) - 1,
            StringUtils.leftPad(String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1), 2, "0"));
        stmt.execute(sql);
        Assert.assertEquals("", getDdlRecordSql(tokenHints));

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "ALTER TABLE t_order EXPIRE LOCAL PARTITION";
        stmt.execute(sql);
        Assert.assertEquals("", getDdlRecordSql(tokenHints));

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "CREATE TABLE t1 (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ") single;";
        stmt.execute(sql);
        Assert.assertTrue(StringUtils.startsWith(getDdlRecordSql(tokenHints), tokenHints));
        Assert.assertEquals(1, getDdlRecordSqlCount(tokenHints));

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "alter table t1\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '2021-01-01'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()";
        stmt.execute(sql);
        Assert.assertEquals("", getDdlRecordSql(tokenHints));

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "alter table t1 remove local partitioning";
        stmt.execute(sql);
        Assert.assertEquals("", getDdlRecordSql(tokenHints));

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "check table t_order with local partition";
        stmt.execute(sql);
        Assert.assertEquals("", getDdlRecordSql(tokenHints));
    }
}

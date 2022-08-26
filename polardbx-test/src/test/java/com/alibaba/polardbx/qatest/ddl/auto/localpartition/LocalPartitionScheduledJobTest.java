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
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

@NotThreadSafe
public class LocalPartitionScheduledJobTest extends LocalPartitionBaseTest {

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

    @Test
    public void testCreateDrop1() throws SQLException {

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("create table %s.%s (c1 int, c2 datetime primary key)", tddlDatabase1,
                primaryTableName)
        );

        String sql = "CREATE SCHEDULE "
            + "FOR LOCAL_PARTITION "
            + "ON `" + tddlDatabase1 + "`.`" + primaryTableName + "`  "
            + "CRON '0 0 12 1/5 * ?'  "
            + "TIMEZONE '+00:00'";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not a local partition table");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("alter table %s.%s local partition by range(c2) interval 1 month DISABLE SCHEDULE",
                tddlDatabase1, primaryTableName)
        );

        validateScheduledJob(tddlConnection, false, tddlDatabase1, primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);
        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format(
                "select * from metadb.scheduled_jobs where table_schema='%s' and table_name='%s'",
                tddlDatabase1, primaryTableName
            ),
            tddlConnection
        );

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(resultSet.getString("schedule_expr"), "0 0 12 1/5 * ?");
        Assert.assertEquals(resultSet.getString("time_zone"), "+00:00");

        int count =
            JdbcUtil
                .executeUpdateAndGetEffectCount(tddlConnection, "DROP SCHEDULE " + resultSet.getLong("schedule_id"));
        Assert.assertEquals(count, 1);
        validateScheduledJob(tddlConnection, false, tddlDatabase1, primaryTableName);
    }

    @Test
    public void testCreateDrop2() throws SQLException {

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("create table %s.%s (c1 int, c2 datetime primary key) \n"
                + "local partition by range(c2) interval 1 month", tddlDatabase1, primaryTableName)
        );

        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);
        String sql = "CREATE SCHEDULE "
            + "FOR LOCAL_PARTITION "
            + "ON `" + tddlDatabase1 + "`.`" + primaryTableName + "`  "
            + "CRON '0 0 12 1/5 * ?'  "
            + "TIMEZONE '+00:00'";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate Scheduled Job For Local Partition Table");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("drop table %s.%s", tddlDatabase1, primaryTableName)
        );

        validateScheduledJob(tddlConnection, false, tddlDatabase1, primaryTableName);
    }

    @Test
    public void testCreateDrop3() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("create table %s.%s (c1 int, c2 datetime primary key) \n"
                + "local partition by range(c2) interval 1 month", tddlDatabase1, primaryTableName)
        );

        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("alter table %s.%s local partition by range(c2) interval 3 month DISABLE SCHEDULE",
                tddlDatabase1, primaryTableName)
        );

        validateScheduledJob(tddlConnection, false, tddlDatabase1, primaryTableName);

    }

    @Test
    public void testCreateDrop4() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("create table %s.%s (c1 int, c2 datetime primary key) \n"
                    + "local partition by range(c2) interval 1 month DISABLE SCHEDULE", tddlDatabase1,
                primaryTableName)
        );

        validateScheduledJob(tddlConnection, false, tddlDatabase1, primaryTableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("alter table %s.%s local partition by range(c2) interval 3 month",
                tddlDatabase1, primaryTableName)
        );

        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);
    }

    @Test
    public void testCreateDrop5() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("create table %s.%s (c1 int, c2 datetime primary key) \n"
                + "local partition by range(c2) interval 1 month", tddlDatabase1, primaryTableName)
        );

        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("alter table %s.%s local partition by range(c2) interval 3 month",
                tddlDatabase1, primaryTableName)
        );

        validateScheduledJob(tddlConnection, true, tddlDatabase1, primaryTableName);
    }

}
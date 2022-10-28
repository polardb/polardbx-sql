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
public class AutoSplitScheduledJobTest extends LocalPartitionBaseTest {

    private String primaryTableName;
    private String gsi1TableName;

    @Before
    public void before() {
        primaryTableName = randomTableName("t_auto_split", 4);
    }

    @After
    public void after() {
    }

    @Test
    public void testCreateDrop1() throws SQLException {

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("create table %s.%s (c1 int, c2 datetime primary key)", tddlDatabase1,
                primaryTableName)
        );

        ResultSet tableDetailRs = JdbcUtil.executeQuery(
            String.format("select * from information_schema.table_detail where table_schema='%s' and table_name='%s'",
                tddlDatabase1, primaryTableName),
            tddlConnection
        );
        Assert.assertTrue(tableDetailRs.next());
        final String tableGroupName = tableDetailRs.getString("TABLE_GROUP_NAME");

        String sql = "CREATE SCHEDULE "
            + "FOR AUTO_SPLIT_TABLE_GROUP "
            + "ON `" + tddlDatabase1 + "`.`" + tableGroupName + "`  "
            + "CRON '0 0 12 1/5 * ?'  "
            + "TIMEZONE '+00:00'";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "Duplicate AUTO_SPLIT_TABLE_GROUP Scheduled Job For TableGroup");

        ResultSet resultSet = JdbcUtil.executeQuery(
            "select * from information_schema.auto_split_schedule",
            tddlConnection
        );

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(resultSet.getString("schedule_expr"), "0 0 12 1/5 * ?");
        Assert.assertEquals(resultSet.getString("time_zone"), "+00:00");

        //test pause
        {
            int count =
                JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection,
                    "PAUSE SCHEDULE " + resultSet.getLong("schedule_id"));
            Assert.assertEquals(count, 1);
            validateScheduledJob(tddlConnection, true, false, resultSet.getLong("schedule_id"));
        }

        //test continue
        {
            int count =
                JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection,
                    "CONTINUE SCHEDULE " + resultSet.getLong("schedule_id"));
            Assert.assertEquals(count, 1);
            validateScheduledJob(tddlConnection, true, true, resultSet.getLong("schedule_id"));
        }

        //test drop
        {
            int count =
                JdbcUtil.executeUpdateAndGetEffectCount(tddlConnection,
                    "DROP SCHEDULE " + resultSet.getLong("schedule_id"));
            Assert.assertEquals(count, 1);
            validateScheduledJob(tddlConnection, false, false, resultSet.getLong("schedule_id"));
        }
    }
}
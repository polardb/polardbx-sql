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

package com.alibaba.polardbx.qatest.ddl.auto.autosplit;

import com.alibaba.polardbx.qatest.ddl.auto.localpartition.LocalPartitionBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public class AutoSplitScheduledJobTest extends LocalPartitionBaseTest {

    private String primaryTableName;
    private String primaryTableName2;
    private String primaryTableName3;

    @Before
    public void before() {
        primaryTableName = randomTableName("ts1", 4);
        primaryTableName2 = randomTableName("ts2", 4);
        primaryTableName3 = randomTableName("ts3", 4);
    }

    @After
    public void after() {
    }

    @Test
    public void test1() throws SQLException {

        //1. 创建一个表: ts1
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE PARTITION TABLE `%s`.`%s` (\n"
                    + "     `id` bigint(20) NOT NULL,\n"
                    + "     `gmt_modified` datetime NOT NULL,\n"
                    + "     PRIMARY KEY (`id`, `gmt_modified`)\n"
                    + ") \n"
                    + "PARTITION BY KEY(`id`,`gmt_modified`)\n"
                    + "PARTITIONS 3",
                tddlDatabase1,
                primaryTableName
            )
        );

        ResultSet tableDetail1 = JdbcUtil.executeQuery(
            String.format("select * from information_schema.table_detail where table_schema='%s' and table_name='%s'",
                tddlDatabase1, primaryTableName),
            tddlConnection
        );
        tableDetail1.next();
        final String tableGroupName1 = tableDetail1.getString("TABLE_GROUP_NAME");

        //2. 创建自动分裂的定时任务
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("CREATE SCHEDULE \n"
                + "FOR AUTO_SPLIT_TABLE_GROUP\n"
                + "ON `%s`.`%s` \n"
                + "params \"max_size=10000000\"\n"
                + "CRON '0 0 * * * ?' \n"
                + "TIMEZONE '+08:00'",
            tddlDatabase1,
            tableGroupName1
        ));

        //3. 重复创建自动分裂的定时任务，期望创建失败
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format("CREATE SCHEDULE \n"
                    + "FOR AUTO_SPLIT_TABLE_GROUP\n"
                    + "ON `%s`.`%s` \n"
                    + "params \"max_size=10000000\"\n"
                    + "CRON '0 0 * * * ?' \n"
                    + "TIMEZONE '+08:00'",
                tddlDatabase1,
                tableGroupName1),
            "Duplicate AUTO_SPLIT_TABLE_GROUP Scheduled Job For TableGroup"
        );

        //4. 创建一个相同定义的表，预期不会被分到同一个表组。因为自动分裂的表，会被打上auto_split=1的标记
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE PARTITION TABLE `%s`.`%s` (\n"
                    + "     `id` bigint(20) NOT NULL,\n"
                    + "     `gmt_modified` datetime NOT NULL,\n"
                    + "     PRIMARY KEY (`id`, `gmt_modified`)\n"
                    + ") \n"
                    + "PARTITION BY KEY(`id`,`gmt_modified`)\n"
                    + "PARTITIONS 3",
                tddlDatabase1,
                primaryTableName2
            )
        );
        ResultSet tableDetail2 = JdbcUtil.executeQuery(
            String.format("select * from information_schema.table_detail where table_schema='%s' and table_name='%s'",
                tddlDatabase1, primaryTableName2),
            tddlConnection
        );
        tableDetail2.next();
        final String tableGroupName2 = tableDetail2.getString("TABLE_GROUP_NAME");
        Assert.assertNotEquals(tableGroupName1, tableGroupName2);

        //5. 删除自动分裂的定时任务
        ResultSet scheduleIdResultSet = JdbcUtil.executeQuery(
            String.format(
                "select * from information_schema.auto_split_schedule where table_schema='%s' and table_group_name='%s'",
                tddlDatabase1, tableGroupName1),
            tddlConnection
        );
        scheduleIdResultSet.next();
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("DROP SCHEDULE %s", scheduleIdResultSet.getString("SCHEDULE_ID"))
        );

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("DROP TABLE %s", primaryTableName2)
        );

        //6. 创建一个相同定义的表，预期会会被分到同一个表组。
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("CREATE PARTITION TABLE `%s`.`%s` (\n"
                    + "     `id` bigint(20) NOT NULL,\n"
                    + "     `gmt_modified` datetime NOT NULL,\n"
                    + "     PRIMARY KEY (`id`, `gmt_modified`)\n"
                    + ") \n"
                    + "PARTITION BY KEY(`id`,`gmt_modified`)\n"
                    + "PARTITIONS 3",
                tddlDatabase1,
                primaryTableName3
            )
        );
        ResultSet tableDetail3 = JdbcUtil.executeQuery(
            String.format("select * from information_schema.table_detail where table_schema='%s' and table_name='%s'",
                tddlDatabase1, primaryTableName3),
            tddlConnection
        );
        tableDetail3.next();
        final String tableGroupName3 = tableDetail3.getString("TABLE_GROUP_NAME");
        Assert.assertEquals(tableGroupName1, tableGroupName3);
    }

}
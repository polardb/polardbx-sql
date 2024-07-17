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

import com.alibaba.polardbx.common.scheduler.SchedulerJobStatus;
import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@BinlogIgnore(ignoreReason = "drop local partition的动作目前无法透传给下游，导致binlog实验室上下游数据不一致，暂时忽略")
public abstract class LocalPartitionBaseTest extends DDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    public void validateNotLocalPartition(Connection connection, String localTableName) {
        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format("check table %s with local partition", localTableName),
            connection
        );
        try {
            Assert.assertTrue(resultSet.next());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try {
            Assert.assertEquals("Not a local partition table", resultSet.getString("PARTITION_DETAIL"));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void validateLocalPartition(Connection connection, String localTableName) {
        String result = JdbcUtil.executeQueryAndGetStringResult(
            String.format("check table %s with local partition", localTableName),
            connection,
            2
        );
        Assert.assertTrue("found local partition inconsistent", StringUtils.equalsIgnoreCase(result, "OK"));
    }

    public void validateLocalPartitionCount(Connection connection, String localTableName, int count) {
        ResultSet result = JdbcUtil.executeQuery(
            String.format("check table %s with local partition", localTableName),
            connection
        );

        try {
            Assert.assertTrue(result.next());
            String msg = result.getString("STATUS");
            Assert.assertTrue("found local partition inconsistent", StringUtils.equalsIgnoreCase(msg, "OK"));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try {
            int partitionCount = result.getInt("PARTITION_COUNT");
            Assert.assertEquals("unexpected local partition count", count, partitionCount);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void validateScheduledJob(Connection connection, boolean exist, String schemaName, String localTableName) {
        validateScheduledJob(connection, exist, true, schemaName, localTableName);
    }

    public void validateScheduledJob(Connection connection, boolean exist, boolean enabled, String schemaName,
                                     String localTableName) {
        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format(
                "select * from metadb.scheduled_jobs where table_schema='%s' and table_name='%s'",
                schemaName, localTableName
            ),
            connection
        );

        try {
            if (exist) {
                Assert.assertTrue(resultSet.next());
            } else {
                Assert.assertFalse(resultSet.next());
            }

            final String scheduledJobStatus = resultSet.getString("status");
            Assert.assertEquals(enabled,
                StringUtils.equalsIgnoreCase(scheduledJobStatus, SchedulerJobStatus.ENABLED.name()));

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void validateScheduledJob(Connection connection, boolean exist, boolean enabled, long scheduleId) {
        ResultSet resultSet = JdbcUtil.executeQuery(
            String.format(
                "select * from metadb.scheduled_jobs where schedule_id=%s",
                scheduleId
            ),
            connection
        );

        try {
            if (exist) {
                Assert.assertTrue(resultSet.next());
            } else {
                Assert.assertFalse(resultSet.next());
            }

            final String scheduledJobStatus = resultSet.getString("status");
            Assert.assertEquals(enabled,
                StringUtils.equalsIgnoreCase(scheduledJobStatus, SchedulerJobStatus.ENABLED.name()));

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public ResultSet getLocalPartitionMeta(Connection connection,
                                           String schemaName,
                                           String localTableName) throws SQLException {
        return JdbcUtil.executeQuery(
            String.format("/*+TDDL:cmd_extra(ENABLE_MPP=FALSE)*/select * from information_schema.local_partitions "
                + "where table_schema='%s' and table_name = '%s'", schemaName, localTableName),
            connection
        );
    }

//    @After
//    public void cleanDb() {
//        cleanDataBase();
//    }
}
package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class InterruptDDLByShardTest extends InterruptDDLTest {

    protected JobInfo executeAsyncDDL(String sql, String taskPrefix, int expectedNumShardDone) throws SQLException {
        return executeAsyncDDL(sql, taskPrefix, expectedNumShardDone, false);
    }

    protected JobInfo executeAsyncDDL(String sql, String taskPrefix, int expectedNumShardDone,
                                      boolean untilSubJobAppears) throws SQLException {
        JdbcUtil.executeSuccess(tddlConnection, HINT_PURE_MODE + sql);
        return fetchCurrentJobUntilAppears(taskPrefix, expectedNumShardDone, untilSubJobAppears);
    }

    protected JobInfo executeSeparateDDL(String sql, String taskPrefix, int expectedNumShardDone) throws SQLException {
        return executeSeparateDDL(sql, taskPrefix, expectedNumShardDone, false);
    }

    protected JobInfo executeSeparateDDL(String sql, String taskPrefix, int expectedNumShardDone,
                                         boolean untilSubJobAppears) throws SQLException {
        new Thread(() -> {
            try (Connection newConn = getNewTddlConnection1()) {
                JdbcUtil.executeSuccess(newConn, sql);
            } catch (SQLException e) {
                Assert.fail("Cannot get new connection");
            }
        }, "DDL-Execution").start();

        return fetchCurrentJobUntilAppears(taskPrefix, expectedNumShardDone, untilSubJobAppears);
    }

    protected JobInfo fetchCurrentJobUntilAppears(String taskPrefix, int expectedNumShardDone,
                                                  boolean untilSubJobAppears) throws SQLException {
        JobInfo job;

        do {
            job = fetchCurrentJob();
        } while (job == null || (untilSubJobAppears && job.subJobs.isEmpty()));

        if (expectedNumShardDone > 0) {
            waitUntilTargetNumPhyDDLsDone(taskPrefix, expectedNumShardDone);
        }

        return job;
    }

    protected void waitUntilTargetNumPhyDDLsDone(String taskPrefix, int expectedNumShardDone) throws SQLException {
        while (!checkIfTargetNumPhyDDLsDone(taskPrefix, expectedNumShardDone)) {
            waitForSeconds(1);
        }
    }

    protected boolean checkIfTargetNumPhyDDLsDone(String taskPrefix, long expectedNumShardDone) throws SQLException {
        String phyDdlDoneInfo = fetchPhyDDLDoneInfo(taskPrefix);
        if (TStringUtil.isNotEmpty(phyDdlDoneInfo)) {
            int countShardDone = 0;
            String[] shards = phyDdlDoneInfo.split(";");
            for (String shard : shards) {
                String[] parts = shard.split(":");
                if (parts.length > 3) {
                    if (Boolean.valueOf(parts[2])) {
                        countShardDone++;
                    }
                } else {
                    countShardDone++;
                }
            }
            return countShardDone >= expectedNumShardDone;
        } else {
            JobInfo jobInfo = fetchCurrentJob();
            return jobInfo == null || jobInfo.parentJob.state.equals(DdlState.COMPLETED.name())
                || jobInfo.parentJob.state.equals(DdlState.ROLLBACK_COMPLETED.name());
        }
    }

}

package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public abstract class InterruptDDLByLockTest extends InterruptDDLTest {

    protected static final int SLEEP_TIME_IN_SEC = 15;

    protected JobInfo executeAsyncDDL(String sql, String taskPrefix, boolean noneDone) throws SQLException {
        return executeAsyncDDL(sql, taskPrefix, noneDone, false);
    }

    protected JobInfo executeAsyncDDL(String sql, String taskPrefix, boolean noneDone, boolean untilSubJobAppears)
        throws SQLException {
        sleepOnPhyTableToSimulateLongRun(noneDone, SLEEP_TIME_IN_SEC);
        JdbcUtil.executeSuccess(tddlConnection, HINT_PURE_MODE + sql);
        return fetchCurrentJobUntilAppears(taskPrefix, noneDone, untilSubJobAppears);
    }

    protected JobInfo executeSeparateDDL(String sql, String taskPrefix, boolean noneDone) throws SQLException {
        return executeSeparateDDL(sql, taskPrefix, noneDone, false);
    }

    protected JobInfo executeSeparateDDL(String sql, String taskPrefix, boolean noneDone, boolean untilSubJobAppears)
        throws SQLException {
        sleepOnPhyTableToSimulateLongRun(noneDone, SLEEP_TIME_IN_SEC);

        new Thread(() -> {
            try (Connection newConn = getNewTddlConnection1()) {
                JdbcUtil.executeSuccess(newConn, sql);
            } catch (SQLException e) {
                Assert.assertTrue(TStringUtil.containsIgnoreCase(e.getMessage(), "Found Paused DDL JOB"));
            }
        }, "DDL-Execution").start();

        return fetchCurrentJobUntilAppears(taskPrefix, noneDone, untilSubJobAppears);
    }

    protected JobInfo fetchCurrentJobUntilAppears(String taskPrefix, boolean noneDone, boolean untilSubJobAppears)
        throws SQLException {
        JobInfo job;

        do {
            job = fetchCurrentJob();
        } while (job == null ||
            DdlState.QUEUED.name().equalsIgnoreCase(job.parentJob.state) ||
            (untilSubJobAppears && job.subJobs.isEmpty()));

        if (!noneDone) {
            waitUntilAtLeastOneShardDone(taskPrefix);
        }

        return job;
    }

    protected void sleepOnPhyTableToSimulateLongRun(boolean all, int timeInSeconds) throws SQLException {
        String sleepSql = HINT_NODE_PUSHED + "select sleep(%s) from %s for update";
        List<Pair<String, String>> groupPhyTables = showTopology();
        for (int i = 0; i < groupPhyTables.size(); i++) {
            String groupNo = groupPhyTables.get(i).getKey();
            String phyTable = groupPhyTables.get(i).getValue();

            if (!all && i < (groupPhyTables.size() - 2)) {
                continue;
            }

            if (!all && i == (groupPhyTables.size() - 1)) {
                timeInSeconds += 10;
            }

            String sql = String.format(sleepSql, groupNo, timeInSeconds, phyTable);
            new Thread(() -> {
                try (Connection newConn = getNewTddlConnection1();
                    PreparedStatement ps = newConn.prepareStatement(sql)) {
                    ps.executeUpdate();
                } catch (SQLException e) {
                    Assert.fail("Failed to sleep on physical tables with a new connection");
                }
            }, String.format("Sleep-On-%s-%s", groupNo, phyTable)).start();
        }
    }

    protected void waitUntilAtLeastOneShardDone(String taskPrefix) throws SQLException {
        while (!checkIfAtLeastOneShardDone(taskPrefix)) {
            waitForSeconds(1);
        }
    }

    protected boolean checkIfAtLeastOneShardDone(String taskPrefix) throws SQLException {
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
            return countShardDone > 0;
        } else {
            JobInfo jobInfo = fetchCurrentJob();
            return jobInfo == null || jobInfo.parentJob.state.equals(DdlState.COMPLETED.name())
                || jobInfo.parentJob.state.equals(DdlState.ROLLBACK_COMPLETED.name());
        }
    }

}

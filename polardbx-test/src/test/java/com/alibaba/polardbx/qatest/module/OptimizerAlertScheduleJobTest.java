package com.alibaba.polardbx.qatest.module;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.scheduler.executor.OptimizerAlertScheduledJob;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

@FileStoreIgnore
public class OptimizerAlertScheduleJobTest extends ReadBaseTestCase {
    protected static final String FIND_SCHEDULE =
        String.format("select schedule_id from metadb.SCHEDULED_JOBS where executor_type='%s'",
            ScheduledJobExecutorType.OPTIMIZER_ALERT.name());

    protected static final String OPTIMIZER_ALERT =
        "select ALERT_COUNT from information_schema.optimizer_alert where ALERT_TYPE='%s'";

    protected static final String SCHEDULE_REMARK =
        String.format("select LASTJOB_REMARK from information_schema.schedule_jobs where JOB_TYPE='%s'",
            ScheduledJobExecutorType.OPTIMIZER_ALERT.name());
    protected static final String FIRE_SCHEDULE = "fire schedule %s";

    protected static final String JOBS_COUNT =
        "select count(1) from metadb." + GmsSystemTables.FIRED_SCHEDULED_JOBS +
            " WHERE fire_time <= UNIX_TIMESTAMP() AND schedule_id = %s";
    protected ErrorCode ERR_FROM_SCHEDULE = ErrorCode.ERR_VIEW;

    public OptimizerAlertScheduleJobTest() {
    }

    @Test
    public void testXplanSlow() {
        testFrameWork(OptimizerAlertType.XPLAN_SLOW, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                String sql;
                sql = "set ENABLE_ALERT_TEST=true";
                statement.execute(sql);
                sql =
                    String.format(
                        "select * from select_base_four_%s where pk =10", ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX);
                statement.execute(sql);
                return true;
            }
        });
    }

    @Test
    public void testPlanCacheFull() {
        testFrameWork(OptimizerAlertType.PLAN_CACHE_FULL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                String sql;
                sql = "set global OPTIMIZER_ALERT_LOG_INTERVAL = 1000";
                statement.execute(sql);
                sql = "set ENABLE_ALERT_TEST=true";
                statement.execute(sql);
                Thread.sleep(2000L);
                sql = "clear plancache";
                statement.execute(sql);
                sql = "select * from select_base_four_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX;
                statement.execute(sql);
                return true;
            } finally {
                try (Connection conn = getPolardbxConnection()) {
                    String sql = "set global OPTIMIZER_ALERT_LOG_INTERVAL = " + 600000;
                    JdbcUtil.executeSuccess(conn, sql);
                }
            }
        });
    }

    @Test
    public void testBKATooMuch() {
        testFrameWork(OptimizerAlertType.BKA_TOO_MUCH, () -> {
            try (Connection conn = getPolardbxConnection()) {
                String table = "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
                String sql =
                    String.format("/*+TDDL:cmd_extra(ENABLE_ALERT_TEST=true) BKA_JOIN(%s, %s)*/ "
                            + "select * from %s A, %S B where A.integer_test = B.integer_test"
                        , table, table, table, table);
                JdbcUtil.executeSuccess(conn, sql);
                return true;
            }
        });
    }

    @Test
    public void testTpSlow() {
        testFrameWork(OptimizerAlertType.TP_SLOW, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                String sql =
                    String.format(
                        "/*+TDDL:cmd_extra(ENABLE_ALERT_TEST=true)*/select * from select_base_four_%s where integer_test =10",
                        ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX);
                statement.execute(sql);
                return true;
            }
        });
    }

    protected void testFrameWork(OptimizerAlertType targetAlert, Callable<?> func) {
        for (int cnt = 3; cnt >= 0; cnt--) {
            try {
                // test single may throw ERR_FROM_SCHEDULE when FIRE_SCHEDULE clashes with auto schedule,
                // retry this three times to avoid the case
                testSingle(targetAlert, func);
                return;
            } catch (TddlRuntimeException e) {
                if (e.getErrorCodeType() != ERR_FROM_SCHEDULE || cnt == 0) {
                    throw e;
                }
            }
        }
    }

    protected void testSingle(OptimizerAlertType targetAlert, Callable<?> func) {
        // find schedule id
        long schedule_id;
        try (ResultSet rs = JdbcUtil.executeQuery(FIND_SCHEDULE, tddlConnection)) {
            if (rs.next()) {
                schedule_id = rs.getLong("schedule_id");
            } else {
                throw new RuntimeException("OPTIMIZER_ALERT schedule job not found!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // clear schedule count
        String result;
        fireSchedule(schedule_id);
        long beforeCount = alertCount(targetAlert);
        // add alert
        try {
            func.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // schedule job did alert
        result = fireSchedule(schedule_id);
        assertThat(result).contains(OptimizerAlertScheduledJob.HAS_ALERT);
        assertThat(result).contains(targetAlert.name());
        long afterCount = alertCount(targetAlert);
        assertWithMessage(String.format("more alerts should be recorded")).that(afterCount)
            .isGreaterThan(beforeCount);

        // no alert happened
        result = fireSchedule(schedule_id);
        assertThat(result).contains(OptimizerAlertScheduledJob.NO_ALERT);
        long finalCount = alertCount(targetAlert);
        assertWithMessage(String.format("no more alert should be recorded")).that(finalCount).isEqualTo(afterCount);
    }

    private String fireSchedule(long id) throws TddlRuntimeException {
        try {
            long targetCnt = countFiredScheduleJobs(tddlConnection, id) + 1;
            JdbcUtil.executeQuery(String.format(FIRE_SCHEDULE, id), tddlConnection);
            Thread.sleep(1000L);

            // wait until the job is finished
            int waitCnt = 0;
            while (!checkScheduleResultState(tddlConnection, id, targetCnt,
                new String[] {
                    FiredScheduledJobState.SUCCESS.name(), FiredScheduledJobState.INTERRUPTED.name()})) {
                waitCnt++;
                assertWithMessage("optimizer alert job is unfinished, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(20);
                Thread.sleep(5 * 1000L);
            }
            ResultSet rs = JdbcUtil.executeQuery(SCHEDULE_REMARK, tddlConnection);
            Assert.assertTrue(rs.next());
            return rs.getString(1);
        } catch (InterruptedException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long alertCount(OptimizerAlertType targetAlert) {
        long count = 0L;
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format(OPTIMIZER_ALERT, targetAlert.name()), tddlConnection)) {
            while (rs.next()) {
                count += rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    private long countFiredScheduleJobs(Connection conn, long id) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(JOBS_COUNT, id), conn)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0L;
    }

    private boolean checkScheduleResultState(Connection conn, long id, long targetCnt, String[] states)
        throws SQLException {
        if (countFiredScheduleJobs(conn, id) != targetCnt) {
            throw new TddlRuntimeException(ERR_FROM_SCHEDULE, "");
        }
        try (ResultSet resultSet = JdbcUtil.executeQuery("show schedule result " + id, conn)) {
            Assert.assertTrue(resultSet.next());
            for (String state : states) {
                if (state.equalsIgnoreCase(resultSet.getString(6))) {
                    return true;
                }
            }
        }
        return false;
    }
}

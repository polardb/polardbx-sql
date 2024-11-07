package com.alibaba.polardbx.qatest.NotThreadSafe;

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.concurrent.Callable;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

@FileStoreIgnore
public class OptimizerAlertScheduleJobTest extends ReadBaseTestCase {
    private static final Log log = LogFactory.getLog(JdbcUtil.class);

    protected static final String FIND_SCHEDULE =
        String.format("select schedule_id from metadb.SCHEDULED_JOBS where executor_type='%s'",
            ScheduledJobExecutorType.OPTIMIZER_ALERT.name());

    protected static final String OPTIMIZER_ALERT =
        "select COMPUTE_NODE, ALERT_COUNT from information_schema.optimizer_alert where ALERT_TYPE='%s'";

    protected static final String SCHEDULE_REMARK =
        String.format("select LASTJOB_REMARK from information_schema.schedule_jobs where JOB_TYPE='%s'",
            ScheduledJobExecutorType.OPTIMIZER_ALERT.name());
    protected static final String FIRE_SCHEDULE = "fire schedule %s";

    protected static final String JOBS_COUNT =
        "select count(1) from metadb." + GmsSystemTables.FIRED_SCHEDULED_JOBS +
            " WHERE fire_time <= UNIX_TIMESTAMP() AND schedule_id = %s";
    protected ErrorCode ERR_FROM_SCHEDULE = ErrorCode.ERR_VIEW;

    protected static final String JOB_STATE_CHECK =
        "select state from metadb.fired_scheduled_jobs where schedule_id=%s and from_unixtime(fire_time)>='%s' order by fire_time limit 1";

    protected static final String ENABLE_DEFAULT_CHECK = "set global ENABLE_ALERT_TEST_DEFAULT= true";

    protected static final String DISABLE_DEFAULT_CHECK = "set global ENABLE_ALERT_TEST_DEFAULT= false";

    protected static final String DISABLE_TP_SLOW_ALERT_THRESHOLD = "set global ENABLE_TP_SLOW_ALERT_THRESHOLD = 0";

    protected static final String ENABLE_TP_SLOW_ALERT_THRESHOLD = "set global ENABLE_TP_SLOW_ALERT_THRESHOLD = 10";

    public OptimizerAlertScheduleJobTest() {
    }

    @Test
    public void testBKATooMuch() {
        testShouldAlert(OptimizerAlertType.BKA_TOO_MUCH, () -> {
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
        testShouldAlert(OptimizerAlertType.TP_SLOW, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                String sql =
                    String.format(
                        "/*+TDDL:cmd_extra(ENABLE_ALERT_TEST=true enable_mpp=false WORKLOAD_TYPE=TP)*/"
                            + "select * from select_base_four_%s where integer_test =10",
                        ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX);
                statement.execute(sql);
                return true;
            }
        });
    }

    @Test
    public void testTpSlowNotAlert() {
        testShouldNotAlert(OptimizerAlertType.TP_SLOW, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                // test direct plan
                String sql = "set ENABLE_ALERT_TEST=true";
                statement.execute(sql);
                sql = "set WORKLOAD_TYPE='TP'";
                statement.execute(sql);
                sql = String.format("select * from select_base_four_%s where pk = 1;",
                    ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX);
                statement.execute(sql);
                sql = String.format("select * from select_base_four_%s where pk = 1;",
                    ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX);
                // test node hint
                statement.execute(sql);
                sql = String.format("/*+TDDL:node(0)*/ select * from select_base_four_%s;",
                    ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX);
                statement.execute(sql);
                return true;
            }
        });
    }

    @Test
    public void testStatisticJobInterrupt() throws SQLException, InterruptedException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("set global alert_statistic_interrupt=true");
            statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='true'");
            statement.execute("analyze table select_base_four_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX);
        }
        // wait 5 sec
        Thread.sleep(5 * 1000L);

        testShouldAlert(OptimizerAlertType.STATISTIC_JOB_INTERRUPT, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });

        testShouldAlert(OptimizerAlertType.STATISTIC_JOB_INTERRUPT, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {

                statement.execute("set global alert_statistic_interrupt=true");
                statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='true'");
                // wait 5 sec
                Thread.sleep(5 * 1000L);
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_hll_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });

        try (Connection connection = getPolardbxConnection()) {
            connection.createStatement().execute("set global alert_statistic_interrupt=false");
        }
    }

    protected void testShouldAlert(OptimizerAlertType targetAlert, Callable<?> func) {
        try (Connection conn = getPolardbxConnection()) {
            JdbcUtil.executeSuccess(conn, DISABLE_DEFAULT_CHECK);
            JdbcUtil.executeSuccess(conn, DISABLE_TP_SLOW_ALERT_THRESHOLD);
            Thread.sleep(5000L);
            for (int cnt = 3; cnt >= 0; cnt--) {
                try {
                    // test single may throw ERR_FROM_SCHEDULE when FIRE_SCHEDULE clashes with auto schedule,
                    // retry this three times to avoid the case
                    testSingle(targetAlert, func, true);
                    return;
                } catch (TddlRuntimeException e) {
                    if (e.getErrorCodeType() != ERR_FROM_SCHEDULE || cnt == 0) {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try (Connection conn = getPolardbxConnection()) {
                JdbcUtil.executeSuccess(conn, ENABLE_DEFAULT_CHECK);
                JdbcUtil.executeSuccess(conn, ENABLE_TP_SLOW_ALERT_THRESHOLD);
                Thread.sleep(2000L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void testShouldNotAlert(OptimizerAlertType targetAlert, Callable<?> func) {
        try (Connection conn = getPolardbxConnection()) {
            JdbcUtil.executeSuccess(conn, DISABLE_DEFAULT_CHECK);
            JdbcUtil.executeSuccess(conn, DISABLE_TP_SLOW_ALERT_THRESHOLD);
            // wait for 5 seconds to make it works
            Thread.sleep(5000L);
            testSingle(targetAlert, func, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try (Connection conn = getPolardbxConnection()) {
                JdbcUtil.executeSuccess(conn, ENABLE_DEFAULT_CHECK);
                JdbcUtil.executeSuccess(conn, ENABLE_TP_SLOW_ALERT_THRESHOLD);
                Thread.sleep(2000L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    protected void testSingle(OptimizerAlertType targetAlert, Callable<?> func, boolean shouldAlert) {
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

        log.error(String.format("start to check alert, before count = %d", beforeCount));
        if (shouldAlert) {
            // schedule job did alert
            result = fireSchedule(schedule_id);
            long afterCount = alertCount(targetAlert);
            assertThat(result).contains(OptimizerAlertScheduledJob.HAS_ALERT);
            assertThat(result).contains(targetAlert.name());
            assertWithMessage(String.format("more alerts should be recorded")).that(afterCount)
                .isGreaterThan(beforeCount);
            // no alert happened
            result = fireSchedule(schedule_id);
            long finalCount = alertCount(targetAlert);
            assertThat(result).contains(OptimizerAlertScheduledJob.NO_ALERT);
            assertWithMessage(String.format("no more alert should be recorded")).that(finalCount).isEqualTo(afterCount);
        } else {
            // schedule job didn't alert
            result = fireSchedule(schedule_id);
            if (result.contains(OptimizerAlertScheduledJob.HAS_ALERT)) {
                assertThat(result).doesNotContain(targetAlert.name());
            }

            // no alert happened
            result = fireSchedule(schedule_id);
            long finalCount = alertCount(targetAlert);
            assertThat(result).contains(OptimizerAlertScheduledJob.NO_ALERT);
            assertWithMessage(String.format("no more alert should be recorded")).that(finalCount)
                .isEqualTo(beforeCount);
        }
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
                System.out.println(rs.getString(1) + " " + rs.getLong(2));
                count += rs.getLong(2);
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

    public static boolean checkScheduleResultState(Connection conn, long id, String[] states, String now)
        throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            String.format(JOB_STATE_CHECK, id, now), conn)) {
            Assert.assertTrue(resultSet.next());
            String curState = resultSet.getString(1);
            System.out.println(curState);
            for (String state : states) {
                if (state.equalsIgnoreCase(curState)) {
                    return true;
                }
            }
        }
        return false;
    }

    @AfterClass
    public static void clean() throws SQLException {
        try (Connection conn = getPolardbxConnection0();
            Statement statement = conn.createStatement()) {
            statement.execute("set global alert_statistic_interrupt=false");
            statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='false'");
            statement.execute("set global alert_statistic_inconsistent=false");
        }
    }
}

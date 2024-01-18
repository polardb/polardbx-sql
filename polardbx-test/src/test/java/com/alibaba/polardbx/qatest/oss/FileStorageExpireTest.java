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

package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.ArchiveOSSTableDataMppTask;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static com.alibaba.polardbx.common.properties.ConnectionParams.OSS_BACKFILL_SPEED_LIMITATION;
import static com.google.common.truth.Truth.assertWithMessage;

public class FileStorageExpireTest extends BaseTestCase {

    private static String testDataBase = "fileStorageExpireDatabase";

    private static Engine engine = PropertiesUtil.engine();

    private String extraHint;

    // Limit back-fill speed to optimize test case
    private static int OSS_BACKFILL_SPEED_LIMITATION_FOR_TEST = 5000;

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    @BeforeClass
    public static void setBackfillParams() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "SET ENABLE_SET_GLOBAL=true");
            JdbcUtil.executeUpdateSuccess(tmpConnection,
                String.format("SET GLOBAL OSS_BACKFILL_SPEED_LIMITATION = %d", OSS_BACKFILL_SPEED_LIMITATION_FOR_TEST));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Before
    public void initTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
            stmt.execute(String.format("create database %s mode = 'auto'", testDataBase));
            String modifyMaintenanceTimeSql =
                "SET GLOBAL MAINTENANCE_TIME_START = '%02d:%02d', GLOBAL MAINTENANCE_TIME_END = '%02d:%02d'";

            // Make sure current time is out of maintenance window
            stmt.execute("SET ENABLE_SET_GLOBAL=true");
            // Non-standard usage
            stmt.execute(String.format(modifyMaintenanceTimeSql, 0, 0, 24, 0));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @After
    public void clearTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute("SET ENABLE_SET_GLOBAL=true");
            stmt.execute("SET GLOBAL PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES=120");
            stmt.execute("set @FP_RANDOM_BACKFILL_EXCEPTION=NULL");
            stmt.execute("set @FP_FAIL_ON_DDL_TASK_NAME=NULL");
            stmt.execute("set @FP_OVERRIDE_NOW=NULL");
            stmt.execute("set @FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION=NULL");
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void clearFP() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.executeSuccess(tmpConnection,
                "set @FP_TTL_PAUSE=NULL, @FP_INJECT_IGNORE_INNER_INTERRUPTION_TO_LOCAL_PARTITION=NULL");
            JdbcUtil.executeSuccess(tmpConnection, "SET ENABLE_SET_GLOBAL=true");
            JdbcUtil.executeSuccess(tmpConnection, String.format("SET GLOBAL OSS_BACKFILL_SPEED_LIMITATION = %s",
                OSS_BACKFILL_SPEED_LIMITATION.getDefault()));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Parameterized.Parameters(name = "{index}:extra_hint={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(
            new String[][] {
                {"FORBID_REMOTE_DDL_TASK=true"},
                {"FORBID_REMOTE_DDL_TASK=false"}
            });
    }

    public FileStorageExpireTest(String extraHint) {
        this.extraHint = extraHint;
    }

    @Test
    public void testExpireContinue() {
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testExpireContinue";
            String ossTestTableName = "oss_" + innodbTestTableName;
            FileStorageTestUtil.prepareInnoTable(conn, innodbTestTableName, 2000);
            FileStorageTestUtil.prepareTTLTable(conn, ossTestTableName, innodbTestTableName, engine);

            String expireSql = String.format(
                "/*+TDDL:cmd_extra(OSS_ORC_INDEX_STRIDE=200,OSS_MAX_ROWS_PER_FILE=1,"
                    + "ENABLE_EXPIRE_FILE_STORAGE_PAUSE=true, ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE=true, %s)*/ "
                    + "alter table %s expire local partition", extraHint, innodbTestTableName);

            long totalCount = FileStorageTestUtil.count(conn, innodbTestTableName);

            continueDDL(conn, expireSql);

            long partCount1 = FileStorageTestUtil.count(conn, innodbTestTableName);
            long partCount2 = FileStorageTestUtil.count(conn, ossTestTableName);

            Assert.assertTrue(partCount2 > 0);
            Assert.assertTrue(totalCount == partCount1 + partCount2);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * test the rollback of archive
     */
    @Test
    public void testExpireRollBackTest() {
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testExpireRollback";
            String ossTestTableName = "oss_" + innodbTestTableName;

            //prepare innodb table
            FileStorageTestUtil.prepareInnoTable(conn, innodbTestTableName, 2000, false);
            FileStorageTestUtil.prepareTTLTable(conn, ossTestTableName, innodbTestTableName, engine);

            String expireSql = String.format(
                "/*+TDDL:cmd_extra(OSS_ORC_INDEX_STRIDE=50,OSS_MAX_ROWS_PER_FILE=200,"
                    + "ENABLE_EXPIRE_FILE_STORAGE_PAUSE=true, ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE=true, %s)*/ "
                    + "alter table %s expire local partition", extraHint, innodbTestTableName);

            // test rollback
            long totalCount = FileStorageTestUtil.count(conn, innodbTestTableName);

            int tests = 10;
            int rollbacks = 0;
            for (int j = 0; j < tests; j++) {
                try {
                    JdbcUtil.executeUpdate(conn, expireSql);
                } catch (Throwable t) {
                    rollbacks++;
                    ResultSet resultSet = JdbcUtil.executeQuery("show ddl", conn);
                    Assert.assertTrue(resultSet.next());
                    String jobId = resultSet.getString("JOB_ID");
                    JdbcUtil.executeUpdateSuccess(conn, String.format("rollback ddl %s", jobId));
                    resultSet.close();
                }
            }
            assertWithMessage("rollback number is not equal to test cases").that(rollbacks).isEqualTo(tests);
            assertWithMessage("Table " + ossTestTableName + " is not empty")
                .that(FileStorageTestUtil.count(conn, ossTestTableName)).isEqualTo(0);
            assertWithMessage("Table " + ossTestTableName + " loses some data")
                .that(FileStorageTestUtil.count(conn, innodbTestTableName)).isEqualTo(totalCount);

            // test continue
            continueDDL(conn, expireSql);

            long partCount1 = FileStorageTestUtil.count(conn, innodbTestTableName);
            long partCount2 = FileStorageTestUtil.count(conn, ossTestTableName);

            Assert.assertTrue(partCount2 > 0);
            Assert.assertTrue(totalCount == partCount1 + partCount2);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * test correctness of LocalPartitionScheduledJob
     */
    @Test
    public void testTTLScheduleJob() {
        // Caution: to make this works, fail point injection must be enabled with
        // vm option "-ea:com.alibaba.polardbx.executor.utils.failpoint..."
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testSchedulePause";
            String ossTestTableName = "oss_" + innodbTestTableName;
            // prepare table, which costs about 10 seconds to back-fill
            FileStorageTestUtil.prepareInnoTable(conn, innodbTestTableName,
                OSS_BACKFILL_SPEED_LIMITATION_FOR_TEST / 12 * 10, true);
            FileStorageTestUtil.prepareTTLTable(conn, ossTestTableName, innodbTestTableName, engine);
            FileStorageTestUtil.pauseSchedule(conn, testDataBase, innodbTestTableName);

            long totalCount = FileStorageTestUtil.count(conn, innodbTestTableName);

            String findTTL = String.format("select schedule_id from metadb.scheduled_jobs where table_schema = '%s' "
                    + "and table_name = '%s' and executor_type = '%s' "
                , testDataBase, innodbTestTableName, "LOCAL_PARTITION");
            ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn);
            Assert.assertTrue(resultSet.next());
            long id = resultSet.getLong(1);
            resultSet.close();
            String fireTTL = "fire schedule " + id;

            int failCnt = 0;
            FileStorageTestUtil.continueSchedule(conn, testDataBase, innodbTestTableName);

            // note that the ttl_pause may be too short or too long, which will cause the test fail
            long[] ttl_pause = new long[] {1, 5, 10, 20};
            for (long pause : ttl_pause) {

                int beforeCnt = countScheduleResult(conn, id);
                JdbcUtil.executeSuccess(conn, String.format("set @FP_TTL_PAUSE='%d'", pause));
                JdbcUtil.executeQuery(fireTTL, conn);
                // sleep to wait for the schedule job finish
                Thread.sleep((pause + 5) * 1000L);
                Assert.assertTrue(countScheduleResult(conn, id) > beforeCnt);

                // wait until the job is finished
                int waitCnt = 0;
                while (!checkScheduleResultState(conn, id,
                    new String[] {
                        FiredScheduledJobState.SUCCESS.name(), FiredScheduledJobState.INTERRUPTED.name(),
                        FiredScheduledJobState.QUEUED.name()})) {
                    waitCnt++;
                    assertWithMessage("TTL job is unfinished, schedule_id is: " + id)
                        .that(waitCnt).isLessThan(25);
                    Thread.sleep(5 * 1000L);
                }

                if (checkScheduleResultState(conn, id,
                    new String[] {FiredScheduledJobState.INTERRUPTED.name(), FiredScheduledJobState.QUEUED.name()})) {
                    failCnt++;
                }
            }
            assertWithMessage("TTL should pause for at least once").that(failCnt).isGreaterThan(0);

            JdbcUtil.executeSuccess(conn, "set @FP_TTL_PAUSE=NULL");
            JdbcUtil.executeQuery(fireTTL, conn);
            // Wait until the job is finished
            int waitCnt = 0;
            while (!checkScheduleResultState(conn, id,
                new String[] {FiredScheduledJobState.SUCCESS.name()})) {
                waitCnt++;
                assertWithMessage("TTL job is unfinished, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(50);
                Thread.sleep(5 * 1000L);
            }
            // test row count continue
            long partCount1 = FileStorageTestUtil.count(conn, innodbTestTableName);
            long partCount2 = FileStorageTestUtil.count(conn, ossTestTableName);

            Assert.assertTrue(partCount2 > 0);
            Assert.assertTrue(totalCount == partCount1 + partCount2);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Test correctness of TTL job in maintenance window:
     * QUEUE -> RUNNING ---(1min)--> INTERRUPTED -> RUNNING -> SUCCESS
     * The workload of back-fill must cost more than 1 min
     */
    @Test
    public void testTTLScheduleJobInterruptedByMaintenanceWindow() {
        try (Connection conn = getConnection()) {
            // Make sure current time is in maintenance window
            JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_SET_GLOBAL=true");
            String modifyMaintenanceTimeSql =
                "SET GLOBAL MAINTENANCE_TIME_START = '%02d:%02d', GLOBAL MAINTENANCE_TIME_END = '%02d:%02d'";
            // Non-standard usage
            JdbcUtil.executeUpdateSuccess(conn, String.format(modifyMaintenanceTimeSql, 0, 0, 24, 0));

            String innodbTestTableName = "testScheduleMaintenanceWindow";
            String ossTestTableName = "oss_" + innodbTestTableName;
            // prepare table, which costs about 60 seconds to back-fill
            FileStorageTestUtil.prepareInnoTable(conn, innodbTestTableName,
                OSS_BACKFILL_SPEED_LIMITATION_FOR_TEST / 12 * 60, true);
            FileStorageTestUtil.prepareTTLTable(conn, ossTestTableName, innodbTestTableName, engine);

            long totalCount = FileStorageTestUtil.count(conn, innodbTestTableName);

            String findTTL = String.format("select schedule_id from metadb.scheduled_jobs where table_schema = '%s' "
                    + "and table_name = '%s' and executor_type = '%s' "
                , testDataBase, innodbTestTableName, "LOCAL_PARTITION");
            ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn);
            Assert.assertTrue(resultSet.next());
            long id = resultSet.getLong(1);

            String fireTTL = "fire schedule " + id;
            JdbcUtil.executeQuery(fireTTL, conn);

            // Wait 2 minutes for scheduled job scanner to fire the job: QUEUE -> RUNNING
            int waitCnt = 0;
            while (countScheduleResult(conn, id) == 0 || !checkScheduleResultState(conn, id,
                new String[] {FiredScheduledJobState.RUNNING.name()})) {
                waitCnt++;
                assertWithMessage("TTL job is not running within 2 minutes, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(25);
                Thread.sleep(5 * 1000L);
            }

            int currentHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
            int nextHour = (currentHour + 1) % 24;
            int currentMinute = Calendar.getInstance().get(Calendar.MINUTE);

            // Make sure current time is out of maintenance window
            JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_SET_GLOBAL=true");
            JdbcUtil.executeUpdateSuccess(conn,
                String.format(modifyMaintenanceTimeSql, nextHour, currentMinute, nextHour, currentMinute));

            // Wait 2 minutes for ScheduleManager to interrupt the job: RUNNING -> INTERRUPTED
            waitCnt = 0;
            while (!checkScheduleResultState(conn, id, new String[] {
                FiredScheduledJobState.INTERRUPTED.name(),
                FiredScheduledJobState.SUCCESS.name()})) {
                waitCnt++;
                assertWithMessage("TTL job is not interrupted or finished within 2 minuts, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(25);
                Thread.sleep(5 * 1000L);
            }

            // Make sure the job is interrupted by maintenance window
            String moduleLogSql = String.format(
                "select event from information_schema.module_event WHERE log_pattern='INTERRUPTED' and event like '%%LOCAL_PARTITION,%d,%%'",
                id);
            resultSet = JdbcUtil.executeQuery(moduleLogSql, conn);
            while (resultSet.next()) {
                Assert.assertTrue(
                    resultSet.getString("EVENT").endsWith("Interrupted by auto interrupter") || resultSet.getString(
                            "EVENT")
                        .endsWith("Out of maintenance window"));
            }
            // Check there is no running ddl
            resultSet = JdbcUtil.executeQuery("show ddl", conn);
            while (resultSet.next()) {
                if (innodbTestTableName.equalsIgnoreCase(resultSet.getString("OBJECT_NAME"))) {
                    Assert.assertTrue(!DdlState.RUNNING.name().equalsIgnoreCase(resultSet.getString("STATE")));
                }
            }

            // Re-fire the schedule job by ScheduleManager: INTERRUPTED -> RUNNING -> SUCCESS
            JdbcUtil.executeQuery(fireTTL, conn);
            JdbcUtil.executeUpdateSuccess(conn, String.format(modifyMaintenanceTimeSql, 0, 0, 24, 0));
            // Wait until the job is finished
            waitCnt = 0;
            while (!checkScheduleResultState(conn, id,
                new String[] {FiredScheduledJobState.SUCCESS.name()})) {
                waitCnt++;
                assertWithMessage("TTL job is unfinished, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(50);
                Thread.sleep(5 * 1000L);
            }

            // test row count continue
            long partCount1 = FileStorageTestUtil.count(conn, innodbTestTableName);
            long partCount2 = FileStorageTestUtil.count(conn, ossTestTableName);

            Assert.assertTrue(partCount2 > 0);
            Assert.assertTrue(totalCount == partCount1 + partCount2);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testTTLScheduleJobBackfillException() {
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testExpireBackfillException";
            String ossTestTableName = "oss_" + innodbTestTableName;
            FileStorageTestUtil.prepareInnoTable(conn, innodbTestTableName, 2000);
            FileStorageTestUtil.prepareTTLTable(conn, ossTestTableName, innodbTestTableName, engine);

            // 通过 FP_RANDOM_BACKFILL_EXCEPTION 注入的异常，
            // 在 ArchiveOSSTableDataTask 的 backfill 阶段会被 catch 住，然后主动 pause DDL
            // 因此无法模拟因异常而被动 PAUSE 的 DDL
            JdbcUtil.executeUpdateSuccess(conn, "set @FP_RANDOM_BACKFILL_EXCEPTION='100'");
            JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_SET_GLOBAL=true");
            // DdlEngineScheduler will fetch this failed job for every 2 seconds and check its pause policy
            JdbcUtil.executeUpdateSuccess(conn, "SET GLOBAL PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES=0");

            String expireSql = String.format(
                "/*+TDDL:cmd_extra(OSS_ORC_INDEX_STRIDE=200, OSS_MAX_ROWS_PER_FILE=1, PURE_ASYNC_DDL_MODE=TRUE, %s)*/ "
                    + "alter table %s expire local partition", extraHint, innodbTestTableName);

            // In PURE_ASYNC_DDL_MODE, this DDL would not raise error at once
            JdbcUtil.executeUpdateSuccess(conn, expireSql);

            String jobId = getJobId(conn, expireSql);

            // Check if the exception arises as expected
            int waitCnt = 0;
            while (!(checkDdlState(conn, jobId, DdlState.PAUSED) &&
                checkDdlPausedPolicyAndRollbackPolicy(conn, jobId, DdlState.PAUSED, DdlState.ROLLBACK_PAUSED))) {
                waitCnt++;
                assertWithMessage("TTL job is not paused as expected within 2 minutes, schedule_id is: " + jobId)
                    .that(waitCnt).isLessThan(25);
                Thread.sleep(5 * 1000L);
            }

            // Make sure that the DDL is not rescheduled by DDL engine
            waitCnt = 0;
            while (checkDdlState(conn, jobId, DdlState.PAUSED)) {
                waitCnt++;
                if (waitCnt == 12) {
                    break;
                }
                Thread.sleep(5 * 1000L);
            }

            assertWithMessage("TTL DDL is rescheduled, schedule_id is: " + jobId).that(waitCnt).isEqualTo(12);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testTTLScheduleJobException() {
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testExpireException";
            String ossTestTableName = "oss_" + innodbTestTableName;
            FileStorageTestUtil.prepareInnoTable(conn, innodbTestTableName, 2000);
            FileStorageTestUtil.prepareTTLTable(conn, ossTestTableName, innodbTestTableName, engine);

            // 通过 FP_FAIL_ON_DDL_TASK_NAME 注入的异常，其行为类似于异常中断的 DDL
            JdbcUtil.executeUpdateSuccess(conn, String.format("set @FP_FAIL_ON_DDL_TASK_NAME='%s'",
                ArchiveOSSTableDataMppTask.class.getSimpleName()));
            JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_SET_GLOBAL=true");
            // DdlEngineScheduler will fetch this failed job for every 2 seconds and check its pause policy
            JdbcUtil.executeUpdateSuccess(conn, "SET GLOBAL PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES=0");

            String expireSql = String.format(
                "/*+TDDL:cmd_extra(OSS_ORC_INDEX_STRIDE=200, OSS_MAX_ROWS_PER_FILE=1, PURE_ASYNC_DDL_MODE=TRUE, %s)*/ "
                    + "alter table %s expire local partition", extraHint, innodbTestTableName);

            // In PURE_ASYNC_DDL_MODE, this DDL would not raise error at once
            JdbcUtil.executeUpdateSuccess(conn, expireSql);

            String jobId = getJobId(conn, expireSql);

            // Check if the exception arises as expected
            int waitCnt = 0;
            while (!(checkDdlState(conn, jobId, DdlState.PAUSED) &&
                checkDdlPausedPolicyAndRollbackPolicy(conn, jobId, DdlState.PAUSED, DdlState.ROLLBACK_RUNNING))) {
                waitCnt++;
                assertWithMessage("TTL job is not paused as expected within 2 minutes, schedule_id is: " + jobId)
                    .that(waitCnt).isLessThan(25);
                Thread.sleep(5 * 1000L);
            }

            // Make sure that the DDL is not rescheduled by DDL engine
            waitCnt = 0;
            while (checkDdlState(conn, jobId, DdlState.PAUSED)) {
                waitCnt++;
                if (waitCnt == 12) {
                    break;
                }
                Thread.sleep(5 * 1000L);
            }

            assertWithMessage("TTL DDL is rescheduled, schedule_id is: " + jobId).that(waitCnt).isEqualTo(12);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testTTLScheduleJobAllocatePartitionException() {
        try (Connection conn = getConnection()) {
            // Make sure current time is in maintenance window
            JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_SET_GLOBAL=true");
            String modifyMaintenanceTimeSql =
                "SET GLOBAL MAINTENANCE_TIME_START = '%02d:%02d', GLOBAL MAINTENANCE_TIME_END = '%02d:%02d'";
            // Non-standard usage
            JdbcUtil.executeUpdateSuccess(conn, String.format(modifyMaintenanceTimeSql, 0, 0, 24, 0));

            String innodbTestTableName = "testExpireAllocatePartitionException";
            String ossTestTableName = "oss_" + innodbTestTableName;
            FileStorageTestUtil.prepareInnoTable(conn, innodbTestTableName, 2000, true);
            FileStorageTestUtil.prepareTTLTable(conn, ossTestTableName, innodbTestTableName, engine);

            // Make it allocate new local partitions
            LocalDate now = LocalDate.now();
            JdbcUtil.executeSuccess(conn,
                String.format("set @FP_OVERRIDE_NOW='%s'", now.plusMonths(2L))
            );

            long totalCount = FileStorageTestUtil.count(conn, innodbTestTableName);

            // Inject exception while doing reorganize partition
            JdbcUtil.executeUpdateSuccess(conn, "set @FP_BEFORE_PHYSICAL_DDL_EXCEPTION='true'");

            String findTTL = String.format("select schedule_id from metadb.scheduled_jobs where table_schema = '%s' "
                    + "and table_name = '%s' and executor_type = '%s' "
                , testDataBase, innodbTestTableName, "LOCAL_PARTITION");
            ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn);
            Assert.assertTrue(resultSet.next());
            long id = resultSet.getLong(1);

            String fireTTL = "fire schedule " + id;
            JdbcUtil.executeQuery(fireTTL, conn);

            // Wait 2 minutes for scheduled job scanner to fire the job: QUEUE -> RUNNING
            int waitCnt = 0;
            while (countScheduleResult(conn, id) == 0 || !checkScheduleResultState(conn, id,
                new String[] {FiredScheduledJobState.RUNNING.name(), FiredScheduledJobState.FAILED.name()})) {
                waitCnt++;
                assertWithMessage("TTL job is not running within 2 minutes, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(25);
                Thread.sleep(5 * 1000L);
            }

            // Check if the physical ddl failed
            waitCnt = 0;
            while (countScheduleResult(conn, id) == 0 || !checkScheduleResultState(conn, id,
                new String[] {FiredScheduledJobState.FAILED.name()})) {
                waitCnt++;
                assertWithMessage("TTL job is not failed within 2 minutes, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(25);
                Thread.sleep(5 * 1000L);
            }

            // Check there is no running ddl and there is one paused ddl
            resultSet = JdbcUtil.executeQuery("show ddl", conn);
            int pausedCnt = 0;
            while (resultSet.next()) {
                if (innodbTestTableName.equalsIgnoreCase(resultSet.getString("OBJECT_NAME"))) {
                    Assert.assertTrue(!DdlState.RUNNING.name().equalsIgnoreCase(resultSet.getString("STATE")));
                    if (DdlState.PAUSED.name().equalsIgnoreCase(resultSet.getString("STATE"))) {
                        pausedCnt++;
                    }
                }
            }
            Assert.assertTrue(pausedCnt != 0, "The physical ddl did not pause as expected");

            // clear FP
            JdbcUtil.executeUpdateSuccess(conn, "set @FP_BEFORE_PHYSICAL_DDL_EXCEPTION=NULL");

            // re-schedule TTL job
            JdbcUtil.executeQuery(fireTTL, conn);
            while (!checkScheduleResultState(conn, id,
                new String[] {FiredScheduledJobState.SUCCESS.name()})) {
                waitCnt++;
                assertWithMessage("TTL job is unfinished, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(50);
                Thread.sleep(5 * 1000L);
            }

            // test row count continue
            long partCount1 = FileStorageTestUtil.count(conn, innodbTestTableName);
            long partCount2 = FileStorageTestUtil.count(conn, ossTestTableName);

            Assert.assertTrue(partCount2 > 0);
            Assert.assertTrue(totalCount == partCount1 + partCount2);

            resultSet = JdbcUtil.executeQuery("show ddl", conn);
            pausedCnt = 0;
            while (resultSet.next()) {
                if (innodbTestTableName.equalsIgnoreCase(resultSet.getString("OBJECT_NAME"))) {
                    Assert.assertTrue(!DdlState.RUNNING.name().equalsIgnoreCase(resultSet.getString("STATE")));
                    if (DdlState.PAUSED.name().equalsIgnoreCase(resultSet.getString("STATE"))) {
                        pausedCnt++;
                    }
                }
            }
            Assert.assertTrue(pausedCnt == 0, "There are still paused DDLs");
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private String getJobId(Connection conn, String expireSql) throws SQLException {
        ResultSet resultSet = JdbcUtil.executeQuery("show full ddl", conn);
        String jobId = null;
        while (resultSet.next()) {
            if (expireSql.equalsIgnoreCase(resultSet.getString("DDL_STMT"))) {
                jobId = resultSet.getString("JOB_ID");
            }
        }
        Assert.assertTrue(jobId != null);
        return jobId;
    }

    private void continueDDL(Connection conn, String expireSql) throws SQLException {
        int cnt = 0;
        try {
            JdbcUtil.executeUpdate(conn, expireSql);
        } catch (Throwable t) {
            String jobId = getJobId(conn, expireSql);
            boolean interrupted = true;
            while (interrupted) {
                cnt++;
                assertWithMessage(expireSql + " \ncan't finish in expected time").that(cnt).isLessThan(1000);
                try {
                    JdbcUtil.executeUpdate(conn, String.format("continue ddl %s", jobId));
                    interrupted = false;
                } catch (Throwable ignore) {
                }
            }
        }
        assertWithMessage(expireSql + " \nshould pause for a few time").that(cnt).isGreaterThan(0);
    }

    public static boolean checkScheduleResultState(Connection conn, long id, String[] states) throws SQLException {
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

    public static int countScheduleResult(Connection conn, long id) {
        return JdbcUtil.getAllResult(JdbcUtil.executeQuery("show schedule result " + id, conn)).size();
    }

    public boolean checkDdlState(Connection conn, String jobId, DdlState state) throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuery("show ddl " + jobId, conn)) {
            Assert.assertTrue(resultSet.next());
            return state.name().equalsIgnoreCase(resultSet.getString("STATE"));
        }
    }

    public boolean checkDdlPausedPolicyAndRollbackPolicy(Connection conn, String jobId,
                                                         DdlState pausedPolicy,
                                                         DdlState rollbackPausedPolicy) throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            "select paused_policy, rollback_paused_policy from metadb.ddl_engine where job_id = " + jobId, conn)) {
            Assert.assertTrue(resultSet.next());
            return pausedPolicy.name().equalsIgnoreCase(resultSet.getString("paused_policy"))
                && rollbackPausedPolicy.name().equalsIgnoreCase(resultSet.getString("rollback_paused_policy"));
        }
    }
}

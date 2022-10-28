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
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.localpartition.LocalPartitionBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.truth.Truth.assertWithMessage;

public class FileStorageExpireTest extends BaseTestCase {

    private static String testDataBase = "fileStorageExpireDatabase";

    private static Engine engine = PropertiesUtil.engine();

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    @BeforeClass
    public static void initTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
            stmt.execute(String.format("create database %s mode = 'auto'", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void clearFP() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.executeSuccess(tmpConnection, "set @FP_TTL_PAUSE=NULL");
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testExpireContinue() {
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testExpireContinue";
            String ossTestTableName = "oss_" + innodbTestTableName;

            String createSql = String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                "    gmt_modified DATETIME NOT NULL,\n" +
                "    PRIMARY KEY (id, gmt_modified)\n" +
                ")\n" +
                "PARTITION BY HASH(id)\n" +
                "PARTITIONS 4\n" +
                "LOCAL PARTITION BY RANGE (gmt_modified)\n" +
                "STARTWITH '2021-01-01'\n" +
                "INTERVAL 1 MONTH\n" +
                "EXPIRE AFTER 3\n" +
                "PRE ALLOCATE 3\n" +
                "PIVOTDATE NOW();", innodbTestTableName);

            String insertSql = String.format(
                "insert into %s (gmt_modified) values ('2021-01-01'),('2021-02-01'),('2021-03-01'),('2021-04-01'),('2021-05-01'),('2021-06-01'),('2021-07-01'),('2021-08-01'),('2021-09-01'),('2021-10-01'),('2021-11-01'),('2021-12-01'),('2022-01-01'),('2022-02-01'),('2022-03-01'),('2022-04-01'),('2022-05-01'),('2022-06-01'),('2022-07-01'),('2022-08-01'),('2022-09-01'),('2022-10-01'),('2022-11-01'),('2022-12-01');",
                innodbTestTableName);
            String reInsertSql = String.format("insert into %s (gmt_modified) select gmt_modified from %s limit 1000",
                innodbTestTableName, innodbTestTableName);
            ;
            String bindingSql =
                String.format("create table %s like %s engine = '%s' archive_mode = 'ttl';", ossTestTableName,
                    innodbTestTableName, engine.name());

            String expireSql = String.format(
                "/*+TDDL:cmd_extra(OSS_ORC_INDEX_STRIDE=200,OSS_MAX_ROWS_PER_FILE=1,"
                    + "ENABLE_EXPIRE_FILE_STORAGE_PAUSE=true, ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE=true)*/ "
                    + "alter table %s expire local partition", innodbTestTableName);

            Statement statement = conn.createStatement();
            statement.execute(createSql);
            FullTypeTestUtil.pauseSchedule(conn, testDataBase, innodbTestTableName);
            statement.execute(insertSql);
            for (int i = 0; i < 20; i++) {
                statement.execute(reInsertSql);
            }
            statement.execute(bindingSql);

            long totalCount = FullTypeTestUtil.count(conn, innodbTestTableName);

            continueDDL(conn, expireSql);

            long partCount1 = FullTypeTestUtil.count(conn, innodbTestTableName);
            long partCount2 = FullTypeTestUtil.count(conn, ossTestTableName);

            Assert.assertTrue(partCount2 > 0);
            Assert.assertTrue(totalCount == partCount1 + partCount2);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * test the rollback of
     */
    @Test
    public void testExpireRollBackTest() {
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testExpireRollback";
            String ossTestTableName = "oss_" + innodbTestTableName;

            //prepare innodb table
            FullTypeTestUtil.prepareInnoTable(conn, innodbTestTableName, 2000);
            FullTypeTestUtil.prepareTTLTable(conn, testDataBase, ossTestTableName, innodbTestTableName, engine);

            String expireSql = String.format(
                "/*+TDDL:cmd_extra(OSS_ORC_INDEX_STRIDE=50,OSS_MAX_ROWS_PER_FILE=200,"
                    + "ENABLE_EXPIRE_FILE_STORAGE_PAUSE=true, ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE=true)*/ "
                    + "alter table %s expire local partition", innodbTestTableName);

            // test rollback
            long totalCount = FullTypeTestUtil.count(conn, innodbTestTableName);

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
                }
            }
            assertWithMessage("rollback number is not equal to test cases").that(rollbacks).isEqualTo(tests);
            assertWithMessage("Table " + ossTestTableName + " is not empty")
                .that(FullTypeTestUtil.count(conn, ossTestTableName)).isEqualTo(0);
            assertWithMessage("Table " + ossTestTableName + " loses some data")
                .that(FullTypeTestUtil.count(conn, innodbTestTableName)).isEqualTo(totalCount);

            // test continue
            continueDDL(conn, expireSql);

            long partCount1 = FullTypeTestUtil.count(conn, innodbTestTableName);
            long partCount2 = FullTypeTestUtil.count(conn, ossTestTableName);

            Assert.assertTrue(partCount2 > 0);
            Assert.assertTrue(totalCount == partCount1 + partCount2);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * test correctness of LocalPartitionScheduledJob
     * TODO: (shengyu) fix that the test occasionally fail due to slow dispatch of schedule job
     */
    @Test
    public void testTTLScheduleJob() {
        // Caution: to make this works, fail point injection must be enabled with
        // vm option "-ea:com.alibaba.polardbx.executor.utils.failpoint..."
        try (Connection conn = getConnection()) {
            String innodbTestTableName = "testSchedulePause";
            String ossTestTableName = "oss_" + innodbTestTableName;
            //prepare table
            FullTypeTestUtil.prepareInnoTable(conn, innodbTestTableName,7000);
            FullTypeTestUtil.prepareTTLTable(conn, testDataBase, ossTestTableName, innodbTestTableName, engine);

            long totalCount = FullTypeTestUtil.count(conn, innodbTestTableName);

            String findTTL = String.format("select schedule_id from metadb.scheduled_jobs where table_schema = '%s' "
                    + "and table_name = '%s' and executor_type = '%s' "
                , testDataBase, innodbTestTableName, "LOCAL_PARTITION");
            ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn);
            Assert.assertTrue(resultSet.next());
            long id = resultSet.getLong(1);
            String fireTTL = "fire schedule " + id;

            int failCnt = 0;
            FullTypeTestUtil.continueSchedule(conn, testDataBase, innodbTestTableName);

            // note that the ttl_pause may be too short or too long, which will cause the test fail
            long[] ttl_pause = new long[]{5, 5, 20, 100};
            for (long pause : ttl_pause) {
                JdbcUtil.executeSuccess(conn, String.format("set @FP_TTL_PAUSE='%d'", pause));

                int beforeCnt = countScheduleResult(conn, id);
                JdbcUtil.executeQuery(fireTTL, conn);
                // sleep to wait for the schedule job finish
                Thread.sleep((pause + 5)  * 1000L);
                Assert.assertTrue(countScheduleResult(conn, id) > beforeCnt);

                // wait until the job is finished
                int waitCnt = 0;
                while(!checkScheduleResultState(conn, id,
                    new String[]{FiredScheduledJobState.SUCCESS.name(), FiredScheduledJobState.FAILED.name()})) {
                    waitCnt++;
                    assertWithMessage("TTL job is unfinished, schedule_id is: " + id)
                        .that(waitCnt).isLessThan(10);
                    Thread.sleep(5  * 1000L);
                }

                int afterCnt = countScheduleResult(conn, id);
                if (afterCnt - beforeCnt > 1) {
                    // maybe ScheduleManager invoke an expire job, just skip the test case
                    JdbcUtil.executeSuccess(conn, "set @FP_TTL_PAUSE='null'");
                    JdbcUtil.executeQuery(fireTTL, conn);
                    return;
                }
                if (checkScheduleResultState(conn, id, new String[]{FiredScheduledJobState.FAILED.name()})) {
                    failCnt++;
                } else {
                    break;
                }
            }
            assertWithMessage("TTL should pause for at least once").that(failCnt).isGreaterThan(0);

            // test row count continue
            long partCount1 = FullTypeTestUtil.count(conn, innodbTestTableName);
            long partCount2 = FullTypeTestUtil.count(conn, ossTestTableName);

            Assert.assertTrue(partCount2 > 0);
            Assert.assertTrue(totalCount == partCount1 + partCount2);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }



    private void continueDDL(Connection conn, String expireSql) throws SQLException{
        int cnt = 0;
        try {
            JdbcUtil.executeUpdate(conn, expireSql);
        } catch (Throwable t) {
            ResultSet resultSet = JdbcUtil.executeQuery("show full ddl", conn);
            String jobId = null;
            while(resultSet.next()) {
                if (expireSql.equalsIgnoreCase(resultSet.getString("DDL_STMT"))) {
                    jobId = resultSet.getString("JOB_ID");
                }
            }
            Assert.assertTrue(jobId != null);
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



    public boolean checkScheduleResultState(Connection conn, long id, String[] states) throws SQLException {
        ResultSet resultSet = JdbcUtil.executeQuery("show schedule result " + id, conn);
        Assert.assertTrue(resultSet.next());
        System.out.println(resultSet.getString(6) + " " + states.length);
        for (String state : states) {
            if (state.equalsIgnoreCase(resultSet.getString(6))) {
                return true;
            }
        }
        return false;
    }

    public int countScheduleResult(Connection conn, long id) throws SQLException {
        return JdbcUtil.getAllResult(JdbcUtil.executeQuery("show schedule result " + id, conn)).size();
    }
}

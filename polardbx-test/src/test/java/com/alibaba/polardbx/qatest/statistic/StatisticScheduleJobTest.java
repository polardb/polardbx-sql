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

package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * @author fangwu
 */
public class StatisticScheduleJobTest extends BaseTestCase {

    public StatisticScheduleJobTest() {
    }

    @After
    public void clearFailPoint() throws InterruptedException {
        String sql = "set @FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION=NULL;";
        Connection c = this.getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(c, sql);
        Thread.sleep(2000L);
    }

    @Test
    public void testAnalyzeTableWithoutHll() throws SQLException, InterruptedException {
        String sql;
        String tableName = "select_base_one_multi_db_one_tb";
        long now = System.currentTimeMillis();
        Thread.sleep(1000L);
        sql = "analyze table " + tableName;
        try (Connection c = this.getPolardbxConnection()) {
            c.createStatement().execute("set global STATISTIC_VISIT_DN_TIMEOUT=600000");
            c.createStatement().execute("set global enable_hll=false");
            c.createStatement().executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            try (Connection c = this.getPolardbxConnection()) {
                c.createStatement().execute("set global STATISTIC_VISIT_DN_TIMEOUT=60000");
                c.createStatement().execute("set global enable_hll=true");
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        }

        // test work result
        // check schedule job step
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sql =
            "select EVENT from information_schema.module_event where MODULE_NAME='STATISTICS' and TIMESTAMP>'"
                + sdf.format(new Date(now)) + "'";
        System.out.println(sql);
        ResultSet moduleRs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        System.out.println("get event log from module_event");
        boolean hasRowCount = false;
        boolean hasSample = false;
        boolean hasPersist = false;
        boolean hasSync = false;
        while (moduleRs.next()) {
            String event = moduleRs.getString("EVENT");
            if (!hasRowCount && event.contains("STATISTIC_ROWCOUNT_COLLECTION FROM ANALYZE")) {
                hasRowCount = true;
                System.out.println(event);
                System.out.println("hasRowCount");
            } else if (!hasSample && event.contains("statistic sample started")) {
                hasSample = true;
                System.out.println(event);
                System.out.println("hasSample");
            } else if (!hasPersist && event.contains("persist tables statistic")) {
                hasPersist = true;
                System.out.println(event);
                System.out.println("hasPersist");
            } else if (!hasSync && event.contains("sync statistic info ")) {
                hasSync = true;
                System.out.println(event);
                System.out.println("hasSync");
            }
            if (hasRowCount && hasSample && hasPersist && hasSync) {
                break;
            }
        }
        Assert.assertTrue(hasRowCount && hasSample && hasPersist && hasSync);

        // check modify time
        sql =
            "select FROM_UNIXTIME(LAST_MODIFY_TIME) AS TIME from information_schema.virtual_statistic where table_name='select_base_one_multi_db_one_tb'";
        ResultSet rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        while (rs.next()) {
            Timestamp timestamp = rs.getTimestamp("TIME");
            if (timestamp.after(new Timestamp(now))) {
                return;
            }
        }
        Assert.fail("no statistic time updated");
    }

    @Ignore
    public void testRowCountCollectJob() throws SQLException, InterruptedException {
        String sql;
        long now = System.currentTimeMillis();
        sql = "select schedule_id from metadb.SCHEDULED_JOBS where executor_type='STATISTIC_ROWCOUNT_COLLECTION'";
        String schedule_id = null;
        try (Connection c = this.getPolardbxConnection()) {
            ResultSet resultSet = c.createStatement().executeQuery(sql);
            resultSet.next();
            schedule_id = resultSet.getString("schedule_id");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        if (schedule_id == null) {
            Assert.fail("Cannot find schedule id for STATISTIC_ROWCOUNT_COLLECTION");
        }

        // record table statistic update info
        sql = "set @FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB='true'";
        JdbcUtil.executeUpdateSuccess(this.getPolardbxConnection(), sql);
        sql = " fire schedule " + schedule_id;
        JdbcUtil.executeUpdateSuccess(this.getPolardbxConnection(), sql);

        // test work result
        sql =
            "select FROM_UNIXTIME(LAST_MODIFY_TIME) AS TIME from information_schema.virtual_statistic where  ndv_source!='cache_line'";
        ResultSet rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        rs.next();
        Timestamp timestamp = rs.getTimestamp("TIME");
        Assert.assertTrue(timestamp.after(new Timestamp(now)));
    }

    @Test
    public void testSampleSketchJob() throws SQLException, InterruptedException {
        String sql;
        long now = System.currentTimeMillis();
        sql = "select schedule_id from metadb.SCHEDULED_JOBS where executor_type='STATISTIC_SAMPLE_SKETCH'";
        String schedule_id = null;
        try (Connection c = this.getPolardbxConnection()) {
            ResultSet resultSet = c.createStatement().executeQuery(sql);
            resultSet.next();
            schedule_id = resultSet.getString("schedule_id");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        if (schedule_id == null) {
            Assert.fail("Cannot find schedule id for STATISTIC_SAMPLE_SKETCH");
        } else {
            System.out.println("get schedule_id:" + schedule_id);
        }

        // record table statistic update info
        sql = "set @FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB='true'";
        Connection c = this.getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(c, sql);
        sql = " fire schedule " + schedule_id;
        JdbcUtil.executeUpdateSuccess(c, sql);

        System.out.println("fire schedule job done:" + schedule_id);

        // test work result
        // check schedule job step
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sql =
            "select EVENT from information_schema.module_event where MODULE_NAME='STATISTICS' and TIMESTAMP>'"
                + sdf.format(new Date(now)) + "'";
        ResultSet moduleRs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        System.out.println("get event log from module_event");
        boolean hasSample = false;
        boolean hasHll = false;
        boolean hasPersist = false;
        boolean hasSync = false;
        boolean hasEnd = false;
        while (moduleRs.next()) {
            String event = moduleRs.getString("EVENT");
            if (!hasPersist && event.contains("persist tables statistic")) {
                hasPersist = true;
                System.out.println(event);
                System.out.println("hasPersist");
            } else if (!hasSync && event.contains("sync statistic info ")) {
                hasSync = true;
                System.out.println(event);
                System.out.println("hasSync");
            } else if (!hasEnd && event.contains("auto analyze STATISTIC_SAMPLE_SKETCH")) {
                hasEnd = true;
                System.out.println(event);
                System.out.println("hasEnd");
            }
            if (hasSample && hasSync && hasEnd && hasHll && hasPersist) {
                break;
            }
        }
        Assert.assertTrue(hasSync && hasEnd && hasPersist);

        // check modify time
        boolean statisticUpdated = false;
        sql =
            "select FROM_UNIXTIME(LAST_MODIFY_TIME) AS TIME from information_schema.virtual_statistic";
        ResultSet rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        while (rs.next()) {
            Timestamp timestamp = rs.getTimestamp("TIME");
            if (timestamp.after(new Timestamp(now))) {
                statisticUpdated = true;
                break;
            }
        }
        rs.close();

        if (!statisticUpdated) {
            Assert.fail("no statistic time updated");
        }
        // check optimize alert
        sql = "select alert_count from information_schema.optimizer_alert where ALERT_TYPE='STATISTIC_MISS'";
        rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        while (rs.next()) {
            int alertCount = rs.getInt("alert_count");
            if (alertCount > 0) {
                Assert.fail("statistic miss alert");
            }
        }
        rs.close();
    }

    @Test
    public void testSampleSketchJobHllException() throws SQLException, InterruptedException {
        String sql;
        long now = System.currentTimeMillis();
        sql = "select schedule_id from metadb.SCHEDULED_JOBS where executor_type='STATISTIC_HLL_SKETCH'";
        String schedule_id = null;
        try (Connection c = this.getPolardbxConnection()) {
            ResultSet resultSet = c.createStatement().executeQuery(sql);
            resultSet.next();
            schedule_id = resultSet.getString("schedule_id");
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        if (schedule_id == null) {
            Assert.fail("Cannot find schedule id for STATISTIC_HLL_SKETCH");
        } else {
            System.out.println("get schedule_id:" + schedule_id);
        }

        // record table statistic update info
        sql = "set @FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB='true'";
        Connection c = this.getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(c, sql);
        sql = "set @FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION='true';";
        JdbcUtil.executeUpdateSuccess(c, sql);
        sql = " fire schedule " + schedule_id;
        JdbcUtil.executeUpdateSuccess(c, sql);

        System.out.println("fire schedule job done:" + schedule_id);

        // test work result
        // check schedule job step
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sql =
            "select EVENT from information_schema.module_event where MODULE_NAME='STATISTICS' and TIMESTAMP>'"
                + sdf.format(new Date(now)) + "'";
        ResultSet moduleRs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        System.out.println("get event log from module_event:" + sql);
        boolean hasEnd = false;
        while (moduleRs.next()) {
            String event = moduleRs.getString("EVENT");
            if (event.contains("auto STATISTIC_HLL_SKETCH")) {
                hasEnd = true;
                System.out.println(event);
                System.out.println("hasEnd");
            }
            if (hasEnd) {
                break;
            }
        }
        moduleRs.close();
        Assert.assertTrue(hasEnd);

        // check optimize alert
        sql = "select alert_count from information_schema.optimizer_alert where ALERT_TYPE='STATISTIC_MISS'";
        ResultSet rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        while (rs.next()) {
            int alertCount = rs.getInt("alert_count");
            if (alertCount > 0) {
                Assert.fail("statistic miss alert");
            }
        }
        rs.close();
    }

    @Test
    public void testAnalyzeTable() throws SQLException {
        String sql;
        String tableName = "select_base_one_multi_db_multi_tb";
        long now = System.currentTimeMillis();
        sql = "analyze table " + tableName;
        try (Connection c = this.getPolardbxConnection()) {
            c.createStatement().execute("set global STATISTIC_VISIT_DN_TIMEOUT=600000");
            c.createStatement().executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            try (Connection c = this.getPolardbxConnection()) {
                c.createStatement().execute("set global STATISTIC_VISIT_DN_TIMEOUT=60000");
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        }

        // test work result
        // check schedule job step
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sql =
            "select EVENT from information_schema.module_event where MODULE_NAME='STATISTICS' and TIMESTAMP>='"
                + sdf.format(new Date(now)) + "'";
        System.out.println(sql);
        ResultSet moduleRs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        System.out.println("get event log from module_event");
        boolean hasRowCount = false;
        boolean hasSample = false;
        boolean hasHll = false;
        boolean hasPersist = false;
        boolean hasSync = false;
        while (moduleRs.next()) {
            String event = moduleRs.getString("EVENT");
            if (!hasRowCount && event.contains("STATISTIC_ROWCOUNT_COLLECTION FROM ANALYZE")) {
                hasRowCount = true;
                System.out.println(event);
                System.out.println("hasRowCount");
            } else if (!hasSample && event.contains("statistic sample started")) {
                hasSample = true;
                System.out.println(event);
                System.out.println("hasSample");
            } else if (!hasHll && event.contains("ndv sketch rebuild")) {
                hasHll = true;
                System.out.println(event);
                System.out.println("hasHll");

            } else if (!hasPersist && event.contains("persist tables statistic")) {
                hasPersist = true;
                System.out.println(event);
                System.out.println("hasPersist");
            } else if (!hasSync && event.contains("sync statistic info ")) {
                hasSync = true;
                System.out.println(event);
                System.out.println("hasSync");
            }
            if (hasRowCount && hasSample && hasHll && hasPersist && hasSync) {
                break;
            }
        }
        Assert.assertTrue(hasRowCount && hasSample && hasHll && hasPersist && hasSync);

        // check modify time
        sql =
            "select FROM_UNIXTIME(LAST_MODIFY_TIME) AS TIME from information_schema.virtual_statistic";
        ResultSet rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        while (rs.next()) {
            Timestamp timestamp = rs.getTimestamp("TIME");
            if (timestamp.after(new Timestamp(now))) {
                return;
            }
        }
        Assert.fail("no statistic time updated");
    }
}

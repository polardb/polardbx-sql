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
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.springframework.transaction.support.ResourceHolderSupport;

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

    @Test
    public void testRowCountCollectJob() throws SQLException, InterruptedException {
        String sql = "";
        long now = System.currentTimeMillis();
        sql = "select schedule_id from metadb.SCHEDULEd_JOBS where executor_type='STATISTIC_ROWCOUNT_COLLECTION'";
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

        /**
         * record table statistic update info
         */

        sql = "set @FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB='true'";
        JdbcUtil.executeUpdateSuccess(this.getPolardbxConnection(), sql);
        sql = " fire schedule " + schedule_id;
        JdbcUtil.executeUpdateSuccess(this.getPolardbxConnection(), sql);

        sql = "select state from metadb.fired_SCHEDULEd_JOBS where schedule_id=" + schedule_id
            + " and gmt_modified>FROM_UNIXTIME(" + now + "/1000)";

        /**
         * waiting job done
         */
        while (true) {
            //timeout control 5 min
            if (System.currentTimeMillis() - now > 5 * 60 * 1000) {
                Assert.fail("timeout:5 min");
                return;
            }
            Thread.sleep(5000);
            ResultSet rs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
            boolean anySucc = false;
            while (rs.next()) {
                String state = rs.getString(1);
                if ("SUCCESS".equalsIgnoreCase(state)) {
                    anySucc = true;
                    rs.close();
                    break;
                }
            }
            rs.close();
            if (anySucc) {
                break;
            }
        }

        /**
         * test work result
         */
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

        sql = "select state from metadb.fired_SCHEDULEd_JOBS where schedule_id=" + schedule_id
            + " and gmt_modified>FROM_UNIXTIME(" + now + "/1000)";

        // waiting job done
        while (true) {
            //timeout control 5 min
            if (System.currentTimeMillis() - now > 5 * 60 * 1000) {
                Assert.fail("timeout:5 min");
                return;
            }
            Thread.sleep(1000);
            ResultSet rs = JdbcUtil.executeQuery(sql, c);
            boolean anySucc = false;
            while (rs.next()) {
                String state = rs.getString(1);
                if ("SUCCESS".equalsIgnoreCase(state)) {
                    anySucc = true;
                    rs.close();
                    break;
                }
            }

            System.out.println("check job any succ:" + anySucc);
            rs.close();
            if (anySucc) {
                break;
            }
        }

        // test work result
        // check schedule job step
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sql =
            "select EVENT from information_schema.module_event where MODULE_NAME='STATISTIC' and TIMESTAMP>'"
                + sdf.format(new Date(now)) + "'";
        ResultSet moduleRs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        System.out.println("get event log from module_event");
        boolean hasStart = false;
        boolean hasSample = false;
        boolean hasSync = false;
        boolean hasEnd = false;
        while (moduleRs.next()) {
            String event = moduleRs.getString("EVENT");
            if (!hasStart && event.contains("STATISTIC_SAMPLE_SKETCH started")) {
                hasStart = true;
                System.out.println(event);
                System.out.println("hasStart");
            } else if (!hasSample && event.contains("sample table")) {
                hasSample = true;
                System.out.println(event);
                System.out.println("hasSample");
            } else if (!hasSync && event.contains("sync statistic info ")) {
                hasSync = true;
                System.out.println(event);
                System.out.println("hasSync");
            } else if (!hasEnd && event.contains("auto analyze STATISTIC_SAMPLE_SKETCH")) {
                hasEnd = true;
                System.out.println(event);
                System.out.println("hasEnd");
            }
        }
        Assert.assertTrue(hasStart && hasSample && hasSync && hasEnd);

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

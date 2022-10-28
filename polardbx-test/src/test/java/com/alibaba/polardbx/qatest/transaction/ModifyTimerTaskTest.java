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

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ModifyTimerTaskTest extends CrudBasedLockTestCase {
    final String DISABLE_MPP_HINT = "/*+TDDL:ENABLE_MPP=false*/";

    @Test
    public void testModifyLogCleanTask() throws SQLException {
        final String VALUE = "VARIABLE_VALUE";
        // Default value.
        long before = 3600 * 24, interval = 3600;

        try {
            // Record the original values.
            final String showLogCleanTaskSql = DISABLE_MPP_HINT + "select * from information_schema.global_variables "
                + "where variable_name like '%PURGE_TRANS_%' "
                + "order by variable_name";
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showLogCleanTaskSql);
            int found = 0;
            while (rs.next()) {
                final String variableName = rs.getString("VARIABLE_NAME");
                if ("PURGE_TRANS_BEFORE".equals(variableName)) {
                    before = Long.parseLong(rs.getString(VALUE));
                    found++;
                } else if ("PURGE_TRANS_INTERVAL".equals(variableName)) {
                    interval = Long.parseLong(rs.getString(VALUE));
                    found++;
                }
            }
            Assert.assertEquals(2, found);

            // Enable SET GLOBAL.
            String sql = "SET ENABLE_SET_GLOBAL = TRUE";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // Modify the log clean parameters.
            long newBefore = 999999, newInterval = 3600 * 2;
            sql = "SET GLOBAL PURGE_TRANS_BEFORE = " + newBefore + ", GLOBAL PURGE_TRANS_INTERVAL = " + newInterval;
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // Sleep 5 seconds, waiting metaDB notifies the listeners.
            try {
                Thread.sleep(5000L);
            } catch (Throwable t) {
                Assert.fail(t.getMessage());
            }

            // Show them again to see if they are changed.
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, showLogCleanTaskSql);
            found = 0;
            while (rs.next()) {
                final String variableName = rs.getString("VARIABLE_NAME");
                if ("PURGE_TRANS_BEFORE".equals(variableName)) {
                    Assert.assertEquals(newBefore, Long.parseLong(rs.getString(VALUE)));
                    found++;
                } else if ("PURGE_TRANS_INTERVAL".equals(variableName)) {
                    Assert.assertEquals(newInterval, Long.parseLong(rs.getString(VALUE)));
                    found++;
                }
            }
            Assert.assertEquals(2, found);
        } finally {
            // Rollback to the original values.
            String sql = "SET GLOBAL PURGE_TRANS_BEFORE = " + before + ", GLOBAL PURGE_TRANS_INTERVAL = " + interval;
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            // Disable SET GLOBAL
            sql = "SET ENABLE_SET_GLOBAL = FALSE";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    @Test
    public void testModifyLogCleanTask2() throws SQLException {
        final String VALUE = "VARIABLE_VALUE";
        final String NAME = "VARIABLE_NAME";
        // Default value.
        long before = 3600 * 24, interval = 3600;

        try {
            // Record the original values.
            final String showLogCleanTaskSql = DISABLE_MPP_HINT + "select * from information_schema.global_variables "
                + "where variable_name like '%PURGE_TRANS_%' "
                + "order by variable_name";
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showLogCleanTaskSql);
            int found = 0;
            while (rs.next()) {
                final String variableName = rs.getString(NAME);
                if ("PURGE_TRANS_BEFORE".equals(variableName)) {
                    before = Long.parseLong(rs.getString(VALUE));
                    found++;
                } else if ("PURGE_TRANS_INTERVAL".equals(variableName)) {
                    interval = Long.parseLong(rs.getString(VALUE));
                    found++;
                }
            }
            Assert.assertEquals(2, found);

            // Enable SET GLOBAL.
            String sql = "SET ENABLE_SET_GLOBAL = TRUE";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // Modify the log clean parameters.
            long newBefore = 1, newInterval = 1;
            sql = "SET GLOBAL PURGE_TRANS_BEFORE = " + newBefore;
            JdbcUtil.executeUpdateFailed(tddlConnection, sql,
                "[PXC-4518][ERR_VALIDATE] invalid parameter: PURGE_TRANS_BEFORE");

            sql = "SET GLOBAL PURGE_TRANS_INTERVAL = " + newInterval;
            JdbcUtil.executeUpdateFailed(tddlConnection, sql,
                "[PXC-4518][ERR_VALIDATE] invalid parameter: PURGE_TRANS_INTERVAL");
        } finally {
            // Rollback to the original values.
            String sql = "SET GLOBAL PURGE_TRANS_BEFORE = " + before + ", GLOBAL PURGE_TRANS_INTERVAL = " + interval;
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            // Disable SET GLOBAL
            sql = "SET ENABLE_SET_GLOBAL = FALSE";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }
}


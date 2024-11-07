/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

/**
 * update timestamp 精度测试
 */
public class UpdateTimestampTest extends CrudBasedLockTestCase {

    private static final int RETRY_TIME = 3;
    String createTableSql = "CREATE TABLE `%s` (\n"
        + "    `pk` bigint(64) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
        + "    `sk` int(11) NOT NULL DEFAULT '0',\n"
        + "    `time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),\n"
        + "    PRIMARY KEY (`pk`),\n"
        + "    INDEX `k_1` (`sk`)\n"
        + ") %s;";

    String tableName;
    String partitionSql;

    public UpdateTimestampTest(String tableName, String partitionSql) {
        this.tableName = tableName;
        this.partitionSql = partitionSql;
    }

    @Parameterized.Parameters(name = "{index}:table={0},partition={1}")
    public static List<String[]> prepare() {
        List<String[]> result = new ArrayList<>();
        result.add(new String[] {
            "update_timestamp_test", "dbpartition by hash(`sk`) tbpartition by hash(`sk`) tbpartitions 4"});
        result.add(new String[] {"update_timestamp_test_br", "broadcast"});
        return result;
    }

    @Before
    public void before() {
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, String.format(createTableSql, tableName, partitionSql));
    }

    @Test
    public void updateTest() {
        String sql = "insert into " + tableName + "(sk) values(2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        int newValue = 5;
        String updateSql = "update " + tableName + " set sk = ";
        String selectSql = "select time from " + tableName;
        int i = 0;
        //重试多次，防止偶发真的为0
        while (i < RETRY_TIME) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, updateSql + newValue);
            List<List<String>> result =
                JdbcUtil.getStringResult(JdbcUtil.executeQuery(selectSql, tddlConnection), false);
            String time = result.get(0).get(0);
            if (!time.contains(".")) {
                continue;
            }
            int microsecond = (int) (Double.parseDouble("0" + time.substring(time.indexOf("."))) * 1e6);
            if (microsecond > 0) {
                break;
            }
            i++;
            newValue++;
        }
        if (i == RETRY_TIME) {
            Assert.fail("update ON UPDATE CURRENT_TIMESTAMP(6) failed!");
        }

        if (partitionSql.contains("broadcast")) {
            assertBroadcastTableSelfSame(tableName, tddlConnection);
        }

        //设置精度为2
        updateSql = "update " + tableName + " set sk = 10, time = current_timestamp(2)";
        i = 0;
        //重试多次，防止偶发真的为0
        while (i < RETRY_TIME) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, updateSql);
            List<List<String>> result =
                JdbcUtil.getStringResult(JdbcUtil.executeQuery(selectSql, tddlConnection), false);
            String time = result.get(0).get(0);
            if (!time.contains(".")) {
                continue;
            }
            int microsecond = (int) (Double.parseDouble("0" + time.substring(time.indexOf("."))) * 1e6);
            int firstTwoDigits = microsecond / 10000;
            int lastFourDigits = microsecond % 10000;
            //后四位为0，前两位不为0
            Assert.assertEquals(0, lastFourDigits);
            if (firstTwoDigits > 0) {
                break;
            }
            i++;
        }

        if (i == RETRY_TIME) {
            Assert.fail("update set ON UPDATE CURRENT_TIMESTAMP(2) failed!");
        }

        if (partitionSql.contains("broadcast")) {
            assertBroadcastTableSelfSame(tableName, tddlConnection);
        }
    }

    @Test
    public void insertDuplicateTest() {
        String sql = "insert into " + tableName + "(pk, sk) values(2, 2)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        int newValue = 5;
        String insertSql = "insert into " + tableName + "(pk, sk) values(2, 2) ON DUPLICATE KEY UPDATE sk = ";
        String selectSql = "select time from " + tableName;
        int i = 0;
        //重试多次，防止偶发真的为0
        while (i < RETRY_TIME) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql + newValue);
            List<List<String>> result =
                JdbcUtil.getStringResult(JdbcUtil.executeQuery(selectSql, tddlConnection), false);
            String time = result.get(0).get(0);
            if (!time.contains(".")) {
                continue;
            }
            int microsecond = (int) (Double.parseDouble("0" + time.substring(time.indexOf("."))) * 1e6);
            if (microsecond > 0) {
                break;
            }
            i++;
            newValue++;
        }
        if (i == RETRY_TIME) {
            Assert.fail("insert ON DUPLICATE KEY UPDATE CURRENT_TIMESTAMP(6) failed!");
        }

        if (partitionSql.contains("broadcast")) {
            assertBroadcastTableSelfSame(tableName, tddlConnection);
        }

        //设置精度为2
        insertSql = "/*+TDDL:CMD_EXTRA(DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=true)*/ "
            + "insert into " + tableName
            + "(pk, sk) values(2, 3) ON DUPLICATE KEY UPDATE sk = 5, time = current_timestamp(2)";
        i = 0;
        //重试多次，防止偶发真的为0
        while (i < RETRY_TIME) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
            List<List<String>> result =
                JdbcUtil.getStringResult(JdbcUtil.executeQuery(selectSql, tddlConnection), false);
            String time = result.get(0).get(0);
            if (!time.contains(".")) {
                continue;
            }
            int microsecond = (int) (Double.parseDouble("0" + time.substring(time.indexOf("."))) * 1e6);
            int firstTwoDigits = microsecond / 10000;
            int lastFourDigits = microsecond % 10000;
            //后四位为0，前两位不为0
            Assert.assertEquals(0, lastFourDigits);
            if (firstTwoDigits > 0) {
                break;
            }
            i++;
        }

        if (i == RETRY_TIME) {
            Assert.fail("insert ON DUPLICATE KEY UPDATE CURRENT_TIMESTAMP(2) failed!");
        }

        if (partitionSql.contains("broadcast")) {
            assertBroadcastTableSelfSame(tableName, tddlConnection);
        }
    }
}

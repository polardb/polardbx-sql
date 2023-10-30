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

package com.alibaba.polardbx.qatest.dml.auto.broadcast;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Insert broadcast table with functions.
 *
 * @author changyuan.lh
 */

public class BroadcastWriteWithFuncTest extends AutoCrudBasedLockTestCase {

    public BroadcastWriteWithFuncTest(String broadcastTableName) {
        this.baseOneTableName = broadcastTableName;
    }

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepareData() {
        String[][] object = {{ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.BROADCAST_TB_SUFFIX},};
        return Arrays.asList(object);
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
    }

    @Test
    public void testFunc() {
        String sql = "INSERT INTO " + baseOneTableName + " (pk, date_test) VALUES ("
            + "123456, DATE(DATE_ADD(NOW(), INTERVAL 1 DAY)))";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

        sql = "INSERT INTO " + baseOneTableName + " (pk, datetime_test) VALUES ("
            + "32908082, DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)))";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

        sql = "INSERT INTO " + baseOneTableName + " (pk, integer_test) VALUES ("
            + "483084027, HOUR(TIMEDIFF(NOW(), DATE_SUB(NOW(), INTERVAL 5 DAY))))";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, false);

        assertBroadcastTableSame(baseOneTableName, tddlConnection, mysqlConnection);
    }

    @Test
    public void testRandomInsert() {
        String sql = "INSERT INTO " + baseOneTableName + " (pk, double_test) VALUES ("
            + "327346, rand())";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "INSERT INTO " + baseOneTableName + " (pk, timestamp_test) VALUES ("
            + "43563, now())";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "INSERT INTO " + baseOneTableName + " (pk, timestamp_test) VALUES ("
            + "4872639, CURRENT_TIMESTAMP())";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "INSERT INTO " + baseOneTableName + " (pk, datetime_test) VALUES ("
            + "3780628, now())";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

    }

    @Test
    public void testUpdate() {
        String sql = "UPDATE " + baseOneTableName + " set integer_test = 12983 order by rand() limit 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set double_test = rand() order by rand() limit 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set double_test = rand() where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set timestamp_test = CURRENT_TIMESTAMP() where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set datetime_test = now() where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set varchar_test = uuid() where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set time_test = curTime() where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set date_test = curDate() where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set integer_test = CONNECTION_ID() where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "UPDATE " + baseOneTableName + " set integer_test = UNIX_TIMESTAMP() where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);
    }

    @Test
    public void testDelete() {
        String sql = "DELETE from " + baseOneTableName + " where rand() < 0.5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);

        sql = "DELETE from " + baseOneTableName + " order by rand() limit 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        assertBroadcastTableSelfSame(baseOneTableName, tddlConnection);
    }
}

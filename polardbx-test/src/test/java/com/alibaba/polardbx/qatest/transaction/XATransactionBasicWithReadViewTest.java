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
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * 测试显式打开/关闭共享ReadView分布式事务的正确性
 *
 * @see XATransactionBasicTest
 */

public class XATransactionBasicWithReadViewTest extends CrudBasedLockTestCase {

    private static final String SELECT_FROM = "SELECT pk, varchar_test, integer_test, char_test, blob_test, " +
        "tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bit_test, bigint_test, float_test, " +
        "double_test, decimal_test, date_test, time_test, datetime_test, year_test FROM ";
    private final boolean supportShareReadView;
    private final boolean isDefaultShareReadView;
    private static final int MAX_DATA_SIZE = 20;

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    public XATransactionBasicWithReadViewTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.useDb(connection, PropertiesUtil.polardbXShardingDBName1());
            isDefaultShareReadView = JdbcUtil.isShareReadView(connection);
            supportShareReadView = JdbcUtil.supportShareReadView(connection);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public void switchShareReadView() throws Exception {
        if (supportShareReadView) {
            JdbcUtil.setShareReadView(!isDefaultShareReadView, tddlConnection);
        }
    }

    @Before
    public void initData() throws Exception {
        String sql = "DELETE FROM  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @Test
    public void testCommitOneShard() throws Exception {
        int startId = (int) (RANDOM_ID + 1);
        tableDataPrepare(baseOneTableName, startId, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        switchShareReadView();

        // When read/write from one single group,
        // should commit one phase.
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */";

        String sql = SELECT_FROM + baseOneTableName + " where pk = " + startId;
        try {
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "update " + baseOneTableName
            + " set integer_test=?, date_test=?, timestamp_test=?, datetime_test=?, varchar_test=?, float_test=? where pk = ?";

        List<Object> param = new ArrayList<>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(columnDataGenerator.float_testValue);
        param.add(startId);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + sql, param, true);

        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

    /**
     * @since 5.3.4
     */
    @Test
    public void testQueryMultiGroup() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        switchShareReadView();

        String sql = SELECT_FROM + baseOneTableName;
        try {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

    /**
     * @since 5.3.4
     */
    @Test
    public void testInsertMultiGroup() throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        switchShareReadView();
        String sql = "insert into " + baseOneTableName
            + "(pk, integer_test, date_test, timestamp_test, datetime_test, varchar_test, float_test)  values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<>();
        param.add(RANDOM_ID);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(columnDataGenerator.float_testValue);

        List<Object> param1 = new ArrayList<>();
        param1.add(RANDOM_ID + 1);
        param1.add(columnDataGenerator.integer_testValue);
        param1.add(columnDataGenerator.date_testValue);
        param1.add(columnDataGenerator.timestamp_testValue);
        param1.add(columnDataGenerator.datetime_testValue);
        param1.add(null);
        param1.add(columnDataGenerator.float_testValue);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param1, true);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = SELECT_FROM + baseOneTableName + " where pk=" + RANDOM_ID;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = SELECT_FROM + baseOneTableName + " where pk=" + (RANDOM_ID + 1);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

}

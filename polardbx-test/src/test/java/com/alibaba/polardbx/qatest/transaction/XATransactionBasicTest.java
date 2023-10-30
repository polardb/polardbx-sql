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
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Test for default distributed transaction scheme（XA for DRDS, TSO for PolarDB-X)
 */

public class XATransactionBasicTest extends CrudBasedLockTestCase {

    private static final int MAX_DATA_SIZE = 20;
    private final String asyncCommit;

    private static final String SELECT_FROM = "SELECT pk, varchar_test, integer_test, char_test, blob_test, " +
        "tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bit_test, bigint_test, float_test, " +
        "double_test, decimal_test, date_test, time_test, datetime_test, year_test FROM ";

    @Parameters(name = "{index}:table={0},asyncCommit={1}")
    public static List<String[]> prepare() {
        List<String[]> ret = new ArrayList<>();
        String[] asyncCommit = {"TRUE", "FALSE"};
        for (String ac : asyncCommit) {
            for (String[] tables : ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE)) {
                ret.add(new String[] {tables[0], ac});
            }
        }
        return ret;
    }

    public XATransactionBasicTest(String baseOneTableName, String asyncCommit) {
        this.baseOneTableName = baseOneTableName;
        this.asyncCommit = asyncCommit;
    }

    @Before
    public void initData() throws Exception {
        String sql = "DELETE FROM  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    /**
     * @since 5.3.4
     */
    @Test
    public void testQueryMultiGroup() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = " + asyncCommit);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

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
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = FALSE");
    }

    /**
     * @since 5.3.4
     */
    @Test
    public void testInsertMultiGroup() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = " + asyncCommit);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

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
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = FALSE");
    }

    /**
     * @since 5.3.4
     */
    @Test
    public void testUpdateWithAutoCommitTrue() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = " + asyncCommit);
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
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

        sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        param.clear();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = SELECT_FROM + baseOneTableName + " where pk=" + RANDOM_ID;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = FALSE");
    }

    /**
     * @since 5.3.4
     */
    @Test
    public void testUpdateWithAutocommitFalse() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = " + asyncCommit);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        sql = SELECT_FROM + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = FALSE");
    }

    /**
     * @since 5.3.4
     */
    @Test(timeout = 60000)
    public void testInsertSelectWithAutocommitFalse() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = " + asyncCommit);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);
            tddlConnection.commit();

            sql = "insert into " + baseOneTableName + "(pk, integer_test) select pk+100, integer_test from  "
                + baseOneTableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null, true);
            tddlConnection.commit();

            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        sql = SELECT_FROM + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = FALSE");
    }

    @Test
    public void testNonParticipant() throws Exception {
        tableDataPrepare(baseOneTableName, (int) (RANDOM_ID + 1), MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = " + asyncCommit);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        // Should commit one phase.
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */";

        String sql = SELECT_FROM + baseOneTableName;
        try {
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "insert into " + baseOneTableName
            + "(pk, integer_test, date_test, timestamp_test, datetime_test, varchar_test, float_test)  values(?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<>();
        param.add(RANDOM_ID);
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.timestamp_testValue);
        param.add(columnDataGenerator.datetime_testValue);
        param.add(null);
        param.add(columnDataGenerator.float_testValue);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, hint + sql, param, true);

        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = FALSE");
    }

    /**
     * @since 5.4.9
     */
    @Test
    public void testApply() throws Exception {
        tableDataPrepare(baseOneTableName, 100,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        String sql = "/*TDDL:CHUNK_SIZE=10*/ select * from " + baseOneTableName
            + " where integer_test not in (select pk+1024 from " + baseOneTableName + " order by pk)";

        explainAllResultMatchAssert("explain " + sql, null, tddlConnection,
            "[\\s\\S]*" + "CorrelateApply" + "[\\s\\S]*");

        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = " + asyncCommit);
            tddlConnection.setAutoCommit(false);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            tddlConnection.commit();
            tddlConnection.setAutoCommit(true);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_ASYNC_COMMIT = FALSE");
        }
    }
}

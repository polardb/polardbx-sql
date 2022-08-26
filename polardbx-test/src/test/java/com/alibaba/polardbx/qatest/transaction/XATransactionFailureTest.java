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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

/**
 * Test for default distributed transaction scheme（XA for DRDS, TSO for PolarDB-X)
 */

public class XATransactionFailureTest extends CrudBasedLockTestCase {

    private static final int MAX_DATA_SIZE = 20;

    private static final String SELECT_FROM = "SELECT pk, varchar_test, integer_test, char_test, blob_test, " +
        "tinyint_test, tinyint_1bit_test, smallint_test, mediumint_test, bit_test, bigint_test, float_test, " +
        "double_test, decimal_test, date_test, time_test, datetime_test, year_test FROM ";

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Collections.singletonList(new String[] {
            ExecuteTableName.UPDATE_DELETE_BASE + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX
        });
    }

    public XATransactionFailureTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Before
    public void initData() throws Exception {
        String sql = "DELETE FROM  " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    @Test
    public void testFailAfterPrimaryCommit() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='SYNC_COMMIT,FAIL_AFTER_PRIMARY_COMMIT') */";
        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        try {
            tddlConnection.commit();
        } catch (Exception ex) {
            // ignore
        }
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        sql = SELECT_FROM + baseOneTableName + " FOR UPDATE";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testFailDuringPrimaryCommit() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='SYNC_COMMIT,FAIL_DURING_PRIMARY_COMMIT') */";
        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        try {
            tddlConnection.commit();
        } catch (Exception ex) {
            // ignore
        }
        mysqlConnection.rollback(); // expect data to be rollbacked
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        sql = SELECT_FROM + baseOneTableName + " FOR UPDATE";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testFailBeforePrimaryCommit() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='SYNC_COMMIT,FAIL_BEFORE_PRIMARY_COMMIT') */";
        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        try {
            tddlConnection.commit();
        } catch (Exception ex) {
            // ignore
        }
        mysqlConnection.rollback(); // expect data to be rollbacked
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        sql = SELECT_FROM + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    @Test
    public void testDeleteBeforeWriteCommitLog() throws Exception {
        tableDataPrepare(baseOneTableName, MAX_DATA_SIZE,
            TableColumnGenerator.getBaseMinColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='DELAY_BEFORE_WRITE_COMMIT_LOG') */";
        String sql = "update " + baseOneTableName + " set integer_test=?, date_test=?,float_test=?";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        param.add(columnDataGenerator.date_testValue);
        param.add(columnDataGenerator.float_testValue);

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);

        try {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, param, true);

            sql = SELECT_FROM + baseOneTableName;
            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        try {
            tddlConnection.commit();
        } catch (Exception ex) {
            // ignore
        }
        mysqlConnection.commit(); // expect data to be committed
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        sql = SELECT_FROM + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }
}

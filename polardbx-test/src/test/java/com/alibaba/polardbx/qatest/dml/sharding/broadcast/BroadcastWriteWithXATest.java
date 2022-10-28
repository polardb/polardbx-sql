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

package com.alibaba.polardbx.qatest.dml.sharding.broadcast;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertVariable;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author chenmo.cm
 */
@Ignore

public class BroadcastWriteWithXATest extends CrudBasedLockTestCase {

    private boolean supportXA;

    public BroadcastWriteWithXATest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.allBaseTypeWithStringRuleOneTable(ExecuteTableName.UPDATE_DELETE_BASE));
    }

    @Before
    public void initData() throws Exception {

        mysqlConnection.setAutoCommit(true);
        tddlConnection.setAutoCommit(true);

        final String sql = "DELETE FROM " + baseOneTableName;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        this.supportXA = JdbcUtil.supportXA(tddlConnection);
    }

    @Test
    public void WriteBroadcastTable_autocommit_0_no_transaction_policy() throws SQLException {
        mysqlConnection.setAutoCommit(false);
        tddlConnection.setAutoCommit(false);

        String sql;
        if (supportXA) {

            sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test, timestamp_test) VALUES"
                + "(1, 'something in broadcast table', 666, '2019-10-16 02:00'), (6, 'something in broadcast table', 777, '2019-10-16 02:00')";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

            sql = "UPDATE " + baseOneTableName
                + " SET integer_test = 1234, timestamp_test='2019-10-16 02:10' WHERE pk = 1";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

            sql = "DELETE FROM " + baseOneTableName + " WHERE pk = 6";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

            sql = "COMMIT";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        } else if (baseOneTableName.contains("one_db")) {
            sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test) VALUES"
                + "(1, 'something in broadcast table', 666)";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

            sql = "COMMIT";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        } else {
            sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test) VALUES"
                + "(1, 'something in broadcast table1', 666), (6, 'something in broadcast table6', 777)";
            executeErrorAssert(tddlConnection,
                sql,
                null,
                "[TDDL-4603][ERR_ACCROSS_DB_TRANSACTION] Transaction accross db is not supported in current transaction policy");

            sql = "ROLLBACK";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        }

        if (supportXA) {
            sql = "SELECT * FROM " + baseOneTableName;
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void WriteBroadcastTable_autocommit_0_no_transaction_policy_rollback() throws SQLException {
        String sql = "INSERT INTO " + baseOneTableName
            + "(pk, varchar_test, integer_test) VALUES(1, 'something in broadcast table', 666)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        mysqlConnection.setAutoCommit(false);
        tddlConnection.setAutoCommit(false);

        if (supportXA) {
            sql = "INSERT INTO " + baseOneTableName
                + "(pk, varchar_test, integer_test) VALUES(6, 'something in broadcast table', 777)";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        } else if (baseOneTableName.contains("broadcast")) {
            sql = "INSERT INTO " + baseOneTableName
                + "(pk, varchar_test, integer_test) VALUES(6, 'something in broadcast table', 777)";
            executeErrorAssert(tddlConnection,
                sql,
                null,
                "[TDDL-4603][ERR_ACCROSS_DB_TRANSACTION] Transaction accross db is not supported in current transaction policy");
        }

        sql = "ROLLBACK";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void WriteBroadcastTable_autocommit_0_free() throws SQLException {
        mysqlConnection.setAutoCommit(false);
        tddlConnection.setAutoCommit(false);

        String sql = "set drds_transaction_policy='free'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test, timestamp_test) VALUES"
            + "(1, 'something in broadcast table', 666, '2019-10-16 02:00'), (6, 'something in broadcast table', 777, '2019-10-16 02:00')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql =
            "UPDATE " + baseOneTableName + " SET integer_test = 1234, timestamp_test='2019-10-16 02:10' WHERE pk = 1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = "DELETE FROM " + baseOneTableName + " WHERE pk = 6";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        assertVariable("drds_transaction_policy", "FREE", tddlConnection, false);

        sql = "COMMIT";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        assertVariable("drds_transaction_policy", "FREE", tddlConnection, false);

        sql = "SELECT * FROM " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void WriteBroadcastTable_autocommit_1_multi_statement() throws SQLException {
        String sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test, timestamp_test) VALUES"
            + "(1, 'something in broadcast table', 666, '2019-10-16 02:00:00'), (6, 'something in broadcast table', 777, '2019-10-16 02:00:00'); SHOW variables LIKE 'drds_transaction_policy'";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test, timestamp_test) VALUES"
            + "(3, 'something in broadcast table', 666, '2019-10-16 02:00:00'), (8, 'something in broadcast table', 777, '2019-10-16 02:00:00'); SHOW variables LIKE 'drds_transaction_policy'";

        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            boolean hasMoreResultSets = ps.execute();
            while (hasMoreResultSets || ps.getUpdateCount() != -1) {
                if (hasMoreResultSets) {
                    try (ResultSet rs = ps.getResultSet()) {
                        rs.next();
                        if (baseOneTableName.equalsIgnoreCase("update_delete_base_broadcast")
                            && supportXA) {
                            String transactionPolicy = rs.getString(2);
                            Assert.assertTrue("XA".equals(transactionPolicy) || "TSO".equals(transactionPolicy));
                        }
                    } finally {

                    }
                }

                hasMoreResultSets = ps.getMoreResults();
            } // end of while
        } finally {

        }
    }

    @Test
    public void WriteBroadcastTable_rollback_test() {
        String sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test) VALUES"
            + "(1, 'something in broadcast table', 666), (6, 'something in broadcast table', 777);";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        ResultSet resultSet =
            JdbcUtil.executeQuerySuccess(tddlConnection, "show topology from " + baseOneTableName);
        String physicalSimpleTableName = baseOneTableName;
        try {
            resultSet.next();
            physicalSimpleTableName = (String) JdbcUtil.getObject(resultSet, 3);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        if (baseOneTableName.equalsIgnoreCase("update_delete_base_broadcast")) {

            if (!usingNewPartDb()) {

                sql = "/*+TDDL:node(2,3)*/DELETE FROM " + physicalSimpleTableName + " WHERE pk = 1";
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

                sql = "/*+TDDL:node(1)*/INSERT INTO " + physicalSimpleTableName
                    + "(pk, varchar_test, integer_test) VALUES"
                    + "(4, 'something in broadcast table', 999);";
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            } else {

                sql = "/*+TDDL:node(1)*/DELETE FROM " + physicalSimpleTableName + " WHERE pk = 1";
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

                sql = "/*+TDDL:node(1)*/INSERT INTO " + physicalSimpleTableName
                    + "(pk, varchar_test, integer_test) VALUES"
                    + "(4, 'something in broadcast table', 999);";
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            }

        }

        sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test) VALUES"
            + "(4, 'something in broadcast table', 666), (3, 'something in broadcast table', 888);";
        if (baseOneTableName.equalsIgnoreCase("update_delete_base_broadcast")) {
            executeErrorAssert(tddlConnection, sql, ImmutableList.of(),
                "Duplicate entry '4' for key 'PRIMARY'");
        } else {
            sql = "INSERT INTO " + baseOneTableName + "(pk, varchar_test, integer_test) VALUES"
                + "(4, 'something in broadcast table', 666), (3, 'something in broadcast table', 888);";
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
        }
    }

    @Test
    @Ignore("不符合并发测试需求，会导致部分case失败！")
    public void WriteBroadcastTable_autocommit_0_no_transaction_policy_after_reload_datasources() throws SQLException {
        JdbcUtil.executeQuery("reload datasources", tddlConnection);
        WriteBroadcastTable_autocommit_0_no_transaction_policy();
    }
}

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

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Test explicit XA transaction
 *
 * @see TsoTransactionTest
 */

public class XATransactionTest extends CrudBasedLockTestCase {
    /**
     * only works for RR isolation level
     */
    private final boolean shareReadView;

    @Parameterized.Parameters(name = "{index}:table={0},shareReadView={1}")
    public static List<Object[]> prepare() throws SQLException {
        boolean supportShareReadView;
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            supportShareReadView = JdbcUtil.supportShareReadView(connection);
        }
        List<Object[]> ret = new ArrayList<>();
        for (String[] tables : ExecuteTableName.allMultiTypeOneTable(ExecuteTableName.UPDATE_DELETE_BASE)) {
            ret.add(new Object[] {tables[0], false});
            if (supportShareReadView) {
                ret.add(new Object[] {tables[0], true});
            }
        }
        return ret;
    }

    public XATransactionTest(String baseOneTableName, boolean shareReadView) {
        this.baseOneTableName = baseOneTableName;
        this.shareReadView = shareReadView;
    }

    @Before
    public void before() throws Exception {
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + baseOneTableName, null);
        final String sql = "insert into " + baseOneTableName + "(pk, integer_test, varchar_test) values(?, ?, ?)";
        final List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            params.add(ImmutableList.of(i, i, "test" + i));
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params, true);
    }

    private void setTrxProperties(Connection conn) throws SQLException {
        JdbcUtil.setShareReadView(shareReadView, conn);
        JdbcUtil.setTxPolicy(ITransactionPolicy.XA, conn);
    }

    @Test
    public void checkTransUsingXA() throws Exception {
        tddlConnection.setAutoCommit(false);
        setTrxProperties(tddlConnection);

        try (Statement stmt = tddlConnection.createStatement()) {
            String varchar_test = "12";
            stmt.execute(" SELECT count(*) FROM " + baseOneTableName);
            stmt.execute(" UPDATE " + baseOneTableName + " set varchar_test = " + varchar_test + " where pk=1");
            stmt.execute(" SELECT count(*) FROM " + baseOneTableName);
            stmt.execute(" UPDATE " + baseOneTableName + " set varchar_test = " + varchar_test + " where pk=1");
            stmt.execute(" SELECT count(*) FROM " + baseOneTableName);
        }
        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);
    }

    @Test
    public void checkTransUsingXA2() throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        setTrxProperties(tddlConnection);

        List<Object> params = Lists.newArrayList("12");
        List<List<Object>> results;
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(0)), results.get(0).get(0));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, " UPDATE " + baseOneTableName
            + " set varchar_test = ? where pk = 1", params);
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(1)), results.get(0).get(0));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, " UPDATE " + baseOneTableName
            + " set varchar_test = ? where pk = 2", params);
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(2)), results.get(0).get(0));
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

    @Test
    public void checkTransUsingXAWithRollback() throws Exception {
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        setTrxProperties(tddlConnection);

        List<Object> params = Lists.newArrayList("12");
        List<List<Object>> results;
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(0)), results.get(0).get(0));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, " UPDATE " + baseOneTableName
            + " set varchar_test = ? where pk = 1", params);
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(1)), results.get(0).get(0));
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, " UPDATE " + baseOneTableName
            + " set varchar_test = ? where pk = 2", params);
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(2)), results.get(0).get(0));
        tddlConnection.rollback();
        mysqlConnection.rollback();
        results =
            selectContentSameAssert(" SELECT count(*) FROM " + baseOneTableName + " where varchar_test = ?", params,
                mysqlConnection, tddlConnection);
        Assert.assertTrue(results.size() == 1 && results.get(0).size() == 1);
        Assert.assertEquals(new JdbcUtil.MyNumber(new BigDecimal(0)), results.get(0).get(0));
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }
}
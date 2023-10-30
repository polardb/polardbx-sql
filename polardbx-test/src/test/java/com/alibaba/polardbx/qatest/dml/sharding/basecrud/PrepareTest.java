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

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Prepare Execute测试
 *
 * @author zilin.zl
 * @since 5.3.7
 */
@NotThreadSafe
public class PrepareTest extends CrudBasedLockTestCase {

    public PrepareTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    @Test
    public void prepareSelectNoTableTest() {
        String prepareSql = "prepare p from 'select CONVERT(? + ?, char)';";
        String setSql = "SET @a=1, @b=10;";
        String executeSql = "execute p using @a, @b;";

        assertPrepareTest(mysqlConnection, tddlConnection, prepareSql, setSql, executeSql);
    }

    @Test
    public void prepareEmptyString() {
        String prepareSql = "prepare p from 'select ?';";
        String setSql = "SET @a='';";
        String executeSql = "execute p using @a;";

        assertPrepareTest(mysqlConnection, tddlConnection, prepareSql, setSql, executeSql);
    }

    @Test
    public void prepareUsingUserDefVar() throws SQLException {
        String setVars = "set @x = 'select ?'";
        executeSetVars(setVars);
        String prepareSql = "prepare p from @x;";
        String setSql = "SET @a='';";
        String executeSql = "execute p using @a;";

        assertPrepareTest(mysqlConnection, tddlConnection, prepareSql, setSql, executeSql);
    }

    private void executeSetVars(String sql) throws SQLException {
        mysqlConnection.createStatement().execute(sql);
        tddlConnection.createStatement().execute(sql);
    }

    @Test
    public void prepareSelectTest() {
        String prepareSql = "prepare p from 'select * from " + baseOneTableName + " where pk= ? ';";
        String setSql = "set @a = " + ColumnDataGenerator.pkValue + ';';
        String executeSql = "execute p using @a;";

        assertPrepareTest(mysqlConnection, tddlConnection, prepareSql, setSql, executeSql);
    }

    @Test
    public void prepareSelectCountTest() {
        String sql =
            "select *,count(*) from "
                + "`"
                + TStringUtil.replace(baseOneTableName, "`", "``")
                + "`"
                + " where pk= ?";
        String prepareSql = "prepare p from '" + sql + "';";
        String setSql = "set @a = " + ColumnDataGenerator.pkValue + ';';
        String executeSql = "execute p using @a;";
        assertPrepareTest(mysqlConnection, tddlConnection, prepareSql, setSql, executeSql);
    }

    @Test
    public void prepareJoinTest() {
        String sql =
            "select * from "
                + baseOneTableName
                + " t1, "
                + baseOneTableName
                + " t2 where t1.pk = t2.pk order by t1.pk";
        String prepareSql = "prepare p from '" + sql + "';";
        String setSql = "set @a = " + ColumnDataGenerator.pkValue + ';';
        String executeSql = "execute p";
        assertPrepareTest(mysqlConnection, tddlConnection, prepareSql, setSql, executeSql);
    }

    @Test
    public void prepareJoinTes2() {
        String sql =
            "select * from "
                + baseOneTableName
                + " t1, "
                + baseOneTableName
                + " t2 where t1.pk = t2.pk" + " and t1.pk = ? order by t1.pk";
        String prepareSql = "prepare p from '" + sql + "';";
        String setSql = "set @a = " + ColumnDataGenerator.pkValue + ';';
        String executeSql = "execute p using @a";
        assertPrepareTest(mysqlConnection, tddlConnection, prepareSql, setSql, executeSql);
    }

    @Test
    @Ignore
    public void testMemoryPool() {
        ResultSet rs1 = JdbcUtil.executeQuerySuccess(tddlConnection, "show memorypool");
        int beforeSize = JdbcUtil.getAllResult(rs1).size();

        String query = "prepare p from 'select ?'";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = tddlConnection.prepareStatement(query);
        } catch (Throwable t) {

        } finally {
            if (preparedStatement != null) {
                JdbcUtil.close(preparedStatement);
            }
        }

        ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection, "show memorypool");
        int afterSize = JdbcUtil.getAllResult(rs2).size();

        Assert.assertTrue("beforeSize:" + beforeSize + ", afterSize:" + afterSize, beforeSize == afterSize);
    }

    @Test
    public void testSetCommand() {
        String sql =
            "SET session sql_mode = 'NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'";
        if (isMySQL80()) {
            sql =
                "SET session sql_mode = 'NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'";
        }
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = tddlConnection.prepareStatement(sql);
        } catch (Throwable t) {
            Assert.fail(sql + " is failed.");
        } finally {
            if (preparedStatement != null) {
                JdbcUtil.close(preparedStatement);
            }
        }
    }

    private void assertPrepareTest(
        Connection mysqlConnection,
        Connection tddlConnection,
        String prepareSql,
        String setSql,
        String executeSql) {
        ResultSet mysqlRs = null;
        ResultSet tddlRs = null;
        Statement mysqlStatement = null;
        Statement tddlStatement = null;
        try {
            mysqlStatement = mysqlConnection.createStatement();
            mysqlStatement.execute(prepareSql);
            mysqlStatement.execute(setSql);
            mysqlRs = mysqlStatement.executeQuery(executeSql);

            tddlStatement = tddlConnection.createStatement();
            tddlStatement.execute(prepareSql);
            tddlStatement.execute(setSql);
            tddlRs = tddlStatement.executeQuery(executeSql);

            List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs);
            List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs);
            Assert.assertTrue(
                "sql语句:" + prepareSql + setSql + executeSql + " 查询的结果集为空，请修改sql语句，保证有结果集",
                mysqlResults.size() != 0);
            assertWithMessage(
                " 顺序情况下：mysql 返回结果与tddl 返回结果不一致 \n sql 语句为：" + prepareSql + setSql + executeSql)
                .that(mysqlResults)
                .containsExactlyElementsIn(tddlResults)
                .inOrder();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(mysqlRs);
            JdbcUtil.close(tddlRs);
            JdbcUtil.close(mysqlStatement);
            JdbcUtil.close(tddlStatement);
        }
    }
}

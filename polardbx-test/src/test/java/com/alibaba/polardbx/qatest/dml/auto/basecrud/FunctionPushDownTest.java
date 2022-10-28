package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.validation.constraints.AssertTrue;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@FileStoreIgnore
public class FunctionPushDownTest extends BaseTestCase {
    protected Connection tddlConnection;
    protected Connection mysqlConnection;

    private static String TABLE_NAME = "test_function";
    private static String PUSH_DOWN_FUNC_NAME = "test_push_down_function";
    private static String NON_PUSH_DOWN_FUNC_NAME = "test_push_down_function";

    @Before
    public void init() {
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, "set global log_bin_trust_function_creators = on");
        JdbcUtil.executeSuccess(mysqlConnection, "set global log_bin_trust_function_creators = on");
        prepareData(tddlConnection);
        prepareData(mysqlConnection);
    }

    private void prepareData(Connection connection) {
        JdbcUtil.executeSuccess(connection, "drop table if exists " + TABLE_NAME);
        JdbcUtil.executeSuccess(connection, String.format("create table %s(a int, b int)", TABLE_NAME));
        JdbcUtil.executeSuccess(connection, String.format("insert into %s values (1,1)", TABLE_NAME));
    }

    @Test
    public void testPushDownFunc() throws SQLException {
        recreateFunction(mysqlConnection, PUSH_DOWN_FUNC_NAME, true);
        recreateFunction(tddlConnection, PUSH_DOWN_FUNC_NAME, true);
        String sql = String.format("select a, %s(b) from %s", PUSH_DOWN_FUNC_NAME, TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        String explainResult = getExplainResult(tddlConnection, sql).toUpperCase();
        // function pushed to dn
        Assert.assertTrue(!explainResult.contains("PROJECT"), "function should be pushed to dn");

        functionPushedToDn(PUSH_DOWN_FUNC_NAME, true);
        // test pushdown udf
        JdbcUtil.executeSuccess(tddlConnection, "pushdown udf");
    }

    private void functionPushedToDn(String function, boolean shouldPush) throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery("select * from information_schema.pushed_function where function = '" + function + "'")) {
            boolean hasResult = rs.next();
            Assert.assertTrue(shouldPush == hasResult, "function push down test failed!");
        }
    }

    @Test
    public void testNonPushDownFunc() throws SQLException {
        recreateFunction(mysqlConnection, NON_PUSH_DOWN_FUNC_NAME, false);
        recreateFunction(tddlConnection, NON_PUSH_DOWN_FUNC_NAME, false);
        String sql = String.format("select a, %s(b) from %s", NON_PUSH_DOWN_FUNC_NAME, TABLE_NAME);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
        String explainResult = getExplainResult(tddlConnection, sql).toUpperCase();
        // function calculated in cn
        Assert.assertTrue(explainResult.contains("PROJECT"), "function should no pushed to dn");

        functionPushedToDn(NON_PUSH_DOWN_FUNC_NAME, false);
    }

    private void recreateFunction(Connection connection, String functionName, boolean isNosql) {
        JdbcUtil.executeSuccess(connection, "drop function if exists " + functionName);
        JdbcUtil.executeSuccess(connection, String.format("create function %s(a int) returns int %s return a + 1", functionName, isNosql ? " no sql" : ""));
    }

    @After
    public void clearDataAndFunction() {
        JdbcUtil.executeSuccess(mysqlConnection, "drop function if exists " + PUSH_DOWN_FUNC_NAME);
        JdbcUtil.executeSuccess(tddlConnection, "drop function if exists " + PUSH_DOWN_FUNC_NAME);
        JdbcUtil.executeSuccess(mysqlConnection, "drop function if exists " + NON_PUSH_DOWN_FUNC_NAME);
        JdbcUtil.executeSuccess(tddlConnection, "drop function if exists " + NON_PUSH_DOWN_FUNC_NAME);
        JdbcUtil.executeSuccess(mysqlConnection, "drop table if exists " + TABLE_NAME);
        JdbcUtil.executeSuccess(tddlConnection, "drop table if exists " + TABLE_NAME);
    }
}

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class ShowParamTest extends ReadBaseTestCase {
    private static final String SET_VARIABLE = "set %s %s = %s";
    private static final String SHOW_VARIABLE = "show %s variables like '%s'";

    private boolean isGlobal;
    private String name;
    private Object setValue;
    private Object expectValue;

    @Parameterized.Parameters(name = "{index}:isGlobal={0},name={1},value={2}")
    public static List<Object[]> prepareDate() {
        return Arrays.asList(data());
    }

    public ShowParamTest(Boolean isGlobal, String name, Object setValue, Object expectValue) {
        this.isGlobal = isGlobal;
        this.name = name;
        this.setValue = setValue;
        this.expectValue = expectValue;
    }

    public static Object[][] data() {

        Object[][] object = {
            {Boolean.TRUE, ConnectionProperties.MAX_PL_DEPTH, 100L, "100"},
            {Boolean.FALSE, ConnectionProperties.MAX_PL_DEPTH, 100L, "100"},
            {Boolean.TRUE, ConnectionProperties.PL_MEMORY_LIMIT, 1000000L, "1000000"},
            {Boolean.FALSE, ConnectionProperties.PL_MEMORY_LIMIT, 1000000L, "1000000"},
            {Boolean.TRUE, ConnectionProperties.PL_CURSOR_MEMORY_LIMIT, 100000L, "100000"},
            {Boolean.FALSE, ConnectionProperties.PL_CURSOR_MEMORY_LIMIT, 100000L, "100000"},
            {Boolean.TRUE, ConnectionProperties.PL_INTERNAL_CACHE_SIZE, 19L, "19"},
            {Boolean.FALSE, ConnectionProperties.PL_INTERNAL_CACHE_SIZE, 19L, "19"},
        };
        return object;

    }

    @Test
    public void testSqlSelectLimit() throws SQLException {
        String setSql = String.format(SET_VARIABLE, isGlobal ? "global" : "session", name, setValue);
        JdbcUtil.executeSuccess(tddlConnection, setSql);
        if (isGlobal) {
            waitForSync();
        }
        checkShowVariable();
        checkShowVariableUsePrepare();
        checkSessionVariable();
    }

    private void waitForSync() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void checkShowVariable() throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(
                String.format(SHOW_VARIABLE, isGlobal ? "global" : "session", name))) {
            if (rs.next()) {
                Assert.assertTrue(expectValue.equals(rs.getObject(2)), "variable value not expected");
            } else {
                Assert.fail("result of show variable is empty");
            }
        }
    }

    private void checkShowVariableUsePrepare() throws SQLException {
        try (PreparedStatement statement = tddlConnection.prepareStatement(
            String.format(SHOW_VARIABLE, isGlobal ? "global" : "session", name));
            ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                Assert.assertTrue(expectValue.equals(rs.getObject(2)), "variable value not expected");
            } else {
                Assert.fail("result of show variable is empty");
            }
        }
    }

    private void checkSessionVariable() throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(String.format(SHOW_VARIABLE, "session", name))) {
            if (rs.next()) {
                Assert.assertTrue(expectValue.equals(rs.getObject(2)), "variable value not expected");
            } else {
                Assert.fail("result of show variable is empty");
            }
        }
    }
}

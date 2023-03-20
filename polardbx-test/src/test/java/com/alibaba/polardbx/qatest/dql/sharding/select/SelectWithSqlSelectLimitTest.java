package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;

public class SelectWithSqlSelectLimitTest extends ReadBaseTestCase {
    private static final String TABLE_NAME = "test_sql_select_limit_1";
    private static final Long RECORD_NUM = 100L;
    private static final String SET_VARIABLE = "set %s sql_select_limit = %s";
    private static final String SHOW_VARIABLE = "show %s variables like 'sql_select_limit'";
    private static String INSERT_DATA = "insert into  " + TABLE_NAME + " values (%s, %s, %s)";
    private static String DROP_TABLE = "drop table if exists " + TABLE_NAME;

    private boolean isGlobal;
    private long limit;

    @Parameterized.Parameters(name = "{index}:isGlobal={0},limit={1}")
    public static List<Object[]> prepareDate() {
        return Arrays.asList(data());
    }

    public SelectWithSqlSelectLimitTest(Boolean isGlobal, Long limit) {
        this.isGlobal = isGlobal;
        this.limit = limit;
    }

    public static Object[][] data() {

        Object[][] object = {
            // set global will affect other test case, so ignore temporarily
//            {Boolean.TRUE, 1L}, {Boolean.TRUE, 2L}, {Boolean.TRUE, 20L}, {Boolean.TRUE, RECORD_NUM},
//            {Boolean.TRUE, RECORD_NUM + 2}, {Boolean.TRUE, Long.MAX_VALUE - 1},
            {Boolean.FALSE, 1L}, {Boolean.FALSE, 2L}, {Boolean.FALSE, 20L}, {Boolean.FALSE, RECORD_NUM},
            {Boolean.FALSE, RECORD_NUM + 2}, {Boolean.FALSE, Long.MAX_VALUE - 1},
        };
        return object;

    }

    @BeforeClass
    public static void prepareData() throws Exception {
        String createSql =
            "CREATE TABLE if not exists `" + TABLE_NAME + "` (\n"
                + "\t`id1` int(10) DEFAULT NULL,\n"
                + "\t`id2` int(10) DEFAULT NULL,\n"
                + "\t`id3` int(10) DEFAULT NULL"
                + ") ";

        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            JdbcUtil.executeUpdateSuccess(tddlConnection, DROP_TABLE);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + " dbpartition by hash(`id1`)");
            for (int i = 0; i < RECORD_NUM; ++i) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(INSERT_DATA, i, i + 1, i + 2));
            }
        }
    }

    @Test
    public void testSqlSelectLimit() throws SQLException {
        String setSql = String.format(SET_VARIABLE, isGlobal ? "global" : "session", limit);
        JdbcUtil.executeSuccess(tddlConnection, setSql);
        if (isGlobal) {
            waitForSync();
        }
        checkSelectContent();
        checkSelectContentUsePrepare();
        checkCount();
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

    private void checkSelectContent() throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery("select * from " + TABLE_NAME + " order by id1")) {
            int recordNum = 0;
            while (rs.next()) {
                if (rs.getLong(1) != recordNum || rs.getLong(2) != recordNum + 1 || rs.getLong(3) != recordNum + 2) {
                    Assert.fail("content of select not expected");
                }
                recordNum++;
            }
            if (recordNum != Math.min(limit, RECORD_NUM)) {
                Assert.fail("records number of select not expected");
            }
        }
    }

    private void checkSelectContentUsePrepare() throws SQLException {
        try (PreparedStatement statement = tddlConnection.prepareStatement(
            "select * from " + TABLE_NAME + " order by id1");
            ResultSet rs = statement.executeQuery()) {
            int recordNum = 0;
            while (rs.next()) {
                if (rs.getLong(1) != recordNum || rs.getLong(2) != recordNum + 1 || rs.getLong(3) != recordNum + 2) {
                    Assert.fail("content of select not expected");
                }
                recordNum++;
            }
            if (recordNum != Math.min(limit, RECORD_NUM)) {
                Assert.fail("records number of select not expected");
            }
        }
    }

    private void checkCount() throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery("select count(*) from " + TABLE_NAME)) {
            if (rs.next()) {
                Assert.assertTrue(rs.getLong(1) == RECORD_NUM, "result of select count(*) is not expected");
            } else if (limit != 0) {
                Assert.fail("result of select count(*) is empty");
            }
        }
    }

    private void checkShowVariable() throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(String.format(SHOW_VARIABLE, isGlobal ? "global" : "session"))) {
            if (rs.next()) {
                Assert.assertTrue(rs.getLong(2) == limit, "variable value not expected");
            } else if (limit != 0) {
                Assert.fail("result of show variable is empty");
            }
        }
    }

    private void checkShowVariableUsePrepare() throws SQLException {
        try (PreparedStatement statement = tddlConnection.prepareStatement(
            String.format(SHOW_VARIABLE, isGlobal ? "global" : "session"));
            ResultSet rs = statement.executeQuery()) {
            if (rs.next()) {
                Assert.assertTrue(rs.getLong(2) == limit, "variable value not expected");
            } else if (limit != 0) {
                Assert.fail("result of show variable is empty");
            }
        }
    }

    private void checkSessionVariable() throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(String.format(SHOW_VARIABLE, "session"))) {
            if (rs.next()) {
                Assert.assertTrue(rs.getLong(2) == limit, "variable value not expected");
            } else if (limit != 0) {
                Assert.fail("result of show variable is empty");
            }
        }
    }

    @AfterClass
    public static void cleanData() throws Exception {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            JdbcUtil.executeUpdateSuccess(tddlConnection, DROP_TABLE);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set session sql_select_limit = " + Long.MAX_VALUE);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global sql_select_limit = " + Long.MAX_VALUE);
        }
    }
}

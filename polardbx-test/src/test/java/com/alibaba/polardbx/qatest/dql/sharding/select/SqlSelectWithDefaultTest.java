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
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.usePrepare;

public class SqlSelectWithDefaultTest extends ReadBaseTestCase {
    private static final String TABLE_NAME = "test_sql_select_limit_default";
    private static final Long RECORD_NUM = 100L;
    private static final String SET_VARIABLE = "set sql_select_limit = default";
    private static final String SHOW_VARIABLE = "show session variables like 'sql_select_limit'";
    private static String INSERT_DATA = "insert into  " + TABLE_NAME + " values (%s, %s, %s)";
    private static String DROP_TABLE = "drop table if exists " + TABLE_NAME;

    private long limit;

    public SqlSelectWithDefaultTest() {
        this.limit = 9223372036854775807L;
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

        String setSql = SET_VARIABLE;
        JdbcUtil.executeSuccess(tddlConnection, setSql);
        checkSelectContent();
        checkSelectContentUsePrepare();
        checkCount();
        checkShowVariable();
        if (!usePrepare()) {
            //skip when prepare mode
            checkShowVariableUsePrepare();
        }
        checkSessionVariable();
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
            ResultSet rs = statement.executeQuery(SHOW_VARIABLE)) {
            if (rs.next()) {
                Assert.assertTrue(rs.getLong(2) == limit, "variable value not expected");
            } else if (limit != 0) {
                Assert.fail("result of show variable is empty");
            }
        }
    }

    private void checkShowVariableUsePrepare() throws SQLException {
        try (PreparedStatement statement = tddlConnection.prepareStatement(
            SHOW_VARIABLE);
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
            ResultSet rs = statement.executeQuery(SHOW_VARIABLE)) {
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

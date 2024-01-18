package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TypeCompareInJoinTest extends ReadBaseTestCase {
    private static final String TABLE_NAME1 = "test_date_1";
    private static final String TABLE_NAME2 = "test_date_2";
    private static final String CREATE_TABLE =
        "CREATE TABLE %s(`pk` bigint(11) NOT NULL AUTO_INCREMENT primary key, `date_test` date, `datetime_test` datetime)";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s";
    private static final String INSERT_DATA = "INSERT INTO %s values (null, '2022-01-01', '2022-01-01 01:02:03')";
    private static final String NOT_PUSH_JOIN_HINT =
        "/*+TDDL: ENABLE_POST_PLANNER=false ENABLE_DIRECT_PLAN=false ENABLE_CBO_PUSH_JOIN=false ENABLE_PUSH_JOIN=false ENABLE_SORT_MERGE_JOIN=false*/ ";
    private static final String CHECK_RESULT =
        "select * from %s AS a join %s as b on a.date_test = b.datetime_test;";
    private static final String CHECK_RESULT2 =
        "select * from %s AS a join %s as b on a.datetime_test = b.date_test;";

    @Before
    public void prepareData() {
        this.tddlConnection = getPolardbxConnection();
        prepareTable(TABLE_NAME1);
        prepareTable(TABLE_NAME2);
    }

    private void prepareTable(String tableName) {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, tableName));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, tableName));
        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, tableName));
    }

    @Test
    public void testTypeCompare() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(String.format(NOT_PUSH_JOIN_HINT + CHECK_RESULT, TABLE_NAME1, TABLE_NAME2),
            tddlConnection);
        if (rs.next()) {
            Assert.fail("result of join should be empty");
        }

        // check type convert place
        rs = JdbcUtil.executeQuery(String.format(NOT_PUSH_JOIN_HINT + CHECK_RESULT2, TABLE_NAME1, TABLE_NAME2),
            tddlConnection);
        if (rs.next()) {
            Assert.fail("result of join should be empty");
        }
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME2));
    }
}

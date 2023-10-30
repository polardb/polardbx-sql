package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelectYearFuncTest extends ReadBaseTestCase {
    private static final String TABLE_NAME = "test_year_func_with_tinyint_type";
    private static final String CREATE_TABLE = "CREATE TABLE %s(c1 tinyint)";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s";
    private static final String INSERT_DATA = "INSERT INTO %s values (-15), (0), (111), (120), (null)";
    private static final String CHECK_RESULT =
        "/*+TDDL: ENABLE_PUSH_PROJECT = false*/ select c1, year(c1) from %s order by c1";

    @Before
    public void getConnection() {
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(mysqlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_NAME));
        JdbcUtil.executeSuccess(mysqlConnection, String.format(INSERT_DATA, TABLE_NAME));
    }

    @Test
    public void testYearType() {
        DataValidator.selectContentSameAssert(String.format(CHECK_RESULT, TABLE_NAME), null, mysqlConnection,
            tddlConnection);
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(mysqlConnection, String.format(DROP_TABLE, TABLE_NAME));
    }
}

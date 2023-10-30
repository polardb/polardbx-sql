package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

public class SelectInValuesList extends BaseTestCase {
    private static final String DROP_TABLE = "drop table if exists %s";
    private static final String CREATE_TABLE = "create table %s(c1 int, c2 int, c3 int)";
    private static final String TABLE_NAME = "table_test_in_values";
    private static final String PARTITION_INFO = " dbpartition by hash(c1) tbpartition by hash(c2) tbpartitions 2";
    private static final String INSERT_DATA = "insert into %s values (%s, %s, %s)";

    protected Connection tddlConnection;
    protected Connection mysqlConnection;

    @Before
    public void init() {
        this.tddlConnection = getPolardbxConnection();
        this.mysqlConnection = getMysqlConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(mysqlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE + PARTITION_INFO, TABLE_NAME));
        JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE, TABLE_NAME));
        for (int i = 0; i <= 4; ++i) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_NAME, i, i, i));
            JdbcUtil.executeSuccess(mysqlConnection, String.format(INSERT_DATA, TABLE_NAME, i, i, i));
        }
    }

    @Test
    public void testMultiColWitExpr() {
        String sql = "select * from %s where (c1, c2) in ((1,1), (1+1, 2), (3+1, 5-1))";
        DataValidator.selectContentSameAssert(String.format(sql, TABLE_NAME), null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testWithUseDefVar() {
        JdbcUtil.executeSuccess(mysqlConnection, "set @x = 2");
        JdbcUtil.executeSuccess(tddlConnection, "set @x = 2");
        String sql = "select * from %s where (c1) in (1, @x, @x+2, 0)";
        DataValidator.selectContentSameAssert(String.format(sql, TABLE_NAME), null, mysqlConnection, tddlConnection);

        // test multi column
        sql = "select * from %s where (c1, c2) in ((1,1), (2,@x), (@x,0))";
        DataValidator.selectContentSameAssert(String.format(sql, TABLE_NAME), null, mysqlConnection, tddlConnection);
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(mysqlConnection, String.format(DROP_TABLE, TABLE_NAME));
    }
}

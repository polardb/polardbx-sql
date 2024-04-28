package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ProcedureWithTrxTest extends BaseTestCase {
    protected Connection tddlConnection;
    String TABLE_NAME = "table_procedure_with_trx1";
    String PROCEDURE_ANME = "procedure_with_trx1";
    String DROP_TABLE = "drop table if exists %s";
    String CREATE_TABLE = "create table %s (a int, b int) dbpartition by hash(a)";
    String DROP_PROCEDURE = "drop procedure if exists %s";
    private String CREATE_PROCEDURE = "create procedure %s(in val int) begin start transaction; \n"
        + "insert into %s values (1, 1); if val = 1 then commit; else rollback; end if; end;";
    private String SELECT_SQL = "select * from %s";

    @Before
    public void getConnection() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_ANME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_PROCEDURE, PROCEDURE_ANME, TABLE_NAME));
    }

    @Test
    public void testSimpleTrxSql() throws SQLException {
        JdbcUtil.executeSuccess(tddlConnection, "call " + PROCEDURE_ANME + "(2)");
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(SELECT_SQL, TABLE_NAME), tddlConnection)) {
            Assert.assertFalse("result should be empty", rs.next());
        }

        JdbcUtil.executeSuccess(tddlConnection, "call " + PROCEDURE_ANME + "(1)");
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(SELECT_SQL, TABLE_NAME), tddlConnection)) {
            Assert.assertTrue("result should not be empty", rs.next());
        }
    }

    @After
    public void dropData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_ANME));
    }
}

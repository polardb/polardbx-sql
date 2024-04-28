package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ProcedureExceptionHandleTest extends BaseTestCase {
    protected Connection tddlConnection;

    static final String PROCEDURE_NAME = "proc_exception_handle";
    static final String TABLE_NAME1 = "tbl_exception_handle_1";
    static final String TABLE_NAME2 = "tbl_exception_handle_2";
    static final String CREATE_PROCEDURE = "create procedure %s(xx int)\n"
        + "declare continue handler for 4006 begin create table %s(a int); insert into %s values (xx); end;\n"
        + "select 1;\n"
        + "drop table if exists %s;\n"
        + "delete from %s; \n"
        + "set @test = 1;\n"
        + "begin\n"
        + "declare exit handler for 4006 set @test2 = 2;\n"
        + "drop table if exists %s;\n"
        + "delete from %s;\n"
        + "set @test2 = 3;\n"
        + "end;";
    static final String DROP_PROCEDURE = "drop procedure if exists %s";

    @Before
    public void prepareData() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_NAME));
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(CREATE_PROCEDURE, PROCEDURE_NAME, TABLE_NAME1, TABLE_NAME1, TABLE_NAME1, TABLE_NAME1,
                TABLE_NAME2, TABLE_NAME2));
    }

    @Test
    public void testExceptionHandle() throws SQLException {
        int number = 111;
        JdbcUtil.executeSuccess(tddlConnection, String.format("call %s(%s)", PROCEDURE_NAME, number));
        ResultSet rs = JdbcUtil.executeQuery("select * from " + TABLE_NAME1, tddlConnection);
        if (!rs.next()) {
            Assert.fail("table should have data but not");
        }
        if (!rs.getObject(1).equals(number)) {
            Assert.fail("data inserted in the table not expected");
        }
        if (rs.next()) {
            Assert.fail("should be exactly one row in the table");
        }
        rs = JdbcUtil.executeQuery("select @test", tddlConnection);
        if (!rs.next() || !rs.getObject(1).equals(1L)) {
            Assert.fail("something wrong with exception handle");
        }
        rs = JdbcUtil.executeQuery("select @test2", tddlConnection);
        if (!rs.next() || !rs.getObject(1).equals(2L)) {
            Assert.fail("something wrong with exception handle");
        }
    }

    @After
    public void dropData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format("drop table if exists %s", TABLE_NAME1));
    }
}

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

@FileStoreIgnore
public class ProcedureRecursiveExceptionTest extends BaseTestCase {
    protected Connection tddlConnection;

    static final String PROCEDURE_NAME_OUTER = "proc_recur_exception_handle";
    static final String PROCEDURE_NAME_INNER = "proc_recur_exception_handle_2";
    static final String TABLE_NAME1 = "tbl_recur_exception_handle_1";
    static final String TABLE_NAME2 = "tbl_recur_exception_handle_2";
    static final String CREATE_PROCEDURE_OUTER = "create procedure %s(xx int)\n"
        + "declare continue handler for 4006 begin create table %s(a int); insert into %s values (xx); end;\n"
        + "select 1;\n"
        + "start transaction;\n"
        + "drop table if exists %s;\n"
        + "delete from %s; \n"
        + "commit;\n"
        + "set @test3 = 1;\n"
        + "call %s()";
    static final String CREATE_PROCEDURE_INNER = "create procedure %s()\n"
        + "begin\n"
        + "declare exit handler for 4006 set @test4 = 2;\n"
        + "start transaction;\n"
        + "drop table if exists %s;\n"
        + "create table %s(a int);\n"
        + "rollback;\n"
        + "drop table if exists %s;\n"
        + "delete from %s;\n"
        + "set @test4 = 3;\n"
        + "end;";
    static final String DROP_PROCEDURE = "drop procedure if exists %s";

    @Before
    public void prepareData() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_NAME_OUTER));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_NAME_INNER));
        System.out.println(
            String.format(CREATE_PROCEDURE_OUTER, PROCEDURE_NAME_OUTER, TABLE_NAME1, TABLE_NAME1, TABLE_NAME1,
                TABLE_NAME1, PROCEDURE_NAME_INNER));
        System.out.println(
            String.format(CREATE_PROCEDURE_INNER, PROCEDURE_NAME_INNER, TABLE_NAME2, TABLE_NAME2, TABLE_NAME2,
                TABLE_NAME2));
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(CREATE_PROCEDURE_OUTER, PROCEDURE_NAME_OUTER, TABLE_NAME1, TABLE_NAME1, TABLE_NAME1,
                TABLE_NAME1, PROCEDURE_NAME_INNER));
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(CREATE_PROCEDURE_INNER, PROCEDURE_NAME_INNER, TABLE_NAME2, TABLE_NAME2, TABLE_NAME2,
                TABLE_NAME2));
    }

    @Test
    public void testRecursiveExceptionHandle() throws SQLException {
        int number = 111;
        JdbcUtil.executeSuccess(tddlConnection, String.format("call %s(%s)", PROCEDURE_NAME_OUTER, number));
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
        rs = JdbcUtil.executeQuery("select @test3", tddlConnection);
        if (!rs.next() || !rs.getObject(1).equals(1L)) {
            Assert.fail("something wrong with exception handle");
        }
        rs = JdbcUtil.executeQuery("select @test4", tddlConnection);
        if (!rs.next() || !rs.getObject(1).equals(2L)) {
            Assert.fail("something wrong with exception handle");
        }
    }

    @After
    public void dropData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_NAME_OUTER));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_NAME_INNER));
        JdbcUtil.executeSuccess(tddlConnection, String.format("drop table if exists %s", TABLE_NAME1));
    }
}

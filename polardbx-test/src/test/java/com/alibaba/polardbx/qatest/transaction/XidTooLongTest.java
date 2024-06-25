package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class XidTooLongTest extends CrudBasedLockTestCase {
    private static final String SCHEMA = "testkkk01234567890123456789012345678901234567890123456789";
    private static final String TABLE = "testkkk01234567890123456789012345678901234567890123456789";

    private Connection polarxConn = getPolardbxConnection();

    @After
    public void after() throws SQLException {
        JdbcUtil.executeSuccess(polarxConn, "rollback");
        JdbcUtil.executeSuccess(polarxConn, "drop database if exists " + SCHEMA);
        polarxConn.close();
    }

    @Test
    public void simpleTest() throws SQLException {
        JdbcUtil.executeSuccess(polarxConn, "drop database if exists " + SCHEMA);
        JdbcUtil.executeSuccess(polarxConn, "create database " + SCHEMA + " mode = auto ");
        JdbcUtil.executeSuccess(polarxConn, "use " + SCHEMA);
        JdbcUtil.executeSuccess(polarxConn, "create table if not exists "
            + TABLE + " (id int primary key) partition by key(id)");

        JdbcUtil.executeSuccess(polarxConn, "set transaction_policy = TSO");
        JdbcUtil.executeSuccess(polarxConn, "begin");
        JdbcUtil.executeSuccess(polarxConn, "insert into " + TABLE + " values(1), (2), (3), (4)");
        JdbcUtil.executeSuccess(polarxConn, "delete from " + TABLE);
        JdbcUtil.executeSuccess(polarxConn, "commit");

        cleanData();

        // Should commit by recover task.
        String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_AFTER_PRIMARY_COMMIT') */";
        JdbcUtil.executeSuccess(polarxConn, "begin");
        JdbcUtil.executeSuccess(polarxConn, hint + "insert into " + TABLE + " values(1), (2), (3), (4)");
        JdbcUtil.executeFailed(polarxConn, "commit", "Failed");

        ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, "select * from " + TABLE
            + " order by id for update");
        // Should see 1, 2, 3, 4
        int i = 1;
        while (rs.next()) {
            Assert.assertEquals(i, rs.getInt(1));
            i++;
        }
        rs.close();

        cleanData();

        // Should rollback by recover task
        hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */";
        JdbcUtil.executeSuccess(polarxConn, "begin");
        JdbcUtil.executeSuccess(polarxConn, hint + "insert into " + TABLE + " values(1), (2), (3), (4)");
        JdbcUtil.executeFailed(polarxConn, "commit", "Failed");

        ResultSet rs2 = JdbcUtil.executeQuerySuccess(polarxConn, "select * from " + TABLE
            + " order by id for update");
        // Should be empty.
        Assert.assertFalse(rs2.next());
        rs2.close();

        hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_DURING_PRIMARY_COMMIT') */";
        JdbcUtil.executeSuccess(polarxConn, "begin");
        JdbcUtil.executeSuccess(polarxConn, hint + "insert into " + TABLE + " values(1), (2), (3), (4)");
        JdbcUtil.executeFailed(polarxConn, "commit", "Failed");

        ResultSet rs3 = JdbcUtil.executeQuerySuccess(polarxConn, "select * from " + TABLE
            + " order by id for update");
        // Should be empty.
        Assert.assertFalse(rs3.next());
        rs3.close();
    }

    private void cleanData() {
        JdbcUtil.executeSuccess(polarxConn, "begin");
        JdbcUtil.executeSuccess(polarxConn, "delete from " + TABLE);
        JdbcUtil.executeSuccess(polarxConn, "commit");
    }
}

package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DdlFailedTest extends DDLBaseNewDBTestCase {

    @Test
    public void testCreateTableFailed() {
        String sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "/*+TDDL:cmd_extra(SKIP_DDL_RESPONSE = true)*/create table t1(a varchar(-100))";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "The DDL job has been rollback");

        sql1 = "create table t1(a varchar(-100))";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "PXC-4614");

        sql1 = "/*+TDDL:cmd_extra(SKIP_DDL_RESPONSE = true)*/create table t1(a varchar(100))";
        JdbcUtil.executeSuccess(tddlConnection, sql1);
    }

    @Test
    public void testCreateGsiFailed() throws SQLException {
        String sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t1(a varchar(100)) partition by key(a) partitions 3";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 =
            "/*+TDDL:cmd_extra(FP_STATISTIC_SAMPLE_ERROR=true)*/alter table t1 add global index gsi1(a) partition by key(a) partitions 5";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "failed with state PAUSED");

        Long jobId = getDDLJobId(tddlConnection);
        sql1 = "rollback ddl " + jobId;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "cannot be rolled back");

        Assert.assertTrue(checkDDLPaused(tddlConnection, jobId));
    }

    @Test
    public void testCreateDropFailed() throws SQLException {
        String sql1 = "drop table if exists wumu.wumu.t1";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "You have an error in your SQL syntax");

        sql1 = "drop table wumu.wumu.t1";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "You have an error in your SQL syntax");

        sql1 = "create table wumu.wumu.t1(a int)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "You have an error in your SQL syntax");
    }

    @Test
    public void testCreateProcedureFailed() {
        String sql = "CREATE PROCEDURE SimpleAddition(IN num1 INT, IN num2 INT, OUT result INT)\n"
            + "BEGIN\n"
            + "  SET result = num1 + num2;\n"
            + "END //";

        String sql1 = "drop procedure if exists SimpleAddition";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "/*+TDDL:cmd_extra(FP_CREATE_PROCEDURE_ERROR=true)*/" + sql;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "injected failure from FP_CREATE_PROCEDURE_ERROR");

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private static Long getDDLJobId(Connection connection) throws SQLException {
        long jobId = -1L;

        String sql = "show ddl";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            jobId = rs.getLong("JOB_ID");
        }
        rs.close();
        return jobId;
    }

    private static boolean checkDDLPaused(Connection connection, Long jobId) throws SQLException {
        String sql = "show ddl " + jobId;
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            String ddlState = rs.getString("STATE");
            rs.close();
            return ddlState.equalsIgnoreCase("PAUSED");
        }
        return false;
    }

    public boolean usingNewPartDb() {
        return true;
    }
}

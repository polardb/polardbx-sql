package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CreateTableWithGsiTest extends DDLBaseNewDBTestCase {

    @Test
    @Ignore
    public void testCreateTableWithGsiRollback() throws SQLException {
        String hint = "/*+TDDL:cmd_extra(SIM_CDC_FAILED=true)*/";
        String dropSql = "drop table if exists t1";
        String createTableSql =
            "create table t1(a int, b int, c int, global index gsi_t1(a) dbpartition by hash(a)) dbpartition by hash(a)";

        JdbcUtil.executeSuccess(tddlConnection, dropSql);
        JdbcUtil.executeUpdateFailed(tddlConnection, hint + createTableSql, "");
        Long jobId = getDDLJobId(tddlConnection);
        JdbcUtil.executeSuccess(tddlConnection, "rollback ddl " + jobId);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
    }

    @Test
    @Ignore
    public void testCreateTableRollback() throws SQLException {
        String hint = "/*+TDDL:cmd_extra(SIM_CDC_FAILED=true)*/";
        String dropSql = "drop table if exists t2";
        String createTableSql = "create table t2(a int, b int, c int)  dbpartition by hash(a)";

        JdbcUtil.executeSuccess(tddlConnection, dropSql);
        JdbcUtil.executeUpdateFailed(tddlConnection, hint + createTableSql, "");
        Long jobId = getDDLJobId(tddlConnection);
        JdbcUtil.executeSuccess(tddlConnection, "rollback ddl " + jobId);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
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
}

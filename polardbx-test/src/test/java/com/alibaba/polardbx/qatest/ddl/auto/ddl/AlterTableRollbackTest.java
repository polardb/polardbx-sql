package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AlterTableRollbackTest extends DDLBaseNewDBTestCase {

    private String tableName = "wumu_test";
    private static final String createOption = " if not exists ";

//    @Test
//    public void testAlterTableModifyColumnRollbackFailed() throws SQLException {
//        String mytable = tableName;
//        dropTableIfExists(mytable);
//        String sql =
//            String.format("create table " + createOption + " %s(a int,b char) DEFAULT CHARSET = utf8mb4", mytable);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
//
//        String hint =
//            "/*+TDDL:cmd_extra(ENABLE_DRDS_MULTI_PHASE_DDL=true,FP_PAUSE_AFTER_DDL_TASK_EXECUTION='FinishTwoPhaseDdlTask')*/";
//        sql = hint + String.format("alter table %s modify column b int", mytable);
//        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "The DDL job has been cancelled or interrupted");
//
//        Long jobId = getDDLJobId(tddlConnection);
//        JdbcUtil.executeUpdateFailed(tddlConnection, "rollback ddl " + jobId, "Cancel/rollback is not supported");
//
//        JdbcUtil.executeSuccess(tddlConnection, "continue ddl " + jobId);
//    }

    @Test
    public void testAlterTableModifyColumnRollbackFailedLegacy() throws SQLException {
        String mytable = tableName;
        dropTableIfExists(mytable);
        String sql =
            String.format("create table " + createOption + " %s(a int,b char) DEFAULT CHARSET = utf8mb4", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String hint =
            "/*+TDDL:cmd_extra(ENABLE_DRDS_MULTI_PHASE_DDL=false,FP_PAUSE_AFTER_DDL_TASK_EXECUTION='AlterTablePhyDdlTask')*/";
        sql = hint + String.format("alter table %s modify column b int", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "The DDL job has been cancelled or interrupted");

        Long jobId = getDDLJobId(tddlConnection);
        JdbcUtil.executeUpdateFailed(tddlConnection, "rollback ddl " + jobId, "Cancel/rollback is not supported");

        JdbcUtil.executeSuccess(tddlConnection, "continue ddl " + jobId);
    }

//    @Test
//    public void testAlterTableDropColumnRollbackFailed() throws SQLException {
//        String mytable = tableName;
//        dropTableIfExists(mytable);
//        String sql =
//            String.format("create table " + createOption + " %s(a int,b char) DEFAULT CHARSET = utf8mb4", mytable);
//        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
//
//        String hint =
//            "/*+TDDL:cmd_extra(ENABLE_DRDS_MULTI_PHASE_DDL=true,FP_PAUSE_AFTER_DDL_TASK_EXECUTION='FinishTwoPhaseDdlTask')*/";
//        sql = hint + String.format("alter table %s drop column b", mytable);
//        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "The DDL job has been cancelled or interrupted");
//
//        Long jobId = getDDLJobId(tddlConnection);
//        JdbcUtil.executeUpdateFailed(tddlConnection, "rollback ddl " + jobId, "Cancel/rollback is not supported");
//
//        JdbcUtil.executeSuccess(tddlConnection, "continue ddl " + jobId);
//    }

    @Test
    public void testAlterTableDropColumnRollbackFailedLegacy() throws SQLException {
        String mytable = tableName;
        dropTableIfExists(mytable);
        String sql =
            String.format("create table " + createOption + " %s(a int,b char) DEFAULT CHARSET = utf8mb4", mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String hint =
            "/*+TDDL:cmd_extra(ENABLE_DRDS_MULTI_PHASE_DDL=false,FP_PAUSE_AFTER_DDL_TASK_EXECUTION='AlterTablePhyDdlTask')*/";
        sql = hint + String.format("alter table %s drop column b", mytable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "The DDL job has been cancelled or interrupted");

        Long jobId = getDDLJobId(tddlConnection);
        JdbcUtil.executeUpdateFailed(tddlConnection, "rollback ddl " + jobId, "Cancel/rollback is not supported");

        JdbcUtil.executeSuccess(tddlConnection, "continue ddl " + jobId);
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

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
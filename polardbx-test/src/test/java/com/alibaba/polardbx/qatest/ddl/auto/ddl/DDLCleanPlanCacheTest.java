package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DDLCleanPlanCacheTest extends DDLBaseNewDBTestCase {

    /**
     * 错误注入需要重新设计，目前只能在leader节点执行，先 ignore
     */
    @Ignore
    @Test
    public void testDDLCLeanPlanCacheTest() throws SQLException {
        String sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t1(a varchar(100), b int) partition by key(a) partitions 3";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 =
            "alter table t1 add clustered index gsi1(a) partition by key(a) partitions 5";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "set @FP_UPDATE_TABLES_VERSION_ERROR = \"true\"";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        try {
            sql1 =
                "/*+TDDL:cmd_extra(FP_UPDATE_TABLES_VERSION_ERROR=true)*/alter table t1 add index idx_a(a,b)";
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

            sql1 = "select count(1) from t1";
            JdbcUtil.executeSuccess(tddlConnection, sql1);

            Assert.assertTrue(getPlanCacheNum(tddlConnection, getDdlSchema(), "t1") == 1L);

            sql1 = "set @FP_UPDATE_TABLES_VERSION_ERROR = NULL";
            JdbcUtil.executeSuccess(tddlConnection, sql1);

            Long jobId = getDDLJobId(tddlConnection);
            sql1 = "continue ddl " + jobId;
            JdbcUtil.executeSuccess(tddlConnection, sql1);

            Assert.assertTrue(getPlanCacheNum(tddlConnection, getDdlSchema(), "t1") == 0L);
        } finally {
            sql1 = "set @FP_UPDATE_TABLES_VERSION_ERROR = NULL";
            JdbcUtil.executeSuccess(tddlConnection, sql1);
        }
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

    private static Long getPlanCacheNum(Connection connection, String schemaName, String tableName)
        throws SQLException {
        String sql = String.format(
            "select count(1) from information_schema.plan_cache where schema_name = '%s' and table_names = '%s'",
            schemaName, tableName);
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            Long num = rs.getLong(1);
            rs.close();
            return num;
        }
        return 0L;
    }

    public boolean usingNewPartDb() {
        return true;
    }
}

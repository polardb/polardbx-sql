package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class CreateDatabaseTest extends DDLBaseNewDBTestCase {
    @Test
    public void testCreateDatabaseFailed() {
        String sql = "create database polardbx";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = "create database metadb";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = "create database mysql";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = "create database performance_schema";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = "create database information_schema";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }
}

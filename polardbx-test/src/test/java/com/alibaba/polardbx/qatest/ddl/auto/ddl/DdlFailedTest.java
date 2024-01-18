package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

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

    public boolean usingNewPartDb() {
        return true;
    }
}

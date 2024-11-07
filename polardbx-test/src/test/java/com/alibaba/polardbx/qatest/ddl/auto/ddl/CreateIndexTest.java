package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class CreateIndexTest extends DDLBaseNewDBTestCase {

    @Test
    public void testCreateIndex() {
        String sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t1(a int, b varchar(100))";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "alter table t1 add global index abc123(b) partition by key(b)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "alter table t1 add global index abc123(b) partition by key(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "alter table t1 add global index Abc123(b) partition by key(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "alter table t1 add index abc123(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "alter table t1 add index Abc123(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "create global index abc123(b) on t1(b) partition by key(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "create global index Abc123(b) on t1(b) partition by key(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "create index abc123(b) on t1(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "create index Abc123(b) on t1(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testCreateIndex2() {
        String sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t1(a int, b varchar(100)) partition by key(a)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "alter table t1 add index abc123(b)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "alter table t1 add global index abc123(b) partition by key(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "alter table t1 add global index Abc123(b) partition by key(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "alter table t1 add index abc123(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "alter table t1 add index Abc123(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "create global index abc123(b) on t1(b) partition by key(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "create global index Abc123(b) on t1(b) partition by key(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "create index abc123(b) on t1(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "create index Abc123(b) on t1(b)";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "");

        sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}


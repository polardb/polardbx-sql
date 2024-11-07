package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class InterruptAlterTableRollbackTest extends DDLBaseNewDBTestCase {

    @Test
    public void testAlterTableInstanceConcurrent() {
        String sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t1(a varchar(10)) partition by key(a) partitions 7";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 =
            "/*+TDDL:cmd_extra(FP_PHYSICAL_DDL_INTERRUPTED=true,ENABLE_DRDS_MULTI_PHASE_DDL=false)*/ alter table t1 add column b int";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "has been interrupted");

        sql1 = "alter table t1 add column b int";
        JdbcUtil.executeSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAlterTableGroupConcurrent() {
        String sql1 = "drop table if exists t2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t2(a varchar(10)) partition by key(a) partitions 6";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 =
            "/*+TDDL:cmd_extra(MERGE_CONCURRENT=true,ENABLE_DRDS_MULTI_PHASE_DDL=false,FP_PHYSICAL_DDL_INTERRUPTED=true)*/ alter table t2 add column b int";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "has been interrupted");

        sql1 = "/*+TDDL:cmd_extra(MERGE_CONCURRENT=true)*/alter table t2 add column b int";
        JdbcUtil.executeSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAlterTableConcurrent() {
        String sql1 = "drop table if exists t3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t3(a varchar(10)) partition by key(a) partitions 8";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 =
            "/*+TDDL:cmd_extra(MERGE_CONCURRENT=true,MERGE_DDL_CONCURRENT=true,ENABLE_DRDS_MULTI_PHASE_DDL=false,FP_PHYSICAL_DDL_INTERRUPTED=true)*/ alter table t3 add column b int";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "has been interrupted");

        sql1 = "/*+TDDL:cmd_extra(MERGE_CONCURRENT=true,MERGE_DDL_CONCURRENT=true)*/alter table t3 add column b int";
        JdbcUtil.executeSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAlterTableSequential() {
        String sql1 = "drop table if exists t4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t4(a varchar(10)) partition by key(a) partitions 5";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 =
            "/*+TDDL:cmd_extra(SEQUENTIAL_CONCURRENT_POLICY=true,ENABLE_DRDS_MULTI_PHASE_DDL=false,FP_PHYSICAL_DDL_INTERRUPTED=true)*/ alter table t4 add column b int";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql1, "has been interrupted");

        sql1 = "/*+TDDL:cmd_extra(SEQUENTIAL_CONCURRENT_POLICY=true)*/alter table t4 add column b int";
        JdbcUtil.executeSuccess(tddlConnection, sql1);
    }

    public boolean usingNewPartDb() {
        return true;
    }
}

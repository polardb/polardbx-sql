package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class RenameTableDDLTest extends DDLBaseNewDBTestCase {

    @Test
    public void testRenameTable() {
        String sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "drop table if exists t2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table t1(a varchar(100))";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "/*+TDDL:cmd_extra(ENABLE_RANDOM_PHY_TABLE_NAME=false)*/rename table t1 to t2";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "drop table t2";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "drop table if exists t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testRenamePhyTable() {
        String sql1 = "drop table if exists t3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "drop table if exists t4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "/*+TDDL:cmd_extra(ENABLE_RANDOM_PHY_TABLE_NAME=false)*/create table t3(a varchar(100))";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "rename table t3 to t4";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "drop table t4";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "drop table if exists t3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}

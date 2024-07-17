package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class RenameTablesDDLTest extends DDLBaseNewDBTestCase {

    @Test
    public void testRenameTableWithAB() {
        String sql = "drop table if exists table_A";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop table if exists table_B";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop table if exists table_tmp";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table table_A (a int) partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        for (int i = 0; i < 10; i++) {
            sql = "drop table if exists table_tmp";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "check table table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "create table table_B like table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "rename table table_A to table_tmp, table_B to table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            System.out.println("rename " + i);
        }
    }

    @Test
    public void testRenameTableWithAB2() {
        String sql = "drop table if exists table_A";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop table if exists table_B";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop table if exists table_tmp";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table table_A (a int) partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        for (int i = 0; i < 10; i++) {
            sql = "drop table if exists table_tmp";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "check table table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "create table table_B like table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "rename table table_A to table_tmp";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "rename table table_B to table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            System.out.println("rename " + i);
        }
    }

    @Test
    public void testRenameTableWithABRename() {
        String sql = "drop table if exists table_A";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop table if exists table_B";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop table if exists table_tmp";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table table_A (a int) partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table table_tmp like table_A";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        for (int i = 0; i < 10; i++) {
            sql = "rename table table_tmp to table_B";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "check table table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "check table table_B";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "rename table table_A to table_tmp, table_B to table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            System.out.println("rename with rename " + i);
        }
    }

    @Test
    public void testRenameTableWithABRename2() {
        String sql = "drop table if exists table_A";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop table if exists table_B";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop table if exists table_tmp";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table table_A (a int) partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table table_tmp like table_A";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        for (int i = 0; i < 10; i++) {
            sql = "rename table table_tmp to table_B";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "check table table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "check table table_B";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "rename table table_A to table_tmp";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = "rename table table_B to table_A";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            System.out.println("rename with rename " + i);
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}

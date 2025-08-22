package com.alibaba.polardbx.qatest.ddl.auto.repartition;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RepartitionSingleOptimizeTest extends DDLBaseNewDBTestCase {

    @Test
    public void testRepartitionSingle() {
        String sql = "drop table if exists re_single_t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t1 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t1");

        sql = "alter table re_single_t1 partition by key(a) partitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t1");

        Assert.assertEquals(phyTableNames, phyTableNames2);
    }

    @Test
    public void testRepartitionSingle2() {
        String sql = "drop table if exists re_single_t2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t2 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t2");

        sql = "alter table re_single_t2 partition by key(a, b) partitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t2");

        Assert.assertEquals(phyTableNames, phyTableNames2);
    }

    @Test
    public void testRepartitionSingle3() {
        String sql = "drop table if exists re_single_t3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t3 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t3");

        sql = "alter table re_single_t3 partition by hash(a, b) partitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t3");

        Assert.assertNotEquals(phyTableNames, phyTableNames2);
    }


    @Test
    public void testRepartitionSingle4() {
        String sql = "drop table if exists re_single_t4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t4 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t4");

        sql = "alter table re_single_t4 partition by hash (a) partitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t4");

        Assert.assertEquals(phyTableNames, phyTableNames2);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}

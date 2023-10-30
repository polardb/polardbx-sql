package com.alibaba.polardbx.qatest.ddl.sharding.gen;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

public class GeneratedColumnSubstituteTest extends DDLBaseNewDBTestCase {
    @Before
    public void init() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global GEN_COL_SUBSTITUTION=TRUE");
    }

    @Test
    public void testPlanCache1() {
        String tableName = "gen_col_plan_cache_tbl_1";
        String createTable =
            String.format("create table %s (a int primary key, b bigint as (a+1) logical) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String insert = String.format("insert into %s(a) values (1),(2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String select = String.format("select * from %s where a+1=2", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, select);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("1"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("2"));

        String explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`b` = ?)") && explain.contains("HitCache:true"));

        String select1 = String.format("select * from %s where a+2=4", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, select1);
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("2"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("3"));

        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`a` + ?)") && explain.contains("HitCache:false"));
        explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`b` = ?)") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`a` + ?)") && explain.contains("HitCache:false"));
    }

    @Test
    public void testPlanCache2() {
        String tableName = "gen_col_plan_cache_tbl_2";
        String createTable =
            String.format("create table %s (a int primary key, b bigint as (a+1) logical) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String insert = String.format("insert into %s(a) values (1),(2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String select = String.format("select * from %s where a+2=4", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, select);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("2"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("3"));

        String explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`a` + ?)") && explain.contains("HitCache:true"));

        String select1 = String.format("select * from %s where a+1=2", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, select1);
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("1"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("2"));

        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`a` + ?)") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`a` + ?)") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`a` + ?)") && explain.contains("HitCache:true"));
    }

    @Test
    public void testPlanCache3() {
        String tableName = "gen_col_plan_cache_tbl_3";
        String createTable =
            String.format("create table %s (a int primary key, b bigint as (a+1) logical) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String insert = String.format("insert into %s(a) values (1),(2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String select = String.format("select * from %s where (a+1)+(a+2)=5", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, select);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("1"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("2"));

        String explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + (`a` + ?)") && explain.contains("HitCache:true"));

        String select1 = String.format("select * from %s where (a+2)+(a+3)=9", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, select1);
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("2"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("3"));

        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`a` + ?) + (`a` + ?)") && explain.contains("HitCache:false"));
        explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`b` + (`a` + ?))") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`a` + ?) + (`a` + ?)") && explain.contains("HitCache:false"));
    }

    @Test
    public void testPlanCache4() {
        String tableName = "gen_col_plan_cache_tbl_4";
        String createTable =
            String.format("create table %s (a int primary key, b bigint as (a+1) logical) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String insert = String.format("insert into %s(a) values (1),(2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String select = String.format("select * from %s where a+1=2", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, select);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("1"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("2"));

        String explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`b` = ?)") && explain.contains("HitCache:true"));

        String select1 = String.format("select * from %s where a+1=3", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, select1);
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("2"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("3"));

        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`b` = ?)") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`b` = ?)") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("(`b` = ?)") && explain.contains("HitCache:true"));
    }

    @Test
    public void testPlanCache5() {
        String tableName = "gen_col_plan_cache_tbl_5";
        String createTable =
            String.format("create table %s (a int primary key, b bigint as (a+1) logical) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String insert = String.format("insert into %s(a) values (1),(2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String select = String.format("select * from %s where (a+1)+(a+1)=4", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, select);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("1"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("2"));

        String explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + `b`") && explain.contains("HitCache:true"));

        String select1 = String.format("select * from %s where (a+1)+(a+2)=7", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, select1);
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("2"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("3"));

        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + (`a` + ?)") && explain.contains("HitCache:false"));
        explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + `b`") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + (`a` + ?)") && explain.contains("HitCache:false"));
    }

    @Test
    public void testPlanCache6() {
        String tableName = "gen_col_plan_cache_tbl_6";
        String createTable =
            String.format("create table %s (a int primary key, b bigint as (a+1) logical) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String insert = String.format("insert into %s(a) values (1),(2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String select = String.format("select * from %s where (a+1)+(a+2)=5", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, select);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("1"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("2"));

        String explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + (`a` + ?)") && explain.contains("HitCache:true"));

        String select1 = String.format("select * from %s where (a+1)+(a+1)=6", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, select1);
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(objects.size() == 1);
        Assert.assertTrue(objects.get(0).get(0).toString().equals("2"));
        Assert.assertTrue(objects.get(0).get(1).toString().equals("3"));

        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + (`a` + ?)") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + (`a` + ?)") && explain.contains("HitCache:true"));
        explain = DataValidator.getExplainAllResult("explain " + select1, null, tddlConnection);
        Assert.assertTrue(explain.contains("`b` + (`a` + ?)") && explain.contains("HitCache:true"));
    }
}

package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class AnalyzeTableTest extends DDLBaseNewDBTestCase {

    private final String testTableName = "analyze_test";
    private final String gsiPrimaryTableName = "analyze_gsi_test";

    private String tableName1 = schemaPrefix + testTableName + "_1";
    private String tableName2 = schemaPrefix + testTableName + "_2";

    public AnalyzeTableTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}});
    }

    @Before
    public void before() throws SQLException {
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
    }

    @Test
    public void testAnalyzeTable() {
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql =
            String.format("create global index %s on %s(id) partition by hash(id) partitions 3", gsiPrimaryTableName,
                tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAnalyzeTable2() {
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql =
            String.format("create global index %s on %s(id) partition by hash(id) partitions 3", gsiPrimaryTableName,
                tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "/*+TDDL:cmd_extra(ENABLE_HLL=false)*/analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAnalyzeTableWithBacktick() {
        String sql1 = "create table " + tableName1 + "(id int, `a` int) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, `a` int) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql =
            String.format("create global index %s on %s(id) partition by hash(id) partitions 3", gsiPrimaryTableName,
                tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAnalyzeTableWithBacktick2() {
        String sql1 = "create table " + tableName1 + "(id int, ``a`` int) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, ``a`` int) partition by hash(id) partitions 3";
        JdbcUtil.executeFailed(tddlConnection, sql1, "syntax error, error in :'int, ``a``");
        JdbcUtil.executeFailed(tddlConnection, sql2, "syntax error, error in :'int, ``a``");
    }

    @Test
    public void testAnalyzeTableWithBacktick3() {
        String sql1 = "create table " + tableName1 + "(id int, ```a``` int) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, ```a``` int) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql =
            String.format("create global index %s on %s(id) partition by hash(id) partitions 3", gsiPrimaryTableName,
                tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAnalyzeTableWithBacktick4() {
        String sql1 = "create table " + tableName1 + "(id int, ````a```` int) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, ````a```` int) partition by hash(id) partitions 3";
        JdbcUtil.executeFailed(tddlConnection, sql1, "syntax error, error in :'int, ````a````");
        JdbcUtil.executeFailed(tddlConnection, sql2, "syntax error, error in :'int, ````a````");
    }

    @Test
    public void testAnalyzeTableWithBacktick5() {
        String sql1 = "create table " + tableName1 + "(id int, `````a````` int) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, `````a````` int) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql =
            String.format("create global index %s on %s(id) partition by hash(id) partitions 3", gsiPrimaryTableName,
                tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testParallelAnalyzeTable() {
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) partition by hash(id) partitions 4";
        String sql2 = "create table " + tableName2 + "(id int, name varchar(20)) partition by hash(id) partitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql =
            String.format("create global index %s on %s(id) partition by hash(id) partitions 3", gsiPrimaryTableName,
                tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "/*+TDDL: HLL_PARALLELISM=8*/analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAnalyzeTableSkipPhyTable() throws SQLException {
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        String sql2 = "create table " + tableName2 + "(id int, name varchar(20)) partition by hash(id) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);

        String checkSql =
            "select count(*) from information_schema.module_event where event like '%SKIP_PHYSICAL_ANALYZE%'";
        ResultSet rs = JdbcUtil.executeQuery(checkSql, tddlConnection);
        rs.next();
        int count1 = rs.getInt(1);
        rs.close();

        sql1 = "/*+TDDL:cmd_extra(SKIP_PHYSICAL_ANALYZE=true)*/analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        rs = JdbcUtil.executeQuery(checkSql, tddlConnection);
        rs.next();
        int count2 = rs.getInt(1);
        assert count2 > count1;
        rs.close();
    }
}

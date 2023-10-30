package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

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
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) dbpartition by hash(id)";
        String sql2 = "create table " + tableName2 + "(id int, name varchar(20)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql =
            String.format("create global index %s on %s(id) dbpartition by hash(id)", gsiPrimaryTableName, tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @Test
    public void testAnalyzeTable2() {
        String sql1 = "create table " + tableName1 + "(id int, name varchar(20)) dbpartition by hash(id)";
        String sql2 = "create table " + tableName2 + "(id int, name varchar(20)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql2);
        String gsiSql =
            String.format("create global index %s on %s(id) dbpartition by hash(id)", gsiPrimaryTableName, tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

        sql1 = "/*+TDDL:cmd_extra(ENABLE_HLL=false)*/analyze table " + tableName1 + "," + tableName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }
}

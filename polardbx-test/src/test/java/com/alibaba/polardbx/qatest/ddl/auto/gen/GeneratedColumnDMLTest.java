package com.alibaba.polardbx.qatest.ddl.auto.gen;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddlAssertErrorAtomic;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class GeneratedColumnDMLTest extends DDLBaseNewDBTestCase {
    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private final Boolean writeOnly;
    private final Boolean strictMode;

    private String tddlSqlMode;
    private String mysqlSqlMode;

    @Parameterized.Parameters(name = "{index}:writeOnly={0},strictMode={1}")
    public static List<Object[]> prepareDate() {
        return ImmutableList.of(new Object[] {Boolean.TRUE, Boolean.TRUE}, new Object[] {Boolean.FALSE, Boolean.FALSE});
    }

    public GeneratedColumnDMLTest(Boolean writeOnly, Boolean strictMode) {
        this.writeOnly = writeOnly;
        this.strictMode = strictMode;
    }

    private static final String[] COLUMNS_NAMES = {"c", "d"};

    private static final String[] COLUMN_DEFS =
        {"c int as (a-id) logical", "d int as (a+id*2) logical, c int as (id+id+a) logical"};

    private static final String[] PART_DEFS =
        {"partition by hash(a) partitions 7", "partition by hash(c) partitions 7", "single", "broadcast", ""};

    private static final String SOURCE_TABLE_NAME = "gen_col_test_src";
    private static final String[][] INSERT_PARAMS = new String[][] {
        new String[] {"(id,a)", "values (0,1)", "(id,a)", "values (0,1)"},
        new String[] {"(a,id)", "values (2,1)", "(id,a)", "values (1,2)"},
        new String[] {"(id,a)", "values (2,1+2)", "(id,a)", "values (2,3)"},
        new String[] {"(id)", "values (3)", "(id,a)", "values (3,1)"},
        new String[] {"(id,a)", "values (4,5),(5,6)", "(id,a)", "values (4,5),(5,6)"},
        new String[] {"(a,id)", "values (7,6),(8,7)", "(id,a)", "values (6,7),(7,8)"},
        new String[] {"(id,a)", "values (8,9),(9,1+9)", "(id,a)", "values (8,9),(9,10)"},
        new String[] {"(id)", "values (10),(11)", "(id,a)", "values (10,1),(11,1)"},
        new String[] {"(id,a)", "select 12,13", "(id,a)", "values (12,13)"},
        new String[] {"(id,a)", "select 13,14 union select 14,14+1", "(id,a)", "values (13,14),(14,15)"},
        new String[] {"(id)", "select 14+1", "(id,a)", "values (15,1)"},
        new String[] {
            "(id,a)", String.format("select id,a from %s where id=100", SOURCE_TABLE_NAME), "(id,a)",
            "values (100,101)"},
        new String[] {
            "(id,a)", String.format("select id,a from %s where id>100 order by id", SOURCE_TABLE_NAME), "(id,a)",
            "values (101,102),(102,103)"},
        new String[] {
            "(id,a)", String.format("select a,a from %s where id=100", SOURCE_TABLE_NAME), "(id,a)",
            "values (101,101)"},
        new String[] {
            "(id,a)", String.format("select a,id from %s where id>100 order by id", SOURCE_TABLE_NAME), "(id,a)",
            "values (102,101),(103,102)"},

        new String[] {
            "(id,a)", "values (0,1),(1,2),(2,3),(100,100),(101,103)", "(id,a)",
            "values (0,1),(1,2),(2,3),(100,100),(101,103)"},
        new String[] {"(id)", "values (1)", "(id,a)", "values (1,1)"},
        new String[] {"(id,a)", "values (3,0+2)", "(id,a)", "values (3,2)"},
        new String[] {"(id,a)", "values (1,2),(2,3)", "(id,a)", "values (1,2),(2,3)"},
        new String[] {
            "(id,a)", String.format("select id,a from %s where id=100", SOURCE_TABLE_NAME), "(id,a)",
            "values (100,101)"},
        new String[] {
            "(id,a)", String.format("select id,a from %s where id>100 order by id", SOURCE_TABLE_NAME), "(id,a)",
            "values (101,102),(102,103)"}
    };

    private static final String[][] UPSERT_PARAMS = new String[][] {
        new String[] {"(id,a)", "values (1,2)", "(id,a)", "values (1,2)"},
        new String[] {"(id,a)", "values (100,100),(101,103)", "(id,a)", "values (100,100),(101,103)"},
        new String[] {
            "(id,a)", "values (1,2) on duplicate key update b=b+1", "(id,a)",
            "values (1,2) on duplicate key update b=b+1"},
        new String[] {
            "(id,a)", "values (1,5),(2,3) on duplicate key update a=id+2", "(id,a)",
            "values (1,5),(2,3) on duplicate key update a=id+2"},
        new String[] {
            "(id,a)", "values (1,5),(2,3) on duplicate key update a=values(a)", "(id,a)",
            "values (1,5),(2,3) on duplicate key update a=values(a)"},
        new String[] {
            "(id,a)", "values (1,5),(2,3) on duplicate key update id=id+10", "(id,a)",
            "values (1,5),(2,3) on duplicate key update id=id+10"},
        new String[] {
            "(id,a)", "values (200,300) on duplicate key update id=id+10,a=a", "(id,a)",
            "values (200,300) on duplicate key update id=id+10,a=a"},
        new String[] {
            "(id,a)", String.format("select id,a from %s where id=100 order by id ", SOURCE_TABLE_NAME)
            + "on duplicate key update id=id+10", "(id,a)", "values (100,101) on duplicate key update id=id+10"},
        new String[] {
            "(id,a)", String.format("select id,a from %s where id>100 order by id ", SOURCE_TABLE_NAME)
            + "on duplicate key update id=id+10", "(id,a)",
            "values (101,102),(102,103) on duplicate key update id=id+10"}
    };

    private static final String[][] UPDATE_PARAMS = new String[][] {
        new String[] {"a=1,id=10,a=id+1", "where id=0"},
        new String[] {"a=1", "where id=1"},
        new String[] {"a=a+1", "where id=1"},
        new String[] {"a=id+2", "where id=1"},
        new String[] {"id=100,a=200", "where id=1"},
        new String[] {"id=a+10", "where id=2"},
        new String[] {"id=1", "where a=20"}
    };

    @Before
    public void initSelectTable() {
        // Create source table for insert select
        dropTableIfExists(SOURCE_TABLE_NAME);
        String createSourceTableSql =
            String.format(
                "create table if not exists %s (`pk` bigint(20) NOT NULL AUTO_INCREMENT PRIMARY KEY, id int, a int)",
                SOURCE_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSourceTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + SOURCE_TABLE_NAME + " (id, a) values(100,101),(101,102),(102,103)");

        tddlSqlMode = JdbcUtil.getSqlMode(tddlConnection);
        mysqlSqlMode = JdbcUtil.getSqlMode(mysqlConnection);

        if (tddlSqlMode == null) {
            tddlSqlMode = "";
        }
        if (mysqlSqlMode == null) {
            mysqlSqlMode = "";
        }

        if (strictMode) {
            setSqlMode("STRICT_TRANS_TABLES", tddlConnection);
            setSqlMode("STRICT_TRANS_TABLES", mysqlConnection);
        } else {
            setSqlMode("", tddlConnection);
            setSqlMode("", mysqlConnection);
        }
    }

    @After
    public void resetSqlMode() {
        setSqlMode(tddlSqlMode, tddlConnection);
        setSqlMode(mysqlSqlMode, mysqlConnection);
    }

    @Test
    public void testGeneratedColumnInsert() throws SQLException {
        String tableName = "gen_col_insert_test_tbl";
        for (String columnDef : COLUMN_DEFS) {
            for (String partDef : PART_DEFS) {
                boolean withGsi = partDef.contains("partition");
                testGeneratedColumnInsertInternal("insert into", tableName, columnDef, partDef, withGsi, false,
                    INSERT_PARAMS);
                testGeneratedColumnInsertInternal("insert into", tableName, columnDef, partDef, withGsi, true,
                    INSERT_PARAMS);
            }
        }
    }

    @Test
    public void testGeneratedColumnReplace() throws SQLException {
        String tableName = "gen_col_replace_test_tbl";
        for (String columnDef : COLUMN_DEFS) {
            for (String partDef : PART_DEFS) {
                boolean withGsi = partDef.contains("partition");
                testGeneratedColumnInsertInternal("replace into", tableName, columnDef, partDef, withGsi, false,
                    INSERT_PARAMS);
                testGeneratedColumnInsertInternal("replace into", tableName, columnDef, partDef, withGsi, true,
                    INSERT_PARAMS);
            }
        }
    }

    @Test
    public void testGeneratedColumnInsertIgnore() throws SQLException {
        String tableName = "gen_col_insert_ignore_test_tbl";
        for (String columnDef : COLUMN_DEFS) {
            for (String partDef : PART_DEFS) {
                boolean withGsi = partDef.contains("partition");
                testGeneratedColumnInsertInternal("insert ignore into", tableName, columnDef, partDef, withGsi, false,
                    INSERT_PARAMS);
                testGeneratedColumnInsertInternal("insert ignore into", tableName, columnDef, partDef, withGsi, true,
                    INSERT_PARAMS);
            }
        }
    }

    @Test
    public void testGeneratedColumnUpsert() throws SQLException {
        String tableName = "gen_col_upsert_test_tbl";
        for (String columnDef : COLUMN_DEFS) {
            for (String partDef : PART_DEFS) {
                boolean withGsi = partDef.contains("partition");
                testGeneratedColumnInsertInternal("insert into", tableName, columnDef, partDef, withGsi, false,
                    UPSERT_PARAMS);
                testGeneratedColumnInsertInternal("insert into", tableName, columnDef, partDef, withGsi, true,
                    UPSERT_PARAMS);
            }
        }
    }

    private void testGeneratedColumnInsertInternal(String op, String tableName, String colDef, String partDef,
                                                   boolean withGsi, boolean withUk, String[][] params)
        throws SQLException {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        if (!createTestTable(tableName, colDef, partDef, withUk, params)) {
            return;
        }
        String[] dml = new String[params.length];
        String[] mysqlDml = new String[params.length];

        for (int i = 0; i < params.length; i++) {
            dml[i] = String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
            mysqlDml[i] = String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
        }

        testGeneratedColumnDmlInternal(tableName, withGsi, false, dml, mysqlDml);
    }

    @Test
    public void testGeneratedColumnUpdate() throws SQLException {
        String tableName = "gen_col_update_test_tbl";
        for (String columnDef : COLUMN_DEFS) {
            for (String partDef : PART_DEFS) {
                boolean withGsi = partDef.contains("partition");
                testGeneratedColumnUpdateInternal(tableName, columnDef, partDef, withGsi, false, UPDATE_PARAMS);
                testGeneratedColumnUpdateInternal(tableName, columnDef, partDef, withGsi, true, UPDATE_PARAMS);
            }
        }
    }

    private void testGeneratedColumnUpdateInternal(String tableName, String colDef, String partDef, boolean withGsi,
                                                   boolean withUk, String[][] params) throws SQLException {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        if (!createTestTable(tableName, colDef, partDef, withUk, params)) {
            return;
        }
        String[] dml = new String[params.length];
        String[] mysqlDml = new String[params.length];

        for (int i = 0; i < params.length; i++) {
            dml[i] = String.format("update %s set %s %s", tableName, params[i][0], params[i][1]);
            mysqlDml[i] = String.format("update %s set %s %s", tableName, params[i][0], params[i][1]);
        }

        testGeneratedColumnDmlInternal(tableName, withGsi, true, dml, mysqlDml);
    }

    private void testGeneratedColumnDmlInternal(String tableName, boolean withGsi, boolean withData, String[] dml,
                                                String[] mysqlDml) throws SQLException {
        if (writeOnly) {
            updateColumnStatus(tddlDatabase1, tableName, 2);
        }

        String insert =
            String.format("insert ignore into %s (id,a) values (1,2),(2,3),(3,4),(19,20),(0,0)", tableName);
        if (withData) {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        }

        List<String> columns = JdbcUtil.getTableColumns(tddlConnection, tableName);
        String selectColumns =
            columns.stream().filter(name -> !"pk".equalsIgnoreCase(name)).collect(Collectors.joining(","));

        for (int i = 0; i < dml.length; i++) {
            // System.out.println(dml[i]);
            // System.out.println(mysqlDml[i]);
            executeOnMysqlAndTddlAssertErrorAtomic(mysqlConnection, tddlConnection, mysqlDml[i], dml[i], null, false);
            if (writeOnly) {
                selectContentSameAssert("select id,a from " + tableName, null, mysqlConnection, tddlConnection);
            } else {
                selectContentSameAssert("select " + selectColumns + " from " + tableName, null, mysqlConnection,
                    tddlConnection);
            }
        }

        if (writeOnly) {
            updateColumnStatus(tddlDatabase1, tableName, 1);
            selectContentSameAssert("select " + selectColumns + " from " + tableName, null, mysqlConnection,
                tddlConnection);
        }

        if (withGsi) {
            String gsiName1 = tableName + "_gsi1";
            String gsiName2 = tableName + "_gsi2";
            String gsiName3 = tableName + "_gsi3";
            dropTableIfExists(gsiName1);
            dropTableIfExists(gsiName2);
            dropTableIfExists(gsiName3);

            String createGsi1 =
                String.format("create global index %s on %s(c) partition by hash(c) partitions 7", gsiName1, tableName);
            String createGsi2 =
                String.format("create global clustered index %s on %s(a) partition by hash(a) partitions 7", gsiName2,
                    tableName);
            String createGsi3 =
                String.format("create global index %s on %s(id) covering(a) partition by hash(id) partitions 7",
                    gsiName3,
                    tableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi3);

            if (writeOnly) {
                updateColumnStatus(tddlDatabase1, getRealGsiName(tddlConnection, tableName, gsiName2), 2);
            }

            String deleteAll = "truncate " + tableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteAll, deleteAll, null, false);
            if (withData) {
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
            }

            for (int i = 0; i < dml.length; i++) {
                executeOnMysqlAndTddlAssertErrorAtomic(mysqlConnection, tddlConnection, mysqlDml[i], dml[i], null,
                    false);
                selectContentSameAssert("select " + selectColumns + " from " + tableName, null, mysqlConnection,
                    tddlConnection);
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName3));
            }

            if (writeOnly) {
                updateColumnStatus(tddlDatabase1, getRealGsiName(tddlConnection, tableName, gsiName2), 1);
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
            }
        }
    }

    private boolean createTestTable(String tableName, String colDef, String partDef, boolean withUk,
                                    String[][] params) {
        // Print args
        System.out.println(tableName + " | " + colDef + " | " + partDef + " | " + withUk);
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        if (withUk) {
            if (partDef.contains("hash(a)") || partDef.equals("")) {
                return false;
            }
            colDef = colDef + " unique key";
        }

        if (writeOnly) {
            if (partDef.contains("hash(c)")) {
                return false;
            }
        }

        String createTable =
            String.format(
                "create table %s (`pk` bigint(20) NOT NULL AUTO_INCREMENT PRIMARY KEY, id int, a int default 1, b int default 2, ts timestamp default '2022-10-10 12:00:00' on update current_timestamp(), %s) ",
                tableName, colDef);
        // UPSERT in broadcast table will not update timestamp properly, skip timestamp column for now
        if (partDef.contains("broadcast") && Arrays.stream(params)
            .anyMatch(param -> param[1].contains("on duplicate key update"))) {
            createTable =
                String.format(
                    "create table %s (`pk` bigint(20) NOT NULL AUTO_INCREMENT PRIMARY KEY, id int, a int default 1, b int default 2, %s) ",
                    tableName, colDef);
        }

        System.out.println(createTable + partDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable.replace("logical", ""));

        return true;
    }

    private void updateColumnStatus(String schemaName, String tableName, int status) {
        try {
            ResultSet rs = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
            ResultSetMetaData rsmd = rs.getMetaData();
            int beforeColumnCnt = rsmd.getColumnCount();

            for (String columnName : COLUMNS_NAMES) {
                String updateStatus = String.format(
                    "/*+TDDL:node('__META_DB__')*/ update columns set status=%d where table_schema='%s' and table_name='%s' and column_name='%s'",
                    status, schemaName, tableName, columnName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, updateStatus);
            }

            String updateTableVersion = String.format(
                "/*+TDDL:node('__META_DB__')*/ update tables set version=version+1 where table_schema='%s' and table_name='%s'",
                schemaName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, updateTableVersion);
            String refresh = String.format(
                "/*+TDDL:node('__META_DB__')*/ update config_listener set op_version=op_version+1 where data_id = 'polardbx.meta.table.%s.%s'",
                schemaName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, refresh);

            int cnt = 0;
            while (cnt < 60) {
                rs = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
                rsmd = rs.getMetaData();
                if (rsmd.getColumnCount() != beforeColumnCnt) {
                    return;
                }
                cnt++;
                if (cnt % 10 == 0) {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, updateTableVersion);
                }
                Thread.sleep(1000);
                JdbcUtil.executeUpdateSuccess(tddlConnection, refresh);
                System.out.println(
                    "wait after update " + schemaName + " " + tableName + " " + beforeColumnCnt + " " + cnt);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        Assert.fail("update column status failed");
    }

    @Test
    public void multiTableUpdateTest() {
        String tableName1 = "gen_col_multi_update_test_tbl_1";
        String tableName2 = "gen_col_multi_update_test_tbl_2";
        String tableName3 = "gen_col_multi_update_test_tbl_3";
        String createTmpl = "create table %s (a int primary key, b int, c int as (a+b) logical)";
        String createTmpl1 = "create table %s (a int primary key, b int)";
        String partDef = " partition by hash(a) partitions 7";

        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        dropTableIfExists(tableName3);
        dropTableIfExistsInMySql(tableName1);
        dropTableIfExistsInMySql(tableName2);
        dropTableIfExistsInMySql(tableName3);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTmpl, tableName1) + partDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTmpl, tableName2) + partDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTmpl1, tableName3) + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createTmpl, tableName1).replace("logical", ""));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createTmpl, tableName2).replace("logical", ""));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createTmpl1, tableName3));

        String[] updateTmpls = {
            "update {0} as t1,{1} as t2 set t1.b=10,t2.b=20 where t1.a=t2.b",
            "update {0} as t1,{1} as t2 set t1.b=10,t2.b=20 where t1.a=t2.a",
            "update {0} as t1,{1} as t2 set t1.b=10 where t1.a=t2.a",
            "update {0} as t1,{1} as t2 set t1.a=t2.a+10 where t1.a=t2.b",
            "update {0} as t1,(select * from {1}) as t2 set t1.b=10 where t1.a=t2.b",
            "update {0} as t1,(select * from {1}) as t2 set t1.b=10 where t1.a=t2.a",
            "update {0} as t1,(select * from {1}) as t2 set t1.b=10 where t1.a=t2.a",
            "update {0} as t1,(select * from {1}) as t2 set t1.a=t2.a+10 where t1.a=t2.b",
            "delete t1,t2 from {0} as t1,{1} as t2 where t1.a=t2.b",
            "delete t1,t2 from {0} as t1,{1} as t2 where t1.a=t2.a",
            "delete t1,t2 from {0} as t1,{1} as t2 where t1.a=t2.a",
            "delete t1,t2 from {0} as t1,{1} as t2 where t1.a=t2.b",
            "delete t1 from {0} as t1,(select * from {1}) as t2 where t1.a=t2.b",
            "delete t1 from {0} as t1,(select * from {1}) as t2 where t1.a=t2.a",
            "delete t1 from {0} as t1,(select * from {1}) as t2 where t1.a=t2.a",
            "delete t1 from {0} as t1,(select * from {1}) as t2 where t1.a=t2.b",};

        multiTableUpdateTestInternal(updateTmpls, tableName1, tableName2);
        multiTableUpdateTestInternal(updateTmpls, tableName1, tableName3);
    }

    private void multiTableUpdateTestInternal(String[] updateTmpls, String tableName1, String tableName2) {
        for (String updateTmpl : updateTmpls) {
            if (writeOnly) {
                updateColumnStatus(tddlDatabase1, tableName1, 2);
            }

            // Clear
            String deleteTmpl = "delete from %s";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(deleteTmpl, tableName1));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(deleteTmpl, tableName2));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(deleteTmpl, tableName1));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(deleteTmpl, tableName2));
            // Insert
            String insertTmpl = "insert into %s(a,b) values (1,2),(2,3),(3,4)";
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertTmpl, tableName1));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertTmpl, tableName2));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(insertTmpl, tableName1));
            JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(insertTmpl, tableName2));

            String update = MessageFormat.format(updateTmpl, tableName1, tableName2);
            // System.out.println(update);
            JdbcUtil.executeUpdateSuccess(tddlConnection, update);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, update);

            if (writeOnly) {
                updateColumnStatus(tddlDatabase1, tableName1, 1);
            }

            selectContentSameAssert("select * from " + tableName1, null, mysqlConnection, tddlConnection, true);
            selectContentSameAssert("select * from " + tableName2, null, mysqlConnection, tddlConnection, true);
        }
    }

    @Test
    public void refColOutRange() {
        String tableName = "gen_col_rf_range_test_tbl";
        String createTmpl = "create table %s (a int primary key, c int as (a+b) logical, b tinyint)";
        String partDef = " partition by hash(a)";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTmpl, tableName) + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createTmpl, tableName).replace("logical", ""));

        String[] dmls = new String[] {
            String.format("insert into %s(a,b) values (10,100)", tableName),
            String.format("insert into %s(a,b) values (10000,10000)", tableName),
            String.format("replace into %s(a,b) values (10,110)", tableName),
            String.format("replace into %s(a,b) values (10000,11000)", tableName),
            String.format("insert into %s(a,b) values (10,110) on duplicate key update b=120", tableName),
            String.format("insert into %s(a,b) values (10000,11000) on duplicate key update b=12000", tableName),
            String.format("update %s set b=10", tableName),
            String.format("update %s set b=1000", tableName),
            String.format("update %s set a=a+10,b=10", tableName),
            String.format("update %s set a=a+10,b=1000", tableName),
        };

        for (String dml : dmls) {
            executeOnMysqlAndTddlAssertErrorAtomic(mysqlConnection, tddlConnection, dml, dml, null, false);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection, true);
        }
    }
}

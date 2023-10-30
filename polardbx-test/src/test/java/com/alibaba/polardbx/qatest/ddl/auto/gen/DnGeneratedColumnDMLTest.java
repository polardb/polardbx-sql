package com.alibaba.polardbx.qatest.ddl.auto.gen;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddlAssertErrorAtomic;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class DnGeneratedColumnDMLTest extends DDLBaseNewDBTestCase {
    private static final String ENABLE_UNIQUE_KEY_ON_GEN_COL = "ENABLE_UNIQUE_KEY_ON_GEN_COL=TRUE";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    public DnGeneratedColumnDMLTest() {
    }

    private static final String[] COLUMNS_NAMES = {"c", "d"};

    private static final String[] COLUMN_DEFS =
        {"d int as (a+id*2), c int as (id+a)", "c int as (a-id)"};

    private static final String[] PART_DEFS =
        {"partition by hash(a)", "single", "broadcast"};

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
            "(id,a)", String.format("select * from %s where id=100", SOURCE_TABLE_NAME), "(id,a)", "values (100,101)"},
        new String[] {
            "(id,a)", String.format("select * from %s where id>100 order by id", SOURCE_TABLE_NAME), "(id,a)",
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
            "(id,a)", String.format("select * from %s where id=100", SOURCE_TABLE_NAME), "(id,a)", "values (100,101)"},
        new String[] {
            "(id,a)", String.format("select * from %s where id>100 order by id", SOURCE_TABLE_NAME), "(id,a)",
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
            "(id,a)", String.format("select * from %s where id=100 order by id ", SOURCE_TABLE_NAME)
            + "on duplicate key update id=id+10", "(id,a)", "values (100,101) on duplicate key update id=id+10"},
        new String[] {
            "(id,a)", String.format("select * from %s where id>100 order by id ", SOURCE_TABLE_NAME)
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
            String.format("create table if not exists %s (id int, a int)", SOURCE_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSourceTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + SOURCE_TABLE_NAME + " values(100,101),(101,102),(102,103)");
    }

    @After
    public void resetSqlMode() {
    }

    @Test
    public void testGeneratedColumnInsert() throws SQLException {
        String tableName = "gen_col_insert_test_tbl";
        for (String columnDef : COLUMN_DEFS) {
            for (String partDef : PART_DEFS) {
                boolean withGsi = partDef.contains("partition");
                testGeneratedColumnInsertInternal("insert into", tableName, columnDef, partDef, withGsi, true,
                    INSERT_PARAMS);
                testGeneratedColumnInsertInternal("insert into", tableName, columnDef, partDef, withGsi, false,
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
                testGeneratedColumnInsertInternal("replace into", tableName, columnDef, partDef, withGsi, true,
                    INSERT_PARAMS);
                testGeneratedColumnInsertInternal("replace into", tableName, columnDef, partDef, withGsi, false,
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
                testGeneratedColumnInsertInternal("insert ignore into", tableName, columnDef, partDef, withGsi, true,
                    INSERT_PARAMS);
                testGeneratedColumnInsertInternal("insert ignore into", tableName, columnDef, partDef, withGsi, false,
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
                testGeneratedColumnInsertInternal("insert into", tableName, columnDef, partDef, withGsi, true,
                    UPSERT_PARAMS);
                testGeneratedColumnInsertInternal("insert into", tableName, columnDef, partDef, withGsi, false,
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
        String insert =
            String.format("insert ignore into %s (id,a) values (1,2),(2,3),(3,4),(19,20),(0,0)", tableName);
        if (withData) {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        }

        for (int i = 0; i < dml.length; i++) {
            System.out.println(dml[i]);
            System.out.println(mysqlDml[i]);
            executeOnMysqlAndTddlAssertErrorAtomic(mysqlConnection, tddlConnection, mysqlDml[i], dml[i], null, false);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }

        if (withGsi) {
            String gsiName2 = tableName + "_gsi2";
            String gsiName3 = tableName + "_gsi3";
            dropTableIfExists(gsiName2);
            dropTableIfExists(gsiName3);

            String createGsi2 =
                String.format("create global clustered index %s on %s(a) partition by hash(a)", gsiName2, tableName);
            String createGsi3 =
                String.format("create global index %s on %s(id) covering(c) partition by hash(id)", gsiName3,
                    tableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi3);

            String deleteAll = "truncate " + tableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteAll, deleteAll, null, false);
            if (withData) {
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
            }

            for (int i = 0; i < dml.length; i++) {
                executeOnMysqlAndTddlAssertErrorAtomic(mysqlConnection, tddlConnection, mysqlDml[i], dml[i], null,
                    false);
                selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
                checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName3));
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

        String mysqlColDef = colDef.replace("auto_gen_add(id,a)", "id+a");
        String createTable =
            String.format(
                "create table %s (id int, a int default 1, b int default 2, ts timestamp default '2022-10-10 12:00:00' on update current_timestamp(), %s) ",
                tableName, colDef);
        String mysqlCreateTable =
            String.format(
                "create table %s (id int, a int default 1, b int default 2, ts timestamp default '2022-10-10 12:00:00' on update current_timestamp(), %s) ",
                tableName, mysqlColDef);
        // UPSERT in broadcast table will not update timestamp properly, skip timestamp column for now
        if (partDef.contains("broadcast") && Arrays.stream(params)
            .anyMatch(param -> param[1].contains("on duplicate key update"))) {
            createTable = String.format("create table %s (id int, a int default 1, %s) ", tableName, colDef);
            mysqlCreateTable = String.format("create table %s (id int, a int default 1, %s) ", tableName, mysqlColDef);
        }

        String hint = buildCmdExtra(ENABLE_UNIQUE_KEY_ON_GEN_COL);
        createTable = hint + createTable;
        System.out.println(createTable + partDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreateTable.replace("logical", ""));

        return true;
    }
}

package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class PkConstraintTest extends DDLBaseNewDBTestCase {
    private static final String PRIMARY_KEY_CHECK = "PRIMARY_KEY_CHECK=TRUE";
    private static final String FOREIGN_KEY_CHECKS = "FOREIGN_KEY_CHECKS=TRUE";
    private static final String FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE = "FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE=TRUE";

    private static final String SOURCE_TABLE_NAME = "pk_test_src_tbl";

    private static final String CREATE_TABLE_TMPL = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int {1}) {2}";

    private static final String[] INSERT_PARAMS = new String[] {
        " values (1,1,1)",
        " values (1,2,2)",
        " values (2,2,2),(2,3,3)",
        " values (3,3,3),(3,3,4)",
        "(c,b,a) values (4,4,4)",
        "(c,b,a) values (4,5,4)",
        "(c,b,a) values (5,5,5),(5,6,5)",
        "(c,b,a) values (6,6,6),(7,6,6)",
        "(a,b) values (8,8),(7+1,9-1)",
        "(a,b) values (8,8),(7+1,9)",
        "(a,b) select 9,10 union select 9,9",
        "(a,b) select 10-2,8",
        String.format(" select * from %s", SOURCE_TABLE_NAME),
        String.format("(a,b) select b,c from %s", SOURCE_TABLE_NAME),
    };

    private static final String[] PK_DEFS = new String[] {
        ", primary key (a)",
        ", primary key (a,b)",
    };

    private static final String[] PART_DEFS = new String[] {
        "dbpartition by hash(a)",
        "dbpartition by hash(b)",
        "dbpartition by hash(b,c)",
        "single",
        "broadcast"
    };

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Ignore
    @Test
    public void testInsertPkConstraint() {
        String op = "insert into";
        String tableName = "pk_test_insert_tbl";

        for (String pkDef : PK_DEFS) {
            for (String partitionDef : PART_DEFS) {
                testInsertPkConstraintInternal(op, tableName, pkDef, partitionDef, INSERT_PARAMS);
            }
        }
    }

    @Ignore
    @Test
    public void testReplacePkConstraint() {
        String op = "replace into";
        String tableName = "pk_test_replace_tbl";

        for (String pkDef : PK_DEFS) {
            for (String partitionDef : PART_DEFS) {
                testInsertPkConstraintInternal(op, tableName, pkDef, partitionDef, INSERT_PARAMS);
            }
        }
    }

    @Ignore
    @Test
    public void testInsertIgnorePkConstraint() {
        String op = "insert ignore into";
        String tableName = "pk_test_insert_ignore_tbl";

        for (String pkDef : PK_DEFS) {
            for (String partitionDef : PART_DEFS) {
                testInsertPkConstraintInternal(op, tableName, pkDef, partitionDef, INSERT_PARAMS);
            }
        }
    }

    private void testInsertPkConstraintInternal(String op, String tableName, String pkDef, String partitionDef,
                                                String[] sqlTemplates) {
        // Create source table for insert select
        dropTableIfExists(SOURCE_TABLE_NAME);
        dropTableIfExistsInMySql(SOURCE_TABLE_NAME);
        String createSourceTableSql =
            String.format("create table if not exists %s (a int, b int, c int)", SOURCE_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSourceTableSql);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createSourceTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + SOURCE_TABLE_NAME + " values(100,101,102),(110,111,112)");
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "insert into " + SOURCE_TABLE_NAME + " values(100,101,102),(110,111,112)");

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        String createSql = MessageFormat.format(CREATE_TABLE_TMPL, tableName, pkDef, partitionDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        String mysqlCreateSql = MessageFormat.format(CREATE_TABLE_TMPL, tableName, pkDef, "");
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreateSql);

        String hint = buildCmdExtra(PRIMARY_KEY_CHECK);
        Set<String> Ignored = new HashSet<>();
        Ignored.add("duplicate");
        Ignored.add("Duplicate");

        for (int i = 0; i < sqlTemplates.length; i++) {
            String dml = String.format("%s %s %s", op, tableName, sqlTemplates[i]);
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, hint + dml, Ignored);
            JdbcUtil.executeUpdateSuccessIgnoreErr(mysqlConnection, dml, Ignored);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    @Ignore
    @Test
    public void testUpsertPkConstraint() {
        String tableName = "pk_test_upsert_tbl";

        for (String partitionDef : PART_DEFS) {
            dropTableIfExists(tableName);
            String createSql =
                String.format("create table %s (a int primary key, b int, c int) %s", tableName, partitionDef);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

            String hint = buildCmdExtra(PRIMARY_KEY_CHECK);
            String sql = String.format("insert into %s values (1,1,1) on duplicate key update a=1", tableName);

            if (partitionDef.contains("hash(b)") || partitionDef.contains("hash(b,c)")) {
                // modify primary key, should fail
                JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
            } else {
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            }

            sql = String.format("insert into %s values (1,1,1) on duplicate key update b=1", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

            sql = String.format("insert into %s values (1,1,1) on duplicate key update c=1", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
        }
    }

    @Ignore
    @Test
    public void testUpdatePkConstraint() {
        String tableName = "pk_test_update_tbl";

        for (String partitionDef : PART_DEFS) {
            dropTableIfExists(tableName);
            String createSql =
                String.format("create table %s (a int primary key, b int, c int) %s", tableName, partitionDef);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

            String hint = buildCmdExtra(PRIMARY_KEY_CHECK);
            String sql = String.format("update %s set a=1", tableName);

            if (partitionDef.contains("hash(b)") || partitionDef.contains("hash(b,c)")) {
                // modify primary key, should fail
                JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
            } else {
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            }

            sql = String.format("update %s set b=1", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

            sql = String.format("update %s set c=1", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
        }
    }
}

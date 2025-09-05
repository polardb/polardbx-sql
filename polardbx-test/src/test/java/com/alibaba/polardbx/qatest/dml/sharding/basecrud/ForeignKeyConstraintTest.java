package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

import static org.junit.Assert.assertEquals;

@NotThreadSafe
public class ForeignKeyConstraintTest extends DDLBaseNewDBTestCase {
    private static final String FOREIGN_KEY_CHECKS = "FOREIGN_KEY_CHECKS=TRUE";
    private static final String FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE = "FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE=TRUE";

    private static final String dataBaseName = "ForeignKeyConstraintDB";

    private static final String CREATE_TABLE_TMPL_2_BASE = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int, "
        + "key (`a`,`b`,`c`), "
        + "key (`b`,`c`), "
        + "{1}) {2}";

    private static final String CREATE_TABLE_TMPL_2 = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int, "
        + "foreign key fk(`{1}`) REFERENCES {2}(`{3}`) ON DELETE CASCADE ON UPDATE CASCADE, "
        + "{4}) {5}";

    private static final String CREATE_TABLE_TMPL_3 = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int, "
        + "foreign key fk(`{1}`) REFERENCES {2}(`{3}`) ON DELETE CASCADE ON UPDATE CASCADE, "
        + "foreign key fk1(`{4}`) REFERENCES {5}(`{6}`) ON DELETE CASCADE ON UPDATE CASCADE, "
        + "{7}) {8}";

    private static final String CREATE_TABLE_TMPL_4 = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int, "
        + "foreign key fk(`{1}`,`{2}`) REFERENCES {3}(`{4}`,`{5}`) ON DELETE CASCADE ON UPDATE CASCADE, "
        + "{6}) {7}";

    private static final String[] PART_DEFS = new String[] {
        "dbpartition by hash(a)",
        "dbpartition by hash(b)",
        "single",
        "broadcast"
    };

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Before
    public void before() {
        doReCreateDatabase();
    }

    @After
    public void after() {
        doClearDatabase();
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String createDbHint = "/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"4\"}})*/";
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = createDbHint + "create database " + dataBaseName + " partition_mode = 'drds'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + dataBaseName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql =
            "/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true)*/drop database if exists " + dataBaseName;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }

    @Test
    public void testInsertFkConstraint() throws SQLException {
        testInsertFkConstraintInternal("insert into", "fk_test_insert_tbl", "fk1", false, "");
    }

    @Test
    public void testReplaceFkConstraint() throws SQLException {
        testInsertFkConstraintInternal("replace into", "fk_test_replace_tbl", "fk2", false, "");
    }

    @Test
    public void testInsertIgnoreFkConstraint() throws SQLException {
        testInsertFkConstraintInternal("insert ignore into", "fk_test_insert_ignore_tbl", "fk3", true, "");
    }

    @Test
    public void testUpsertFkConstraint() throws SQLException {
        testInsertFkConstraintInternal("insert into", "fk_test_insert_tbl", "fk1", false,
            " on duplicate key update b = values(b)");
    }

    public void testInsertFkConstraintInternal(String op, String tableName, String fkName, boolean isInsertIgnore,
                                               String upsertSql)
        throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName1 = tableName + "_1";
        String tableName2 = tableName + "_2";

        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);
        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists(tableName2);
            dropTableIfExists(tableName1);

            String createSql1 =
                MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, tableName1, "primary key (a)", partitionDef1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql1);

            String sql = String.format("insert into %s values (1,2,3), (4,5,6)", tableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

            for (String partitionDef2 : PART_DEFS) {
                System.out.println(partitionDef1 + " | " + partitionDef2);

                dropTableIfExists(tableName2);

                // one fk
                String createSql2 =
                    MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "b", tableName1, "a", "primary key (a)",
                        partitionDef2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);

                sql = String.format("%s %s values (2,1,3), (3,1,4), (4,4,5)%s", op, tableName2, upsertSql);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                ResultSet rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                List<List<Object>> result = JdbcUtil.getAllResult(rs);
                assertEquals(3, result.size());

                // FK does not exist, should fail, unless it's insert ignore
                sql = String.format("%s %s values (1,2,3), (5,4,7)%s", op, tableName2, upsertSql);
                if (isInsertIgnore) {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                    // insert (5,4,7) successfully
                    rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                        String.format("select * from %s where c = 7", tableName2));
                    result = JdbcUtil.getAllResult(rs);
                    assertEquals(1, result.size());
                } else {
                    JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
                }

                // multi fk
                dropTableIfExists(tableName2);
                createSql2 =
                    MessageFormat.format(CREATE_TABLE_TMPL_3, tableName2,
                        "a", tableName1, "b",
                        "b", tableName1, "a",
                        "primary key (a)", partitionDef2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);

                sql = String.format("%s %s values (2,4,6), (5,1,7)%s", op, tableName2, upsertSql);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                result = JdbcUtil.getAllResult(rs);
                assertEquals(2, result.size());

                sql = String.format("%s %s values (3,1,8)%s", op, tableName2, upsertSql);
                if (isInsertIgnore) {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                } else {
                    JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
                }

                // fk with multi columns
                dropTableIfExists(tableName2);
                createSql2 =
                    MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                        "b", "c", tableName1, "a", "b",
                        "primary key (a)", partitionDef2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);

                sql = String.format("%s %s values (6,1,2), (7,4,5)%s", op, tableName2, upsertSql);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                result = JdbcUtil.getAllResult(rs);
                assertEquals(2, result.size());

                sql = String.format("%s %s values (8,4,5), (9,1,3)%s", op, tableName2, upsertSql);
                if (isInsertIgnore) {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                    // insert (8,4,5) successfully
                    rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                        String.format("select * from %s where a = 8", tableName2));
                    result = JdbcUtil.getAllResult(rs);
                    assertEquals(1, result.size());
                } else {
                    JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
                }
            }
        }
    }

    @Test
    public void testInsertNullValue() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName1 = "fk_test_insert_null_tbl" + "_1";
        String tableName2 = "fk_test_insert_null_tbl" + "_2";

        dropTableIfExists(tableName2);
        dropTableIfExists(tableName1);

        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);
        String createSql1 =
            MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, tableName1, "primary key (a)", "dbpartition by hash(a)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql1);

        String createSql2 =
            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "b", tableName1, "a", "primary key (a)",
                "dbpartition by hash(a)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);

        String sql = String.format("insert into %s values (1,null,3), (4,null,6)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

        sql = String.format("insert into %s values (1,null,3), (4,5,6)", tableName2);
        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

        // fix bug: update null value
        sql = String.format("update %s set b = null where a int (1,4)", tableName2);
        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

        dropTableIfExists(tableName2);
        createSql2 =
            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                "b", "c", tableName1, "a", "b",
                "primary key (a)", "dbpartition by hash(a)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);

        sql =
            String.format("insert into %s values (1,null,null), (4,null,null), (5,null,100), (6,100,null)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
    }

}

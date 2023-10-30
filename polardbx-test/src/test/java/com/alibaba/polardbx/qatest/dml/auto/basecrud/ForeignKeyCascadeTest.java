package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ForeignKeyCascadeTest extends DDLBaseNewDBTestCase {
    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private static final String dataBaseName = "ForeignKeyCascadeDB";

    private static final String FOREIGN_KEY_CHECKS = "FOREIGN_KEY_CHECKS=TRUE";
    private static final String FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE = "FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE=TRUE";

    private static final String SOURCE_TABLE_NAME = "fk_test_src_tbl";

    private static final String CREATE_TABLE_TMPL_1 = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int {1}) {2}";

    private static final String CREATE_TABLE_TMPL_2_BASE = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int, "
        + "key (`a`,`b`,`c`), "
        + "key (`b`,`c`), "
        + "key (`c`),"
        + "{1}) {2}";

    private static final String CREATE_TABLE_TMPL_2 = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int, "
        + "foreign key fk(`{1}`) REFERENCES {2}(`{3}`) ON DELETE {6} ON UPDATE {6}, "
        + "{4}) {5}";

    private static final String CREATE_TABLE_TMPL_3 = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int, "
        + "foreign key fk(`{1}`) REFERENCES {2}(`{3}`) ON DELETE {9} ON UPDATE {9}, "
        + "foreign key fk1(`{4}`) REFERENCES {5}(`{6}`) ON DELETE {9} ON UPDATE {9}, "
        + "{7}) {8}";

    private static final String CREATE_TABLE_TMPL_4 = "create table {0}("
        + "a int, "
        + "b int, "
        + "c int, "
        + "key (`b`,`a`), "
        + "foreign key fk(`{1}`,`{2}`) REFERENCES {3}(`{4}`,`{5}`) ON DELETE {8} ON UPDATE {8}, "
        + "{6}) {7}";

    private static final String[] PART_DEFS = new String[] {
        "partition by hash(a) partitions 7",
        "partition by hash(b) partitions 7",
        "partition by hash(b,c) partitions 7",
        "single",
        "broadcast"
    };

    private static final String[] FK_OPTIONS = new String[] {
        "CASCADE",
        "NO ACTION",
        "RESTRICT",
        "SET NULL",
        "SET DEFAULT"
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
        tddlSql = createDbHint + "create database " + dataBaseName + " partition_mode = 'auto'";
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
    public void testFkDeleteCascade() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName1 = SOURCE_TABLE_NAME + "_1";
        String tableName2 = SOURCE_TABLE_NAME + "_2";
        String tableName3 = SOURCE_TABLE_NAME + "_3";
        String tableName4 = SOURCE_TABLE_NAME + "_4";

        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);
        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists(tableName4);
            dropTableIfExists(tableName3);
            dropTableIfExists(tableName2);
            dropTableIfExists(tableName1);
            String createSql1 =
                MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, tableName1, "primary key (a)", partitionDef1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql1);

            for (String partitionDef2 : PART_DEFS) {
                for (String option : FK_OPTIONS) {
                    System.out.println(partitionDef1 + " | " + partitionDef2);
                    System.out.println("FK OPTION: " + option);

                    dropTableIfExists(tableName4);
                    dropTableIfExists(tableName3);
                    dropTableIfExists(tableName2);

                    switch (option) {
                    case "CASCADE":
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        String createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "b", tableName1, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        String createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName3, "c", tableName2, "b",
                                "primary key (a)",
                                partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                        // delete common
                        String sql = String.format("insert into %s values (1,2,2)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where a = 1", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        ResultSet rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        List<List<Object>> result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        // delete primary key / sharding key
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "a", tableName1, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName3, "a", tableName2, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,4,5)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,6,7)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where a = 1", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        // delete with multi foreign keys
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName2,
                                "a", tableName1, "b",
                                "b", tableName1, "a",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName3,
                                "a", tableName2, "b",
                                "b", tableName2, "a",
                                "primary key (a)", partitionDef2, option);
                        String createSql4 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName4,
                                "a", tableName3, "b",
                                "b", tableName3, "a",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,5)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,6)", tableName4);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        // delete with foreign keys with multi columns
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                                "a", "b", tableName1, "a", "b",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName3,
                                "c", "b", tableName2, "b", "a",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (5,1,2)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        // delete one of referenced columns
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (5,1,2)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where b = 2", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        result = JdbcUtil.getAllResult(rs);
                        assertEquals(0, result.size());
                        break;

                    case "RESTRICT":
                    case "NO ACTION":
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "b", tableName1, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName3, "c", tableName2, "b",
                                "primary key (a)",
                                partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                        // delete common
                        sql = String.format("insert into %s values (1,2,2)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where a = 1", tableName1);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

                        // delete with multi foreign keys
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName2,
                                "a", tableName1, "b",
                                "b", tableName1, "a",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName3,
                                "a", tableName2, "b",
                                "b", tableName2, "a",
                                "primary key (a)", partitionDef2, option);
                        createSql4 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName4,
                                "a", tableName3, "b",
                                "b", tableName3, "a",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);

                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,5)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,6)", tableName4);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where c = 3", tableName1);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

                        // delete with foreign keys with multi columns
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                                "a", "b", tableName1, "a", "b",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName3,
                                "c", "b", tableName2, "b", "a",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);

                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (5,1,2)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where c = 3", tableName1);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

                        // delete one of referenced columns
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName3));
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName2));
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (5,1,2)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where b = 2", tableName1);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
                        break;

                    case "SET NULL":
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "b", tableName1, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);

                        // delete common
                        sql = String.format("insert into %s values (1,2,2)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where a = 1", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));

                        // delete with multi foreign keys
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName2,
                                "b", tableName1, "a",
                                "c", tableName1, "b",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));

                        // delete with foreign keys with multi columns
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                                "b", "c", tableName1, "a", "b",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));

                        // delete one of referenced columns
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName2));

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("delete from %s where b = 2", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));

                        break;

                    case "SET DEFAULT":
                        // do not support in mysql
                    }
                }
            }
        }
    }

    @Test
    public void testFkUpdateCascade() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName1 = SOURCE_TABLE_NAME + "_1";
        String tableName2 = SOURCE_TABLE_NAME + "_2";
        String tableName3 = SOURCE_TABLE_NAME + "_3";
        String tableName4 = SOURCE_TABLE_NAME + "_4";

        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);
        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists(tableName4);
            dropTableIfExists(tableName3);
            dropTableIfExists(tableName2);
            dropTableIfExists(tableName1);
            String createSql1 =
                MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, tableName1, "primary key (a)", partitionDef1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql1);

            for (String partitionDef2 : PART_DEFS) {
                for (String option : FK_OPTIONS) {
                    System.out.println(partitionDef1 + " | " + partitionDef2);
                    System.out.println("FK OPTION: " + option);

                    dropTableIfExists(tableName4);
                    dropTableIfExists(tableName3);
                    dropTableIfExists(tableName2);

                    switch (option) {
                    case "CASCADE":
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        String createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "b", tableName1, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        String createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName3, "c", tableName2, "b",
                                "primary key (a)",
                                partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                        // update common
                        String sql = String.format("insert into %s values (1,2,2)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set a = 3 where a = 1", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        ResultSet rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(2), 3);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(3), 3);

                        // update restriction
                        sql = String.format("update %s set b = 4 where c = 2", tableName2);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

                        sql = String.format("insert into %s values (4,2,2)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set b = 4 where c = 2", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(3), 4);

                        // update primary key / sharding key
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "a", tableName1, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName3, "a", tableName2, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,4,5)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,6,7)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set a = 5 where b = 2", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 5);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 5);

                        // update with multi foreign keys
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName2,
                                "a", tableName1, "b",
                                "b", tableName1, "a",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName3,
                                "a", tableName2, "b",
                                "b", tableName2, "a",
                                "primary key (a)", partitionDef2, option);
                        String createSql4 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName4,
                                "a", tableName3, "b",
                                "b", tableName3, "a",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,5)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,6)", tableName4);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set a = 7, b = 8 where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 8);
                        assertEquals(rs.getLong(2), 7);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 7);
                        assertEquals(rs.getLong(2), 8);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName4));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 8);
                        assertEquals(rs.getLong(2), 7);

                        sql = String.format("update %s set c = 6, b = 10, a = 9 where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName1));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(3), 6);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 10);
                        assertEquals(rs.getLong(2), 9);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 9);
                        assertEquals(rs.getLong(2), 10);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName4));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 10);
                        assertEquals(rs.getLong(2), 9);

                        // update with foreign keys with multi columns
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                                "a", "b", tableName1, "a", "b",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName3,
                                "c", "b", tableName2, "b", "a",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (5,1,2)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set c = 6, b = 8, a = 7 where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(1), 7);
                        assertEquals(rs.getLong(2), 8);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(2), 7);
                        assertEquals(rs.getLong(3), 8);

                        // update one of referenced columns
                        sql = String.format("update %s set b = 9 where c = 6", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(2), 9);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertEquals(rs.getLong(3), 9);
                        break;
                    case "RESTRICT":
                    case "NO ACTION":
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "b", tableName1, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName3, "c", tableName2, "b",
                                "primary key (a)",
                                partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                        // update common
                        sql = String.format("insert into %s values (1,2,2)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set a = 3 where a = 1", tableName1);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

                        // update with multi foreign keys
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName2,
                                "a", tableName1, "b",
                                "b", tableName1, "a",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName3,
                                "a", tableName2, "b",
                                "b", tableName2, "a",
                                "primary key (a)", partitionDef2, option);
                        createSql4 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName4,
                                "a", tableName3, "b",
                                "b", tableName3, "a",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);

                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,4)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (1,2,5)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,6)", tableName4);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set a = 7, b = 8 where c = 3", tableName1);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

                        // update with foreign keys with multi columns
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                                "b", "c", tableName1, "a", "b",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName3,
                                "c", "b", tableName2, "b", "c",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);

                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set c = 6, b = 8, a = 7 where c = 3", tableName1);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

                        // update one of referenced columns
                        sql = String.format("update %s set b = 9 where c = 3", tableName1);
                        JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
                        break;

                    case "SET NULL":
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName2, "b", tableName1, "a",
                                "primary key (a)",
                                partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_2, tableName3, "c", tableName2, "b",
                                "primary key (a)",
                                partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                        // update common
                        sql = String.format("insert into %s values (1,2,2)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (2,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set a = 3 where a = 1", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(3));

                        // update with multi foreign keys
                        dropTableIfExists(tableName4);
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName2,
                                "b", tableName1, "a",
                                "c", tableName1, "b",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_3, tableName3,
                                "c", tableName2, "b",
                                "b", tableName2, "c",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set a = 7, b = 8 where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));

                        // update with foreign keys with multi columns
                        dropTableIfExists(tableName3);
                        dropTableIfExists(tableName2);

                        createSql2 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                                "b", "c", tableName1, "a", "b",
                                "primary key (a)", partitionDef2, option);
                        createSql3 =
                            MessageFormat.format(CREATE_TABLE_TMPL_4, tableName3,
                                "c", "b", tableName2, "b", "c",
                                "primary key (a)", partitionDef2, option);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set c = 6, b = 8, a = 7 where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));

                        // update one of referenced columns
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName3));
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName2));
                        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("delete from %s", tableName1));
                        sql = String.format("insert into %s values (1,2,3)", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,1,2)", tableName2);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("insert into %s values (3,2,1)", tableName3);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                        sql = String.format("update %s set b = 9 where c = 3", tableName1);
                        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));

                        rs =
                            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                        Assert.assertTrue(rs.next());
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));
                        break;

                    case "SET DEFAULT":
                        // do not support in mysql
                    }
                }
            }
        }
    }

    @Test
    public void testFkDeleteSingeTableCascade() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName = SOURCE_TABLE_NAME;
        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);

        for (String partitionDef : PART_DEFS) {
            System.out.println(partitionDef);

            dropTableIfExists(tableName);
            String createSql =
                MessageFormat.format(CREATE_TABLE_TMPL_2, tableName, "b", tableName, "a", "primary key (a)",
                    partitionDef, "CASCADE");
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

            String sql = String.format("insert into %s values (1,2,3)", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

            sql = String.format("insert into %s values (1,1,3)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("insert into %s values (2,1,4)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("insert into %s values (3,2,5)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("insert into %s values (4,3,6)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("delete from %s where c = 3", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

            ResultSet rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName));
            List<List<Object>> result = JdbcUtil.getAllResult(rs);
            assertEquals(0, result.size());
        }
    }

    @Test
    public void testFkUpdateSingeTableCascade() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName = SOURCE_TABLE_NAME;
        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);

        for (String partitionDef : PART_DEFS) {
            System.out.println(partitionDef);

            dropTableIfExists(tableName);
            String createSql =
                MessageFormat.format(CREATE_TABLE_TMPL_2, tableName, "b", tableName, "a", "primary key (a)",
                    partitionDef, "CASCADE");
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

            String sql = String.format("insert into %s values (1,2,3)", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

            sql = String.format("insert into %s values (1,1,3)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("insert into %s values (2,1,4)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

            sql = String.format("update %s set a = 7, b = 8 where c = 3", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
            sql = String.format("update %s set a = 8, b = 8 where c = 3", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");

            sql = String.format("insert into %s values (7,7,5)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            // Cannot use self-referential ON UPDATE CASCADE or ON UPDATE SET NULL operations
            sql = String.format("update %s set a = 8, b = 7 where c = 3", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql, "");
        }
    }

    @Test
    public void testFkUpdateDiffColumnCascade() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName1 = SOURCE_TABLE_NAME + "_1";
        String tableName2 = SOURCE_TABLE_NAME + "_2";
        String tableName3 = SOURCE_TABLE_NAME + "_3";

        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);
        String createSql1 =
            "create table %s(%s int, %s int, %s int, primary key (%s), key(`%s`,`%s`)) partition by hash(%s) partitions 7";
        String createSql2 = "create table %s(%s int, %s int, %s int, key(`%s`,`%s`), "
            + "foreign key fk(%s, %s) REFERENCES %s(%s, %s) ON DELETE CASCADE ON UPDATE CASCADE, "
            + "primary key (%s)) partition by hash(%s) partitions 7";

        dropTableIfExists(tableName3);
        dropTableIfExists(tableName2);
        dropTableIfExists(tableName1);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createSql1,
            tableName1, "a", "b", "c", "a", "a", "b", "a"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createSql2,
            tableName2, "d", "e", "f", "d", "e", "d", "e", tableName1, "a", "b", "d", "d"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createSql2,
            tableName3, "g", "h", "i", "h", "i", "h", "i", tableName2, "d", "e", "g", "g"));

        String sql = String.format("insert into %s values (1,2,3)", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
        sql = String.format("insert into %s values (1,2,4)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
        sql = String.format("insert into %s values (5,1,2)", tableName3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
        sql = String.format("update %s set c = 6, b = 8, a = 7 where c = 3", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getLong(1), 7);
        assertEquals(rs.getLong(2), 8);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getLong(2), 7);
        assertEquals(rs.getLong(3), 8);

        sql = String.format("update %s set b = 9 where c = 6", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getLong(2), 9);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getLong(3), 9);
    }

    @Test
    public void testFkDeleteSetNullCascade() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName1 = SOURCE_TABLE_NAME + "_1";
        String tableName2 = SOURCE_TABLE_NAME + "_2";
        String tableName3 = SOURCE_TABLE_NAME + "_3";

        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists(tableName3);
            dropTableIfExists(tableName2);
            dropTableIfExists(tableName1);
            String createSql1 =
                MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, tableName1, "primary key (a)", partitionDef1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql1);

            for (String partitionDef2 : PART_DEFS) {
                System.out.println(partitionDef1 + " | " + partitionDef2);

                dropTableIfExists(tableName3);
                dropTableIfExists(tableName2);

                String createSql2 =
                    MessageFormat.format(CREATE_TABLE_TMPL_4, tableName2,
                        "b", "c", tableName1, "a", "b",
                        "primary key (a)", partitionDef2, "SET NULL");
                String createSql3 =
                    MessageFormat.format(CREATE_TABLE_TMPL_4, tableName3,
                        "c", "b", tableName2, "b", "c",
                        "primary key (a)", partitionDef2, "CASCADE");
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);

                String sql = String.format("insert into %s values (1,2,3)", tableName1);
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                sql = String.format("insert into %s values (3,1,2)", tableName2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                sql = String.format("insert into %s values (3,2,1)", tableName3);
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

                sql = String.format("delete from %s", tableName1);
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

                ResultSet rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName2));
                Assert.assertTrue(rs.next());
                assertNull(rs.getObject(2));
                assertNull(rs.getObject(3));

                rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", tableName3));
                Assert.assertTrue(rs.next());
                assertNull(rs.getObject(2));
                assertNull(rs.getObject(3));
            }
        }
    }

    @Test
    public void testFkCascadeDepthLimit() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String tableName = SOURCE_TABLE_NAME;
        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);

        for (String partitionDef : PART_DEFS) {
            System.out.println(partitionDef);

            dropTableIfExists(tableName);
            String createSql =
                MessageFormat.format(CREATE_TABLE_TMPL_2, tableName, "b", tableName, "a", "primary key (a)",
                    partitionDef, "CASCADE");
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

            String sql = String.format("insert into %s (a,b) values (0,0),(1,0),(2,1),(3,2),(4,3),(5,4),(6,5),(7,6),\n"
                + "(8,7),(9,8),(10,9),(11,10),(12,11),(13,12),(14,13),(15,14)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

            sql = String.format("delete from %s where a = 0", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

            sql = String.format("delete from %s where a = 15", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            sql = String.format("delete from %s where a = 0", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    @Test
    public void testMultiTableUpdateDeleteWithoutRelocate() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String parent1 = SOURCE_TABLE_NAME + "_p1";
        String parent2 = SOURCE_TABLE_NAME + "_p2";
        String child1 = SOURCE_TABLE_NAME + "_c1";
        String child2 = SOURCE_TABLE_NAME + "_c2";

        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);
        for (String partitionDef1 : PART_DEFS) {
            if (partitionDef1.contains("hash(b")) {
                continue;
            }
            dropTableIfExists(child1);
            dropTableIfExists(child2);
            dropTableIfExists(parent1);
            dropTableIfExists(parent2);
            String createSql1 =
                MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, parent1, "primary key (a)", partitionDef1);
            String createSql2 =
                MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, parent2, "primary key (a)", partitionDef1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);

            for (String partitionDef2 : PART_DEFS) {
                if (partitionDef1.contains("hash(b")) {
                    continue;
                }
                System.out.println(partitionDef1 + " | " + partitionDef2);
                dropTableIfExists(child1);
                dropTableIfExists(child2);
                String createSql3 =
                    MessageFormat.format(CREATE_TABLE_TMPL_2, child1, "c", parent1, "b",
                        "primary key (a)",
                        partitionDef2, "CASCADE");
                String createSql4 =
                    MessageFormat.format(CREATE_TABLE_TMPL_2, child2, "c", parent2, "b",
                        "primary key (a)",
                        partitionDef2, "CASCADE");
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
                JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);

                // update common
                String sql = String.format("insert into %s values (2,1,3)", parent1);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                sql = String.format("insert into %s values (4,5,6)", parent2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                sql = String.format("insert into %s values (2,2,1)", child1);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                sql = String.format("insert into %s values (4,4,5)", child2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
                sql = String.format("update %s,%s set %s.b = 10,%s.b = 50", parent1, parent2, parent1, parent2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                ResultSet rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child1));
                Assert.assertTrue(rs.next());
                assertEquals(rs.getLong(3), 10);

                rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
                Assert.assertTrue(rs.next());
                assertEquals(rs.getLong(3), 50);

                sql = String.format("delete %s,%s from %s,%s", parent1, parent2, parent1, parent2);
                JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

                rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child1));
                List<List<Object>> result = JdbcUtil.getAllResult(rs);
                assertEquals(0, result.size());

                rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
                result = JdbcUtil.getAllResult(rs);
                assertEquals(0, result.size());
            }
        }
    }

    @Test
    public void testMultiTableUpdateDeleteWithRelocate() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String parent1 = SOURCE_TABLE_NAME + "_p1";
        String parent2 = SOURCE_TABLE_NAME + "_p2";
        String child1 = SOURCE_TABLE_NAME + "_c1";
        String child2 = SOURCE_TABLE_NAME + "_c2";

        String hint = buildCmdExtra(FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE);

        dropTableIfExists(child1);
        dropTableIfExists(child2);
        dropTableIfExists(parent1);
        dropTableIfExists(parent2);
        // actual partition key is column b
        String createSql1 =
            MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, parent1, "primary key (a)",
                "partition by key(b,c) partitions 7");
        // add global index on column b
        String createSql2 =
            MessageFormat.format(CREATE_TABLE_TMPL_2_BASE, parent2, "primary key (a)",
                "partition by hash(a) partitions 7");
        String addGsi =
            String.format("alter table %s add global index idx(`b`) partition by hash(b) partitions 7", parent2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, addGsi);

        for (String partitionDef2 : PART_DEFS) {
            System.out.println(partitionDef2);
            dropTableIfExists(child1);
            dropTableIfExists(child2);
            String createSql3 =
                MessageFormat.format(CREATE_TABLE_TMPL_2, child1, "c", parent1, "c",
                    "primary key (a)",
                    partitionDef2, "CASCADE");
            String createSql4 =
                MessageFormat.format(CREATE_TABLE_TMPL_2, child2, "c", parent2, "b",
                    "primary key (a)",
                    partitionDef2, "CASCADE");
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql3);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql4);

            // update common
            String sql = String.format("insert into %s values (2,3,1)", parent1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("insert into %s values (4,5,6)", parent2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("insert into %s values (2,2,1)", child1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("insert into %s values (4,4,5)", child2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);
            sql = String.format("update %s,%s set %s.c = 10,%s.b = 50", parent1, parent2, parent1, parent2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

            ResultSet rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child1));
            Assert.assertTrue(rs.next());
            assertEquals(rs.getLong(3), 10);

            rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
            Assert.assertTrue(rs.next());
            assertEquals(rs.getLong(3), 50);

            sql = String.format("delete %s,%s from %s,%s", parent1, parent2, parent1, parent2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sql);

            rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child1));
            List<List<Object>> result = JdbcUtil.getAllResult(rs);
            assertEquals(0, result.size());

            rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
            result = JdbcUtil.getAllResult(rs);
            assertEquals(0, result.size());
        }
    }
}

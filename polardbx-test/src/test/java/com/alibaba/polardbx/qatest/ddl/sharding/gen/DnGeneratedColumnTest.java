package com.alibaba.polardbx.qatest.ddl.sharding.gen;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class DnGeneratedColumnTest extends DDLBaseNewDBTestCase {
    @Test
    public void testCreateTableFailed() {
        String tableName = "dn_gen_col_create_f_tbl";
        String gsiName = tableName + "_gsi";
        String createTable =
            String.format("create table %s (a int primary key, b int, c int as (a)) dbpartition by hash(c)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a), global unique index %s(c) dbpartition by hash(c)) dbpartition by hash(a)",
            tableName, gsiName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a), global index %s(c) dbpartition by hash(c)) dbpartition by hash(a)",
            tableName, gsiName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a), global unique clustered index %s(c) dbpartition by hash(c)) dbpartition by hash(a)",
            tableName, gsiName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a), clustered index %s(c) dbpartition by hash(c)) dbpartition by hash(a)",
            tableName, gsiName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int, b int, c int as (a) stored primary key) dbpartition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int, b int, c int as (a) stored, primary key (a,c)) dbpartition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a) stored, local unique index (a,c)) dbpartition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable =
            String.format("create table %s (a int primary key, b int, c int as (a)) dbpartition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format("create global index %s on %s(c) dbpartition by hash(c)", gsiName, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsi, "");

        createGsi =
            String.format("create global clustered index %s on %s(c) dbpartition by hash(c)", gsiName, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsi, "");

        createGsi = String.format("create global unique index %s on %s(c) dbpartition by hash(c)", gsiName, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsi, "");

        createGsi = String.format("create global clustered unique index %s on %s(c) dbpartition by hash(c)", gsiName,
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsi, "");
    }

    @Test
    public void testCreateTable() throws SQLException {
        String tableName = "dn_gen_col_create_tbl";
        String gsiName1 = tableName + "_gsi_1";
        String gsiName2 = tableName + "_gsi_2";
        String createTable = String.format("create table %s (a int primary key, b int, c int as (a+1))", tableName);
        String partDef = " dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        String insert = String.format("insert into %s(a,b) values (1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        String createGsi =
            String.format("create global clustered index %s on %s(b) dbpartition by hash(b)", gsiName1, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);
        createGsi =
            String.format("create global index %s on %s(b) covering(c) dbpartition by hash(b)", gsiName2, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);
    }

    @Test
    public void testAlterTable() throws SQLException {
        String tableName = "dn_gen_col_alter_tbl";
        String gsiName = tableName + "_gsi";
        String createTable = String.format("create table %s (a int primary key, b int)", tableName);
        String partDef = " dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        String createGsi =
            String.format("create global clustered index %s on %s(b) dbpartition by hash(b)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("insert into %s(a,b) values (1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        String alter = String.format("alter table %s add column c int as (a+b) stored", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);

        insert = String.format("insert into %s(a,b) values (3,4)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);

        alter = String.format("alter table %s add column d int as (a+c) unique", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);

        insert = String.format("insert into %s(a,b) values (5,6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);
    }

    @Test
    public void testAlterTableFailed() throws SQLException {
        String tableName = "dn_gen_col_alter_tbl_fail";
        String createTable =
            String.format("create table %s (a int primary key, b int, c int as (a+b) stored, d int)", tableName);
        String partDef = " dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);

        String alter = String.format("alter table %s modify column b bigint algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column b e bigint algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s modify column c bigint algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column c e bigint algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s modify column d bigint as (a-b) algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column d e bigint as (a-b) algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s modify column c bigint", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column c e bigint", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s modify column d bigint as (a-b)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column d e bigint as (a-b)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");
    }

    @Test
    public void testRepartition() {
        String tableName = "dn_gen_col_repart";
        String create = String.format("create table %s (a int primary key, b int as (a+1))", tableName);
        String partDef = "dbpartition by hash(a)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String insert = String.format("insert into %s(a) values (1),(2),(3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String alter = String.format("alter table %s broadcast", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        alter = String.format("alter table %s single", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        alter = String.format("alter table %s dbpartition by hash(b)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");
    }

    @Test
    public void testMoveDb() throws SQLException {
        String dbName = "dn_gen_move_db";

        Connection conn = getPolardbxConnection();

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName);
        JdbcUtil.executeUpdateSuccess(conn, "create database " + dbName + " mode=drds");
        JdbcUtil.executeUpdateSuccess(conn, "use " + dbName);

        String tableName = "dn_gen_move_db_test_tbl";
        String gsiName = tableName + "_gsi";

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b), global index %s(b) covering(a) dbpartition by hash(b,a)) dbpartition by hash(a,b)",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(conn, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));

        Map<String, String> groupInstId = new HashMap();

        String sql = String.format("show ds where db ='%s'", dbName);
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        Set<String> instIds = new HashSet<>();
        while (rs.next()) {
            groupInstId.put(rs.getString("GROUP"), rs.getString("STORAGE_INST_ID"));
            instIds.add(rs.getString("STORAGE_INST_ID"));
        }
        rs.close();

        String srcGroup = groupInstId.keySet().stream().filter(k -> !k.contains("SINGLE")).findFirst().get();
        String dstStorageId =
            instIds.stream().filter(instId -> !groupInstId.get(srcGroup).equalsIgnoreCase(instId)).findFirst()
                .get();

        String tddlSql = String.format(
            "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true, CN_ENABLE_CHANGESET=false)*/ %s to '%s'",
            srcGroup, dstStorageId);
        JdbcUtil.executeUpdateSuccess(conn, tddlSql);

        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName);
        conn.close();

    }

    @Test
    public void testMoveDbChangeset() throws SQLException {
        String dbName = "dn_gen_move_db_cs";

        Connection conn = getPolardbxConnection();

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName);
        JdbcUtil.executeUpdateSuccess(conn, "create database " + dbName + " mode=drds");
        JdbcUtil.executeUpdateSuccess(conn, "use " + dbName);

        String tableName = "dn_gen_move_db_test_tbl";
        String gsiName = tableName + "_gsi";

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b), global index %s(b) covering(a) dbpartition by hash(b,a)) dbpartition by hash(a,b)",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(conn, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));

        Map<String, String> groupInstId = new HashMap();

        String sql = String.format("show ds where db ='%s'", dbName);
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        Set<String> instIds = new HashSet<>();
        while (rs.next()) {
            groupInstId.put(rs.getString("GROUP"), rs.getString("STORAGE_INST_ID"));
            instIds.add(rs.getString("STORAGE_INST_ID"));
        }
        rs.close();

        String srcGroup = groupInstId.keySet().stream().filter(k -> !k.contains("SINGLE")).findFirst().get();
        String dstStorageId =
            instIds.stream().filter(instId -> !groupInstId.get(srcGroup).equalsIgnoreCase(instId)).findFirst()
                .get();

        String tddlSql = String.format(
            "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true, CN_ENABLE_CHANGESET=true)*/ %s to '%s'",
            srcGroup, dstStorageId);
        JdbcUtil.executeUpdateSuccess(conn, tddlSql);

        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName);
        conn.close();

    }

    @Test
    public void testCreateDbAs() throws SQLException {
        String dbName1 = "dn_gen_create_as_db_1";
        String dbName2 = "dn_gen_create_as_db_2";
        Connection conn = getPolardbxConnection();

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName1);
        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName2);
        JdbcUtil.executeUpdateSuccess(conn, "create database " + dbName1 + " mode=drds");
        JdbcUtil.executeUpdateSuccess(conn, "use " + dbName1);

        String tableName = "dn_gen_create_db_test_tbl";
        String gsiName = tableName + "_gsi";

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b), global index %s(b) covering(a) dbpartition by hash(b,a)) dbpartition by hash(a,b)",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(conn, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));

        String sql = String.format("create database %s as %s", dbName2, dbName1);
        JdbcUtil.executeUpdateSuccess(conn, sql);
        JdbcUtil.executeUpdateSuccess(conn, "use " + dbName2);

        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName1);
        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName2);
        conn.close();

    }

    @Test
    public void testDmlFailed() {
        String tableName = "dn_gen_col_dml_fail_tbl";
        String create = String.format("create table %s (a int primary key, b int as (a+1))", tableName);
        String[] partDefs = new String[] {"", "single", "broadcast", "dbpartition by hash(a)"};

        for (String partDef : partDefs) {
            dropTableIfExists(tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);

            String sql = String.format("insert into %s values (1)", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

            sql = String.format("insert into %s values (1,2)", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

            sql = String.format("insert into %s(a,b) values (1,2)", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

            sql = String.format("insert into %s(b) values (2)", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

            sql = String.format("insert into %s select * from %s", tableName, tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

            sql = String.format("insert into %s(b) values (2) on duplicate key update b=b+1", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

            sql = String.format("update %s set a=1,b=2", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

            sql = String.format("update %s set b=2", tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        }
    }

    @Test
    public void testCreateTableLike() throws SQLException {
        String tableName = "dn_gen_col_like";
        String likeTable = tableName + "_2";

        dropTableIfExists(tableName);
        dropTableIfExists(likeTable);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) virtual) dbpartition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String likeSql = String.format("create table %s like %s", likeTable, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, likeSql);

        ResultSet rs = JdbcUtil.executeQuery("show create table " + tableName, tddlConnection);
        rs.next();
        String tableDef = rs.getString(2);

        rs = JdbcUtil.executeQuery("show create table " + likeTable, tddlConnection);
        rs.next();
        String likeTableDef = rs.getString(2);

        Assert.assertTrue(tableDef.equalsIgnoreCase(likeTableDef.replace(likeTable, tableName)));
    }

    @Test
    public void testCreateTableComment() throws SQLException {
        String tableName = "dn_gen_col_comment";
        String likeTable = tableName + "_2";

        dropTableIfExists(tableName);
        dropTableIfExists(likeTable);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) stored comment '123') dbpartition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String likeSql = String.format("create table %s like %s", likeTable, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, likeSql);

        ResultSet rs = JdbcUtil.executeQuery("show create table " + tableName, tddlConnection);
        rs.next();
        String tableDef = rs.getString(2);

        rs = JdbcUtil.executeQuery("show create table " + likeTable, tddlConnection);
        rs.next();
        String likeTableDef = rs.getString(2);

        Assert.assertTrue(tableDef.equalsIgnoreCase(likeTableDef.replace(likeTable, tableName)));
    }

    @Test
    public void testCreateTableComment1() throws SQLException {
        String tableName = "dn_gen_col_comment1";
        String likeTable = tableName + "_2";

        dropTableIfExists(tableName);
        dropTableIfExists(likeTable);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) virtual comment '123') dbpartition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String likeSql = String.format("create table %s like %s", likeTable, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, likeSql);

        ResultSet rs = JdbcUtil.executeQuery("show create table " + tableName, tddlConnection);
        rs.next();
        String tableDef = rs.getString(2);

        rs = JdbcUtil.executeQuery("show create table " + likeTable, tddlConnection);
        rs.next();
        String likeTableDef = rs.getString(2);

        Assert.assertTrue(tableDef.equalsIgnoreCase(likeTableDef.replace(likeTable, tableName)));
    }

    @Test
    public void testShowColumns() throws SQLException {
        String tableName = "dn_gen_col_show_columns";
        String createTable =
            String.format("create table %s (a int primary key, b int as (a+1) stored) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String sql = String.format("show columns from %s", tableName);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        boolean found = false;
        while (rs.next()) {
            if (rs.getString("Field").equalsIgnoreCase("b")) {
                Assert.assertTrue(rs.getString("Extra").equalsIgnoreCase("STORED GENERATED"));
                found = true;
            }
        }
        Assert.assertTrue(found);

        sql = String.format("show full columns from %s", tableName);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        found = false;
        while (rs.next()) {
            if (rs.getString("Field").equalsIgnoreCase("b")) {
                Assert.assertTrue(rs.getString("Extra").equalsIgnoreCase("STORED GENERATED"));
                found = true;
            }
        }
        Assert.assertTrue(found);

        sql = String.format("select * from information_schema.columns where table_schema='%s' and table_name='%s'",
            tddlDatabase1, tableName);
        System.out.println(sql);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        found = false;
        while (rs.next()) {
            if (rs.getString("COLUMN_NAME").equalsIgnoreCase("b")) {
                Assert.assertTrue(rs.getString("COLUMN_DEFAULT") == null);
                Assert.assertTrue(rs.getString("EXTRA").equalsIgnoreCase("STORED GENERATED"));
                Assert.assertTrue(rs.getString("GENERATION_EXPRESSION").equalsIgnoreCase("(`a` + 1)"));
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testAlterTableUniqueIndexFailed() throws SQLException {
        String tableName = "dn_gen_col_alter_unique_fail";
        String createTable =
            String.format("create table %s (a int primary key, b int, c int as (a+b) stored, d int)", tableName);
        String partDef = " dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);

        String alter = String.format("alter table %s add unique local index l1(c)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s add unique local key l1(c)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("create local unique index l1 on %s(c)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("create local unique key l1 on %s(c)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s drop primary key, add primary key(c)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");
    }
}

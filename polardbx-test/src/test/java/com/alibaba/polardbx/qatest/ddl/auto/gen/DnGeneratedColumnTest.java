package com.alibaba.polardbx.qatest.ddl.auto.gen;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class DnGeneratedColumnTest extends DDLBaseNewDBTestCase {
    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testCreateTableFailed() {
        String tableName = "dn_gen_col_create_f_tbl";
        String gsiName = tableName + "_gsi";
        String createTable =
            String.format("create table %s (a int primary key, b int, c int as (a)) partition by hash(c)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a), global unique index %s(c) partition by hash(c)) partition by hash(a)",
            tableName, gsiName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a), global index %s(c) partition by hash(c)) partition by hash(a)",
            tableName, gsiName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a), global unique clustered index %s(c) partition by hash(c)) partition by hash(a)",
            tableName, gsiName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a), clustered index %s(c) partition by hash(c)) partition by hash(a)",
            tableName, gsiName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int, b int, c int as (a) stored primary key) partition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int, b int, c int as (a) stored, primary key (a,c)) partition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable = String.format(
            "create table %s (a int primary key, b int, c int as (a) stored, local unique index (a,c)) partition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createTable, "");

        createTable =
            String.format("create table %s (a int primary key, b int, c int as (a)) partition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String createGsi = String.format("create global index %s on %s(c) partition by hash(c)", gsiName, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsi, "");

        createGsi = String.format("create global clustered index %s on %s(c) partition by hash(c)", gsiName, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsi, "");

        createGsi = String.format("create global unique index %s on %s(c) partition by hash(c)", gsiName, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsi, "");

        createGsi =
            String.format("create global clustered unique index %s on %s(c) partition by hash(c)", gsiName, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, createGsi, "");
    }

    @Test
    public void testCreateTable() throws SQLException {
        String tableName = "dn_gen_col_create_tbl";
        String gsiName1 = tableName + "_gsi_1";
        String gsiName2 = tableName + "_gsi_2";
        String createTable = String.format("create table %s (a int primary key, b int, c int as (a+1))", tableName);
        String partDef = " partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        String insert = String.format("insert into %s(a,b) values (1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        String createGsi =
            String.format("create global clustered index %s on %s(b) partition by hash(b)", gsiName1, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);
        createGsi =
            String.format("create global index %s on %s(b) covering(c) partition by hash(b)", gsiName2, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));
    }

    @Test
    public void testAlterTable() throws SQLException {
        String tableName = "dn_gen_col_alter_tbl";
        String gsiName = tableName + "_gsi";
        String createTable = String.format("create table %s (a int primary key, b int)", tableName);
        String partDef = " partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        String createGsi =
            String.format("create global clustered index %s on %s(b) partition by hash(b)", gsiName, tableName);
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
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));

        alter = String.format("alter table %s add column d int as (a+c) unique", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter);

        insert = String.format("insert into %s(a,b) values (5,6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
    }

    @Test
    public void testAlterTableFailed() throws SQLException {
        String tableName = "dn_gen_col_alter_tbl_fail";
        String createTable =
            String.format("create table %s (a int primary key, b int, c int as (a+b), d int)", tableName);
        String partDef = " partition by hash(a)";
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
        String partDef = "partition by hash(a)";

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

        alter = String.format("alter table %s partition by hash(b)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");
    }

    @Test
    public void testSplitKey() {
        String tableName = "dn_gen_col_split";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b), global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));

        String splitSql = String.format("alter table %s split into partitions 10 by hot value(1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);
        splitSql = String.format("alter table %s split into partitions 10 by hot value(1)",
            getRealGsiName(tddlConnection, tableName, gsiName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testAutoSplitKey() {
        String tableName = "dn_gen_col_auto_split";
        String gsiName = tableName + "_gsi";
        String tgName1 = tableName + "_tg_1";
        String tgName2 = tableName + "_tg_2";

        String createTgSql = String.format("create tablegroup %s", tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);
        createTgSql = String.format("create tablegroup %s", tgName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b), global index %s(b,a)) tablegroup=%s", tableName,
            gsiName,
            tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        createSql = String.format(
            "alter table %s set tablegroup=%s", getRealGsiName(tddlConnection, tableName, gsiName), tgName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));

        String splitSql = String.format("alter table %s split into partitions 1 by hot value(1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);
        splitSql = String.format("alter table %s split into partitions 10 by hot value(1)",
            getRealGsiName(tddlConnection, tableName, gsiName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testMovePartition() throws SQLException {
        String tableName = "dn_gen_move_part";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b), global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));

        String curInstId = null;
        String sql = String.format(
            "select storage_inst_id from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'",
            tddlDatabase1, tableName, "p1");
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        curInstId = rs.getString(1);

        sql = String.format("show ds where db ='%s'", tddlDatabase1);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Set<String> instIds = new HashSet<>();
        while (rs.next()) {
            if (!curInstId.equalsIgnoreCase(rs.getString("STORAGE_INST_ID"))) {
                instIds.add(rs.getString("STORAGE_INST_ID"));
            }
        }
        rs.close();

        sql = String.format("/*+TDDL:CMD_EXTRA(CN_ENABLE_CHANGESET=false)*/ alter table %s move partitions %s to '%s'",
            tableName, "p1", instIds.iterator().next());
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testMovePartitionChangeset() throws SQLException {
        String tableName = "dn_gen_move_part_cs";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b), global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));

        String curInstId = null;
        String sql = String.format(
            "select storage_inst_id from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'",
            tddlDatabase1, tableName, "p1");
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        curInstId = rs.getString(1);

        sql = String.format("show ds where db ='%s'", tddlDatabase1);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        Set<String> instIds = new HashSet<>();
        while (rs.next()) {
            if (!curInstId.equalsIgnoreCase(rs.getString("STORAGE_INST_ID"))) {
                instIds.add(rs.getString("STORAGE_INST_ID"));
            }
        }
        rs.close();

        sql = String.format("/*+TDDL:CMD_EXTRA(CN_ENABLE_CHANGESET=true)*/ alter table %s move partitions %s to '%s'",
            tableName, "p1", instIds.iterator().next());
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testAlterTableGroup() throws SQLException {
        String tableName = "dn_gen_col_alter_tg";
        String tgName1 = tableName + "_tg_1";
        String tgName2 = tableName + "_tg_2";

        String createTgSql = String.format("create tablegroup %s", tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);
        createTgSql = String.format("create tablegroup %s", tgName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b)) PARTITION BY RANGE (a) (\n"
                + "    PARTITION p0 VALUES LESS THAN (10),\n"
                + "    PARTITION p1 VALUES LESS THAN (20),\n"
                + "    PARTITION p2 VALUES LESS THAN (30),\n"
                + "    PARTITION p3 VALUES LESS THAN (40),\n"
                + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
                + ") tablegroup=%s", tableName, tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));

        String splitSql =
            String.format("/*+TDDL:CMD_EXTRA(CN_ENABLE_CHANGESET=false)*/ alter tablegroup %s split partition p4 into ("
                + "PARTITION p40 VALUES LESS THAN (50),"
                + "PARTITION p41 VALUES LESS THAN MAXVALUE)", tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testAlterTableGroupChangeset() throws SQLException {
        String tableName = "dn_gen_col_alter_tg_cs";
        String tgName1 = tableName + "_tg_1";
        String tgName2 = tableName + "_tg_2";

        String createTgSql = String.format("create tablegroup %s", tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);
        createTgSql = String.format("create tablegroup %s", tgName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b)) PARTITION BY RANGE (a) (\n"
                + "    PARTITION p0 VALUES LESS THAN (10),\n"
                + "    PARTITION p1 VALUES LESS THAN (20),\n"
                + "    PARTITION p2 VALUES LESS THAN (30),\n"
                + "    PARTITION p3 VALUES LESS THAN (40),\n"
                + "    PARTITION p4 VALUES LESS THAN MAXVALUE\n"
                + ") tablegroup=%s", tableName, tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));

        String splitSql = String.format(
            "/*+TDDL:CMD_EXTRA(CN_ENABLE_CHANGESET=true)*/ alter tablegroup %s split partition p4 into ("
                + "PARTITION p40 VALUES LESS THAN (50),"
                + "PARTITION p41 VALUES LESS THAN MAXVALUE)", tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
    }

    @Test
    public void testDmlFailed() {
        String tableName = "dn_gen_col_dml_fail_tbl";
        String create = String.format("create table %s (a int primary key, b int as (a+1))", tableName);
        String[] partDefs = new String[] {"", "single", "broadcast", "partition by hash(a)"};

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

    private void checkOnSingleCn() {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show mpp");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        org.junit.Assume.assumeTrue(objects.size() == 1);
    }

    @Test
    public void testOssTable() throws SQLException {
        checkOnSingleCn();

        String tableName = "dn_gen_col_archive";
        String gsiName = tableName + "_gsi";
        String ossName = tableName + "_oss";

        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) virtual, global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));

        try {
            String fileStoragePath = "'file_uri' ='file:///tmp/orc_" + System.currentTimeMillis() + "/'";
            String createDataSourceSql = "create filestorage local_disk with (" + fileStoragePath + ");";

            Statement stmt = tddlConnection.createStatement();
            stmt.execute(createDataSourceSql);
        } catch (SQLException t) {
            if (!StringUtils.contains(t.getMessage(), "FileStorage LOCAL_DISK already exists ")) {
                throw t;
            }
        }

        String archiveSql =
            String.format("create table %s like %s engine='local_disk' archive_mode='loading'", ossName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, archiveSql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, ossName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + ossName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }
    }

    @Test
    public void testCreateTableLike() throws SQLException {
        String tableName = "dn_gen_col_like";
        String likeTable = tableName + "_2";

        dropTableIfExists(tableName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) stored) partition by key(a,b) partitions 13",
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
            "create table %s (a int primary key, b int, c int as (a+b) stored comment '123') partition by key(a,b) partitions 13",
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
            "create table %s (a int primary key, b int, c int as (a+b) virtual comment '123') partition by key(a,b) partitions 13",
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
            String.format("create table %s (a int primary key, b int as (a+1)) partition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String sql = String.format("show columns from %s", tableName);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        boolean found = false;
        while (rs.next()) {
            if (rs.getString("Field").equalsIgnoreCase("b")) {
                Assert.assertTrue(rs.getString("Extra").equalsIgnoreCase("VIRTUAL GENERATED"));
                found = true;
            }
        }
        Assert.assertTrue(found);

        sql = String.format("show full columns from %s", tableName);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        found = false;
        while (rs.next()) {
            if (rs.getString("Field").equalsIgnoreCase("b")) {
                Assert.assertTrue(rs.getString("Extra").equalsIgnoreCase("VIRTUAL GENERATED"));
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
                Assert.assertTrue(rs.getString("EXTRA").equalsIgnoreCase("VIRTUAL GENERATED"));
                Assert.assertTrue(rs.getString("GENERATION_EXPRESSION").equalsIgnoreCase("(`a` + 1)"));
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testAlterTableUniqueIndexFailed() throws SQLException {
        if (isMySQL80()) {
            return;
        }

        String tableName = "dn_gen_col_alter_unique_fail";
        String createTable =
            String.format("create table %s (a int primary key, b int, c int as (a+b) stored, d int)", tableName);
        String partDef = " partition by hash(a)";
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

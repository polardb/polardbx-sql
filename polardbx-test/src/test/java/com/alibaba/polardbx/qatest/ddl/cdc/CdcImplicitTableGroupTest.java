package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.cdc.SQLHelper;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.executor.ddl.ImplicitTableGroupUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
@Slf4j
public class CdcImplicitTableGroupTest extends CdcBaseTest {

    private static final String dbNamePrefix = "cdc_implicit_tb_test_";
    private static final AtomicInteger dbNameSuffixSeed = new AtomicInteger(0);

    @Test
    public void testCreateTableWithDefaultSingleDb() {
        executeWithCallBack(parameter -> testCreateTableWithDefaultSingleDb(
                parameter.getSchemaName(),
                parameter.getStmt(),
                parameter.getReplaySqlList()), "cdc_default_single_db",
            "CREATE DATABASE if NOT EXISTS `cdc_default_single_db` "
                + "CHARSET = `utf8mb4` COLLATE = `utf8mb4_general_ci` MODE = 'auto' DEFAULT_SINGLE = 'on'");
    }

    @Test
    public void testCreateTableWithoutGsi() {
        executeWithCallBack(parameter -> testCreateTableWithoutGsi(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()));
    }

    @Test
    public void testCreateTableWithGsi() {
        executeWithCallBack(parameter -> testCreateTableWithGsi(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testCreateGlobalIndexExplicitDef() {
        executeWithCallBack(parameter -> testCreateGlobalIndexExplicitDef(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testCreateGlobalIndexImplicitDef() {
        executeWithCallBack(parameter -> testCreateGlobalIndexImplicitDef(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testAlterTableAddGsiExplicitDef() {
        executeWithCallBack(parameter -> testAlterTableAddGsiExplicitDef(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testAlterTableAddGsiImplicitDef() {
        executeWithCallBack(parameter -> testAlterTableAddGsiImplicitDef(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testAlterIndexImplicitTg() {
        executeWithCallBack(parameter -> testAlterIndexImplicitTg(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testSubPartitionTableBasic() {
        executeWithCallBack(parameter -> testSubPartitionTable_Split(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testSubPartitionTable_CreateEquivalent() {
        executeWithCallBack(parameter -> testSubPartitionTable_CreateEquivalent(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testSubPartitionTable_CreateDifferentColumn() {
        executeWithCallBack(parameter -> testSubPartitionTable_CreateDifferentColumn(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    @Test
    public void testModifyPartitions() {
        executeWithCallBack(parameter -> testModifyPartitions(
            parameter.getSchemaName(),
            parameter.getStmt(),
            parameter.getReplaySqlList()
        ));
    }

    private void executeWithCallBack(Consumer<Parameter> consumer) {
        String dbName = dbNamePrefix + dbNameSuffixSeed.incrementAndGet();
        executeWithCallBack(consumer, dbName, "create database " + dbName + "  mode = 'auto'");
    }

    @SneakyThrows
    private void executeWithCallBack(Consumer<Parameter> consumer, String dbName, String createDbSql) {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists " + dbName);
            stmt.executeUpdate(createDbSql);
            stmt.executeUpdate("use " + dbName);

            ArrayList<String> replaySqlList = new ArrayList<>();
            Parameter parameter = new Parameter();
            parameter.setStmt(stmt);
            parameter.setReplaySqlList(replaySqlList);
            parameter.setSchemaName(dbName);
            consumer.accept(parameter);

            if (!replaySqlList.isEmpty()) {
                log.info("start to replay all sql for {}.", dbName);
                stmt.executeUpdate("drop database if exists " + dbName);
                stmt.execute("use polardbx");
                stmt.executeUpdate(createDbSql);
                stmt.executeUpdate("use " + dbName);
                for (String sql : replaySqlList) {
                    try {
                        log.info("prepare to replay ddl sql : {}", sql);
                        stmt.execute(sql);
                        log.info("sql is successfully replayed : {}", sql);
                    } catch (Throwable t) {
                        log.error("sql replayed error!!", t);
                        throw t;
                    }
                }
            }
        }
    }

    @SneakyThrows
    private void testCreateTableWithDefaultSingleDb(String schemaName, Statement stmt, List<String> replaySqlList) {
        String createSql1 = "CREATE TABLE `t1` ( "
            + "`id` int(11) NOT NULL AUTO_INCREMENT, "
            + "`k` int(11) NOT NULL DEFAULT '0', "
            + "`c` char(120) NOT NULL DEFAULT '', "
            + "`pad` char(60) NOT NULL DEFAULT '', "
            + "PRIMARY KEY (`id`) ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
        stmt.execute(createSql1);
        String markSql = getMarkSqlForImplicitTableGroup(schemaName, "t1");
        Set<String> defTg1 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t1", markSql, defTg1);
        replaySqlList.add(markSql);

        String createSql2 = "CREATE TABLE `t2` ( "
            + "`id` int(11) NOT NULL AUTO_INCREMENT, "
            + "`k` int(11) NOT NULL DEFAULT '0', "
            + "`c` char(120) NOT NULL DEFAULT '', "
            + "`pad` char(60) NOT NULL DEFAULT '', "
            + "PRIMARY KEY (`id`) ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
        stmt.execute(createSql2);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t2");
        Set<String> defTg2 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t2", markSql, defTg2);
        replaySqlList.add(markSql);

        String createSql3 = "CREATE TABLE `t3` ( "
            + "`id` int(11) NOT NULL AUTO_INCREMENT, "
            + "`k` int(11) NOT NULL DEFAULT '0', "
            + "`c` char(120) NOT NULL DEFAULT '', "
            + "`pad` char(60) NOT NULL DEFAULT '', "
            + "PRIMARY KEY (`id`) ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
        stmt.execute(createSql3);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t3");
        Set<String> defTg3 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t3", markSql, defTg3);
        replaySqlList.add(markSql);

        String createSql4 = "CREATE TABLE `t4` ( "
            + "`id` int(11) NOT NULL AUTO_INCREMENT, "
            + "`k` int(11) NOT NULL DEFAULT '0', "
            + "`c` char(120) NOT NULL DEFAULT '', "
            + "`pad` char(60) NOT NULL DEFAULT '', "
            + "PRIMARY KEY (`id`) ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
        stmt.execute(createSql4);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t4");
        Set<String> defTg4 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t4", markSql, defTg4);
        replaySqlList.add(markSql);

        //Assert.assertEquals(defTg1, defTg3);
        //Assert.assertEquals(defTg2, defTg4);
    }

    @SneakyThrows
    private void testCreateTableWithoutGsi(String schemaName, Statement stmt, List<String> replaySqlList) {
        // execute and check for basic explicit partition table
        String tableName = "tb_" + RandomUtils.nextLong();
        String createSql = "create table " + tableName + "("
            + "a int primary key,"
            + "b varchar(20)) "
            + "partition by key(a) partitions 3";
        executeAndCheckCreateTable(stmt, schemaName, tableName, createSql, replaySqlList);

        // execute and check for basic implicit partition table
        tableName = "tb_" + RandomUtils.nextLong();
        createSql = "create table " + tableName + "("
            + "a int primary key,"
            + "b varchar(20)) ";
        executeAndCheckCreateTable(stmt, schemaName, tableName, createSql, replaySqlList);

        // execute and check for single table
        tableName = "tb_" + RandomUtils.nextLong();
        createSql = "create table " + tableName + "("
            + "a int primary key,"
            + "b varchar(20)) single";
        executeAndCheckCreateTable(stmt, schemaName, tableName, createSql, replaySqlList);

        // execute and check for broadcast table
        tableName = "tb_" + RandomUtils.nextLong();
        createSql = "create table " + tableName + "("
            + "a int primary key,"
            + "b varchar(20)) broadcast";
        executeAndCheckCreateTable(stmt, schemaName, tableName, createSql, replaySqlList);

        // execute and check for single table with auto_partition = 0
        tableName = "tb_" + RandomUtils.nextLong();
        String sqlBefore = "set @auto_partition=0";
        String sqlAfter = "set @auto_partition=1";
        createSql = "create table " + tableName + "("
            + "a int primary key,"
            + "b varchar(20))";
        stmt.execute(sqlBefore);
        stmt.execute(createSql);
        Set<String> tableGroups = new HashSet<>();
        String markSql = getMarkSqlForImplicitTableGroup(schemaName, tableName);
        implicitTableGroupChecker.checkSql(schemaName, tableName, markSql, tableGroups);
        stmt.execute(sqlAfter);
    }

    @SneakyThrows
    private void testCreateTableWithGsi(String schemaName, Statement stmt, List<String> replaySqlList) {
        String tableName = "tb_" + RandomUtils.nextLong();
        String createSql = "create table " + tableName + "("
            + " a int primary key,"
            + " b int,"
            + " c int,"
            + " d varchar(10),"
            + " global index (b) partition by key(b) partitions 2, "
            + " unique global index g2(c) partition by key(c) partitions 2, "
            + " unique global key g3(c) partition by key(c) partitions 2,"
            + " global unique index g4(c) partition by key(c) partitions 2,"
            + " global unique key g5(c) partition by key(c) partitions 2)"
            + " partition by key(a) partitions 3";
        executeAndCheckCreateTable(stmt, schemaName, tableName, createSql, replaySqlList);

        tableName = "tb_" + RandomUtils.nextLong();
        createSql = "create table " + tableName + "("
            + " a int primary key,"
            + " b int,"
            + " c int,"
            + " d varchar(10) unique,"
            + " index (b),"
            + " index (b),"
            + " key (b),"
            + " key (b),"
            + " unique key (b),"
            + " unique key (b),"
            + " unique index (b),"
            + " unique index (b),"
            + " index g1(b),"
            + " key g2(b), "
            + " unique key g3(b), "
            + " unique index g4(b))";
        executeAndCheckCreateTable(stmt, schemaName, tableName, createSql, replaySqlList);
    }

    @SneakyThrows
    private void testCreateGlobalIndexExplicitDef(String schemaName, Statement stmt, List<String> replayList) {
        String tableName1 = "tb_" + RandomUtils.nextLong();
        String tableName2 = "tb_" + RandomUtils.nextLong();
        List<String> tableNames = Lists.newArrayList(tableName1, tableName2);
        for (String tableName : tableNames) {
            String createSql = "create table " + tableName + "("
                + " id int primary key,"
                + " a int,"
                + " b int,"
                + " c int,"
                + " d varchar(10))"
                + " partition by key(a) partitions 3";
            stmt.execute(createSql);
            replayList.add(createSql);
        }

        // create global index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create global index g1 on " + tableName1 + "(a) partition by key(a) partitions 2", replayList);

        // create global index again
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create global index g11 on " + tableName1 + "(a) partition by key(a) partitions 2", replayList);

        // create global index again with different partition rule
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create global index g12 on " + tableName1 + "(a) partition by key(a) partitions 8", replayList);

        //create global unique index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create global unique index g2 on " + tableName1 + "(b) partition by key(b) partitions 3", replayList);

        //create global unique index again
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create global unique index g21 on " + tableName1 + "(b) partition by key(b) partitions 3", replayList);

        //create global unique index again with different partition rule
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create global unique index g22 on " + tableName1 + "(b) partition by key(b) partitions 8", replayList);

        //create unique global index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique global index g3 on " + tableName1 + "(c) partition by key(c) partitions 3", replayList);

        //create unique global index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique global index g31 on " + tableName1 + "(c) partition by key(c) partitions 3", replayList);

        //create unique global index with different partition rule
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique global index g32 on " + tableName1 + "(c) partition by key(c) partitions 6", replayList);
    }

    @SneakyThrows
    private void testCreateGlobalIndexImplicitDef(String schemaName, Statement stmt, List<String> replaySqlList) {
        String tableName1 = "tb_" + RandomUtils.nextLong();
        String tableName2 = "tb_" + RandomUtils.nextLong();
        List<String> tableNames = Lists.newArrayList(tableName1, tableName2);
        for (String tableName : tableNames) {
            String createSql = "create table " + tableName + "("
                + " id int primary key,"
                + " a int,"
                + " b varchar(10),"
                + " c int,"
                + " d varchar(10))";
            stmt.execute(createSql);
            replaySqlList.add(createSql);
        }

        // create index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create index g1 on " + tableName1 + "(a)", replaySqlList);

        // create index again
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create index g11 on " + tableName1 + "(a)", replaySqlList);

        // create index again
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create index g12 on " + tableName1 + "(a)", replaySqlList);

        //create unique index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique index g2 on " + tableName1 + "(b)", replaySqlList);

        //create unique index again
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique index g21 on " + tableName1 + "(b)", replaySqlList);

        //create unique index again
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique index g22 on " + tableName1 + "(b)", replaySqlList);

        //create unique key
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique key g3 on " + tableName1 + "(c)", replaySqlList);

        //create unique key again
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique key g31 on " + tableName1 + "(c)", replaySqlList);

        //create unique key again
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "create unique key g32 on " + tableName1 + "(c)", replaySqlList);
    }

    @SneakyThrows
    private void testAlterTableAddGsiExplicitDef(String schemaName, Statement stmt, List<String> replaySqlList) {
        String tableName1 = "tb_" + RandomUtils.nextLong();
        String tableName2 = "tb_" + RandomUtils.nextLong();
        List<String> tableNames = Lists.newArrayList(tableName1, tableName2);
        for (String tableName : tableNames) {
            String createSql = "create table " + tableName + "("
                + " id int primary key,"
                + " a int,"
                + " b int,"
                + " c int,"
                + " d varchar(10))"
                + " partition by key(a) partitions 3";
            stmt.execute(createSql);
            replaySqlList.add(getMarkSqlForImplicitTableGroup(schemaName, tableName));
        }

        // add global index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add global index g1(a) partition by key(a) partitions 2",
            replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add global index g12(a) partition by key(a) partitions 2",
            replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add global index g13(a) partition by key(a) partitions 5",
            replaySqlList);

        // add global unique index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add global unique index g2(b) partition by key(b) partitions 3",
            replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add global unique index g21(b) partition by key(b) partitions 7",
            replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add global unique index g22(b) partition by key(b) partitions 9",
            replaySqlList);

        // add unique global index
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique global index g3(c) partition by key(c) partitions 2",
            replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique global index g31(c) partition by key(c) partitions 3",
            replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique global index g32(c) partition by key(c) partitions 10",
            replaySqlList);
    }

    @SneakyThrows
    private void testAlterTableAddGsiImplicitDef(String schemaName, Statement stmt, List<String> replaySqlList) {
        String tableName1 = "tb_" + RandomUtils.nextLong();
        String tableName2 = "tb_" + RandomUtils.nextLong();
        List<String> tableNames = Lists.newArrayList(tableName1, tableName2);
        for (String tableName : tableNames) {
            String createSql = "create table " + tableName + "("
                + " id int primary key,"
                + " a int,"
                + " b varchar(10),"
                + " c int,"
                + " d varchar(10))";
            stmt.execute(createSql);
            replaySqlList.add(createSql);
        }

        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add key k1(a)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add key k2(b)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add key k3(c)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add key (c)", replaySqlList);
        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add key (c), add key (c)", replaySqlList);
        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add key (a), add key (b), add key(c)", replaySqlList);

        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique key k4(a)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique key k5(b)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique key k6(c)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique key (c)", replaySqlList);
        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique key (c), add unique key (c)", replaySqlList);
        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique key (a), add unique key (b), add unique key (c)", replaySqlList);

        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add index idx1(a)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add index idx2(b)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add index idx3(c)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add index (b)", replaySqlList);
        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add index (b), add index(b)", replaySqlList);
        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add index (a), add index (b), add index (c)", replaySqlList);

        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique index idx4(a)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique index idx5(b)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique index idx6(c)", replaySqlList);
        executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique index (b)", replaySqlList);
        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique index (b), add unique index (b)", replaySqlList);
        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add unique index (a), add unique index (b), add unique index (c)",
            replaySqlList);

        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1
                + " add key kk1(a), add unique key kk2(b), add index kk3(c), add unique index kk4(d)", replaySqlList);

        executeAndCheckCreateIndexMulti(stmt, schemaName, tableName1, tableName2,
            "alter table " + tableName1 + " add key (a), add unique key (b), add index(c), add unique index(d)",
            replaySqlList);
    }

    @SneakyThrows
    private void testAlterIndexImplicitTg(String schemaName, Statement stmt, List<String> replaySqlList) {
        String tableName1 = "lc_rc_tp1_" + RandomUtils.nextLong();
        String tableName2 = "lc_rc_tp1_" + RandomUtils.nextLong();
        List<String> tableNames = Lists.newArrayList(tableName1, tableName2);
        for (String tableName : tableNames) {
            String sql1 = "create table if not exists " + tableName + " ( "
                + "a bigint unsigned not null, "
                + "b bigint unsigned not null, "
                + "c datetime NOT NULL, "
                + "d varchar(16) NOT NULL, "
                + "e varchar(16) NOT NULL ) "
                + "partition by list columns (a,b) "
                + "subpartition by range columns (c,d) subpartition template ( "
                + "     subpartition sp0 values less than ('2020-01-01','a'), "
                + "     subpartition sp1 values less than (maxvalue,maxvalue) ) ( "
                + "     partition p0 values in ((5,5),(6,6)), "
                + "     partition p1 values in ((7,7),(8,8)) "
                + ")";
            String sql2 = "create global index `gsi_lc` on `" + tableName + "` ( a,b,c,d ) "
                + "partition by list columns (a,b) ( "
                + "     partition p0 values in ((5,5),(6,6),(9,9)), "
                + "     partition p1 values in ((7,7),(8,8)) "
                + ")";
            String sql3 = "create global index `gsi_rc` on `" + tableName + "` ( c,d,a,b ) "
                + "partition by range columns (c,d) ( "
                + "     partition sp0 values less than ('2021-01-01','a'), "
                + "     partition sp1 values less than (maxvalue,maxvalue) "
                + ")";
            String sql4 = "create global index `gsi_lc_rc` on `" + tableName + "` ( a,b,c,d ) "
                + "partition by list columns (a,b) subpartition by range columns (c,d) subpartition template ( "
                + "     subpartition sp0 values less than ('2021-01-01','a'), "
                + "     subpartition sp1 values less than (maxvalue,maxvalue) "
                + ") ( partition p0 values in ((5,5),(6,6),(9,9)), partition p1 values in ((7,7),(8,8)) )";
            stmt.execute(sql1);
            stmt.execute(sql2);
            stmt.execute(sql3);
            stmt.execute(sql4);
            replaySqlList.add(sql1);
            replaySqlList.add(sql2);
            replaySqlList.add(sql3);
            replaySqlList.add(sql4);
        }

        String sql5 = "/*# add **/alter index gsi_lc on table " + tableName1
            + " add partition ( partition p2 values in ((11,11),(10,10)) )";
        executeAlterIndexSql(stmt, schemaName, tableName1, tableName2, sql5, replaySqlList);

        String sql6 = "alter index gsi_lc_rc on table " + tableName1 + " modify partition p1 add values ( (15,15) )";
        executeAlterIndexSql(stmt, schemaName, tableName1, tableName2, sql6, replaySqlList);

        String sql7 = "/*# split **/alter index gsi_lc_rc on table " + tableName1 + " split subpartition sp1 into ( "
            + "     subpartition sp1 values less than ('2022-01-01','a'), "
            + "     subpartition sp2 values less than (maxvalue, maxvalue) "
            + ")";
        executeAlterIndexSql(stmt, schemaName, tableName1, tableName2, sql7, replaySqlList);

        String sql8 = "alter index gsi_lc_rc on table " + tableName1 + " merge subpartitions sp1,sp2 to sp1";
        executeAlterIndexSql(stmt, schemaName, tableName1, tableName2, sql8, replaySqlList);

        String sql9 = "/*# reorg*/alter index gsi_lc_rc on table " + tableName1 +
            " reorganize subpartition sp0,sp1 into ( "
            + "     subpartition sp4 values less than ('2021-01-01','a'), "
            + "     subpartition sp5 values less than ('2028-01-01','a'), "
            + "     subpartition sp3 values less than (maxvalue, maxvalue) "
            + ")";
        executeAlterIndexSql(stmt, schemaName, tableName1, tableName2, sql9, replaySqlList);

        String sql10 = "/*# rename*/alter index gsi_lc_rc on table " + tableName1 + " "
            + "rename subpartition sp4 to sp0, sp3 to sp1";
        executeAlterIndexSql(stmt, schemaName, tableName1, tableName2, sql10, replaySqlList);
    }

    @SneakyThrows
    private void testSubPartitionTable_Split(String schemaName, Statement stmt, List<String> replaySqlList) {
        // test split partition
        String sqlTemplate = "CREATE TABLE `%s` ( "
            + "`a` bigint(20) UNSIGNED NOT NULL, "
            + "`b` bigint(20) UNSIGNED NOT NULL, "
            + "`c` datetime NOT NULL, "
            + "`d` varchar(16) NOT NULL, "
            + "`e` varchar(16) NOT NULL ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 "
            + "PARTITION BY HASH(`c`,`d`) PARTITIONS 4 SUBPARTITION BY HASH(`a`,`b`) SUBPARTITIONS 4";

        String table1 = "t_sub_partition_implicit_1";
        String table2 = "t_sub_partition_implicit_2";
        String sql1 = String.format(sqlTemplate, table1);
        String sql2 = String.format(sqlTemplate, table2);
        stmt.execute(sql1);
        stmt.execute(sql2);
        replaySqlList.add(sql1);
        replaySqlList.add(sql2);

        String sql3 = "alter table " + table1 + " split partition p1";
        stmt.execute(sql3);
        String markSql = getMarkSqlForImplicitTableGroup(schemaName, table1);
        Set<String> tableGroups = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, table1, markSql, tableGroups);
        replaySqlList.add(markSql);

        String rewriteSql = ImplicitTableGroupUtil.rewriteTableName(markSql, table2);
        stmt.execute(rewriteSql);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, table2);
        Set<String> tableGroups2 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, table2, markSql, tableGroups2);
        Assert.assertEquals(tableGroups, tableGroups2);
        replaySqlList.add(markSql);
    }

    @SneakyThrows
    private void testSubPartitionTable_CreateEquivalent(String schemaName, Statement stmt, List<String> replaySqlList) {
        // test equivalent create table
        String createSqlDef1 = "create table if not exists k_lc_tp1 ( "
            + "a bigint unsigned not null, "
            + "b bigint unsigned not null, "
            + "c datetime NOT NULL, "
            + "d varchar(16) NOT NULL, "
            + "e varchar(16) NOT NULL ) "
            + "partition by key (c,d) partitions 2 "
            + "subpartition by list columns (a,b) ( subpartition sp0 values in ((5,5),(6,6)), subpartition sp1 values in ((7,7),(8,8)) )";
        stmt.execute(createSqlDef1);
        String markSql = getMarkSqlForImplicitTableGroup(schemaName, "k_lc_tp1");
        Set<String> defTg1 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "k_lc_tp1", markSql, defTg1);
        replaySqlList.add(markSql);

        String createSqlDef2 = "create table if not exists k_lc_tp1st ( "
            + "a bigint unsigned not null, "
            + "b bigint unsigned not null, "
            + "c datetime NOT NULL, "
            + "d varchar(16) NOT NULL, "
            + "e varchar(16) NOT NULL ) "
            + "partition by key (c,d) partitions 2 "
            + "subpartition by list columns (a,b) subpartition template ( subpartition sp0 values in ((5,5),(6,6)), subpartition sp1 values in ((7,7),(8,8)) )";
        stmt.execute(createSqlDef2);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "k_lc_tp1st");
        Set<String> defTg2 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "k_lc_tp1st", markSql, defTg2);
        replaySqlList.add(markSql);

        String createSqlDef3 = "create table if not exists k_lc_tp2 ( "
            + "a bigint unsigned not null, "
            + "b bigint unsigned not null, "
            + "c datetime NOT NULL, "
            + "d varchar(16) NOT NULL, "
            + "e varchar(16) NOT NULL ) "
            + "partition by key (c,d) "
            + "subpartition by list columns (a,b) ( subpartition sp0 values in ((5,5),(6,6)), subpartition sp1 values in ((7,7),(8,8)) ) ( partition p1, partition p2 )";
        stmt.execute(createSqlDef3);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "k_lc_tp2");
        Set<String> defTg3 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "k_lc_tp2", markSql, defTg3);
        replaySqlList.add(markSql);

        Assert.assertEquals(defTg1, defTg2);
        Assert.assertEquals(defTg1, defTg3);
    }

    @SneakyThrows
    private void testSubPartitionTable_CreateDifferentColumn(String schemaName, Statement stmt,
                                                             List<String> replaySqlList) {
        String createSql1 = "CREATE TABLE L_K_T1(A INT, B INT) "
            + "PARTITION BY LIST(A) SUBPARTITION BY KEY(B) SUBPARTITIONS 3 ( "
            + "PARTITION P1 VALUES IN (1,2,3), "
            + "PARTITION P2 VALUES IN (5,6,7))";
        stmt.execute(createSql1);
        String markSql = getMarkSqlForImplicitTableGroup(schemaName, "l_k_t1");
        Set<String> defTg1 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "l_k_t1", markSql, defTg1);
        replaySqlList.add(markSql);

        String createSql2 = "CREATE TABLE L_KC_T1(A INT, B INT) "
            + "PARTITION BY LIST(A) SUBPARTITION BY KEY(B,A) SUBPARTITIONS 3 ( "
            + "PARTITION P1 VALUES IN (1,2,3), "
            + "PARTITION P2 VALUES IN (5,6,7))";
        stmt.execute(createSql2);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "l_kc_t1");
        Set<String> defTg2 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "l_kc_t1", markSql, defTg2);
        replaySqlList.add(markSql);

        Assert.assertEquals(defTg1, defTg2);
    }

    @SneakyThrows
    private void testModifyPartitions(String schemaName, Statement stmt, List<String> replaySqlList) {
        // rename partition
        String createSqlTemplate = "CREATE TABLE `%s` (\n"
            + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c_bit_1` bit(1) DEFAULT NULL,\n"
            + "  `c_bit_8` bit(8) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT '10000000'\n"
            + "PARTITION BY RANGE(`id`)\n"
            + "(PARTITION `p1` VALUES LESS THAN (100040) ENGINE = InnoDB,\n"
            + " PARTITION `p2` VALUES LESS THAN (100080) ENGINE = InnoDB,\n"
            + " PARTITION `p3` VALUES LESS THAN (100120) ENGINE = InnoDB,\n"
            + " PARTITION `p4` VALUES LESS THAN (100160) ENGINE = InnoDB,\n"
            + " PARTITION `p5` VALUES LESS THAN (100200) ENGINE = InnoDB,\n"
            + " PARTITION `p6` VALUES LESS THAN (100240) ENGINE = InnoDB,\n"
            + " PARTITION `p7` VALUES LESS THAN (100280) ENGINE = InnoDB,\n"
            + " PARTITION `p8` VALUES LESS THAN (100320) ENGINE = InnoDB)";
        String tableName1 = "tT1";
        String tableName2 = "tT2";
        String createSql1 = String.format(createSqlTemplate, tableName1);
        String createSql2 = String.format(createSqlTemplate, tableName2);
        String renamePartitionSql = "alter table " + tableName1 + " rename partition p1 to p1";

        stmt.execute(createSql1);
        String markSql = getMarkSqlForImplicitTableGroup(schemaName, tableName1);
        Set<String> defTg1 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, tableName1, markSql, defTg1);
        replaySqlList.add(markSql);

        stmt.execute(createSql2);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, tableName2);
        Set<String> defTg2 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, tableName2, markSql, defTg2);
        replaySqlList.add(markSql);

        stmt.execute(renamePartitionSql);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, tableName1);
        Set<String> defTg3 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, tableName1, markSql, defTg3);
        replaySqlList.add(markSql);

        Assert.assertEquals(defTg1, defTg2);
        Assert.assertEquals(defTg1, defTg3);

        // modify partition column
        String createSql4Modify = "create table t_modify("
            + " a int(11) not null, "
            + " b tinyint(11) default null,"
            + " c bigint(19) default null,"
            + " GLOBAL INDEX `gsi_1` (`b`) COVERING (`a`)\n"
            + "    PARTITION BY KEY(`b`)\n"
            + "    PARTITIONS 3,\n"
            + " GLOBAL INDEX `gsi_2` (`b`) COVERING (`a`)\n"
            + "    PARTITION BY KEY(`b`)\n"
            + "    PARTITIONS 5,\n"
            + " GLOBAL INDEX `gsi_3` (`c`) COVERING (`a`)\n"
            + "    PARTITION BY KEY(`c`)\n"
            + "    PARTITIONS 5,\n"
            + " index idx4 (b),"
            + " index idx5 (a),"
            + " primary key(a)) "
            + " partition by key(`b`,`c`) partitions 3 "
            + " subpartition by key (a, b) subpartitions 4";
        stmt.execute(createSql4Modify);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t_modify");
        Set<String> defTg4 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t_modify", markSql, defTg4);
        Assert.assertEquals(4, defTg4.size());
        replaySqlList.add(markSql);

        String modifyPartitionColSql = "alter table t_modify modify column b mediumint";
        stmt.execute(modifyPartitionColSql);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t_modify");
        SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) SQLHelper.parseSql(markSql);
        Assert.assertNotNull(alterTableStatement.getTargetImplicitTableGroup());
        Set<String> defTg5 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t_modify", markSql, defTg5);
        Assert.assertEquals(3, defTg5.size());
        replaySqlList.add(markSql);
        Assert.assertNotEquals(defTg4, defTg5);

        stmt.execute(modifyPartitionColSql);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t_modify");
        alterTableStatement = (SQLAlterTableStatement) SQLHelper.parseSql(markSql);
        Assert.assertNotNull(alterTableStatement.getTargetImplicitTableGroup());
        replaySqlList.add(markSql);

        // modify partition column with omc
        String createSql4OMC = "CREATE TABLE `omc_with_replace_2xe` (\n"
            + "  `a` int(11) DEFAULT NULL,\n"
            + "  `b` int(11) DEFAULT NULL,\n"
            + "  GLOBAL INDEX `omc_with_replace_2xe_idx_1` (`b`) COVERING (`a`)\n"
            + "    PARTITION BY KEY(`b`)\n"
            + "    PARTITIONS 3,\n"
            + "  UNIQUE KEY `b` (`b`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100838 DEFAULT CHARSET = utf8mb4\n"
            + " PARTITION BY KEY(`b`)\n"
            + " PARTITIONS 3";
        stmt.execute(createSql4OMC);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "omc_with_replace_2xe");
        Set<String> defTg6 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "omc_with_replace_2xe", markSql, defTg6);
        Assert.assertEquals(1, defTg6.size());
        replaySqlList.add(markSql);

        String modifyPartition4OMC = "/*+TDDL:CMD_EXTRA(OMC_FORCE_TYPE_CONVERSION=TRUE,OMC_ALTER_TABLE_WITH_GSI=TRUE)*/"
            + " ALTER TABLE omc_with_replace_2xe MODIFY COLUMN b bigint";
        stmt.execute(modifyPartition4OMC);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "omc_with_replace_2xe");
        alterTableStatement = (SQLAlterTableStatement) SQLHelper.parseSql(markSql);
        Assert.assertNotNull(alterTableStatement.getTargetImplicitTableGroup());
        Set<String> defTg7 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "omc_with_replace_2xe", markSql, defTg7);
        replaySqlList.add(markSql);
        Assert.assertNotEquals(defTg6, defTg7);

        // modify partition column with ugsi
        String createSql4Ugsi = "CREATE TABLE `modify_sk_gsi_test_tblO` (\n"
            + "  `a` int(11) NOT NULL,\n"
            + "  `b` bigint(11) DEFAULT NULL,\n"
            + "  `c` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`a`),\n"
            + "  UNIQUE CLUSTERED INDEX `modify_sk_gsi_test_tbl_idxKCP` (`b`)\n"
            + "    PARTITION BY KEY(`b`)\n"
            + "    PARTITIONS 3\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`c`)\n"
            + "PARTITIONS 3";
        stmt.execute(createSql4Ugsi);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "modify_sk_gsi_test_tblO");
        Set<String> defTg8 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "modify_sk_gsi_test_tblO", markSql, defTg8);
        Assert.assertEquals(2, defTg8.size());
        replaySqlList.add(markSql);

        String modifyPartition4Ugsi = "ALTER TABLE modify_sk_gsi_test_tblO MODIFY COLUMN c bigint";
        stmt.execute(modifyPartition4Ugsi);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "modify_sk_gsi_test_tblO");
        Set<String> defTg9 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "modify_sk_gsi_test_tblO", markSql, defTg9);
        Assert.assertEquals(1, defTg9.size());
        replaySqlList.add(markSql);

        // test auto partition
        String autoPartitionCreateSql = "create table t_auto_partition("
            + " a int(11) not null, "
            + " b tinyint(11) default null,"
            + " c bigint(19) default null,"
            + " INDEX `gsi_1` (`a`),"
            + " INDEX `gsi_2` (`b`),"
            + " INDEX `gsi_3` (`c`),"
            + " primary key(a))";
        stmt.execute(autoPartitionCreateSql);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t_auto_partition");
        Set<String> defTg10 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t_auto_partition", markSql, defTg10);
        Assert.assertEquals(3, defTg10.size());
        replaySqlList.add(markSql);

        String modifyAutoPartitionSql = "alter table t_auto_partition modify column a bigint not null";
        stmt.execute(modifyAutoPartitionSql);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t_auto_partition");
        Set<String> defTg11 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t_auto_partition", markSql, defTg11);
        Assert.assertEquals(1, defTg11.size());
        replaySqlList.add(markSql);

        String modifyAutoPartitionSql2 = "alter table t_auto_partition modify column b bigint not null";
        stmt.execute(modifyAutoPartitionSql2);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t_auto_partition");
        Set<String> defTg12 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t_auto_partition", markSql, defTg12);
        Assert.assertEquals(1, defTg11.size());
        replaySqlList.add(markSql);

        String createTable4RemovePartition1 =
            "create table t1(a int, b bigint, c int, index i1(b)) partition by key(a) partitions 2";
        stmt.execute(createTable4RemovePartition1);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t1");
        replaySqlList.add(markSql);

        String createTable4RemovePartition2 =
            "create table t2(a int, b bigint, c int, index i1(a)) partition by key(a) partitions 2";
        stmt.execute(createTable4RemovePartition2);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t2");
        replaySqlList.add(markSql);

        String createTable4RemovePartition3 =
            "create table t3(a int, b bigint, c int, index i1(a), primary key(a)) partition by key(a) partitions 2";
        stmt.execute(createTable4RemovePartition3);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t3");
        replaySqlList.add(markSql);

        String alterTable4RemovePartition1 = "alter table t1 remove partitioning";
        stmt.execute(alterTable4RemovePartition1);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t1");
        Set<String> tgRemove1 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t1", markSql, tgRemove1);
        Assert.assertEquals(1, tgRemove1.size());
        replaySqlList.add(markSql);

        String alterTable4RemovePartition2 = "alter table t2 remove partitioning";
        stmt.execute(alterTable4RemovePartition2);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t2");
        Set<String> tgRemove2 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t2", markSql, tgRemove2);
        Assert.assertEquals(2, tgRemove2.size());
        replaySqlList.add(markSql);

        String alterTable4RemovePartition3 = "alter table t3 remove partitioning";
        stmt.execute(alterTable4RemovePartition3);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t3");
        Set<String> tgRemove3 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t3", markSql, tgRemove3);
        Assert.assertEquals(1, tgRemove3.size());
        replaySqlList.add(markSql);

        // test modify partition numbers
        String createTable4ModifyPartitionNum = "/*+TDDL:cmd_extra(AUTO_PARTITION=true)*/"
            + "create table if not exists t_modify_partition_num(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a))";
        stmt.execute(createTable4ModifyPartitionNum);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t_modify_partition_num");
        Set<String> tgModifyPartitionNum = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t_modify_partition_num", markSql, tgModifyPartitionNum);
        Assert.assertEquals(1, tgModifyPartitionNum.size());
        replaySqlList.add(markSql);

        String alterTable4ModifyPartitionNum =
            "/*+TDDL:cmd_extra(GSI_BUILD_LOCAL_INDEX_LATER=true,GSI_BACKFILL_BY_PK_RANGE=true,GSI_BACKFILL_BY_PK_PARTITION=false)*/ "
                + "alter table t_modify_partition_num partitions 31";
        stmt.execute(alterTable4ModifyPartitionNum);
        markSql = getMarkSqlForImplicitTableGroup(schemaName, "t_modify_partition_num");
        Set<String> tgModifyPartitionNum2 = new HashSet<>();
        implicitTableGroupChecker.checkSql(schemaName, "t_modify_partition_num", markSql, tgModifyPartitionNum2);
        Assert.assertEquals(1, tgModifyPartitionNum2.size());
        replaySqlList.add(markSql);
    }

    private void executeAndCheckCreateIndexMulti(Statement stmt, String schemaName, String tableName1,
                                                 String tableName2, String sql,
                                                 List<String> replaySqlList) {
        try {
            executeAndCheckCreateIndex(stmt, schemaName, tableName1, tableName2, sql, replaySqlList);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(
                StringUtils.contains(e.getMessage(), "Multi alter specifications when create GSI not support yet"));
        }
    }

    private void executeAndCheckCreateIndex(Statement stmt, String schemaName, String tableName1, String tableName2,
                                            String sql, List<String> replaySqlList)
        throws SQLException {
        // create index
        Set<String> tableGroups1 = new HashSet<>();
        stmt.execute(sql);
        System.out.println("submit sql : " + sql);
        String markSql1 = getMarkSqlForImplicitTableGroup(schemaName, tableName1);
        implicitTableGroupChecker.checkSql(schemaName, tableName1, markSql1, tableGroups1);
        System.out.println("mark sql : " + markSql1);
        replaySqlList.add(markSql1);

        // create index with implicit table group
        Set<String> tableGroups2 = new HashSet<>();
        String addIndexSql2 = ImplicitTableGroupUtil.rewriteTableName(markSql1, tableName2);
        stmt.execute(addIndexSql2);
        System.out.println("submit sql : " + addIndexSql2);
        String markSql2 = getMarkSqlForImplicitTableGroup(schemaName, tableName2);
        implicitTableGroupChecker.checkSql(schemaName, tableName2, markSql2, tableGroups2);
        assertSqlEquals(addIndexSql2, markSql2);
        Assert.assertEquals(tableGroups1, tableGroups2);
        System.out.println("mark sql : " + markSql2);
        replaySqlList.add(markSql2);

        System.out.println();
    }

    private void executeAndCheckCreateTable(Statement stmt, String dbName, String tableName, String createSql,
                                            List<String> replaySqlList)
        throws SQLException {
        // execute create sql & check mark sql
        stmt.execute(createSql);
        Set<String> tableGroups = new HashSet<>();
        String markSql = getMarkSqlForImplicitTableGroup(dbName, tableName);
        implicitTableGroupChecker.checkSql(dbName, tableName, markSql, tableGroups);
        replaySqlList.add(markSql);

        // execute create sql with implicit table group & check mark sql
        String newTableName = "tb_new_" + RandomUtils.nextLong();
        String rewriteSql = ImplicitTableGroupUtil.rewriteTableName(markSql, newTableName);
        stmt.execute(rewriteSql);

        Set<String> tableGroups2 = new HashSet<>();
        markSql = getMarkSqlForImplicitTableGroup(dbName, newTableName);
        implicitTableGroupChecker.checkSql(dbName, newTableName, markSql, tableGroups2);
        assertSqlEquals(rewriteSql, markSql);
        Assert.assertEquals(tableGroups, tableGroups2);
        replaySqlList.add(markSql);

        // drop table
        String dropSql1 = "drop table " + tableName;
        String dropSql2 = "drop table " + newTableName;
        stmt.execute(dropSql1);
        stmt.execute(dropSql2);
        Set<String> allTableGroups = getTableGroups(stmt);
        Assert.assertTrue(Sets.intersection(allTableGroups, tableGroups).isEmpty());
        replaySqlList.add(dropSql1);
        replaySqlList.add(dropSql2);

        // re-execute mark sql again & check mark sql
        stmt.execute(rewriteSql);
        Set<String> tableGroups3 = new HashSet<>();
        markSql = getMarkSqlForImplicitTableGroup(dbName, newTableName);
        implicitTableGroupChecker.checkSql(dbName, newTableName, markSql, tableGroups3);
        assertSqlEquals(rewriteSql, markSql);
        Assert.assertEquals(tableGroups, tableGroups3);
        replaySqlList.add(markSql);

        // create table like
        String likeTableName = "t_like_" + RandomUtils.nextLong();
        Set<String> tableGroups4 = new HashSet<>();
        String createLikeSql = "create table " + likeTableName + " like " + newTableName;
        stmt.execute(createLikeSql);
        markSql = getMarkSqlForImplicitTableGroup(dbName, likeTableName);
        implicitTableGroupChecker.checkSql(dbName, likeTableName, markSql, tableGroups4);
        Assert.assertEquals(tableGroups, tableGroups4);
        replaySqlList.add(markSql);

        // create table like with implicit sql
        String likeTableName2 = "t_like_" + RandomUtils.nextLong();
        Set<String> tableGroups5 = new HashSet<>();
        String creatLikeSql2 = ImplicitTableGroupUtil.rewriteTableName(markSql, likeTableName2);
        stmt.execute(creatLikeSql2);
        markSql = getMarkSqlForImplicitTableGroup(dbName, likeTableName2);
        implicitTableGroupChecker.checkSql(dbName, likeTableName, markSql, tableGroups5);
        Assert.assertEquals(tableGroups, tableGroups5);
        replaySqlList.add(markSql);

        // drop table
        stmt.execute("drop table " + newTableName);
        stmt.execute("drop table " + likeTableName);
        stmt.execute("drop table " + likeTableName2);
    }

    private void executeAlterIndexSql(Statement stmt, String schemaName, String tableName1, String tableName2,
                                      String sql,
                                      List<String> replaySqlList)
        throws SQLException {

        Set<String> tableGroups1 = new HashSet<>();
        stmt.execute(sql);
        System.out.println("submit alter index sql : " + sql);
        String markSql1 = getMarkSqlForImplicitTableGroup(schemaName, tableName1);
        implicitTableGroupChecker.checkSql(schemaName, tableName1, markSql1, tableGroups1);
        System.out.println("mark alter index sql : " + markSql1);
        replaySqlList.add(markSql1);

        Set<String> tableGroups2 = new HashSet<>();
        String alterIndexSql2 = ImplicitTableGroupUtil.rewriteTableName(markSql1, tableName2);
        stmt.execute(alterIndexSql2);
        System.out.println("submit alter index sql : " + alterIndexSql2);
        String markSql2 = getMarkSqlForImplicitTableGroup(schemaName, tableName2);
        implicitTableGroupChecker.checkSql(schemaName, tableName2, markSql2, tableGroups2);
        Assert.assertEquals(alterIndexSql2, markSql2);
        Assert.assertEquals(tableGroups1, tableGroups2);
        System.out.println("mark alter index sql : " + markSql2);
        replaySqlList.add(markSql2);
    }

    @Data
    private static class Parameter {
        Statement stmt;
        List<String> replaySqlList;
        String schemaName;
    }
}

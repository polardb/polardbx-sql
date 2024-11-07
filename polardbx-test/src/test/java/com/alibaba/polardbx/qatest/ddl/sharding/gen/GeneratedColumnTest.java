package com.alibaba.polardbx.qatest.ddl.sharding.gen;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddlAssertErrorAtomic;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

public class GeneratedColumnTest extends DDLBaseNewDBTestCase {
    private final boolean isRDS80 = StorageInfoManager.checkRDS80(ConnectionManager.getInstance().getMysqlDataSource());

    @Test
    public void testCreateTableGeneratedColumnFailed() {
        String tableName = "gen_col_create_failed_tbl";
        dropTableIfExists(tableName);

        String createSqlTemplate =
            String.format("create table %s(a int, b int, e int as (a-b) logical, f double, %%s) dbpartition by hash(a)",
                tableName);
        String[] illegalColDefs = {
            "c int as (t1.a+b) logical", "c int as (a+c) logical", "c int as (a+d) logical",
            "c int as (a-b) logical default 10", "c int as (f) logical, f int as (c) logical", "c int as (f) logical",
            "c double as (a) logical", "c int as (a+b) virtual logical", "c int as (a+b) stored logical"};

        for (String colDef : illegalColDefs) {
            String create = String.format(createSqlTemplate, colDef);
            JdbcUtil.executeUpdateFailed(tddlConnection, create, "");
        }
    }

    @Test
    public void testCreateTableGeneratedColumn() {
        String tableName = "gen_col_create_tbl";
        String gsiName = tableName + "_gsi";

        String createSqlTemplate =
            String.format("create table %s(a int, b int, e int as (a-b) logical, %%s)", tableName);
        String partDef = "dbpartition by hash(a)";
        String gsiDef = String.format(", clustered index %s(c) dbpartition by hash(c)", gsiName);

        String[] colDefs = {
            "c int as (a+b) logical", "c int as (a) logical, d int as (a+c) logical", "c int as (a+b) logical unique",
            "c int as (a+b) logical"};
        String insert = String.format("insert into %s(a,b) values(2,3),(3,4)", tableName);

        for (String colDef : colDefs) {
            dropTableIfExists(tableName);
            dropTableIfExistsInMySql(tableName);

            String tddlCreate = String.format(createSqlTemplate, colDef + gsiDef) + partDef;
            String mysqlCreate = String.format(createSqlTemplate, colDef);
            JdbcUtil.executeUpdateSuccess(tddlConnection, tddlCreate);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreate.replace("logical", ""));

            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testAlterTableGeneratedColumnFailed() {
        String tableName = "gen_col_alter_failed_tbl";
        dropTableIfExists(tableName);

        String createSql =
            String.format(
                "create table %s(a int primary key, b int, e int as (a-b) logical, f int, h double) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String alterTableTemplate = String.format("alter table %s %%s", tableName);
        String[] illegalColDefs = {
            "add column c int as (t1.a+b) logical", "add column c int as (a+c) logical",
            "add column c int as (a+d) logical",
            "add column c int as (a-b) logical default 10",
            "add column c int as (a+d) logical, add column d int as (c) logical",
            "add column c int as (a) logical, add column d int", "drop column e, drop column f", "drop column g",
            "add column c double as (a) logical", "add column c int as (h) logical",
            "add column c int as (b) logical, add column d int as (c) logical",
            "add column c int as (non_exist_func()) logical", "add column c int as (a+b) virtual logical",
            "add column c int as (a+b) stored logical"};
        for (String colDef : illegalColDefs) {
            String alter = String.format(alterTableTemplate, colDef);
            JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");
        }
    }

    @Test
    public void testAlterTableGeneratedColumn() {
        String tableName = "gen_col_alter_tbl";

        String createSql = String.format("create table %s(a int, b int, e int as (a-b) logical)", tableName);
        String partDef = "dbpartition by hash(a)";

        String[] colDefs =
            {
                "c int as (a+b) logical", "c int as (a) logical, add column d int as (a-b) logical",
                "c int as (a+b) logical unique", "c int as (a+b) logical"};

        for (String colDef : colDefs) {
            dropTableIfExists(tableName);
            dropTableIfExistsInMySql(tableName);

            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql + partDef);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, createSql.replace("logical", ""));

            String insert = String.format("insert into %s(a,b) values(2,3),(3,4)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

            String alterTable = String.format("alter table %s add column %s", tableName, colDef);

            JdbcUtil.executeUpdateSuccess(tddlConnection, alterTable);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, alterTable.replace("logical", ""));

            insert = String.format("insert into %s(a,b) values(4,5),(5,6)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testGeneratedModifyFailed() {
        String tableName = "gen_col_modify_fail_tbl";
        dropTableIfExists(tableName);

        String createSql =
            String.format(
                "create table %s(a int primary key, b int, c int as (a+b) logical, e int) dbpartition by hash(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // modify ref column
        String alterSql = String.format("alter table %s modify column b bigint", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // change ref column
        alterSql = String.format("alter table %s change column b d bigint", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // modify ref column
        alterSql = String.format("alter table %s modify column b bigint, algorithm=omc", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // change ref column
        alterSql = String.format("alter table %s change column b d bigint, algorithm=omc", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // modify gen column
        alterSql = String.format("alter table %s modify column c bigint", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // change gen column
        alterSql = String.format("alter table %s change column c d bigint", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // modify gen column
        alterSql = String.format("alter table %s modify column c bigint, algorithm=omc", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // change gen column
        alterSql = String.format("alter table %s change column c d bigint, algorithm=omc", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // drop ref column
        alterSql = String.format("alter table %s drop column b", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // edit ref column default value
        alterSql = String.format("alter table %s alter column b set default 1", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // edit gen column default value
        alterSql = String.format("alter table %s alter column c set default 1", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // modify to gen column
        alterSql = String.format("alter table %s modify column e int as (a+b) logical", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // change to gen column
        alterSql = String.format("alter table %s change column e f int as (a+b) logical", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // modify to gen column
        alterSql = String.format("alter table %s modify column e int as (a+b) logical, algorithm=omc", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // change to gen column
        alterSql = String.format("alter table %s change column e f int as (a+b) logical, algorithm=omc", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));
    }

    @Test
    public void testGeneratedColumnUniqueRollback() throws SQLException {
        String tableName = "gen_col_unique_rollback_tbl";
        String gsiName = tableName + "_gsi";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String create = String.format("create table %s (a int primary key, b int) ", tableName);
        String partDef = "dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String createGsi =
            String.format("create clustered index %s on %s(b) dbpartition by hash(b)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        String insert = String.format("insert into %s values (1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);

        String alter = String.format("alter table %s add column c int as (a-b) logical unique", tableName);
        executeOnMysqlAndTddlAssertErrorAtomic(mysqlConnection, tddlConnection, alter.replace("logical", ""), alter,
            null, false);

        alter = String.format("alter table %s add column d int as (a+b) logical unique first", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, alter.replace("logical", ""), alter, null, false);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        assertSameIndexInfo(tableName, tableName, gsiName);
        assertSameIndexInfo(gsiName, tableName, "");
    }

    private void assertSameIndexInfo(String tddlTableName, String mysqlTableName, String gsiName) {
        String sql = "show index from ";
        ResultSet mysqlRs = JdbcUtil.executeQuerySuccess(mysqlConnection, sql + mysqlTableName);
        ResultSet tddlRs = JdbcUtil.executeQuerySuccess(tddlConnection, sql + tddlTableName);

        List<List<Object>> mysqlResults = JdbcUtil.getAllResult(mysqlRs, false);
        List<List<Object>> tddlResults = JdbcUtil.getAllResult(tddlRs, false);

        // Remove gsi and auto shard key
        tddlResults.removeIf(row -> row.get(2).toString().equalsIgnoreCase(gsiName));
        tddlResults.removeIf(row -> row.get(2).toString().contains("auto_shard"));

        // Remove table name and cardinality from results
        for (List<Object> list : mysqlResults) {
            list.remove(0);
            list.remove(5);
        }
        for (List<Object> list : tddlResults) {
            list.remove(0);
            list.remove(5);
        }
        if (isRDS80) {
            for (List<Object> list : mysqlResults) {
                list.remove(12);
                list.remove(11);
            }
        }
        assertWithMessage("Index not match")
            .that(tddlResults)
            .containsExactlyElementsIn(mysqlResults);
    }

    @Ignore
    @Test
    public void testAutoUpdateColumn() throws SQLException {
        String tableName = "gen_col_auto_update_test_tbl";
        String gsiName = tableName + "_gsi";

        dropTableIfExists(tableName);

        String create = String.format(
            "create table %s (a int primary key, d int default 10) ",
            tableName);
        String partDef = "dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String[] dml = {
            String.format("insert into %s(a) values (1)", tableName),
            String.format("update %s set d=d", tableName),
            // String.format("update %s set a=a", tableName),
            String.format("update %s set d=d+1", tableName),
            String.format("update %s set a=a+1", tableName),
            String.format("update %s set b='2022-01-01 00:00:00'", tableName),
            String.format("replace into %s(a) values (2)", tableName),
            String.format("insert into %s(a) values (2) on duplicate key update a=a+1", tableName),
            String.format("insert into %s(a) select 2", tableName),
            String.format("delete from %s", tableName),
        };

        for (String sql : dml) {
            System.out.println(sql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, sql);
            String select = String.format("select * from %s where c != date_add(b, INTERVAL 20 SECOND)", tableName);
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, select);
            List<List<Object>> object = JdbcUtil.getAllResult(rs);
            Assert.assertTrue(object.isEmpty());
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection, true);
        }

        String createGsi =
            String.format("create clustered index %s on %s(b) dbpartition by YYYYMM(b)", gsiName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        for (String sql : dml) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            String select = String.format("select * from %s where c != date_add(b, INTERVAL 20 SECOND)", tableName);
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, select);
            List<List<Object>> object = JdbcUtil.getAllResult(rs);
            Assert.assertTrue(object.isEmpty());
        }
    }

    @Test
    public void testMultipleGeneratedColumn() {
        String tableName = "multi_gen_col_test_tbl";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String create = String.format("create table %s (a int primary key, b int)", tableName);
        String partDef = "dbpartition by hash(a)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String alter = String.format("alter table %s add column c int as (a*2) logical", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter.replace("logical", ""));
        alter = String.format("alter table %s add column d int as (a+c) logical", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter.replace("logical", ""));
        alter = String.format("alter table %s add column e int as (b*2) logical", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter.replace("logical", ""));
        alter = String.format("alter table %s add column f int as (b+e) logical", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter.replace("logical", ""));

        alter = String.format("alter table %s add column g int as (f+a) logical", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, alter.replace("logical", ""));

        String insert = String.format("insert into %s(a,b) values (1,2),(3+1,4)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        String update = String.format("update %s set a=a+1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, update);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
//        List<List<String>> trace = getTrace(tddlConnection);
//        Assert.assertTrue(trace.stream().map(t -> t.get(11)).noneMatch(s -> s.contains("SELECT") && !s.contains(
//            "(`a` + ?), (((`a` + ?) * 2)), (((`a` + ?) + (((`a` + ?) * 2)))), ((`f` + (`a` + ?)))")));

        update = String.format("update %s set b=a+1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, update);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
//        trace = getTrace(tddlConnection);
//        Assert.assertTrue(trace.stream().map(t -> t.get(11)).noneMatch(s -> s.contains("SELECT") && !s.contains(
//            "(`a` + ?), (((`a` + ?) * 2)), (((`a` + ?) + (((`a` + ?) * 2)))), (((((`a` + ?) + (((`a` + ?) * 2)))) + `a`))")));

        update = String.format("update %s set a=a+1,b=b+1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, update);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
//        trace = getTrace(tddlConnection);
//        Assert.assertTrue(trace.stream().map(t -> t.get(11)).noneMatch(s -> s.contains("SELECT") && !s.contains(
//            "(`a` + ?), (`b` + ?), (((`b` + ?) * 2)), (((`a` + ?) * 2)), (((`b` + ?) + (((`b` + ?) * 2)))), (((`a` + ?) + (((`a` + ?) * 2)))), (((((`b` + ?) + (((`b` + ?) * 2)))) + (`a` + ?)))")));

        String upsert = String.format("insert into %s(a,b) values (6,7) on duplicate key update a=a+1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + upsert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, upsert);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        upsert = String.format("insert into %s(a,b) values (7,7) on duplicate key update b=a+1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + upsert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, upsert);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        upsert = String.format("insert into %s(a,b) values (7,8) on duplicate key update a=a+1,b=b+1", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + upsert);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, upsert);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testAutoIncColumnFailed() {
        String tableName = "auto_inc_test_tbl";
        String create = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical auto_increment unique) dbpartition by hash(a)",
            tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, create).contains("can not be auto_increment"));
        create = String.format(
            "create table %s (a int primary key auto_increment, b int, c int as (a+b) logical) dbpartition by hash(a)",
            tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, create).contains("can not be auto_increment"));

        create = String.format("create table %s (a int primary key, b int) dbpartition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        String alter =
            String.format("alter table %s add column c int as (a+b) logical auto_increment unique", tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alter).contains("can not be auto_increment"));

        String tableName1 = tableName + "_1";
        create = String.format("create table %s (a int primary key auto_increment, b int) dbpartition by hash(a)",
            tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        alter = String.format("alter table %s add column c int as (a+b) logical", tableName1);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alter).contains("can not be auto_increment"));
    }

    @Test
    public void testAutoUpdateColumnFailed() {
        String tableName = "auto_update_test_tbl";
        String create = String.format(
            "create table %s (a int primary key, b datetime, c datetime on update current_timestamp() as (b) logical) dbpartition by hash(a)",
            tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, create).contains("can not be auto update"));
        create = String.format(
            "create table %s (a int primary key, b datetime on update current_timestamp(), c int as (b) logical) dbpartition by hash(a)",
            tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, create).contains("can not be auto update"));

        create = String.format("create table %s (a int primary key, b datetime) dbpartition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        String alter =
            String.format("alter table %s add column c datetime on update current_timestamp() as (b) logical",
                tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alter).contains("can not be auto update"));

        String tableName1 = tableName + "_1";
        create = String.format(
            "create table %s (a int primary key auto_increment, b datetime on update current_timestamp()) dbpartition by hash(a)",
            tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        alter = String.format("alter table %s add column c datetime as (b) logical", tableName1);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alter).contains("can not be auto update"));
    }

    @Test
    public void testGeneratedColumnMultipleTableUpdate() throws SQLException {
        String tableName1 = "gen_col_update_test_mulit_tbl_1";
        String tableName2 = "gen_col_update_test_mulit_tbl_2";
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        dropTableIfExistsInMySql(tableName1);
        dropTableIfExistsInMySql(tableName2);

        String createTmpl =
            "create table %s (a int primary key, b int, c int as (a-b) logical, d int as (c+1) logical, e int)";
        String partDef = " dbpartition by hash(e)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTmpl, tableName1) + partDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTmpl, tableName2) + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createTmpl, tableName1).replace("logical", ""));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(createTmpl, tableName2).replace("logical", ""));

        String insert1 = String.format("insert into %s(a,b) values (1,2)", tableName1);
        String insert2 = String.format("insert into %s(a,b) values (2,3)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert2);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert1);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insert2);

        String update =
            String.format("update %s as t1,%s as t2 set t1.a=t2.a+10,t2.b=t1.b+10 where t2.a=t1.b", tableName1,
                tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, update);
        selectContentSameAssert("select * from " + tableName1, null, mysqlConnection, tddlConnection);
        selectContentSameAssert("select * from " + tableName2, null, mysqlConnection, tddlConnection);

        update = String.format("update %s as t1,%s as t2 set t1.a=t2.a+10,t2.b=t1.b+10 where t2.a=t1.b", tableName1,
            tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, update);
        selectContentSameAssert("select * from " + tableName1, null, mysqlConnection, tddlConnection);
        selectContentSameAssert("select * from " + tableName2, null, mysqlConnection, tddlConnection);

        update =
            MessageFormat.format("update {0},{1} as t1 set {0}.a=t1.a+20,t1.b={0}.b+20 where t1.a={0}.b", tableName1,
                tableName2);
        System.out.println(update);
        JdbcUtil.executeUpdateSuccess(tddlConnection, update);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, update);
        selectContentSameAssert("select * from " + tableName1, null, mysqlConnection, tddlConnection);
        selectContentSameAssert("select * from " + tableName2, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testOutRangeGeneratedColumn() {
        String tableName = "out_range_test_tbl";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        String create = String.format(
            "create table %s (a int primary key, b int, c tinyint as (b) logical, d int as (c+1) logical)",
            tableName);
        String partDef = "dbpartition by hash(d)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create.replace("logical", ""));

        Connection tddlConn = getPolardbxConnection();
        Connection mysqlConn = getMysqlConnection();
        String tddlSqlMode = JdbcUtil.getSqlMode(tddlConn);
        String mysqlSqlMode = JdbcUtil.getSqlMode(mysqlConn);

        try {
            setSqlMode("", tddlConn);
            setSqlMode("", mysqlConn);

            String insert = String.format("insert into %s(a,b) values (22222,33333)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConn, insert);
            JdbcUtil.executeUpdateSuccess(mysqlConn, insert);
            selectContentSameAssert("select * from " + tableName + " where d=128", null, mysqlConn, tddlConn);

            String update = String.format("update %s set b=33334", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConn, update);
            JdbcUtil.executeUpdateSuccess(mysqlConn, update);
            selectContentSameAssert("select * from " + tableName + " where d=128", null, mysqlConn, tddlConn);

            String upsert =
                String.format("insert into %s(a,b) values (22222,33333) on duplicate key update a=a+1,b=33335",
                    tableName);
            JdbcUtil.executeUpdateSuccess(tddlConn, upsert);
            JdbcUtil.executeUpdateSuccess(mysqlConn, upsert);
            selectContentSameAssert("select * from " + tableName + " where d=128", null, mysqlConn, tddlConn);
        } finally {
            setSqlMode(tddlSqlMode, tddlConn);
            setSqlMode(mysqlSqlMode, mysqlConn);
        }
    }

    @Test
    public void testGeneratedColumnDefaultNull() {
        String tableName = "gen_col_default_test_tbl";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        String create = String.format(
            "create table %s (a int primary key, b int, c tinyint as (b) logical, d int as (c+1) logical)",
            tableName);
        String partDef = "dbpartition by hash(d)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create.replace("logical", ""));

        Connection tddlConn = getPolardbxConnection();
        Connection mysqlConn = getMysqlConnection();

        setSqlMode("", tddlConn);
        setSqlMode("", mysqlConn);

        String insert = String.format("insert into %s(a,b) values (22222,c)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConn, insert);
        JdbcUtil.executeUpdateSuccess(mysqlConn, insert);
        selectContentSameAssert("select * from " + tableName, null, mysqlConn, tddlConn);

        String update = String.format("update %s set b=33334", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConn, update);
        JdbcUtil.executeUpdateSuccess(mysqlConn, update);
        selectContentSameAssert("select * from " + tableName, null, mysqlConn, tddlConn);

        String upsert =
            String.format("insert into %s(a,b) values (22222,33333) on duplicate key update a=a+1,b=default(c)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConn, upsert);
        if (!isMySQL80()) {
            JdbcUtil.executeUpdateSuccess(mysqlConn, upsert);
            selectContentSameAssert("select * from " + tableName, null, mysqlConn, tddlConn);
        }
    }

    @Test
    public void testRepartition() {
        String tableName = "gen_col_repart";
        String create = String.format("create table %s (a int primary key, b int as (a+1) logical)", tableName);
        String partDef = "dbpartition by hash(a)";

        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create.replace("logical", ""));

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
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testMoveDb() throws SQLException {
        String dbName = "gen_move_db";

        Connection conn = getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName);
        JdbcUtil.executeUpdateSuccess(conn, "create database " + dbName + " mode=drds");
        JdbcUtil.executeUpdateSuccess(conn, "use " + dbName);

        String tableName = "dn_gen_move_db_test_tbl";
        String gsiName = tableName + "_gsi";

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical, global index %s(b) covering(a) dbpartition by hash(b,a)) dbpartition by hash(a,b)",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(conn, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select * from " + tableName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        Map<String, String> groupInstId = new HashMap();

        String sql = String.format("show ds where db ='%s'", dbName);
        rs = JdbcUtil.executeQuery(sql, conn);
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
            "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, PHYSICAL_BACKFILL_ENABLE=false, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true, CN_ENABLE_CHANGESET=false)*/ %s to '%s'",
            srcGroup, dstStorageId);
        JdbcUtil.executeUpdateSuccess(conn, tddlSql);

        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));
        rs = JdbcUtil.executeQuerySuccess(conn, "select * from " + tableName + " order by a");
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName);
        conn.close();

    }

    @Test
    public void testMoveDbChangeset() throws SQLException {
        String dbName = "gen_move_db_cs";

        Connection conn = getPolardbxConnection();

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName);
        JdbcUtil.executeUpdateSuccess(conn, "create database " + dbName + " mode=drds");
        JdbcUtil.executeUpdateSuccess(conn, "use " + dbName);

        String tableName = "dn_gen_move_db_test_tbl";
        String gsiName = tableName + "_gsi";

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical, global index %s(b) covering(a) dbpartition by hash(b,a)) dbpartition by hash(a,b)",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(conn, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select * from " + tableName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        Map<String, String> groupInstId = new HashMap();

        String sql = String.format("show ds where db ='%s'", dbName);
        rs = JdbcUtil.executeQuery(sql, conn);
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
            "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, PHYSICAL_BACKFILL_ENABLE=false, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true, CN_ENABLE_CHANGESET=true)*/ %s to '%s'",
            srcGroup, dstStorageId);
        JdbcUtil.executeUpdateSuccess(conn, tddlSql);

        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));
        rs = JdbcUtil.executeQuerySuccess(conn, "select * from " + tableName + " order by a");
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName);
        conn.close();

    }

    @Test
    public void testCreateDbAs() throws SQLException {
        String dbName1 = "gen_create_as_db_1";
        String dbName2 = "gen_create_as_db_2";

        Connection conn = getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName1);
        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName2);
        JdbcUtil.executeUpdateSuccess(conn, "create database " + dbName1 + " mode=drds");
        JdbcUtil.executeUpdateSuccess(conn, "use " + dbName1);

        String tableName = "gen_create_db_test_tbl";
        String gsiName = tableName + "_gsi";

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical, global index %s(b) covering(a) dbpartition by hash(b,a)) dbpartition by hash(a,b)",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(conn, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select * from " + tableName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        String sql = String.format("create database %s as %s", dbName2, dbName1);
        JdbcUtil.executeUpdateSuccess(conn, sql);
        JdbcUtil.executeUpdateSuccess(conn, "use " + dbName2);

        Assert.assertEquals(100, getDataNumFromTable(conn, tableName));
        rs = JdbcUtil.executeQuerySuccess(conn, "select * from " + tableName + " order by a");
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName1);
        JdbcUtil.executeUpdateSuccess(conn, "drop database if exists " + dbName2);
        conn.close();

    }

    @Test
    public void testShowColumns() throws SQLException {
        String tableName = "gen_col_show_columns";
        String createTable =
            String.format("create table %s (a int primary key, b int as (a+1) logical) dbpartition by hash(b)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        String sql = String.format("show columns from %s", tableName);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        boolean found = false;
        while (rs.next()) {
            if (rs.getString("Field").equalsIgnoreCase("b")) {
                Assert.assertTrue(rs.getString("Extra").equalsIgnoreCase("LOGICAL GENERATED"));
                found = true;
            }
        }
        Assert.assertTrue(found);

        sql = String.format("show full columns from %s", tableName);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        found = false;
        while (rs.next()) {
            if (rs.getString("Field").equalsIgnoreCase("b")) {
                Assert.assertTrue(rs.getString("Extra").equalsIgnoreCase("LOGICAL GENERATED"));
                found = true;
            }
        }
        Assert.assertTrue(found);

        sql = String.format("select * from information_schema.columns where table_schema='%s' and table_name='%s'",
            tddlDatabase1, tableName);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        found = false;
        while (rs.next()) {
            if (rs.getString("COLUMN_NAME").equalsIgnoreCase("b")) {
                Assert.assertTrue(rs.getString("COLUMN_DEFAULT") == null);
                Assert.assertTrue(rs.getString("EXTRA").equalsIgnoreCase("LOGICAL GENERATED"));
                Assert.assertTrue(rs.getString("GENERATION_EXPRESSION").equalsIgnoreCase("((`a` + 1))"));
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testDmlFailed() {
        String tableName = "gen_col_dml_fail_tbl";
        String create = String.format("create table %s (a int primary key, b int as (a+1) logical)", tableName);
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
        String tableName = "gen_col_like";
        String likeTable = tableName + "_2";

        dropTableIfExists(tableName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical) dbpartition by hash(a)",
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
        String tableName = "gen_col_comment";
        String likeTable = tableName + "_2";

        dropTableIfExists(tableName);
        dropTableIfExists(likeTable);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical comment '123') dbpartition by hash(a)",
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
    public void testCreateTableColName() throws SQLException {
        String tableName = "gen_col_name";

        dropTableIfExists(tableName);
        String createSql = String.format(
            "create table %s (a int primary key, b int) dbpartition by hash(a) ",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        String insert = String.format("insert into %s(a,b) values (1,2)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String[] columnNames = new String[] {"c", "`c`", "```c```", "`c```", "`3`", "`\"f\"`"};
        for (String columnName : columnNames) {
            String alter =
                String.format("alter table %s add column %s int as (a) logical not null", tableName, columnName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, alter);

            ResultSet rs = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
            List<List<Object>> objects = JdbcUtil.getAllResult(rs);

            Assert.assertTrue(objects.size() == 1);
            Assert.assertTrue("1".equalsIgnoreCase(objects.get(0).get(0).toString()));
            Assert.assertTrue("2".equalsIgnoreCase(objects.get(0).get(1).toString()));
            Assert.assertTrue("1".equalsIgnoreCase(objects.get(0).get(2).toString()));

            alter = String.format("alter table %s drop column %s", tableName, columnName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        }
    }
}

package com.alibaba.polardbx.qatest.ddl.auto.gen;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddlAssertErrorAtomic;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

public class GeneratedColumnTest extends DDLBaseNewDBTestCase {
    private final boolean isRDS80 = StorageInfoManager.checkRDS80(ConnectionManager.getInstance().getMysqlDataSource());

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testCreateTableGeneratedColumnFailed() {
        String tableName = "gen_col_create_failed_tbl";
        dropTableIfExists(tableName);

        String createSqlTemplate =
            String.format("create table %s(a int, b int, e int as (a-b) logical, f double, %%s) partition by hash(a)",
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
        String partDef = "partition by hash(a)";
        String gsiDef = String.format(", clustered index %s(c) partition by hash(c)", gsiName);

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
                "create table %s(a int primary key, b int, e int as (a-b) logical, f int, h double) partition by hash(a)",
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
        String partDef = "partition by hash(a)";

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
                "create table %s(a int primary key, b int, c int as (a+b) logical, e int) partition by hash(a)",
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
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_ONLINE_MODIFY_COLUMN"));

        // change ref column
        alterSql = String.format("alter table %s change column b d bigint, algorithm=omc", tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_ONLINE_MODIFY_COLUMN"));

        // modify gen column
        alterSql = String.format("alter table %s modify column c bigint", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // change gen column
        alterSql = String.format("alter table %s change column c d bigint", tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_OPTIMIZER"));

        // modify gen column
        alterSql = String.format("alter table %s modify column c bigint, algorithm=omc", tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_ONLINE_MODIFY_COLUMN"));

        // change gen column
        alterSql = String.format("alter table %s change column c d bigint, algorithm=omc", tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_ONLINE_MODIFY_COLUMN"));

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
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_ONLINE_MODIFY_COLUMN"));

        // change to gen column
        alterSql = String.format("alter table %s change column e f int as (a+b) logical, algorithm=omc", tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alterSql).contains("ERR_ONLINE_MODIFY_COLUMN"));
    }

    @Test
    public void testGeneratedColumnUniqueRollback() throws SQLException {
        String tableName = "gen_col_unique_rollback_tbl";
        String gsiName = tableName + "_gsi";

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String create = String.format("create table %s (a int primary key, b int) ", tableName);
        String partDef = "partition by hash(a) partitions 7";
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String createGsi = String.format("create clustered index %s on %s(b) partition by hash(b)", gsiName, tableName);
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
        assertSameIndexInfo(getRealGsiName(tddlConnection, tableName, gsiName), tableName, "");
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
        String partDef = "partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, create);

        String[] dml = {
            String.format("insert into %s(a) values (1)", tableName),
            String.format("update %s set d=d", tableName),
            String.format("update %s set a=a", tableName),
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

        String createGsi = String.format("create clustered index %s on %s(b) partition by hash(b)", gsiName, tableName);
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
        String partDef = "partition by hash(a)";

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
    public void testAutoIncColumn() {
        String tableName = "auto_inc_test_tbl";
        String create = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical auto_increment unique) partition by hash(a)",
            tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, create).contains("can not be auto_increment"));
        create = String.format(
            "create table %s (a int primary key auto_increment, b int, c int as (a+b) logical) partition by hash(a)",
            tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, create).contains("can not be auto_increment"));

        create = String.format("create table %s (a int primary key, b int) partition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        String alter =
            String.format("alter table %s add column c int as (a+b) logical auto_increment unique", tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alter).contains("can not be auto_increment"));

        String tableName1 = tableName + "_1";
        create =
            String.format("create table %s (a int primary key auto_increment, b int) partition by hash(a)", tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        alter = String.format("alter table %s add column c int as (a+b) logical", tableName1);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, alter).contains("can not be auto_increment"));
    }

    @Test
    public void testAutoUpdateColumnFailed() {
        String tableName = "auto_update_test_tbl";
        String create = String.format(
            "create table %s (a int primary key, b datetime, c datetime on update current_timestamp() as (b) logical) partition by hash(a)",
            tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, create).contains("can not be auto update"));
        create = String.format(
            "create table %s (a int primary key, b datetime on update current_timestamp(), c int as (b) logical) partition by hash(a)",
            tableName);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, create).contains("can not be auto update"));

        create = String.format("create table %s (a int primary key, b datetime) partition by hash(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        String alter =
            String.format("alter table %s add column c datetime on update current_timestamp() as (b) logical",
                tableName);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, alter).contains("can not be auto update"));

        String tableName1 = tableName + "_1";
        create = String.format(
            "create table %s (a int primary key auto_increment, b datetime on update current_timestamp()) partition by hash(a)",
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
        String partDef = " partition by hash(e)";
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
        String partDef = "partition by hash(d) partitions 7";
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
        String partDef = "partition by hash(d)";
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
        String partDef = "partition by hash(a)";

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

        alter = String.format("alter table %s partition by hash(b)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSplitKey() {
        String tableName = "gen_col_split";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical, global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        String splitSql = String.format("alter table %s split into partitions 10 by hot value(1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);
        splitSql = String.format("alter table %s split into partitions 10 by hot value(1)",
            getRealGsiName(tddlConnection, tableName, gsiName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }
    }

    @Test
    public void testAutoSplitKey() {
        String tableName = "gen_col_auto_split";
        String gsiName = tableName + "_gsi";
        String tgName1 = tableName + "_tg_1";
        String tgName2 = tableName + "_tg_2";

        String createTgSql = String.format("create tablegroup %s", tgName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);
        createTgSql = String.format("create tablegroup %s", tgName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTgSql);

        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical, global index %s(b,a)) tablegroup=%s",
            tableName, gsiName,
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
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        String splitSql = String.format("alter table %s split into partitions 1 by hot value(1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);
        splitSql = String.format("alter table %s split into partitions 10 by hot value(1)",
            getRealGsiName(tddlConnection, tableName, gsiName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, splitSql);

        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }
    }

    @Test
    public void testMovePartition() throws SQLException {
        String tableName = "gen_move_part";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical, global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        String curInstId = null;
        String sql = String.format(
            "select storage_inst_id from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'",
            tddlDatabase1, tableName, "p1");
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
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
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }
    }

    @Test
    public void testMovePartitionChangeset() throws SQLException {
        String tableName = "gen_move_part_cs";
        String gsiName = tableName + "_gsi";
        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical, global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

        String curInstId = null;
        String sql = String.format(
            "select storage_inst_id from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'",
            tddlDatabase1, tableName, "p1");
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
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
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }
    }

    @Test
    public void testShowColumns() throws SQLException {
        String tableName = "gen_col_show_columns";
        String createTable =
            String.format("create table %s (a int primary key, b int as (a+1) logical) partition by hash(b)",
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

        String tableName = "gen_col_archive";
        String gsiName = tableName + "_gsi";
        String ossName = tableName + "_oss";

        dropTableIfExists(tableName);
        dropTableIfExists(gsiName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical, global index %s(b) covering(a) partition by key(b,a) partitions 14) partition by key(a,b) partitions 13",
            tableName, gsiName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < 100; i++) {
            String insertSql = String.format("insert into %s(a,b) values (%d,%d)", tableName, i, i + 1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        Assert.assertEquals(100, getDataNumFromTable(tddlConnection, tableName));
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + tableName + " order by a");
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }

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
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select * from " + ossName + " order by a");
        objects = JdbcUtil.getAllResult(rs);
        Assert.assertEquals(100, objects.size());
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(String.valueOf(i), objects.get(i).get(0).toString());
            Assert.assertEquals(String.valueOf(i + 1), objects.get(i).get(1).toString());
            Assert.assertEquals(String.valueOf(i + i + 1), objects.get(i).get(2).toString());
        }
    }

    @Test
    public void testCreateTableLike() throws SQLException {
        String tableName = "gen_col_like";
        String likeTable = tableName + "_2";

        dropTableIfExists(tableName);
        String createSql = String.format(
            "create table %s (a int primary key, b int, c int as (a+b) logical) partition by key(a,b) partitions 13",
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
            "create table %s (a int primary key, b int, c int as (a+b) logical comment '123') partition by key(a,b) partitions 13",
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
            "create table %s (a int primary key, b int) partition by key(a,b) partitions 13",
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

    @Test
    public void testLocalPartitionTable() throws SQLException {
        checkOnSingleCn();

        String ossTableName = "gen_col_local_part_oss";
        String innodbTableName = "gen_col_local_part";
        int startYear = Calendar.getInstance().get(Calendar.YEAR) - 1;
        Statement statement = tddlConnection.createStatement();
        statement.execute(String.format("CREATE TABLE %s (\n" +
            "    id bigint NOT NULL AUTO_INCREMENT,\n" +
            "    unix_time bigint NOT NULL,\n" +
            "    gmt_modified DATETIME NOT NULL AS (FROM_UNIXTIME(unix_time)) LOGICAL,\n" +
            "    PRIMARY KEY (id, gmt_modified)\n" +
            ")\n" +
            "PARTITION BY HASH(id) PARTITIONS 8\n" +
            "LOCAL PARTITION BY RANGE (gmt_modified)\n" +
            "STARTWITH '%d-01-01'\n" +
            "INTERVAL 1 MONTH\n" +
            "EXPIRE AFTER 1\n" +
            "PRE ALLOCATE 3\n" +
            "PIVOTDATE NOW();", innodbTableName, startYear));

        long[] dates =
            {
                LocalDate.of(startYear - 1, 12, 1).atStartOfDay(ZoneId.of("UTC")).toInstant().getEpochSecond(),
                LocalDate.of(startYear, 1, 1).atStartOfDay(ZoneId.of("UTC")).toInstant().getEpochSecond(),
                LocalDate.of(startYear, 2, 1).atStartOfDay(ZoneId.of("UTC")).toInstant().getEpochSecond(),
                LocalDate.of(startYear, 3, 1).atStartOfDay(ZoneId.of("UTC")).toInstant().getEpochSecond(),
                LocalDate.of(startYear, 4, 1).atStartOfDay(ZoneId.of("UTC")).toInstant().getEpochSecond(),
                LocalDate.of(startYear, 5, 1).atStartOfDay(ZoneId.of("UTC")).toInstant().getEpochSecond()};
        for (long date : dates) {
            StringBuilder insert = new StringBuilder();
            insert.append("insert into ").append(innodbTableName).append(" ('unix_time') values ");
            for (int i = 0; i < 999; i++) {
                insert.append("('").append(date).append("')").append(",");
            }
            insert.append("('").append(date).append("')");
            statement.executeUpdate(insert.toString());
        }

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

        statement.executeUpdate(
            String.format("create table %s like %s engine = 'local_disk' archive_mode ='ttl'", ossTableName,
                innodbTableName));

        long count1 = FileStorageTestUtil.count(tddlConnection, ossTableName);
        statement.executeUpdate(
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p%d0101", innodbTableName, startYear));
        long count2 = FileStorageTestUtil.count(tddlConnection, ossTableName);
        statement.executeUpdate(
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p%d0201", innodbTableName, startYear));
        long count3 = FileStorageTestUtil.count(tddlConnection, ossTableName);
        statement.executeUpdate(
            String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p%d0301", innodbTableName, startYear));
        long count4 = FileStorageTestUtil.count(tddlConnection, ossTableName);

        Assert.assertTrue(count1 == 0);
        Assert.assertTrue(count1 < count2);
        Assert.assertTrue(count2 < count3);
        Assert.assertTrue(count3 < count4);
    }
}

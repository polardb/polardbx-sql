package com.alibaba.polardbx.qatest.ddl.auto.partitionkey;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertWithMessage;

public class ModifyPartitionKeyTest extends DDLBaseNewDBTestCase {

    private static final String USE_OLD_CHECKER = "GSI_BACKFILL_USE_FASTCHECKER=FALSE";
    private static final String SLOW_HINT = "GSI_DEBUG=\"slow\"";

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

    @Test
    public void testModifyPartitionKeyTypeOnTableWithGsi() {
        String tableName = "modify_sk_gsi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "modify_sk_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql = String.format("create table %s (a int primary key, b int, c int) partition by key(c)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            String.format(" create unique clustered index `%s` on %s (`b`) partition by key(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column c bigint", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testModifyPartitionKeyTypeOnTableWithGsi2() {
        String tableName = "modify_sk_gsi_test_tbl_fa" + RandomUtils.getStringBetween(1, 5);
        String indexName = "modify_sk_gsi_test_tbl_fa_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql =
            String.format(
                "create table %s (a int primary key, b int, c int) partition by hash(`b`) partitions 3",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create global index `%s` on %s (`b`, `c`) partition by hash(`c`) partitions 3",
            indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column c bigint", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testModifyPartitionColumnFailed() {
        String tableName = "modify_sk_test_tbl_li_f" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b varchar(255), c int) partition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column a text", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s modify column a blob(10)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s change column a aa int(10)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s modify column a bigint, modify c bigint", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testModifyPartitionColumnWithUpperCase() {
        String tableName = "modify_sk_test_tbl_upper" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b varchar(255), c int, global index upperGsi(a,b) partition by key(a)) partition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 'abc', 999)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (1, 'abc', 999)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (2, 'abc', 999)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column A bigint", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column a varchar(10)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column A int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testModifyPartitionColumnOnMultipleColumnsFailed() {
        String tableName = "modify_sk_multi_col_f" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int, c int) partition by hash(`b`) partitions 7",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint, modify column c bigint", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testModifyPartitionKeyRollback() {
        String tableName = "modify_sk_rollback_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b bigint, c bigint, d bigint, e bigint) partition by hash(`b`, `c`, `d`, `e`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 99999, -99999, 127, -128)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b tinyint,", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        sql = String.format("alter table %s modify column c smallint,", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        sql = String.format("alter table %s modify column d mediumint,", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = String.format("alter table %s modify column e int,", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "99999");
            Assert.assertEquals(rs.getString(3), "-99999");
            Assert.assertEquals(rs.getString(4), "127");
            Assert.assertEquals(rs.getString(5), "-128");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testModifyPartitionKeySqlModeTest() {
        String tableName = "modify_sk_sql_mode_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b bigint, c bigint, d bigint, e bigint) partition by hash(`b`, `c`, `d`, `e`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "set sql_mode=\"\"";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 99999, -99999, 127, -128)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b tinyint,", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = String.format("alter table %s modify column c smallint,", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = String.format("alter table %s modify column d mediumint,", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = String.format("alter table %s modify column e int,", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "127");
            Assert.assertEquals(rs.getString(3), "-32768");
            Assert.assertEquals(rs.getString(4), "127");
            Assert.assertEquals(rs.getString(5), "-128");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    @Ignore
    public void testModifyPartitionKeySimpleCheckerTest() {
        String tableName = "modify_sk_simple_checker_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(`a`) partitions 3", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(USE_OLD_CHECKER) + String.format("alter table %s modify column a varchar(11)", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "1");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testModifyPartitionKeyCheckerTest() {
        String tableName = "modify_sk_checker_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column a varchar(11),", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column a bigint,", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "1");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testShowTable() throws Exception {
        String tableName = "modify_sk_show_table_tbl";
        testShowTableInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s modify column b int,",
                tableName));
        testShowTableInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s change column b b int,",
                tableName));
    }

    private void testShowTableInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`b`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        final ExecutorService threadPool = Executors.newCachedThreadPool();
        String showSql = String.format("show create table %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showSql);
        rs.next();
        final String tableDef = rs.getString(2);

        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Callable<Void> showTableTask =
            () -> {
                Connection connection = getPolardbxConnection();
                try {
                    while (!shouldStop.get()) {
                        ResultSet rs1 = JdbcUtil.executeQuerySuccess(connection, showSql);
                        rs1.next();
                        Assert.assertEquals(rs1.getString(2), tableDef);
                    }
                } finally {
                    connection.close();
                }
                return null;
            };
        Future<Void> result = threadPool.submit(showTableTask);

        JdbcUtil.executeSuccess(tddlConnection, alterSql);
        shouldStop.set(true);

        result.get();
    }

    @Test
    public void testShowColumns() throws Exception {
        String tableName = "modify_sk_show_columns_tbl" + RandomUtils.getStringBetween(1, 5);

        testShowColumnsInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s modify column b int",
                tableName));
        testShowColumnsInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s change column b b int",
                tableName));
    }

    public void testShowColumnsInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`b`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        final ExecutorService threadPool = Executors.newCachedThreadPool();
        String showSql = String.format("show full columns from %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showSql);
        List<List<Object>> results = JdbcUtil.getAllResult(rs);

        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Callable<Void> showTableTask =
            () -> {
                Connection connection = getPolardbxConnection();
                try {
                    while (!shouldStop.get()) {
                        ResultSet rs1 = JdbcUtil.executeQuerySuccess(connection, showSql);
                        List<List<Object>> results1 = JdbcUtil.getAllResult(rs1);
                        assertWithMessage("列信息发生改变")
                            .that(results)
                            .containsExactlyElementsIn(results1);
                    }
                } finally {
                    connection.close();
                }
                return null;
            };
        Future<Void> result = threadPool.submit(showTableTask);

        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);
        shouldStop.set(true);

        result.get();
    }

    @Test
    public void testDescTable() throws Exception {
        String tableName = "modify_sk_desc_table_tbl" + RandomUtils.getStringBetween(1, 5);

        testDescTableInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s modify column b int",
                tableName));
        testDescTableInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s change column b b int",
                tableName));
    }

    public void testDescTableInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`b`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        final ExecutorService threadPool = Executors.newCachedThreadPool();
        String showSql = String.format("desc %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showSql);
        List<List<Object>> results = JdbcUtil.getAllResult(rs);

        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Callable<Void> showTableTask =
            () -> {
                Connection connection = getPolardbxConnection();
                try {
                    while (!shouldStop.get()) {
                        ResultSet rs1 = JdbcUtil.executeQuerySuccess(connection, showSql);
                        List<List<Object>> results1 = JdbcUtil.getAllResult(rs1);
                        assertWithMessage("列信息发生改变")
                            .that(results)
                            .containsExactlyElementsIn(results1);
                    }
                } finally {
                    connection.close();
                }
                return null;
            };
        Future<Void> result = threadPool.submit(showTableTask);

        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);
        shouldStop.set(true);

        result.get();
    }

    @Test
    public void testModifyPartitionKeyNullColumn() {
        String tableName = "modify_sk_null_column";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int ) partition by hash(b) partitions 7", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0,1),(2,null)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint not null,", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
            Assert.assertEquals(rs.getString(2), "1");
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }

        sql = String.format("select * from %s where a=2", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "2");
            Assert.assertNull(rs.getString(2));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testModifyPartitionKeyWithUGSI() {
        String tableName = "modify_sk_ugsi";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b varchar(10)) partition by hash(a) partitions 3",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            String.format("alter table %s add unique global index ugsi1(b) partition by hash(b) partitions 3",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0,'a'),(2,'b')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b varchar(20) default 'abc'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testTableStatistic() {
        String tableName = "omc_table_statistic_modify";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(a) partitions 3", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("analyze table %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "select count(1) from information_schema.virtual_statistic where schema_name='%s' and table_name='%s' and cardinality >= 0",
            tddlDatabase1, tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }

        sql = String.format("alter table %s modify column a varchar(10)", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format(
            "select count(1) from information_schema.virtual_statistic where schema_name='%s' and table_name='%s' and cardinality >= 0",
            tddlDatabase1, tableName);
        ResultSet rs1 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs1.next());
            Assert.assertEquals(2, rs1.getInt(1));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs1);
        }
    }
}

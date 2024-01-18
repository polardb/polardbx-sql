package com.alibaba.polardbx.qatest.ddl.auto.primarykey;

import com.alibaba.polardbx.qatest.ddl.auto.partitionkey.ModifyPartitionKeyTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DropAddPrimaryKeyTest extends ModifyPartitionKeyTest {

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
    public void testModifyPrimaryKeyOnTableWithGsi() {
        String tableName = "modify_pk_gsi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "modify_pk_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql = String.format("create table %s (a int primary key, b int, c int) partition by key(c)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            String.format(" create unique clustered index `%s` on %s (`b`) partition by key(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop primary key, add primary key(a,b)", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testModifyPrimaryKeyOnTableWithGsi2() {
        String tableName = "modify_pk_gsi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "modify_pk_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql = String.format("create table %s (a int primary key, b int, c int) partition by key(c)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            String.format(" create unique clustered index `%s` on %s (`b`) partition by key(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop primary key, add primary key(b)", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testModifyPrimaryKeyOnTableWithGsi3() {
        String tableName = "modify_pk_gsi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "modify_pk_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql = String.format("create table %s (a int, b int, c int) partition by key(c)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            String.format(" create unique clustered index `%s` on %s (`b`) partition by key(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add primary key(a)", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testModifyPrimaryKeyOnTableWithGsi4() {
        String tableName = "modify_pk_gsi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "modify_pk_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql =
            String.format(
                "create table %s (a int auto_increment primary key, b int, c int, local index idx_a(`a`)) partition by key(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            String.format(" create unique clustered index `%s` on %s (`b`) partition by key(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s drop primary key, add primary key(b)", tableName);
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testModifyPrimaryKeyFailed() {
        String tableName = "modify_pk_test_tbl_li_f" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b varchar(255), c int) partition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add primary key(b)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s drop primary key", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s drop primary key, add primary key(d)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testModifyPrimaryKeyFailed2() {
        String tableName = "modify_pk_test_tbl_li_f" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int auto_increment primary key, b varchar(255), c int) partition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add primary key(b)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s drop primary key", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s drop primary key, add primary key(b)", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testModifyPartitionKeySimpleCheckerTest() {
        String tableName = "modify_sk_simple_checker_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(`a`) partitions 3", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(USE_OLD_CHECKER) + String.format("alter table %s drop primary key, add primary key(b)",
            tableName);
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
    public void testModifyPartitionKeySimpleCheckerTest2() {
        String tableName = "modify_sk_simple_checker_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int, b int) partition by hash(`a`) partitions 3", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(USE_OLD_CHECKER) + String.format("alter table %s add primary key(a)",
            tableName);
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
    public void testShowColumns() throws Exception {
        String tableName = "modify_pk_show_columns_tbl" + RandomUtils.getStringBetween(1, 5);

        testShowColumnsInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s drop primary key, add primary key(a)",
                tableName));
    }

    @Test
    public void testDescTable() throws Exception {
        String tableName = "modify_sk_desc_table_tbl" + RandomUtils.getStringBetween(1, 5);

        testDescTableInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s drop primary key, add primary key(a)",
                tableName));
        testDescTableInternal(tableName,
            buildCmdExtra(SLOW_HINT) + String.format("alter table %s drop primary key, add primary key(a)",
                tableName));
    }

}

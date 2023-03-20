/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Before;
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

public class OnlineModifyColumnTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC ";
    private static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION=TRUE";
    private static final String OMC_ALTER_TABLE_WITH_GSI = "OMC_ALTER_TABLE_WITH_GSI=TRUE";
    private static final String OMC_SKIP_BACK_FILL = "OMC_SKIP_BACK_FILL=TRUE";
    private static final String OMC_USE_SIMPLE_CHECKER = "OMC_USE_SIMPLE_CHECKER=TRUE";
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
    public void testOnlineModifyColumnOnTableWithGsi() {
        String tableName = "omc_gsi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql = String.format("create table %s (a int primary key, b int, c int)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(" create unique clustered index `%s` on %s (`b`)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            buildCmdExtra(OMC_ALTER_TABLE_WITH_GSI) + String.format("alter table %s modify column c bigint", tableName)
                + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testOnlineModifyColumnOnTableWithGsiFailed() {
        String tableName = "omc_gsi_test_tbl_f" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_gsi_test_tbl_f_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql =
            String.format("create table %s (a int primary key, b int, c int) partition by hash(`a`) partitions 7",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create global index `%s` on %s (`b`) covering (`c`) partition by hash(`b`) partitions 7",
            indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column c bigint", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testOnlineModifyColumnOnTableWithGsiForce() throws Exception {
        String tableName = "omc_gsi_test_tbl_fa" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_gsi_test_tbl_fa_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(indexName);
        String sql =
            String.format(
                "create table %s (a int primary key, b int, c int) partition by hash(`b`) partitions 7",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create global index `%s` on %s (`b`) covering (`c`) partition by hash(`b`) partitions 7",
            indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            buildCmdExtra(OMC_ALTER_TABLE_WITH_GSI) + String.format("alter table %s modify column c bigint", tableName)
                + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testOnlineModifyColumnLocalIndexFailed() {
        String tableName = "omc_test_tbl_li_f" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_test_tbl_li_f_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b varchar(255), c int) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create local index %s on %s(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // BLOB/TEXT column 'xxx' used in key specification without a key length
        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b text", tableName)
            + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b blob(10)",
            tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("drop index %s on %s", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create local index %s on %s(b(10))", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b text", tableName)
            + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b blob(10)",
            tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
    }

    @Test
    public void testOnlineModifyColumnOnMultipleColumnsFailed() {
        String tableName = "omc_multi_col_f" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int, c int) partition by hash(`b`) partitions 7",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint, modify column c bigint", tableName)
            + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
    }

    @Test
    public void testOnlineModifyColumnRollback() {
        String tableName = "omc_rollback_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b bigint, c bigint, d bigint, e bigint) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 99999, -99999, 127, -128)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b tinyint,",
            tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column c smallint,",
            tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column d mediumint,",
            tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column e int,", tableName)
            + USE_OMC_ALGORITHM;
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
    public void testOnlineModifyColumnLocalIndexRollback() {
        String tableName = "omc_rollback_li_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_rollback_li_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b bigint, c bigint, d bigint, e bigint) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create index %s on %s(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, null, -99999, 127, -128)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b varchar(0),",
            tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column c smallint,",
            tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column d mediumint,",
            tableName) + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column e int,", tableName)
            + USE_OMC_ALGORITHM;
        execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

        sql = String.format("select * from %s where a=0", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "0");
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
    public void testOnlineModifyColumnFulltextIndexRollback() {
        String tableName = "omc_rollback_fi_test_tbl" + RandomUtils.getStringBetween(1, 5);
        String indexName = "omc_rollback_fi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int, c int, d int, e int, f varchar(64)) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create fulltext index %s on %s(f)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 99999, -99999, 127, -128, '123')", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column d bigint,",
            tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        sql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column e bigint,",
            tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

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
    public void testOnlineModifyColumnSimpleCheckerTest() {
        String tableName = "omc_simple_checker_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(`a`) partitions 7", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(OMC_SKIP_BACK_FILL, OMC_USE_SIMPLE_CHECKER) + String.format(
            "alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = buildCmdExtra(OMC_USE_SIMPLE_CHECKER) + String.format("alter table %s modify column b bigint,", tableName)
            + USE_OMC_ALGORITHM;
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
    public void testOnlineModifyColumnCheckerTest() {
        String tableName = "omc_checker_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`a`) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(OMC_SKIP_BACK_FILL) + String.format("alter table %s modify column b bigint,", tableName)
            + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = String.format("alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
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
        String tableName = "omc_show_table_tbl" + RandomUtils.getStringBetween(1, 5);
        testShowTableInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b int,",
                tableName) + USE_OMC_ALGORITHM);
        testShowTableInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s change column b b int,",
                tableName) + USE_OMC_ALGORITHM);
    }

    private void testShowTableInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`a`) partitions 7",
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
        String tableName = "omc_show_columns_tbl" + RandomUtils.getStringBetween(1, 5);

        testShowColumnsInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b int,",
                tableName) + USE_OMC_ALGORITHM);
        testShowColumnsInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s change column b b int,",
                tableName) + USE_OMC_ALGORITHM);
    }

    private void testShowColumnsInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`a`) partitions 7",
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
        String tableName = "omc_desc_table_tbl" + RandomUtils.getStringBetween(1, 5);

        testDescTableInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s modify column b int,",
                tableName) + USE_OMC_ALGORITHM);
        testDescTableInternal(tableName,
            buildCmdExtra(SLOW_HINT, OMC_FORCE_TYPE_CONVERSION) + String.format("alter table %s change column b b int,",
                tableName) + USE_OMC_ALGORITHM);
    }

    private void testDescTableInternal(String tableName, String alterSql) throws Exception {
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(`a`) partitions 7",
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
    public void testOnlineModifyColumnLocalIndexRollbackTest() {
        String tableName = "omc_local_index_rollback_test";
        String indexName = tableName + "_idx";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int) partition by hash(a) partitions 7",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create local index %s on %s(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(OMC_SKIP_BACK_FILL, OMC_USE_SIMPLE_CHECKER) + String.format(
            "alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = buildCmdExtra(OMC_USE_SIMPLE_CHECKER) + String.format("alter table %s modify column b bigint,", tableName)
            + USE_OMC_ALGORITHM;
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
    public void testOnlineModifyColumnLocalIndexRollbackTestAutoPartition() {
        String tableName = "omc_local_index_rollback_test_ap";
        String indexName = tableName + "_idx";
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a int primary key, b int)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("create local index %s on %s(b)", indexName, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0, 1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = buildCmdExtra(OMC_SKIP_BACK_FILL, OMC_USE_SIMPLE_CHECKER) + String.format(
            "alter table %s modify column b bigint,", tableName) + USE_OMC_ALGORITHM;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = buildCmdExtra(OMC_USE_SIMPLE_CHECKER) + String.format("alter table %s modify column b bigint,", tableName)
            + USE_OMC_ALGORITHM;
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
    public void testOnlineModifyColumnNullColumn() {
        String tableName = "omc_null_column";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int ) partition by hash(a) partitions 7", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0,1),(2,null)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint null,", tableName) + USE_OMC_ALGORITHM;
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
    public void testOnlineModifyColumnColumnName() {
        String tableName = "```omc_column_name```";
        dropTableIfExists(tableName);
        String sql =
            String.format("create table %s (a int primary key, b int) partition by hash(a) partitions 7", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table %s values (0,1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String sqlMode = JdbcUtil.getSqlMode(tddlConnection);
        setSqlMode("STRICT_TRANS_TABLES", tddlConnection);
        try {
            sql = String.format("alter table %s change column b ```c``` bigint not null unique,", tableName)
                + USE_OMC_ALGORITHM;
            execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);

            sql = String.format("select * from %s", tableName);
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
        } finally {
            setSqlMode(sqlMode, tddlConnection);
        }
    }
}
